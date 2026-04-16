#!/usr/bin/env python3
"""
Slurm Billing System - 完善的 Slurm 作业计费系统
功能：
  - 自动收集作业资源使用数据
  - 多维度计费（CPU/内存/GPU/存储）
  - 灵活的费率配置
  - 支持账户层级计费
  - 账单查询和报表导出
  - 与 Slurm-Web 集成

作者: Assistant
版本: 1.0.0
"""

import os
import sys
import sqlite3
import yaml
import json
import argparse
import subprocess
import logging
import datetime
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from collections import defaultdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/var/log/slurm-bill/billing.log', mode='a')
    ]
)
logger = logging.getLogger('slurm-bill')


@dataclass
class JobRecord:
    """作业记录数据结构"""
    job_id: str
    job_name: str
    user: str
    account: str
    partition: str
    state: str
    submit_time: str
    start_time: str
    end_time: str
    elapsed: str
    elapsed_seconds: int
    ncpus: int
    nnodes: int
    req_mem: str
    max_rss_mb: float
    alloc_gpus: int
    billing_units: Decimal
    cost: Decimal
    created_at: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.datetime.now().isoformat()


@dataclass
class BillingRate:
    """计费费率配置"""
    cpu_per_hour: Decimal      # CPU 每小时费用
    memory_gb_per_hour: Decimal  # 内存每GB每小时费用
    gpu_per_hour: Decimal      # GPU 每小时费用
    node_per_hour: Decimal     # 整节点每小时费用（可选）
    
    # 折扣配置
    account_discounts: Dict[str, Decimal] = None  # 账户折扣
    user_discounts: Dict[str, Decimal] = None     # 用户折扣
    
    def __post_init__(self):
        if self.account_discounts is None:
            self.account_discounts = {}
        if self.user_discounts is None:
            self.user_discounts = {}


class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self, db_path: str = '/var/lib/slurm-bill/billing.db'):
        self.db_path = db_path
        self._ensure_dir()
        self._init_tables()
    
    def _ensure_dir(self):
        """确保数据库目录存在"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_tables(self):
        """初始化数据库表"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 作业记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    job_name TEXT,
                    user TEXT NOT NULL,
                    account TEXT,
                    partition TEXT,
                    state TEXT,
                    submit_time TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    elapsed TEXT,
                    elapsed_seconds INTEGER,
                    ncpus INTEGER,
                    nnodes INTEGER,
                    req_mem TEXT,
                    max_rss_mb REAL,
                    alloc_gpus INTEGER DEFAULT 0,
                    billing_units TEXT,
                    cost TEXT,
                    created_at TEXT,
                    UNIQUE(job_id, end_time)
                )
            ''')
            
            # 创建索引
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_job_user ON job_records(user)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_job_account ON job_records(account)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_job_time ON job_records(end_time)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_job_state ON job_records(state)
            ''')
            
            # 计费周期表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS billing_cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_name TEXT UNIQUE NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    status TEXT DEFAULT 'open',
                    created_at TEXT
                )
            ''')
            
            # 账户余额表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS account_balance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT UNIQUE NOT NULL,
                    balance TEXT DEFAULT '0.00',
                    credit_limit TEXT DEFAULT '0.00',
                    last_updated TEXT
                )
            ''')
            
            # 同步状态表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_sync_time TEXT NOT NULL,
                    last_job_id TEXT,
                    sync_type TEXT DEFAULT 'incremental',
                    created_at TEXT,
                    updated_at TEXT
                )
            ''')
            
            conn.commit()
            logger.info("数据库表初始化完成")
    
    def insert_job(self, job: JobRecord) -> bool:
        """插入或更新作业记录"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # 先检查是否存在相同 job_id 的记录
                cursor.execute('SELECT id FROM job_records WHERE job_id = ?', (job.job_id,))
                existing = cursor.fetchone()
                
                if existing:
                    # 更新已有记录
                    cursor.execute('''
                        UPDATE job_records SET
                            job_name = ?,
                            user = ?,
                            account = ?,
                            partition = ?,
                            state = ?,
                            submit_time = ?,
                            start_time = ?,
                            end_time = ?,
                            elapsed = ?,
                            elapsed_seconds = ?,
                            ncpus = ?,
                            nnodes = ?,
                            req_mem = ?,
                            max_rss_mb = ?,
                            alloc_gpus = ?,
                            billing_units = ?,
                            cost = ?,
                            created_at = ?
                        WHERE job_id = ?
                    ''', (
                        job.job_name, job.user, job.account, job.partition, job.state,
                        job.submit_time, job.start_time, job.end_time, job.elapsed, job.elapsed_seconds,
                        job.ncpus, job.nnodes, job.req_mem, job.max_rss_mb, job.alloc_gpus,
                        str(job.billing_units), str(job.cost), job.created_at,
                        job.job_id
                    ))
                else:
                    # 插入新记录
                    cursor.execute('''
                        INSERT INTO job_records 
                        (job_id, job_name, user, account, partition, state,
                         submit_time, start_time, end_time, elapsed, elapsed_seconds,
                         ncpus, nnodes, req_mem, max_rss_mb, alloc_gpus,
                         billing_units, cost, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        job.job_id, job.job_name, job.user, job.account, job.partition, job.state,
                        job.submit_time, job.start_time, job.end_time, job.elapsed, job.elapsed_seconds,
                        job.ncpus, job.nnodes, job.req_mem, job.max_rss_mb, job.alloc_gpus,
                        str(job.billing_units), str(job.cost), job.created_at
                    ))
                
                conn.commit()
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"插入作业记录失败: {e}")
            return False
    
    def get_jobs(self, 
                 user: str = None,
                 account: str = None,
                 start_date: str = None,
                 end_date: str = None,
                 state: str = None) -> List[Dict]:
        """查询作业记录"""
        query = "SELECT * FROM job_records WHERE 1=1"
        params = []
        
        if user:
            query += " AND user = ?"
            params.append(user)
        if account:
            query += " AND account = ?"
            params.append(account)
        if start_date:
            query += " AND end_time >= ?"
            params.append(start_date)
        if end_date:
            query += " AND end_time <= ?"
            params.append(end_date)
        if state:
            query += " AND state = ?"
            params.append(state)
        
        query += " ORDER BY end_time DESC"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_billing_summary(self,
                           group_by: str = 'user',
                           start_date: str = None,
                           end_date: str = None,
                           account: str = None) -> List[Dict]:
        """获取计费汇总统计"""
        query = f'''
            SELECT 
                {group_by} as group_key,
                COUNT(*) as job_count,
                SUM(elapsed_seconds) as total_cpu_seconds,
                SUM(ncpus * elapsed_seconds) as total_cpu_core_seconds,
                SUM(alloc_gpus * elapsed_seconds) as total_gpu_seconds,
                SUM(CAST(cost AS DECIMAL)) as total_cost,
                SUM(CAST(billing_units AS DECIMAL)) as total_billing_units,
                AVG(CAST(cost AS DECIMAL)) as avg_cost_per_job
            FROM job_records
            WHERE state IN ('COMPLETED', 'CD', 'FAILED', 'F', 'TIMEOUT', 'TO', 'CANCELLED', 'CA')
               OR state LIKE 'CANCELLED%'
        '''
        params = []
        
        if start_date:
            query += " AND end_time >= ?"
            params.append(start_date)
        if end_date:
            query += " AND end_time <= ?"
            params.append(end_date)
        if account:
            query += " AND account = ?"
            params.append(account)
        
        query += f" GROUP BY {group_by} ORDER BY total_cost DESC"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_sync_status(self) -> Optional[Dict]:
        """获取同步状态"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM sync_status ORDER BY id DESC LIMIT 1
            ''')
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def update_sync_status(self, last_sync_time: str, last_job_id: str = None, sync_type: str = 'incremental') -> bool:
        """更新同步状态"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                now = datetime.datetime.now().isoformat()
                
                # 检查是否存在记录
                cursor.execute('SELECT id FROM sync_status LIMIT 1')
                existing = cursor.fetchone()
                
                if existing:
                    cursor.execute('''
                        UPDATE sync_status SET
                            last_sync_time = ?,
                            last_job_id = ?,
                            sync_type = ?,
                            updated_at = ?
                        WHERE id = ?
                    ''', (last_sync_time, last_job_id, sync_type, now, existing[0]))
                else:
                    cursor.execute('''
                        INSERT INTO sync_status (last_sync_time, last_job_id, sync_type, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (last_sync_time, last_job_id, sync_type, now, now))
                
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"更新同步状态失败: {e}")
            return False


class SlurmCollector:
    """Slurm 数据收集器"""
    
    @staticmethod
    def run_sacct(start_time: datetime.datetime = None,
                  end_time: datetime.datetime = None) -> List[Dict]:
        """运行 sacct 命令获取作业数据（包括数组作业和作业步）"""
        
        # 构建时间参数
        # 注意：sacct 的 --starttime 基于 End 时间过滤，行为有些特殊
        # 为了获取所有相关作业，我们需要：
        # 1. 扩大开始时间范围（提前15天），确保能获取到结束时间较早的作业
        # 2. 不使用 --endtime 参数，避免过滤掉结束时间未知的作业（如 PENDING）
        if start_time:
            # 向后扩展15天，确保覆盖到结束时间较早的作业
            extended_start = start_time - datetime.timedelta(days=15)
            start_str = extended_start.strftime('%Y-%m-%dT%H:%M:%S')
            time_arg = f"--starttime={start_str}"
        else:
            # 默认获取最近24小时的作业
            start_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
            time_arg = f"--starttime={start_str}"
        
        return SlurmCollector._fetch_jobs(time_arg)
    
    @staticmethod
    def run_sacct_incremental(last_sync_time: datetime.datetime = None) -> List[Dict]:
        """增量获取作业数据 - 只获取自上次同步以来的新作业"""
        if last_sync_time:
            start_str = last_sync_time.strftime('%Y-%m-%dT%H:%M:%S')
            time_arg = f"--starttime={start_str}"
        else:
            start_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
            time_arg = f"--starttime={start_str}"
        
        return SlurmCollector._fetch_jobs(time_arg)
    
    @staticmethod
    def _fetch_jobs(time_arg: str) -> List[Dict]:
        """核心方法：执行 sacct 命令并解析作业数据"""
        
        # 第一步：获取所有主作业（使用 --duplicates 获取所有记录，包括 REQUEUED/PENDING/CANCELLED 等）
        cmd_main = (
            f"sacct -a -P --duplicates --format="
            f"JobID,JobName,User,Account,Partition,State,Submit,Start,End,Elapsed,"
            f"NCPUS,NNodes,ReqMem,MaxRSS,AllocTRES "
            f"{time_arg} --noheader"
        )
        
        # 第二步：获取所有作业步（包括数组作业步）
        cmd_steps = (
            f"sacct -a -P --duplicates --format="
            f"JobID,JobName,User,Account,Partition,State,Submit,Start,End,Elapsed,"
            f"NCPUS,NNodes,ReqMem,MaxRSS,AllocTRES "
            f"{time_arg} --noheader"
        )
        
        try:
            # 获取主作业
            result_main = subprocess.run(
                cmd_main, shell=True, capture_output=True, text=True, timeout=300
            )
            if result_main.returncode != 0:
                logger.error(f"sacct 主作业查询失败: {result_main.stderr}")
                return []
            
            # 获取所有作业步
            result_steps = subprocess.run(
                cmd_steps, shell=True, capture_output=True, text=True, timeout=300
            )
            
            # 解析主作业，建立基础信息映射
            # 注意：同一个 job_id 可能有多个记录（REQUEUED、PENDING、CANCELLED等）
            # 我们需要选择最准确的记录（优先有实际结束时间的）
            main_jobs = {}
            for line in result_main.stdout.strip().split('\n'):
                if not line:
                    continue
                
                parts = line.split('|')
                if len(parts) < 15:
                    continue
                
                job_id = parts[0].strip()
                user = parts[2].strip()
                end_time = parts[8].strip()
                state = parts[5].strip()
                
                # 跳过 user 为空的记录
                if not user:
                    continue
                
                # 解析 AllocTRES 获取 GPU 信息
                alloc_gpus = SlurmCollector._parse_gpu_count(parts[14])
                
                # 解析 MaxRSS
                max_rss_mb = SlurmCollector._parse_memory(parts[13])
                
                job_data = {
                    'job_id': job_id,
                    'job_name': parts[1],
                    'user': user,
                    'account': parts[3] or 'default',
                    'partition': parts[4],
                    'state': state,
                    'submit_time': parts[6],
                    'start_time': parts[7],
                    'end_time': end_time,
                    'elapsed': parts[9],
                    'elapsed_seconds': SlurmCollector._parse_elapsed(parts[9]),
                    'ncpus': int(parts[10]) if parts[10].isdigit() else 1,
                    'nnodes': int(parts[11]) if parts[11].isdigit() else 1,
                    'req_mem': parts[12],
                    'max_rss_mb': max_rss_mb,
                    'alloc_gpus': alloc_gpus,
                    'is_array': False,
                }
                
                # 如果已存在相同 job_id 的记录，选择更准确的
                if job_id in main_jobs:
                    existing = main_jobs[job_id]
                    existing_end = existing.get('end_time', '')
                    
                    # 优先选择有实际结束时间的（非 Unknown/None）
                    has_end_time = end_time and end_time not in ('Unknown', 'None', '')
                    existing_has_end = existing_end and existing_end not in ('Unknown', 'None', '')
                    
                    if has_end_time and not existing_has_end:
                        # 新记录有结束时间，旧记录没有，使用新记录
                        main_jobs[job_id] = job_data
                    elif has_end_time and existing_has_end:
                        # 两者都有结束时间，选择结束时间更晚的（最新的）
                        if end_time > existing_end:
                            main_jobs[job_id] = job_data
                    # 如果新记录没有结束时间但旧记录有，保留旧记录
                else:
                    main_jobs[job_id] = job_data
            
            # 解析作业步，合并到主作业或作为独立记录
            jobs = list(main_jobs.values())
            processed_steps = set()
            
            # 用于收集每个主作业的所有作业步状态
            job_step_states = {}
            
            if result_steps.returncode == 0:
                for line in result_steps.stdout.strip().split('\n'):
                    if not line:
                        continue
                    
                    parts = line.split('|')
                    if len(parts) < 15:
                        continue
                    
                    job_id = parts[0].strip()
                    user = parts[2].strip()
                    step_state = parts[5].strip()
                    
                    # 跳过主作业（已处理）
                    if job_id in main_jobs:
                        continue
                    
                    # 解析数组作业ID (如 203.0, 203_0, 203_[0])
                    base_job_id = SlurmCollector._get_base_job_id(job_id)
                    
                    # 如果这是数组作业步且主作业存在，合并资源使用和状态
                    if base_job_id and base_job_id in main_jobs:
                        main_job = main_jobs[base_job_id]
                        
                        # 收集作业步状态
                        if base_job_id not in job_step_states:
                            job_step_states[base_job_id] = []
                        job_step_states[base_job_id].append(step_state)
                        
                        # 累加资源使用（只处理一次每个作业步）
                        step_key = f"{base_job_id}:{job_id}"
                        if step_key not in processed_steps:
                            processed_steps.add(step_key)
                            
                            # 解析资源
                            step_gpus = SlurmCollector._parse_gpu_count(parts[14])
                            step_rss = SlurmCollector._parse_memory(parts[13])
                            step_ncpus = int(parts[10]) if parts[10].isdigit() else 0
                            step_elapsed = SlurmCollector._parse_elapsed(parts[9])
                            
                            # 累加 GPU 和内存使用
                            main_job['alloc_gpus'] = max(main_job['alloc_gpus'], step_gpus)
                            main_job['max_rss_mb'] = max(main_job['max_rss_mb'], step_rss)
                            
                            # 标记为数组作业
                            main_job['is_array'] = True
                
                # 根据所有作业步的状态确定主作业的最终状态
                for base_job_id, states in job_step_states.items():
                    if base_job_id in main_jobs:
                        main_job = main_jobs[base_job_id]
                        main_job['state'] = SlurmCollector._aggregate_job_states(states)
                    
                    # 对于独立作业步（如 .batch），如果 user 为空，尝试从主作业继承
                    elif not user and base_job_id and base_job_id in main_jobs:
                        main_job = main_jobs[base_job_id]
                        
                        # 创建作业步记录（继承主作业信息）
                        step_data = {
                            'job_id': job_id,
                            'job_name': parts[1] or main_job['job_name'],
                            'user': main_job['user'],
                            'account': main_job['account'],
                            'partition': main_job['partition'],
                            'state': parts[5],
                            'submit_time': parts[6],
                            'start_time': parts[7],
                            'end_time': parts[8],
                            'elapsed': parts[9],
                            'elapsed_seconds': SlurmCollector._parse_elapsed(parts[9]),
                            'ncpus': int(parts[10]) if parts[10].isdigit() else 1,
                            'nnodes': int(parts[11]) if parts[11].isdigit() else 1,
                            'req_mem': parts[12],
                            'max_rss_mb': SlurmCollector._parse_memory(parts[13]),
                            'alloc_gpus': SlurmCollector._parse_gpu_count(parts[14]),
                            'is_step': True,
                            'parent_job_id': base_job_id,
                        }
                        jobs.append(step_data)
                    
                    # 对于独立的数组作业步（没有主作业），创建新记录
                    elif user:
                        step_data = {
                            'job_id': job_id,
                            'job_name': parts[1],
                            'user': user,
                            'account': parts[3] or 'default',
                            'partition': parts[4],
                            'state': parts[5],
                            'submit_time': parts[6],
                            'start_time': parts[7],
                            'end_time': parts[8],
                            'elapsed': parts[9],
                            'elapsed_seconds': SlurmCollector._parse_elapsed(parts[9]),
                            'ncpus': int(parts[10]) if parts[10].isdigit() else 1,
                            'nnodes': int(parts[11]) if parts[11].isdigit() else 1,
                            'req_mem': parts[12],
                            'max_rss_mb': SlurmCollector._parse_memory(parts[13]),
                            'alloc_gpus': SlurmCollector._parse_gpu_count(parts[14]),
                            'is_array_step': True,
                        }
                        jobs.append(step_data)
            
            # 注意：不过滤作业，让调用者决定如何处理
            # 如果需要按时间过滤，应在调用后处理
            
            logger.info(f"成功收集 {len(jobs)} 条作业记录（包含 {len(main_jobs)} 个主作业）")
            return jobs
            
        except subprocess.TimeoutExpired:
            logger.error("sacct 命令超时")
            return []
        except Exception as e:
            logger.error(f"收集作业数据失败: {e}")
            return []
    
    @staticmethod
    def _get_base_job_id(job_id: str) -> Optional[str]:
        """
        从作业步ID获取主作业ID
        例如: '123.0' -> '123', '123.batch' -> '123', '123_0' -> '123'
        """
        if not job_id:
            return None
        
        # 处理各种作业步格式
        # 123.0, 123.batch, 123.extern
        if '.' in job_id:
            return job_id.split('.')[0]
        
        # 123_0 (某些 Slurm 配置)
        if '_' in job_id:
            return job_id.split('_')[0]
        
        # 123[0] 或 123_[0] (数组作业表示)
        if '[' in job_id:
            return job_id.split('[')[0]
        
        return None
    
    @staticmethod
    def _aggregate_job_states(states: List[str]) -> str:
        """
        根据所有作业步的状态确定主作业的最终状态
        
        状态优先级（从高到低）：
        RUNNING > PENDING > SUSPENDED > FAILED > TIMEOUT > CANCELLED > COMPLETED
        
        特殊处理：
        - 如果有任意作业步在运行，主作业为 RUNNING
        - 如果有任意作业步失败，主作业为 FAILED
        - 如果所有作业步都完成，主作业为 COMPLETED
        - 如果部分完成部分取消，标记为 PARTIAL
        """
        if not states:
            return 'UNKNOWN'
        
        # 状态优先级（数字越大优先级越高）
        priority = {
            'RUNNING': 100,
            'R': 100,
            'PENDING': 90,
            'PD': 90,
            'SUSPENDED': 80,
            'S': 80,
            'FAILED': 70,
            'F': 70,
            'TIMEOUT': 60,
            'TO': 60,
            'NODE_FAIL': 55,
            'NF': 55,
            'CANCELLED': 50,
            'CA': 50,
            'COMPLETED': 40,
            'CD': 40,
            'COMPLETING': 35,
            'CG': 35,
        }
        
        # 检查是否有特殊状态（如 CANCELLED by xxx）
        has_cancelled = any('CANCELLED' in s for s in states)
        cancelled_by = [s for s in states if 'CANCELLED by' in s]
        
        # 如果有任意作业在运行，返回 RUNNING
        if any(s in ['RUNNING', 'R'] for s in states):
            return 'RUNNING'
        
        # 如果有任意作业在等待，返回 PENDING
        if any(s in ['PENDING', 'PD'] for s in states):
            return 'PENDING'
        
        # 如果有任意作业被暂停，返回 SUSPENDED
        if any(s in ['SUSPENDED', 'S'] for s in states):
            return 'SUSPENDED'
        
        # 如果有任意作业失败，返回 FAILED
        if any(s in ['FAILED', 'F'] for s in states):
            return 'FAILED'
        
        # 如果有任意作业超时，返回 TIMEOUT
        if any(s in ['TIMEOUT', 'TO'] for s in states):
            return 'TIMEOUT'
        
        # 如果有任意作业节点失败，返回 NODE_FAIL
        if any(s in ['NODE_FAIL', 'NF'] for s in states):
            return 'NODE_FAIL'
        
        # 如果所有作业都完成了，返回 COMPLETED
        if all(s in ['COMPLETED', 'CD', 'COMPLETING', 'CG'] for s in states):
            return 'COMPLETED'
        
        # 如果有取消的作业
        if has_cancelled:
            # 如果有部分完成部分取消，返回 PARTIAL
            if any(s in ['COMPLETED', 'CD'] for s in states):
                return 'PARTIAL'
            # 如果全部被取消，返回 CANCELLED（保留取消者信息）
            if cancelled_by:
                # 返回最常见的取消原因
                from collections import Counter
                most_common = Counter(cancelled_by).most_common(1)[0][0]
                return most_common
            return 'CANCELLED'
        
        # 其他情况，返回优先级最高的状态
        max_priority = -1
        result_state = 'UNKNOWN'
        for state in states:
            base_state = state.split()[0]  # 处理 "CANCELLED by xxx" 这样的情况
            p = priority.get(base_state, 0)
            if p > max_priority:
                max_priority = p
                result_state = state
        
        return result_state
    
    @staticmethod
    def _parse_elapsed(elapsed_str: str) -> int:
        """解析 Elapsed 时间为秒数"""
        if not elapsed_str or elapsed_str == 'None':
            return 0
        
        try:
            days = 0
            if '-' in elapsed_str:
                day_part, time_part = elapsed_str.split('-')
                days = int(day_part)
                elapsed_str = time_part
            
            parts = elapsed_str.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = map(int, parts)
            elif len(parts) == 2:
                hours, minutes = map(int, parts)
                seconds = 0
            else:
                return 0
            
            return days * 86400 + hours * 3600 + minutes * 60 + seconds
        except:
            return 0
    
    @staticmethod
    def _parse_memory(mem_str: str) -> float:
        """解析内存字符串为 MB"""
        if not mem_str:
            return 0.0
        
        try:
            mem_str = mem_str.strip()
            if mem_str.endswith('M'):
                return float(mem_str[:-1])
            elif mem_str.endswith('G'):
                return float(mem_str[:-1]) * 1024
            elif mem_str.endswith('T'):
                return float(mem_str[:-1]) * 1024 * 1024
            elif mem_str.endswith('K'):
                return float(mem_str[:-1]) / 1024
            else:
                return float(mem_str) / (1024 * 1024)  # 假设是字节
        except:
            return 0.0
    
    @staticmethod
    def _parse_gpu_count(alloc_tres: str) -> int:
        """从 AllocTRES 解析 GPU 数量"""
        if not alloc_tres:
            return 0
        
        import re
        # 匹配 gres/gpu=数字 (如: gres/gpu=2)
        match = re.search(r'gres/gpu[=:](\d+)', alloc_tres, re.IGNORECASE)
        if match:
            return int(match.group(1))
        
        # 注意: billing=N 是 Slurm 的计费权重单位，不是 GPU 数量
        # 不要在这里解析 billing
        
        return 0


class BillingCalculator:
    """计费计算器"""
    
    def __init__(self, config_path: str = '/etc/slurm-bill/config.yaml'):
        self.config = self._load_config(config_path)
        self.rate = self._parse_rate()
    
    def _load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        default_config = {
            'billing': {
                'cpu_per_hour': 0.10,
                'memory_gb_per_hour': 0.02,
                'gpu_per_hour': 2.00,
                'node_per_hour': 0.0,
                'currency': 'CNY',
                'min_charge': 0.01,
                'rounding': '0.01'
            },
            'discounts': {
                'accounts': {},
                'users': {}
            },
            'partitions': {
                'default_multiplier': 1.0
            }
        }
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                    if config:
                        default_config.update(config)
            else:
                logger.warning(f"配置文件不存在: {config_path}，使用默认配置")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
        
        return default_config
    
    def _parse_rate(self) -> BillingRate:
        """解析费率配置"""
        billing = self.config.get('billing', {}) or {}
        discounts = self.config.get('discounts', {}) or {}
        
        # 安全获取折扣配置，处理 None 情况
        account_discounts_raw = discounts.get('accounts') or {}
        user_discounts_raw = discounts.get('users') or {}
        
        return BillingRate(
            cpu_per_hour=Decimal(str(billing.get('cpu_per_hour', 0.10))),
            memory_gb_per_hour=Decimal(str(billing.get('memory_gb_per_hour', 0.02))),
            gpu_per_hour=Decimal(str(billing.get('gpu_per_hour', 2.00))),
            node_per_hour=Decimal(str(billing.get('node_per_hour', 0.0))),
            account_discounts={
                k: Decimal(str(v)) 
                for k, v in account_discounts_raw.items()
            },
            user_discounts={
                k: Decimal(str(v)) 
                for k, v in user_discounts_raw.items()
            }
        )
    
    def calculate_job_cost(self, job: Dict) -> Tuple[Decimal, Decimal]:
        """
        计算单个作业的费用
        返回: (billing_units, cost)
        """
        elapsed_hours = Decimal(job['elapsed_seconds']) / Decimal(3600)
        
        # 计算各资源费用
        cpu_cost = elapsed_hours * self.rate.cpu_per_hour * job['ncpus']
        
        # 内存费用（转换为 GB）
        mem_gb = Decimal(job['max_rss_mb']) / Decimal(1024)
        mem_cost = elapsed_hours * self.rate.memory_gb_per_hour * mem_gb
        
        # GPU 费用
        gpu_cost = elapsed_hours * self.rate.gpu_per_hour * job['alloc_gpus']
        
        # 节点费用
        node_cost = elapsed_hours * self.rate.node_per_hour * job['nnodes']
        
        # 总计费单位（原始资源消耗）
        billing_units = cpu_cost + mem_cost + gpu_cost + node_cost
        
        # 应用分区倍率
        partition = job.get('partition', 'default')
        partition_multiplier = Decimal(str(
            self.config.get('partitions', {}).get(partition, 1.0)
        ))
        cost = billing_units * partition_multiplier
        
        # 应用账户折扣
        account = job.get('account', '')
        if account in self.rate.account_discounts:
            discount = self.rate.account_discounts[account]
            cost = cost * (Decimal(1) - discount)
        
        # 应用用户折扣
        user = job.get('user', '')
        if user in self.rate.user_discounts:
            discount = self.rate.user_discounts[user]
            cost = cost * (Decimal(1) - discount)
        
        # 四舍五入
        rounding = self.config.get('billing', {}).get('rounding', '0.01')
        cost = cost.quantize(Decimal(rounding), rounding=ROUND_HALF_UP)
        billing_units = billing_units.quantize(Decimal(rounding), rounding=ROUND_HALF_UP)
        
        # 最低消费
        min_charge = Decimal(str(self.config.get('billing', {}).get('min_charge', 0.01)))
        if cost > 0 and cost < min_charge:
            cost = min_charge
        
        return billing_units, cost


class BillingEngine:
    """计费引擎主类"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.calculator = BillingCalculator()
        self.collector = SlurmCollector()
    
    def collect_and_bill(self, 
                         start_time: datetime.datetime = None,
                         end_time: datetime.datetime = None) -> Dict:
        """收集数据并计费"""
        
        # 1. 收集作业数据
        jobs = self.collector.run_sacct(start_time, end_time)
        
        if not jobs:
            logger.info("没有新的作业数据需要处理")
            return {'processed': 0, 'new_records': 0, 'total_cost': Decimal(0)}
        
        # 2. 计算费用并存储
        new_count = 0
        total_cost = Decimal(0)
        
        for job_data in jobs:
            # 计算费用
            billing_units, cost = self.calculator.calculate_job_cost(job_data)
            
            # 创建作业记录
            job_record = JobRecord(
                job_id=job_data['job_id'],
                job_name=job_data['job_name'],
                user=job_data['user'],
                account=job_data['account'],
                partition=job_data['partition'],
                state=job_data['state'],
                submit_time=job_data['submit_time'],
                start_time=job_data['start_time'],
                end_time=job_data['end_time'],
                elapsed=job_data['elapsed'],
                elapsed_seconds=job_data['elapsed_seconds'],
                ncpus=job_data['ncpus'],
                nnodes=job_data['nnodes'],
                req_mem=job_data['req_mem'],
                max_rss_mb=job_data['max_rss_mb'],
                alloc_gpus=job_data['alloc_gpus'],
                billing_units=billing_units,
                cost=cost
            )
            
            # 插入数据库
            if self.db.insert_job(job_record):
                new_count += 1
                total_cost += cost
        
        result = {
            'processed': len(jobs),
            'new_records': new_count,
            'total_cost': total_cost
        }
        
        logger.info(f"计费完成: 处理 {result['processed']} 条记录, "
                   f"新增 {result['new_records']} 条, 总费用 {result['total_cost']}")
        
        return result
    
    def generate_report(self,
                       start_date: str = None,
                       end_date: str = None,
                       group_by: str = 'user',
                       format: str = 'table') -> str:
        """生成账单报表"""
        
        # 设置默认时间范围（上月）
        if not start_date:
            today = datetime.date.today()
            first_day = today.replace(day=1)
            last_month = first_day - datetime.timedelta(days=1)
            start_date = last_month.replace(day=1).isoformat()
            end_date = first_day.isoformat()
        
        # 获取汇总数据
        summary = self.db.get_billing_summary(
            group_by=group_by,
            start_date=start_date,
            end_date=end_date
        )
        
        if format == 'json':
            return json.dumps(summary, indent=2, default=str)
        
        elif format == 'csv':
            import csv
            import io
            output = io.StringIO()
            if summary:
                writer = csv.DictWriter(output, fieldnames=summary[0].keys())
                writer.writeheader()
                writer.writerows(summary)
            return output.getvalue()
        
        else:  # table format
            if not summary:
                return "没有计费数据"
            
            lines = [
                f"\n{'='*100}",
                f"计费报表 ({start_date} 至 {end_date})",
                f"{'='*100}",
                f"{'分组':<20} {'作业数':>10} {'CPU核时':>15} {'GPU卡时':>15} {'计费单位':>15} {'费用':>15}",
                f"{'-'*100}"
            ]
            
            total_cost = Decimal(0)
            for item in summary:
                cpu_hours = Decimal(item['total_cpu_core_seconds']) / 3600
                gpu_hours = Decimal(item['total_gpu_seconds']) / 3600
                
                lines.append(
                    f"{item['group_key']:<20} "
                    f"{item['job_count']:>10} "
                    f"{cpu_hours:>15.2f} "
                    f"{gpu_hours:>15.2f} "
                    f"{Decimal(item['total_billing_units']):>15.2f} "
                    f"{Decimal(item['total_cost']):>15.2f}"
                )
                total_cost += Decimal(item['total_cost'])
            
            lines.extend([
                f"{'-'*100}",
                f"{'总计':<20} {'':>10} {'':>15} {'':>15} {'':>15} {total_cost:>15.2f}",
                f"{'='*100}"
            ])
            
            return '\n'.join(lines)


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        description='Slurm Billing System - Slurm 计费系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  # 运行计费收集（通常由 cron 调用）
  python3 slurm_bill.py collect

  # 生成上月报表
  python3 slurm_bill.py report --group-by account

  # 导出 CSV 报表
  python3 slurm_bill.py report --format csv --output bill.csv

  # 查询用户账单
  python3 slurm_bill.py query --user username --start 2024-01-01
        '''
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # collect 命令
    collect_parser = subparsers.add_parser('collect', help='收集作业数据并计费')
    collect_parser.add_argument('--start', help='开始时间 (YYYY-MM-DD)')
    collect_parser.add_argument('--end', help='结束时间 (YYYY-MM-DD)')
    
    # report 命令
    report_parser = subparsers.add_parser('report', help='生成账单报表')
    report_parser.add_argument('--start', help='开始日期 (YYYY-MM-DD)')
    report_parser.add_argument('--end', help='结束日期 (YYYY-MM-DD)')
    report_parser.add_argument('--group-by', choices=['user', 'account', 'partition'],
                              default='user', help='分组方式')
    report_parser.add_argument('--format', choices=['table', 'json', 'csv'],
                              default='table', help='输出格式')
    report_parser.add_argument('--output', '-o', help='输出文件')
    
    # query 命令
    query_parser = subparsers.add_parser('query', help='查询详细记录')
    query_parser.add_argument('--user', help='查询指定用户')
    query_parser.add_argument('--account', help='查询指定账户')
    query_parser.add_argument('--start', help='开始日期')
    query_parser.add_argument('--end', help='结束日期')
    query_parser.add_argument('--limit', type=int, default=100, help='限制条数')
    
    # init 命令
    init_parser = subparsers.add_parser('init', help='初始化数据库')
    
    # sync 命令 - 同步所有作业（包括数组作业和作业步）
    sync_parser = subparsers.add_parser('sync', help='同步作业数据（增量或全量）')
    sync_parser.add_argument('--days', '-d', type=int, default=2,
                            help='增量同步最近N天的作业（默认：2）')
    sync_parser.add_argument('--all', '-a', action='store_true',
                            help='同步所有历史作业')
    sync_parser.add_argument('--full', '-f', action='store_true',
                            help='强制全量同步（忽略增量状态）')
    sync_parser.add_argument('--dry-run', '-n', action='store_true',
                            help='试运行，不实际写入数据库')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    engine = BillingEngine()
    
    if args.command == 'collect':
        start = datetime.datetime.fromisoformat(args.start) if args.start else None
        end = datetime.datetime.fromisoformat(args.end) if args.end else None
        result = engine.collect_and_bill(start, end)
        print(f"计费收集完成: {result}")
    
    elif args.command == 'report':
        report = engine.generate_report(
            start_date=args.start,
            end_date=args.end,
            group_by=args.group_by,
            format=args.format
        )
        if args.output:
            with open(args.output, 'w') as f:
                f.write(report)
            print(f"报表已保存到: {args.output}")
        else:
            print(report)
    
    elif args.command == 'query':
        db = DatabaseManager()
        jobs = db.get_jobs(
            user=args.user,
            account=args.account,
            start_date=args.start,
            end_date=args.end
        )
        
        print(f"\n{'='*120}")
        print(f"查询结果 ({len(jobs)} 条记录)")
        print(f"{'='*120}")
        print(f"{'作业ID':<12} {'用户':<12} {'账户':<12} {'状态':<10} {'核数':>6} {'GPU':>4} {'时长':>10} {'费用':>10}")
        print(f"{'-'*120}")
        
        for job in jobs[:args.limit]:
            print(f"{job['job_id']:<12} {job['user']:<12} {job['account']:<12} "
                  f"{job['state']:<10} {job['ncpus']:>6} {job['alloc_gpus']:>4} "
                  f"{job['elapsed']:>10} {Decimal(job['cost']):>10.2f}")
        
        if len(jobs) > args.limit:
            print(f"... 还有 {len(jobs) - args.limit} 条记录")
        print(f"{'='*120}")
    
    elif args.command == 'init':
        # 数据库已在初始化时创建
        print("数据库初始化完成")
        print(f"数据库路径: /var/lib/slurm-bill/billing.db")
    
    elif args.command == 'sync':
        # 同步作业数据 - 支持增量同步和全量同步
        from datetime import datetime, timedelta
        
        db = DatabaseManager()
        
        # 确定同步模式
        is_full_sync = args.all or args.full
        
        if is_full_sync:
            if args.all:
                start = datetime.now() - timedelta(days=365*10)
                print("全量同步：同步所有历史作业...")
            else:
                start = datetime.now() - timedelta(days=args.days)
                print(f"全量同步：同步最近 {args.days} 天的作业...")
            sync_type = 'full'
            jobs = engine.collector.run_sacct(start, datetime.now())
        else:
            # 增量同步模式
            sync_status = db.get_sync_status()
            if sync_status and sync_status.get('last_sync_time'):
                last_sync = datetime.fromisoformat(sync_status['last_sync_time'])
                print(f"增量同步：从上次同步时间 {last_sync.strftime('%Y-%m-%d %H:%M:%S')} 开始...")
                jobs = engine.collector.run_sacct_incremental(last_sync)
                sync_type = 'incremental'
            else:
                print("首次同步，使用默认时间范围（最近2天）...")
                start = datetime.now() - timedelta(days=2)
                jobs = engine.collector.run_sacct(start, datetime.now())
                sync_type = 'first_sync'
        
        if not jobs:
            print("没有找到作业数据")
            sys.exit(0)
        
        print(f"\n找到 {len(jobs)} 个作业记录")
        print(f"{'='*60}")
        
        # 统计信息
        inserted = 0
        updated = 0
        skipped = 0
        total = len(jobs)
        
        # 进度显示
        PROGRESS_INTERVAL = 100
        
        for idx, job_data in enumerate(jobs):
            # 计算费用
            billing_units, cost = engine.calculator.calculate_job_cost(job_data)
            
            # 创建作业记录
            job_record = JobRecord(
                job_id=job_data['job_id'],
                job_name=job_data['job_name'],
                user=job_data['user'],
                account=job_data['account'],
                partition=job_data['partition'],
                state=job_data['state'],
                submit_time=job_data['submit_time'],
                start_time=job_data['start_time'],
                end_time=job_data['end_time'],
                elapsed=job_data['elapsed'],
                elapsed_seconds=job_data['elapsed_seconds'],
                ncpus=job_data['ncpus'],
                nnodes=job_data['nnodes'],
                req_mem=job_data['req_mem'],
                max_rss_mb=job_data['max_rss_mb'],
                alloc_gpus=job_data['alloc_gpus'],
                billing_units=billing_units,
                cost=cost
            )
            
            # 进度显示
            if (idx + 1) % PROGRESS_INTERVAL == 0 or idx + 1 == total:
                progress = (idx + 1) / total * 100
                print(f"进度: {idx + 1}/{total} ({progress:.1f}%)")
            
            if args.dry_run:
                existing = engine.db.get_jobs(start_date=job_data['submit_time'], end_date=job_data['end_time'])
                existing_ids = {j['job_id'] for j in existing}
                if job_data['job_id'] in existing_ids:
                    print(f"[DRY-RUN] 将更新: {job_data['job_id']} ({job_data['user']}) - {job_data['state']}")
                    updated += 1
                else:
                    print(f"[DRY-RUN] 将插入: {job_data['job_id']} ({job_data['user']}) - {job_data['state']}")
                    inserted += 1
            else:
                if engine.db.insert_job(job_record):
                    inserted += 1
        
        print(f"{'='*60}")
        
        # 更新同步状态
        if not args.dry_run:
            now_str = datetime.now().isoformat()
            db.update_sync_status(now_str, sync_type=sync_type)
            print(f"同步状态已更新: {now_str} (模式: {sync_type})")
        
        print(f"\n同步完成:")
        print(f"  总作业数: {total}")
        print(f"  新增/更新: {inserted}")
        if args.dry_run:
            print(f"  (试运行模式，未实际写入数据库)")


if __name__ == '__main__':
    main()
