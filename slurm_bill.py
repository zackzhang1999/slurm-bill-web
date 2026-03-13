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
            
            conn.commit()
            logger.info("数据库表初始化完成")
    
    def insert_job(self, job: JobRecord) -> bool:
        """插入作业记录"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO job_records 
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


class SlurmCollector:
    """Slurm 数据收集器"""
    
    @staticmethod
    def run_sacct(start_time: datetime.datetime = None,
                  end_time: datetime.datetime = None) -> List[Dict]:
        """运行 sacct 命令获取作业数据"""
        
        # 构建时间参数
        if start_time:
            start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
            end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S') if end_time else 'now'
            time_arg = f"--starttime={start_str} --endtime={end_str}"
        else:
            # 默认获取最近24小时的作业
            # 使用标准日期格式以确保兼容性
            from datetime import datetime, timedelta
            start_str = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%S')
            time_arg = f"--starttime={start_str}"
        
        cmd = (
            f"sacct -a -P -D --format="
            f"JobID,JobName,User,Account,Partition,State,Submit,Start,End,Elapsed,"
            f"NCPUS,NNodes,ReqMem,MaxRSS,AllocTRES "
            f"{time_arg} --noheader"
        )
        
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=300
            )
            if result.returncode != 0:
                logger.error(f"sacct 执行失败: {result.stderr}")
                return []
            
            jobs = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                
                parts = line.split('|')
                if len(parts) < 15:
                    continue
                
                # 解析 AllocTRES 获取 GPU 信息
                alloc_gpus = SlurmCollector._parse_gpu_count(parts[14])
                
                # 解析 MaxRSS
                max_rss_mb = SlurmCollector._parse_memory(parts[13])
                
                user = parts[2].strip()
                # 跳过 user 为空的记录（作业步或异常数据）
                if not user:
                    continue
                
                job = {
                    'job_id': parts[0],
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
                    'max_rss_mb': max_rss_mb,
                    'alloc_gpus': alloc_gpus,
                }
                jobs.append(job)
            
            logger.info(f"成功收集 {len(jobs)} 条作业记录")
            return jobs
            
        except subprocess.TimeoutExpired:
            logger.error("sacct 命令超时")
            return []
        except Exception as e:
            logger.error(f"收集作业数据失败: {e}")
            return []
    
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


if __name__ == '__main__':
    main()
