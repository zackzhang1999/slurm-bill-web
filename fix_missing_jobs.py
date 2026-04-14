#!/usr/bin/env python3
"""
修复缺失作业数据脚本
扫描 sacct 中的所有作业并补充到数据库
"""

import os
import sys
import sqlite3
import subprocess
from datetime import datetime, timedelta
from decimal import Decimal

DB_PATH = '/var/lib/slurm-bill/billing.db'

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def run_sacct_all():
    """运行 sacct 获取所有作业"""
    # 获取所有历史作业
    cmd = (
        "sacct -a -P --duplicates --format="
        "JobID,JobName,User,Account,Partition,State,Submit,Start,End,Elapsed,"
        "NCPUS,NNodes,ReqMem,MaxRSS,AllocTRES "
        "--starttime=2024-01-01 --noheader"
    )
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"sacct 执行失败: {result.stderr}")
            return []
        
        jobs = {}
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            
            parts = line.split('|')
            if len(parts) < 15:
                continue
            
            job_id = parts[0].strip()
            user = parts[2].strip()
            
            # 跳过没有 user 的作业步
            if not user:
                continue
            
            # 只保留主作业（不含 . 或 _ 的）
            if '.' in job_id or '_' in job_id or '[' in job_id:
                continue
            
            # 解析 GPU
            alloc_gpus = 0
            alloc_tres = parts[14]
            if alloc_tres:
                import re
                match = re.search(r'gres/gpu[=:](\d+)', alloc_tres, re.IGNORECASE)
                if match:
                    alloc_gpus = int(match.group(1))
            
            # 解析 MaxRSS
            max_rss_mb = 0.0
            mem_str = parts[13]
            if mem_str:
                mem_str = mem_str.strip()
                try:
                    if mem_str.endswith('M'):
                        max_rss_mb = float(mem_str[:-1])
                    elif mem_str.endswith('G'):
                        max_rss_mb = float(mem_str[:-1]) * 1024
                    elif mem_str.endswith('K'):
                        max_rss_mb = float(mem_str[:-1]) / 1024
                except:
                    pass
            
            # 解析时长
            elapsed_seconds = 0
            elapsed_str = parts[9]
            if elapsed_str and elapsed_str != 'Unknown':
                try:
                    days = 0
                    if '-' in elapsed_str:
                        day_part, time_part = elapsed_str.split('-')
                        days = int(day_part)
                        elapsed_str = time_part
                    
                    time_parts = elapsed_str.split(':')
                    if len(time_parts) == 3:
                        hours, minutes, seconds = map(int, time_parts)
                        elapsed_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds
                except:
                    pass
            
            jobs[job_id] = {
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
                'elapsed_seconds': elapsed_seconds,
                'ncpus': int(parts[10]) if parts[10].isdigit() else 1,
                'nnodes': int(parts[11]) if parts[11].isdigit() else 1,
                'req_mem': parts[12],
                'max_rss_mb': max_rss_mb,
                'alloc_gpus': alloc_gpus,
            }
        
        return list(jobs.values())
    except Exception as e:
        print(f"收集作业数据失败: {e}")
        return []

def fix_database():
    """修复数据库中的缺失作业"""
    print("="*70)
    print("修复缺失作业数据")
    print("="*70)
    
    # 获取 sacct 中的所有作业
    print("\n正在从 sacct 获取所有作业...")
    sacct_jobs = run_sacct_all()
    print(f"找到 {len(sacct_jobs)} 个主作业")
    
    # 获取数据库中的作业
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT job_id, state FROM job_records")
    db_jobs = {row['job_id']: row['state'] for row in cursor.fetchall()}
    print(f"数据库中已有 {len(db_jobs)} 个作业")
    
    # 找出缺失的作业
    missing_jobs = []
    for job in sacct_jobs:
        if job['job_id'] not in db_jobs:
            missing_jobs.append(job)
    
    print(f"\n缺失作业数量: {len(missing_jobs)}")
    
    if not missing_jobs:
        print("\n✓ 没有缺失的作业，数据库已完整！")
        conn.close()
        return
    
    # 显示前10个缺失的作业
    print("\n缺失的作业（前10个）:")
    for job in missing_jobs[:10]:
        print(f"  - {job['job_id']}: {job['user']} / {job['state']}")
    
    if len(missing_jobs) > 10:
        print(f"  ... 还有 {len(missing_jobs) - 10} 个")
    
    # 询问是否修复
    print("\n是否修复这些缺失的作业？ (yes/no): ", end='')
    response = input().strip().lower()
    
    if response not in ('yes', 'y'):
        print("已取消修复")
        conn.close()
        return
    
    # 插入缺失的作业
    inserted = 0
    for job in missing_jobs:
        # 计算简单费用
        elapsed_hours = job['elapsed_seconds'] / 3600
        cpu_cost = Decimal(job['ncpus']) * Decimal(str(elapsed_hours)) * Decimal('0.10')
        gpu_cost = Decimal(job['alloc_gpus']) * Decimal(str(elapsed_hours)) * Decimal('2.00')
        cost = cpu_cost + gpu_cost
        
        try:
            cursor.execute('''
                INSERT INTO job_records 
                (job_id, job_name, user, account, partition, state,
                 submit_time, start_time, end_time, elapsed, elapsed_seconds,
                 ncpus, nnodes, req_mem, max_rss_mb, alloc_gpus,
                 billing_units, cost, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job['job_id'], job['job_name'], job['user'], job['account'], 
                job['partition'], job['state'], job['submit_time'], job['start_time'], 
                job['end_time'], job['elapsed'], job['elapsed_seconds'],
                job['ncpus'], job['nnodes'], job['req_mem'], job['max_rss_mb'], 
                job['alloc_gpus'], str(cost), str(cost), datetime.now().isoformat()
            ))
            inserted += 1
            print(f"  + 已插入: {job['job_id']} ({job['user']}) - {job['state']}")
        except sqlite3.Error as e:
            print(f"  ✗ 插入失败 {job['job_id']}: {e}")
    
    conn.commit()
    conn.close()
    
    print("\n" + "="*70)
    print(f"修复完成: 已插入 {inserted} 个缺失的作业")
    print("="*70)

if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        print(f"错误: 数据库不存在: {DB_PATH}")
        sys.exit(1)
    
    fix_database()
