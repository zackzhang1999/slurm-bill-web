#!/usr/bin/env python3
"""
Slurm Billing Web - 演示模式
用于演示Web界面功能（无需真实数据库）
"""

import sqlite3
import os
from datetime import datetime, timedelta
from decimal import Decimal
import random

DB_PATH = 'demo.db'

def create_demo_database():
    """创建演示数据库"""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            job_name TEXT,
            user TEXT,
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
            created_at TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS account_balance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT NOT NULL,
            account TEXT NOT NULL,
            balance TEXT DEFAULT '0.00',
            credit_limit TEXT DEFAULT '0.00',
            total_recharged TEXT DEFAULT '0.00',
            total_consumed TEXT DEFAULT '0.00',
            alert_threshold TEXT DEFAULT '0.00',
            status TEXT DEFAULT 'active',
            last_updated TEXT,
            UNIQUE(user, account)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS recharge_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT NOT NULL,
            account TEXT NOT NULL,
            amount TEXT NOT NULL,
            balance_after TEXT NOT NULL,
            recharge_type TEXT DEFAULT 'cash',
            operator TEXT,
            remark TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 生成演示数据
    users = [
        ('zhangsan', 'research', 1000),
        ('lisi', 'research', 800),
        ('wangwu', 'student', 200),
        ('zhaoliu', 'student', 150),
        ('admin', 'admin', 0),
    ]
    
    partitions = ['cpu', 'gpu', 'high_mem', 'debug']
    states = ['COMPLETED', 'COMPLETED', 'COMPLETED', 'COMPLETED', 'FAILED', 'CANCELLED']
    
    # 插入作业记录
    now = datetime.now()
    for i in range(200):
        user, account, _ = random.choice(users)
        partition = random.choice(partitions)
        state = random.choice(states)
        
        # 随机时间（过去30天内）
        days_ago = random.randint(0, 30)
        hours_ago = random.randint(0, 23)
        end_time = now - timedelta(days=days_ago, hours=hours_ago)
        elapsed_hours = random.randint(1, 24)
        elapsed_seconds = elapsed_hours * 3600
        start_time = end_time - timedelta(seconds=elapsed_seconds)
        
        ncpus = random.choice([1, 2, 4, 8, 16])
        alloc_gpus = random.choice([0, 0, 0, 1, 2, 4]) if partition == 'gpu' else 0
        
        # 计算费用
        cpu_cost = Decimal(ncpus) * Decimal(elapsed_hours) * Decimal('0.1')
        gpu_cost = Decimal(alloc_gpus) * Decimal(elapsed_hours) * Decimal('2.0')
        cost = cpu_cost + gpu_cost
        
        job_id = str(100000 + i)
        
        cursor.execute('''
            INSERT INTO job_records 
            (job_id, job_name, user, account, partition, state,
             submit_time, start_time, end_time, elapsed, elapsed_seconds,
             ncpus, nnodes, alloc_gpus, cost, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            job_id, f'job_{i}', user, account, partition, state,
            start_time.isoformat(), start_time.isoformat(), end_time.isoformat(),
            f'{elapsed_hours}:00:00', elapsed_seconds,
            ncpus, 1, alloc_gpus, str(cost), end_time.isoformat()
        ))
    
    # 插入余额数据
    for user, account, balance in users:
        cursor.execute('''
            INSERT INTO account_balance 
            (user, account, balance, credit_limit, total_recharged, total_consumed, status, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user, account, str(balance), '100', str(balance + random.randint(100, 500)), 
              str(random.randint(50, 400)), 'active', now.isoformat()))
        
        # 插入充值记录
        for _ in range(random.randint(1, 3)):
            amount = random.randint(100, 500)
            cursor.execute('''
                INSERT INTO recharge_records 
                (user, account, amount, balance_after, recharge_type, operator, remark, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user, account, str(amount), str(balance), 'cash', 'admin', '充值', 
                  (now - timedelta(days=random.randint(1, 30))).isoformat()))
    
    conn.commit()
    conn.close()
    print(f"✓ 演示数据库创建成功: {DB_PATH}")
    print(f"  - 200条作业记录")
    print(f"  - 5个用户账户")
    print(f"  - 充值记录和历史数据")

if __name__ == '__main__':
    create_demo_database()
    print("\n现在可以启动Web服务器查看演示:")
    print("  bash start.sh")
