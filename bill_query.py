#!/usr/bin/env python3
"""
Slurm Billing Query Tool - 账单查询工具
提供交互式命令行界面查询计费数据

用法:
  python3 bill_query.py                    # 交互式模式
  python3 bill_query.py user username      # 查询指定用户
  python3 bill_query.py account accname    # 查询指定账户
  python3 bill_query.py top [n]            # 显示消费排行
"""

import sys
import os
import sqlite3
import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict
from prettytable import PrettyTable
import readline  # 启用命令行历史

DB_PATH = '/var/lib/slurm-bill/billing.db'
CONFIG_PATH = '/etc/slurm-bill/config.yaml'


# 加载费率配置
_billing_rates = None

def load_billing_rates():
    """加载计费费率配置"""
    global _billing_rates
    if _billing_rates is not None:
        return _billing_rates
    
    # 默认费率
    rates = {
        'cpu_per_hour': Decimal('0.10'),
        'memory_gb_per_hour': Decimal('0.02'),
        'gpu_per_hour': Decimal('2.00'),
        'currency': 'CNY'
    }
    
    try:
        if os.path.exists(CONFIG_PATH):
            import yaml
            with open(CONFIG_PATH, 'r') as f:
                config = yaml.safe_load(f)
            
            billing = config.get('billing', {})
            rates['cpu_per_hour'] = Decimal(str(billing.get('cpu_per_hour', 0.10)))
            rates['memory_gb_per_hour'] = Decimal(str(billing.get('memory_gb_per_hour', 0.02)))
            rates['gpu_per_hour'] = Decimal(str(billing.get('gpu_per_hour', 2.00)))
            rates['currency'] = billing.get('currency', 'CNY')
    except Exception as e:
        print(f"警告: 加载配置文件失败，使用默认费率: {e}")
    
    _billing_rates = rates
    return rates


def format_rate_info(rates: dict) -> str:
    """格式化费率信息"""
    return (f"费率: CPU {rates['cpu_per_hour']}元/核/小时 | "
            f"GPU {rates['gpu_per_hour']}元/卡/小时 | "
            f"内存 {rates['memory_gb_per_hour']}元/GB/小时")


class BillingQuery:
    """账单查询类"""
    
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.row_factory = sqlite3.Row
    
    def close(self):
        self.conn.close()
    
    def get_user_summary(self, user: str = None, days: int = 30) -> List[Dict]:
        """获取用户消费汇总"""
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        sql = '''
            SELECT 
                user,
                COUNT(*) as job_count,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours,
                SUM(CAST(cost AS DECIMAL)) as total_cost,
                AVG(CAST(cost AS DECIMAL)) as avg_cost
            FROM job_records
            WHERE (end_time >= ? OR end_time = 'Unknown' OR end_time IS NULL)
              AND (state IN ('COMPLETED', 'CD', 'FAILED', 'F', 'TIMEOUT', 'TO', 'CANCELLED', 'CA')
                   OR state LIKE 'CANCELLED%')
        '''
        params = [start_date]
        
        if user:
            sql += " AND user = ?"
            params.append(user)
        
        sql += " GROUP BY user ORDER BY total_cost DESC"
        
        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def get_account_summary(self, account: str = None, days: int = 30) -> List[Dict]:
        """获取账户消费汇总"""
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        sql = '''
            SELECT 
                account,
                COUNT(*) as job_count,
                COUNT(DISTINCT user) as user_count,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours,
                SUM(CAST(cost AS DECIMAL)) as total_cost
            FROM job_records
            WHERE (end_time >= ? OR end_time = 'Unknown' OR end_time IS NULL)
              AND (state IN ('COMPLETED', 'CD', 'FAILED', 'F', 'TIMEOUT', 'TO', 'CANCELLED', 'CA')
                   OR state LIKE 'CANCELLED%')
        '''
        params = [start_date]
        
        if account:
            sql += " AND account = ?"
            params.append(account)
        
        sql += " GROUP BY account ORDER BY total_cost DESC"
        
        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def get_user_jobs(self, user: str, days: int = 7, limit: int = 50) -> List[Dict]:
        """获取用户作业详情"""
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM job_records
            WHERE user = ? AND end_time >= ?
            ORDER BY end_time DESC
            LIMIT ?
        ''', (user, start_date, limit))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_daily_stats(self, days: int = 30) -> List[Dict]:
        """获取每日统计"""
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                DATE(end_time) as date,
                COUNT(*) as job_count,
                COUNT(DISTINCT user) as active_users,
                SUM(CAST(cost AS DECIMAL)) as daily_cost
            FROM job_records
            WHERE DATE(end_time) >= ? AND state IN ('COMPLETED', 'CD')
            GROUP BY DATE(end_time)
            ORDER BY date DESC
        ''', (start_date,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_partition_stats(self, days: int = 30) -> List[Dict]:
        """获取分区统计"""
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                partition,
                COUNT(*) as job_count,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours,
                SUM(CAST(cost AS DECIMAL)) as total_cost
            FROM job_records
            WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
            GROUP BY partition
            ORDER BY total_cost DESC
        ''', (start_date,))
        return [dict(row) for row in cursor.fetchall()]


def print_user_summary(results: List[Dict], title: str = "用户消费汇总"):
    """打印用户汇总表 - 使用当前费率实时计算费用"""
    if not results:
        print("没有数据")
        return
    
    rates = load_billing_rates()
    
    table = PrettyTable()
    table.field_names = ["用户", "作业数", "CPU核时", "GPU卡时", "总费用", "平均费用"]
    table.align = "r"
    table.align["用户"] = "l"
    
    total_cost = Decimal(0)
    total_cpu_hours = Decimal(0)
    total_gpu_hours = Decimal(0)
    
    for r in results:
        # 使用当前费率实时计算费用
        cpu_hours = Decimal(str(r['cpu_core_hours']))
        gpu_hours = Decimal(str(r['gpu_hours']))
        cpu_cost = cpu_hours * rates['cpu_per_hour']
        gpu_cost = gpu_hours * rates['gpu_per_hour']
        current_cost = cpu_cost + gpu_cost
        
        table.add_row([
            r['user'],
            r['job_count'],
            f"{cpu_hours:.2f}",
            f"{gpu_hours:.2f}",
            f"{current_cost:.2f}",
            f"{current_cost / r['job_count']:.2f}" if r['job_count'] > 0 else "0.00"
        ])
        total_cost += current_cost
        total_cpu_hours += cpu_hours
        total_gpu_hours += gpu_hours
    
    # 计算费用明细
    cpu_cost = total_cpu_hours * rates['cpu_per_hour']
    gpu_cost = total_gpu_hours * rates['gpu_per_hour']
    
    print(f"\n{'='*85}")
    print(f"{title}")
    print(f"{'='*85}")
    print(f"📋 当前费率: CPU {rates['cpu_per_hour']}元/核/小时 | GPU {rates['gpu_per_hour']}元/卡/小时")
    print(f"{'-'*85}")
    print(table)
    print(f"{'-'*85}")
    print(f"💰 费用明细:")
    print(f"   CPU费用: {total_cpu_hours:.2f} 核时 × {rates['cpu_per_hour']} 元/核时 = {cpu_cost:.2f} 元")
    print(f"   GPU费用: {total_gpu_hours:.2f} 卡时 × {rates['gpu_per_hour']} 元/卡时 = {gpu_cost:.2f} 元")
    print(f"   {'─'*50}")
    print(f"   总计费用: {total_cost:.2f} 元")
    print(f"{'='*85}\n")


def print_account_summary(results: List[Dict], title: str = "账户消费汇总"):
    """打印账户汇总表 - 使用当前费率实时计算费用"""
    if not results:
        print("没有数据")
        return
    
    rates = load_billing_rates()
    
    table = PrettyTable()
    table.field_names = ["账户", "作业数", "用户数", "CPU核时", "GPU卡时", "总费用"]
    table.align = "r"
    table.align["账户"] = "l"
    
    total_cost = Decimal(0)
    total_cpu_hours = Decimal(0)
    total_gpu_hours = Decimal(0)
    
    for r in results:
        # 使用当前费率实时计算费用
        cpu_hours = Decimal(str(r['cpu_core_hours']))
        gpu_hours = Decimal(str(r['gpu_hours']))
        cpu_cost = cpu_hours * rates['cpu_per_hour']
        gpu_cost = gpu_hours * rates['gpu_per_hour']
        current_cost = cpu_cost + gpu_cost
        
        table.add_row([
            r['account'],
            r['job_count'],
            r['user_count'],
            f"{cpu_hours:.2f}",
            f"{gpu_hours:.2f}",
            f"{current_cost:.2f}"
        ])
        total_cost += current_cost
        total_cpu_hours += cpu_hours
        total_gpu_hours += gpu_hours
    
    # 计算费用明细
    cpu_cost = total_cpu_hours * rates['cpu_per_hour']
    gpu_cost = total_gpu_hours * rates['gpu_per_hour']
    
    print(f"\n{'='*85}")
    print(f"{title}")
    print(f"{'='*85}")
    print(f"📋 当前费率: CPU {rates['cpu_per_hour']}元/核/小时 | GPU {rates['gpu_per_hour']}元/卡/小时")
    print(f"{'-'*85}")
    print(table)
    print(f"{'-'*85}")
    print(f"💰 费用明细:")
    print(f"   CPU费用: {total_cpu_hours:.2f} 核时 × {rates['cpu_per_hour']} 元/核时 = {cpu_cost:.2f} 元")
    print(f"   GPU费用: {total_gpu_hours:.2f} 卡时 × {rates['gpu_per_hour']} 元/卡时 = {gpu_cost:.2f} 元")
    print(f"   {'─'*50}")
    print(f"   总计费用: {total_cost:.2f} 元")
    print(f"{'='*85}\n")


def print_job_details(jobs: List[Dict], title: str = "作业详情"):
    """打印作业详情 - 使用当前费率实时计算费用"""
    if not jobs:
        print("没有数据")
        return
    
    rates = load_billing_rates()
    
    table = PrettyTable()
    table.field_names = ["作业ID", "名称", "账户", "分区", "状态", "核数", "GPU", "时长", "费用"]
    table.align = "r"
    table.align["名称"] = "l"
    
    for job in jobs:
        # 使用当前费率实时计算费用
        # 从elapsed解析时长
        elapsed = job['elapsed'] if job['elapsed'] else '00:00:00'
        parts = elapsed.split(':')
        if len(parts) == 3:
            hours = int(parts[0]) + int(parts[1])/60 + int(parts[2])/3600
        elif len(parts) == 2:
            hours = int(parts[0])/60 + int(parts[1])/3600
        else:
            hours = 0
        
        ncpus = int(job['ncpus']) if job['ncpus'] else 0
        alloc_gpus = int(job['alloc_gpus']) if job['alloc_gpus'] else 0
        
        cpu_cost = Decimal(ncpus) * Decimal(str(hours)) * rates['cpu_per_hour']
        gpu_cost = Decimal(alloc_gpus) * Decimal(str(hours)) * rates['gpu_per_hour']
        current_cost = cpu_cost + gpu_cost
        
        table.add_row([
            job['job_id'][:12],
            job['job_name'][:20] if job['job_name'] else '-',
            job['account'],
            job['partition'],
            job['state'],
            ncpus,
            alloc_gpus,
            job['elapsed'],
            f"{current_cost:.2f}"
        ])
    
    print(f"\n{'='*100}")
    print(f"{title} ({len(jobs)} 条) - 当前费率: CPU {rates['cpu_per_hour']}元/核/小时 | GPU {rates['gpu_per_hour']}元/卡/小时")
    print(f"{'='*100}")
    print(table)
    print(f"{'='*100}\n")


def print_daily_stats(results: List[Dict], title: str = "每日统计"):
    """打印每日统计"""
    if not results:
        print("没有数据")
        return
    
    table = PrettyTable()
    table.field_names = ["日期", "作业数", "活跃用户", "日费用"]
    table.align = "r"
    table.align["日期"] = "l"
    
    total_cost = Decimal(0)
    for r in results:
        table.add_row([
            r['date'],
            r['job_count'],
            r['active_users'],
            f"{Decimal(r['daily_cost']):.2f}"
        ])
        total_cost += Decimal(r['daily_cost'])
    
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    print(table)
    print(f"总计: {total_cost:.2f} 元")
    print(f"{'='*60}\n")


def print_partition_stats(results: List[Dict], title: str = "分区统计"):
    """打印分区统计 - 使用当前费率实时计算费用"""
    if not results:
        print("没有数据")
        return
    
    rates = load_billing_rates()
    
    table = PrettyTable()
    table.field_names = ["分区", "作业数", "CPU核时", "GPU卡时", "总费用"]
    table.align = "r"
    table.align["分区"] = "l"
    
    total_cost = Decimal(0)
    for r in results:
        # 使用当前费率实时计算费用
        cpu_hours = Decimal(str(r['cpu_core_hours']))
        gpu_hours = Decimal(str(r['gpu_hours']))
        cpu_cost = cpu_hours * rates['cpu_per_hour']
        gpu_cost = gpu_hours * rates['gpu_per_hour']
        current_cost = cpu_cost + gpu_cost
        
        table.add_row([
            r['partition'],
            r['job_count'],
            f"{cpu_hours:.2f}",
            f"{gpu_hours:.2f}",
            f"{current_cost:.2f}"
        ])
        total_cost += current_cost
    
    print(f"\n{'='*70}")
    print(f"{title}")
    print(f"{'='*70}")
    print(f"📋 当前费率: CPU {rates['cpu_per_hour']}元/核/小时 | GPU {rates['gpu_per_hour']}元/卡/小时")
    print(f"{'-'*70}")
    print(table)
    print(f"{'-'*70}")
    print(f"💰 总计费用: {total_cost:.2f} 元")
    print(f"{'='*70}\n")


def interactive_mode():
    """交互式查询模式"""
    print("""
╔═══════════════════════════════════════════════════════════╗
║           Slurm Billing Query Tool 交互式模式              ║
║                    计费查询系统 v1.0.0                      ║
╠═══════════════════════════════════════════════════════════╣
║  可用命令:                                                ║
║    user [用户名] [天数]    - 查询用户消费汇总              ║
║    account [账户名] [天数] - 查询账户消费汇总              ║
║    jobs [用户名] [天数]    - 查询用户作业详情              ║
║    daily [天数]            - 每日统计                     ║
║    partition [天数]        - 分区统计                     ║
║    top [n] [天数]          - 消费排行前N                  ║
║    help                    - 显示帮助                     ║
║    quit/exit               - 退出                         ║
╚═══════════════════════════════════════════════════════════╝
    """)
    
    query = BillingQuery()
    
    try:
        while True:
            try:
                cmd = input("billing> ").strip()
                if not cmd:
                    continue
                
                parts = cmd.split()
                action = parts[0].lower()
                
                if action in ['quit', 'exit', 'q']:
                    break
                
                elif action == 'help':
                    print("命令帮助...")
                
                elif action == 'user':
                    user = parts[1] if len(parts) > 1 else None
                    days = int(parts[2]) if len(parts) > 2 else 30
                    results = query.get_user_summary(user, days)
                    print_user_summary(results, f"最近 {days} 天用户消费汇总")
                
                elif action == 'account':
                    account = parts[1] if len(parts) > 1 else None
                    days = int(parts[2]) if len(parts) > 2 else 30
                    results = query.get_account_summary(account, days)
                    print_account_summary(results, f"最近 {days} 天账户消费汇总")
                
                elif action == 'jobs':
                    if len(parts) < 2:
                        print("用法: jobs <用户名> [天数]")
                        continue
                    user = parts[1]
                    days = int(parts[2]) if len(parts) > 2 else 7
                    jobs = query.get_user_jobs(user, days)
                    print_job_details(jobs, f"{user} 最近 {days} 天作业详情")
                
                elif action == 'daily':
                    days = int(parts[1]) if len(parts) > 1 else 30
                    results = query.get_daily_stats(days)
                    print_daily_stats(results, f"最近 {days} 天每日统计")
                
                elif action == 'partition':
                    days = int(parts[1]) if len(parts) > 1 else 30
                    results = query.get_partition_stats(days)
                    print_partition_stats(results, f"最近 {days} 天分区统计")
                
                elif action == 'top':
                    n = int(parts[1]) if len(parts) > 1 else 10
                    days = int(parts[2]) if len(parts) > 2 else 30
                    results = query.get_user_summary(days=days)[:n]
                    print_user_summary(results, f"最近 {days} 天消费排行前 {n}")
                
                else:
                    print(f"未知命令: {action}")
                    
            except KeyboardInterrupt:
                print()
                continue
            except Exception as e:
                print(f"错误: {e}")
    
    finally:
        query.close()
        print("再见!")


def main():
    parser = argparse.ArgumentParser(description='Slurm Billing Query Tool')
    parser.add_argument('command', nargs='?', choices=['user', 'account', 'jobs', 'daily', 'partition', 'top', 'interactive'],
                       help='查询命令')
    parser.add_argument('target', nargs='?', help='查询目标（用户名或账户名）')
    parser.add_argument('--days', '-d', type=int, default=30, help='查询天数')
    parser.add_argument('--limit', '-l', type=int, default=50, help='限制条数')
    
    args = parser.parse_args()
    
    # 如果没有参数，进入交互式模式
    if not args.command:
        interactive_mode()
        return
    
    query = BillingQuery()
    
    try:
        if args.command == 'user':
            results = query.get_user_summary(args.target, args.days)
            print_user_summary(results)
        
        elif args.command == 'account':
            results = query.get_account_summary(args.target, args.days)
            print_account_summary(results)
        
        elif args.command == 'jobs':
            if not args.target:
                print("错误: 需要指定用户名")
                return
            jobs = query.get_user_jobs(args.target, args.days, args.limit)
            print_job_details(jobs)
        
        elif args.command == 'daily':
            results = query.get_daily_stats(args.days)
            print_daily_stats(results)
        
        elif args.command == 'partition':
            results = query.get_partition_stats(args.days)
            print_partition_stats(results)
        
        elif args.command == 'top':
            n = int(args.target) if args.target else 10
            results = query.get_user_summary(days=args.days)[:n]
            print_user_summary(results, f"消费排行前 {n}")
        
        elif args.command == 'interactive':
            interactive_mode()
    
    finally:
        query.close()


if __name__ == '__main__':
    main()
