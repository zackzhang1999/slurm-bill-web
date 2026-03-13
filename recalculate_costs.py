#!/usr/bin/env python3
"""
重新计算作业费用脚本
当修改 config.yaml 费率后，用新费率重新计算所有历史作业的费用并更新数据库

用法:
  python3 recalculate_costs.py              # 重新计算所有作业
  python3 recalculate_costs.py --days 30    # 只重新计算最近30天的作业
  python3 recalculate_costs.py --dry-run    # 试运行，不实际更新数据库
"""

import sys
import os
import sqlite3
import argparse
import yaml
from datetime import datetime, timedelta
from decimal import Decimal

DB_PATH = '/var/lib/slurm-bill/billing.db'
CONFIG_PATH = '/etc/slurm-bill/config.yaml'


def load_billing_rates():
    """加载计费费率配置"""
    rates = {
        'cpu_per_hour': Decimal('0.10'),
        'memory_gb_per_hour': Decimal('0.02'),
        'gpu_per_hour': Decimal('2.00'),
        'min_charge': Decimal('0.01'),
        'currency': 'CNY'
    }
    
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r') as f:
                config = yaml.safe_load(f)
            
            billing = config.get('billing', {})
            rates['cpu_per_hour'] = Decimal(str(billing.get('cpu_per_hour', 0.10)))
            rates['memory_gb_per_hour'] = Decimal(str(billing.get('memory_gb_per_hour', 0.02)))
            rates['gpu_per_hour'] = Decimal(str(billing.get('gpu_per_hour', 2.00)))
            rates['min_charge'] = Decimal(str(billing.get('min_charge', 0.01)))
            rates['currency'] = billing.get('currency', 'CNY')
            print(f"✓ 加载配置文件: {CONFIG_PATH}")
        else:
            print(f"⚠ 配置文件不存在，使用默认费率: {CONFIG_PATH}")
    except Exception as e:
        print(f"⚠ 加载配置文件失败，使用默认费率: {e}")
    
    return rates


def parse_elapsed(elapsed_str):
    """解析时长字符串为小时数"""
    if not elapsed_str or elapsed_str == 'Unknown':
        return 0.0
    
    parts = elapsed_str.split(':')
    try:
        if len(parts) == 3:
            # HH:MM:SS
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])
            return hours + minutes / 60.0 + seconds / 3600.0
        elif len(parts) == 2:
            # MM:SS
            minutes = int(parts[0])
            seconds = int(parts[1])
            return minutes / 60.0 + seconds / 3600.0
    except (ValueError, IndexError):
        pass
    
    return 0.0


def calculate_job_cost(job, rates):
    """计算单个作业的费用"""
    ncpus = int(job['ncpus']) if job['ncpus'] else 0
    alloc_gpus = int(job['alloc_gpus']) if job['alloc_gpus'] else 0
    elapsed_hours = parse_elapsed(job['elapsed'])
    
    # CPU费用
    cpu_cost = Decimal(ncpus) * Decimal(str(elapsed_hours)) * rates['cpu_per_hour']
    
    # 内存费用 (假设每核2GB内存)
    mem_gb = ncpus * 2
    mem_cost = Decimal(str(mem_gb)) * Decimal(str(elapsed_hours)) * rates['memory_gb_per_hour']
    
    # GPU费用
    gpu_cost = Decimal(alloc_gpus) * Decimal(str(elapsed_hours)) * rates['gpu_per_hour']
    
    # 总费用
    total = cpu_cost + mem_cost + gpu_cost
    
    # 最低消费
    if total > 0 and total < rates['min_charge']:
        total = rates['min_charge']
    else:
        total = total.quantize(Decimal('0.01'))
    
    return total


def recalculate_costs(days=None, dry_run=False):
    """重新计算作业费用"""
    rates = load_billing_rates()
    
    print(f"\n{'='*70}")
    print(f"重新计算作业费用")
    print(f"{'='*70}")
    print(f"当前费率:")
    print(f"  CPU: {rates['cpu_per_hour']} 元/核/小时")
    print(f"  GPU: {rates['gpu_per_hour']} 元/卡/小时")
    print(f"  内存: {rates['memory_gb_per_hour']} 元/GB/小时")
    print(f"  最低消费: {rates['min_charge']} 元")
    print(f"{'='*70}\n")
    
    # 连接数据库
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 查询需要更新的作业
    if days:
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        cursor.execute('''
            SELECT * FROM job_records 
            WHERE end_time >= ? OR end_time = 'Unknown' OR end_time IS NULL
            ORDER BY end_time DESC
        ''', (start_date,))
        print(f"查询范围: 最近 {days} 天的作业")
    else:
        cursor.execute('SELECT * FROM job_records ORDER BY end_time DESC')
        print(f"查询范围: 所有历史作业")
    
    jobs = cursor.fetchall()
    print(f"找到 {len(jobs)} 个作业\n")
    
    if not jobs:
        print("没有需要更新的作业")
        conn.close()
        return
    
    # 统计
    updated_count = 0
    skipped_count = 0
    total_old_cost = Decimal(0)
    total_new_cost = Decimal(0)
    
    print(f"{'作业ID':<15} {'用户':<10} {'旧费用':>10} {'新费用':>10} {'差异':>10} {'状态'}")
    print(f"{'-'*70}")
    
    for job in jobs:
        job_id = job['job_id'][:12]
        user = job['user'][:8]
        old_cost = Decimal(str(job['cost'])) if job['cost'] else Decimal(0)
        
        # 用新费率计算
        new_cost = calculate_job_cost(job, rates)
        
        diff = new_cost - old_cost
        total_old_cost += old_cost
        total_new_cost += new_cost
        
        # 判断是否更新
        if abs(diff) > Decimal('0.001'):  # 差异大于0.001才更新
            status = "更新"
            updated_count += 1
            
            if not dry_run:
                cursor.execute('''
                    UPDATE job_records 
                    SET cost = ?
                    WHERE id = ?
                ''', (str(new_cost), job['id']))
        else:
            status = "跳过"
            skipped_count += 1
        
        # 只显示前20条和有变化的
        if updated_count <= 20 or status == "更新":
            print(f"{job_id:<15} {user:<10} {old_cost:>10.2f} {new_cost:>10.2f} {diff:>+10.2f} {status}")
        elif updated_count == 21:
            print(f"... (更多作业省略)")
    
    print(f"{'-'*70}")
    print(f"\n统计汇总:")
    print(f"  总作业数: {len(jobs)}")
    print(f"  需要更新: {updated_count}")
    print(f"  无需更新: {skipped_count}")
    print(f"\n费用变化:")
    print(f"  原总费用: {total_old_cost:.2f} 元")
    print(f"  新总费用: {total_new_cost:.2f} 元")
    print(f"  差异: {total_new_cost - total_old_cost:+.2f} 元 ({(total_new_cost/total_old_cost - 1)*100:+.1f}%)")
    
    if dry_run:
        print(f"\n⚠ 试运行模式，未实际更新数据库")
    else:
        conn.commit()
        print(f"\n✓ 数据库已更新")
    
    conn.close()
    print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(
        description='重新计算作业费用 - 修改费率后更新数据库',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  # 试运行（查看变化但不更新）
  python3 recalculate_costs.py --dry-run
  
  # 更新最近30天的作业
  python3 recalculate_costs.py --days 30
  
  # 更新所有历史作业
  python3 recalculate_costs.py
        '''
    )
    parser.add_argument('--days', '-d', type=int, default=None,
                       help='只重新计算最近N天的作业（默认：所有）')
    parser.add_argument('--dry-run', '-n', action='store_true',
                       help='试运行，不实际更新数据库')
    
    args = parser.parse_args()
    
    # 检查数据库
    if not os.path.exists(DB_PATH):
        print(f"错误: 数据库不存在: {DB_PATH}")
        sys.exit(1)
    
    recalculate_costs(days=args.days, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
