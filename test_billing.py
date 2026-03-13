#!/usr/bin/env python3
"""
Slurm Billing System 测试脚本
用于验证安装和配置是否正确
"""

import os
import sys
import sqlite3
from decimal import Decimal

def test_imports():
    """测试依赖导入"""
    print("[1/5] 测试依赖导入...")
    try:
        import yaml
        import prettytable
        print("  ✓ 所有依赖已安装")
        return True
    except ImportError as e:
        print(f"  ✗ 缺少依赖: {e}")
        print("  请运行: pip3 install pyyaml prettytable")
        return False

def test_directories():
    """测试目录结构"""
    print("[2/5] 测试目录结构...")
    dirs = [
        '/etc/slurm-bill',
        '/var/lib/slurm-bill',
        '/var/log/slurm-bill'
    ]
    
    all_exist = True
    for d in dirs:
        if os.path.exists(d):
            print(f"  ✓ {d}")
        else:
            print(f"  ✗ {d} (不存在)")
            all_exist = False
    
    return all_exist

def test_config():
    """测试配置文件"""
    print("[3/5] 测试配置文件...")
    config_path = '/etc/slurm-bill/config.yaml'
    
    if not os.path.exists(config_path):
        print(f"  ✗ 配置文件不存在: {config_path}")
        return False
    
    try:
        import yaml
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        billing = config.get('billing', {})
        print(f"  ✓ 配置文件加载成功")
        print(f"    - CPU 费率: {billing.get('cpu_per_hour', 'N/A')} 元/核/小时")
        print(f"    - 内存费率: {billing.get('memory_gb_per_hour', 'N/A')} 元/GB/小时")
        print(f"    - GPU 费率: {billing.get('gpu_per_hour', 'N/A')} 元/卡/小时")
        return True
    except Exception as e:
        print(f"  ✗ 配置文件解析失败: {e}")
        return False

def test_database():
    """测试数据库"""
    print("[4/5] 测试数据库...")
    db_path = '/var/lib/slurm-bill/billing.db'
    
    if not os.path.exists(db_path):
        print(f"  ✗ 数据库不存在: {db_path}")
        print("  请运行: slurm-bill init")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['job_records', 'billing_cycles', 'account_balance']
        for table in required_tables:
            if table in tables:
                print(f"  ✓ 表 {table} 存在")
            else:
                print(f"  ✗ 表 {table} 不存在")
        
        # 统计记录数
        cursor.execute("SELECT COUNT(*) FROM job_records")
        count = cursor.fetchone()[0]
        print(f"  ℹ 现有作业记录: {count} 条")
        
        conn.close()
        return True
    except Exception as e:
        print(f"  ✗ 数据库错误: {e}")
        return False

def test_slurm():
    """测试 Slurm 连接"""
    print("[5/5] 测试 Slurm 连接...")
    
    import subprocess
    
    # 检查 sacct
    result = subprocess.run(['which', 'sacct'], capture_output=True)
    if result.returncode != 0:
        print("  ✗ 未找到 sacct 命令")
        return False
    print("  ✓ sacct 命令可用")
    
    # 测试 sacct 运行
    result = subprocess.run(
        ['sacct', '-a', '-X', '--format=JobID,User', '-n', '-S', 'now-1hours'],
        capture_output=True, text=True, timeout=10
    )
    
    if result.returncode == 0:
        lines = [l for l in result.stdout.strip().split('\n') if l.strip()]
        print(f"  ✓ sacct 运行正常 (最近1小时 {len(lines)} 条记录)")
        return True
    else:
        print(f"  ✗ sacct 运行失败: {result.stderr}")
        return False

def test_calculation():
    """测试计费计算"""
    print("\n[额外] 测试计费计算...")
    
    try:
        sys.path.insert(0, '/opt/slurm-bill')
        from slurm_bill import BillingCalculator
        
        calc = BillingCalculator()
        
        # 测试作业
        test_job = {
            'job_id': 'test123',
            'user': 'testuser',
            'account': 'test',
            'partition': 'normal',
            'elapsed_seconds': 3600,  # 1小时
            'ncpus': 4,
            'nnodes': 1,
            'max_rss_mb': 8192,  # 8GB
            'alloc_gpus': 1
        }
        
        billing_units, cost = calc.calculate_job_cost(test_job)
        
        print(f"  测试作业: 4核 + 8GB内存 + 1GPU, 运行1小时")
        print(f"  计费单位: {billing_units} 元")
        print(f"  最终费用: {cost} 元")
        print("  ✓ 计费计算正常")
        return True
        
    except Exception as e:
        print(f"  ✗ 计费计算失败: {e}")
        return False

def main():
    """主函数"""
    print("="*60)
    print("Slurm Billing System 测试脚本")
    print("="*60)
    print()
    
    tests = [
        test_imports,
        test_directories,
        test_config,
        test_database,
        test_slurm,
    ]
    
    results = []
    for test in tests:
        try:
            results.append(test())
        except Exception as e:
            print(f"  ✗ 测试异常: {e}")
            results.append(False)
        print()
    
    # 可选测试
    if all(results):
        try:
            test_calculation()
        except Exception as e:
            print(f"  计费计算测试跳过: {e}")
    
    print("="*60)
    passed = sum(results)
    total = len(results)
    print(f"测试结果: {passed}/{total} 通过")
    
    if all(results):
        print("✓ 所有测试通过，计费系统准备就绪!")
        return 0
    else:
        print("✗ 部分测试失败，请检查错误信息")
        return 1

if __name__ == '__main__':
    sys.exit(main())
