#!/usr/bin/env python3
"""
Slurm Epilog Script - 作业结束后实际扣费

配置方法:
1. 将此脚本复制到 /etc/slurm/slurm_epilog.py
2. 在 slurm.conf 中添加:
   Epilog=/etc/slurm/slurm_epilog.py
3. 设置权限:
   chmod +x /etc/slurm/slurm_epilog.py
   chown root:root /etc/slurm/slurm_epilog.py

工作原理:
- Slurm在作业结束后调用此脚本
- 脚本获取作业实际运行时间和资源使用
- 计算实际费用并从余额中扣除
- 如果实际费用与预估不同，进行多退少补
"""

import os
import sys
import subprocess
import sqlite3
import re
from decimal import Decimal, ROUND_UP
from datetime import datetime

# 确保能找到我们的模块
sys.path.insert(0, '/opt/slurm-bill')

from balance_manager import BalanceManager

# Slurm环境变量
SLURM_JOB_ID = os.environ.get('SLURM_JOB_ID', '0')
SLURM_JOB_USER = os.environ.get('SLURM_JOB_USER', '')
SLURM_JOB_ACCOUNT = os.environ.get('SLURM_JOB_ACCOUNT', 'default')


def get_job_actual_usage() -> dict:
    """获取作业实际资源使用情况"""
    try:
        result = subprocess.run(
            ['sacct', '-j', SLURM_JOB_ID, '-n', '-P', 
             '--format=JobID,Elapsed,CPUTime,MaxRSS,AllocTRES,State'],
            capture_output=True, text=True, timeout=30
        )
        
        if result.returncode != 0:
            print(f"[Epilog] sacct 错误: {result.stderr}", file=sys.stderr)
            return None
        
        lines = result.stdout.strip().split('\n')
        if not lines:
            return None
        
        main_job_line = None
        for line in lines:
            if line.startswith(SLURM_JOB_ID + '|'):
                main_job_line = line
                break
        
        if not main_job_line:
            main_job_line = lines[0]
        
        parts = main_job_line.split('|')
        if len(parts) < 6:
            return None
        
        return {
            'job_id': parts[0],
            'elapsed': parts[1],
            'cpu_time': parts[2],
            'max_rss': parts[3],
            'alloc_tres': parts[4],
            'state': parts[5]
        }
    except Exception as e:
        print(f"[Epilog] 获取作业使用错误: {e}", file=sys.stderr)
        return None


def parse_elapsed_to_hours(elapsed_str: str) -> float:
    """解析运行时长为小时数"""
    if not elapsed_str or elapsed_str == 'None':
        return 0.0
    
    try:
        days = 0
        if '-' in elapsed_str:
            day_part, elapsed_str = elapsed_str.split('-')
            days = int(day_part)
        
        parts = elapsed_str.split(':')
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
        elif len(parts) == 2:
            hours, minutes = map(int, parts)
            seconds = 0
        else:
            return 0.0
        
        return days * 24 + hours + minutes / 60 + seconds / 3600
    except:
        return 0.0


def parse_gpu_from_tres(tres_str: str) -> int:
    """从 AllocTRES 解析 GPU 数量"""
    if not tres_str:
        return 0
    
    match = re.search(r'gres/gpu[=:](\d+)', tres_str, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 0


def calculate_actual_cost(usage: dict) -> Decimal:
    """根据实际使用情况计算费用"""
    try:
        from slurm_bill import BillingCalculator
        
        calculator = BillingCalculator()
        rates = calculator.rate
        
        elapsed_hours = parse_elapsed_to_hours(usage.get('elapsed', '00:00:00'))
        
        # 从数据库获取作业配置
        conn = sqlite3.connect('/var/lib/slurm-bill/billing.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT ncpus, alloc_gpus FROM job_records 
            WHERE job_id = ?
        ''', (SLURM_JOB_ID,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            ncpus = row[0]
            alloc_gpus = row[1]
        else:
            # 从 sacct 获取
            ncpus = 1
            alloc_gpus = parse_gpu_from_tres(usage.get('alloc_tres', ''))
        
        # 计算费用
        cpu_cost = Decimal(ncpus) * Decimal(str(elapsed_hours)) * rates.cpu_per_hour
        mem_gb = ncpus * 2
        mem_cost = Decimal(str(mem_gb)) * Decimal(str(elapsed_hours)) * rates.memory_gb_per_hour
        gpu_cost = Decimal(alloc_gpus) * Decimal(str(elapsed_hours)) * rates.gpu_per_hour
        
        total = cpu_cost + mem_cost + gpu_cost
        
        # 设置最小费用为0.01元（1分钱），避免短作业费用为0
        if total > 0 and total < Decimal('0.01'):
            total = Decimal('0.01')
        else:
            total = total.quantize(Decimal('0.01'))
        
        return total
    except Exception as e:
        print(f"[Epilog] 计算费用错误: {e}", file=sys.stderr)
        return Decimal('0')


def main():
    """Epilog主函数"""
    print(f"[Epilog] 处理作业 {SLURM_JOB_ID} 的扣费...", file=sys.stderr)
    
    if not SLURM_JOB_USER:
        print("[Epilog] 错误: 无法获取用户名", file=sys.stderr)
        return 0
    
    # 获取实际使用情况
    usage = get_job_actual_usage()
    if not usage:
        print("[Epilog] 警告: 无法获取作业实际使用情况", file=sys.stderr)
        return 0
    
    print(f"[Epilog] 作业状态: {usage.get('state')}", file=sys.stderr)
    print(f"[Epilog] 运行时长: {usage.get('elapsed')}", file=sys.stderr)
    
    # 计算实际费用
    actual_cost = calculate_actual_cost(usage)
    
    if actual_cost <= 0:
        print("[Epilog] 费用为0，跳过扣费", file=sys.stderr)
        return 0
    
    print(f"[Epilog] 实际费用: {actual_cost} 元", file=sys.stderr)
    
    # 执行扣费
    manager = BalanceManager()
    
    # 先尝试通过 charge_job 扣费（有预扣记录的情况）
    success, msg = manager.charge_job(SLURM_JOB_ID, actual_cost)
    
    if success:
        print(f"[Epilog] ✓ {msg}", file=sys.stderr)
    elif "未找到作业" in msg:
        # 没有预扣记录，执行直接扣费
        print(f"[Epilog] 未找到预扣记录，执行直接扣费", file=sys.stderr)
        
        # 检查余额是否充足
        can_submit, check_msg, info = manager.check_balance(
            SLURM_JOB_USER, actual_cost, SLURM_JOB_ACCOUNT
        )
        
        if not can_submit:
            print(f"[Epilog] ⚠ 余额不足: {check_msg}", file=sys.stderr)
            # 记录欠款，但不阻止作业完成
            print(f"[Epilog] ⚠ 记录欠费，请用户及时充值", file=sys.stderr)
            # 仍然扣费（允许欠费）
        
        # 执行直接扣费
        deduct_success, deduct_msg = manager.deduct_balance(
            SLURM_JOB_USER, actual_cost, SLURM_JOB_ACCOUNT, SLURM_JOB_ID
        )
        
        if deduct_success:
            print(f"[Epilog] ✓ 直接扣费成功: {deduct_msg}", file=sys.stderr)
            
            # 创建消费记录
            try:
                conn = sqlite3.connect('/var/lib/slurm-bill/billing.db')
                cursor = conn.cursor()
                now = datetime.now().isoformat()
                cursor.execute('''
                    INSERT OR IGNORE INTO consumption_records 
                    (job_id, user, account, estimated_cost, actual_cost, status, created_at, charged_at)
                    VALUES (?, ?, ?, ?, ?, 'charged', ?, ?)
                ''', (SLURM_JOB_ID, SLURM_JOB_USER, SLURM_JOB_ACCOUNT, 
                      str(actual_cost), str(actual_cost), now, now))
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"[Epilog] 创建消费记录失败: {e}", file=sys.stderr)
        else:
            print(f"[Epilog] ✗ 直接扣费失败: {deduct_msg}", file=sys.stderr)
    else:
        print(f"[Epilog] ✗ 扣费失败: {msg}", file=sys.stderr)
    
    # 检查余额预警
    try:
        balance = manager.get_or_create_balance(SLURM_JOB_USER, SLURM_JOB_ACCOUNT)
        if balance.balance < balance.alert_threshold:
            print(f"[Epilog] ⚠ 余额预警: 用户 {SLURM_JOB_USER} 余额 {balance.balance} 元，请及时充值", file=sys.stderr)
    except:
        pass
    
    return 0


if __name__ == '__main__':
    try:
        exit_code = main()
    except Exception as e:
        print(f"[Epilog] 致命错误: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        exit_code = 0
    
    sys.exit(exit_code)
