#!/usr/bin/env python3
"""
Slurm Prolog Script - 作业提交前检查余额

配置方法:
1. 将此脚本复制到 /etc/slurm/slurm_prolog.py
2. 在 slurm.conf 中添加:
   Prolog=/etc/slurm/slurm_prolog.py
3. 设置权限:
   chmod +x /etc/slurm/slurm_prolog.py
   chown root:root /etc/slurm/slurm_prolog.py

工作原理:
- Slurm在作业启动前调用此脚本
- 脚本检查用户余额是否充足
- 如果余额不足，返回非0退出码，阻止作业运行
"""

import os
import sys
import json
from datetime import datetime
from decimal import Decimal

# 确保能找到我们的模块
sys.path.insert(0, '/opt/slurm-bill')

from balance_manager import BalanceManager
from slurm_bill import BillingCalculator

# Slurm环境变量
SLURM_JOB_ID = os.environ.get('SLURM_JOB_ID', '0')
SLURM_JOB_USER = os.environ.get('SLURM_JOB_USER', '')
SLURM_JOB_ACCOUNT = os.environ.get('SLURM_JOB_ACCOUNT', 'default')
SLURM_JOB_PARTITION = os.environ.get('SLURM_JOB_PARTITION', 'normal')
SLURM_JOB_NNODES = int(os.environ.get('SLURM_JOB_NUM_NODES', '1'))
SLURM_JOB_NCPUS = int(os.environ.get('SLURM_JOB_CPUS_PER_NODE', '1'))

# 时间限制 - PrologSlurmctld 可能没有 SLURM_JOB_TIMELIMIT，尝试从其他变量计算
SLURM_JOB_TIMELIMIT = os.environ.get('SLURM_JOB_TIMELIMIT', '')
if not SLURM_JOB_TIMELIMIT:
    # 尝试从 START_TIME 和 END_TIME 计算
    try:
        start = int(os.environ.get('SLURM_JOB_START_TIME', '0'))
        end = int(os.environ.get('SLURM_JOB_END_TIME', '0'))
        if start > 0 and end > start:
            limit_seconds = end - start
            # 限制最大1小时，防止无限制作业导致费用过高
            # 用户如需更长时间，必须显式指定 --time
            if limit_seconds > 3600:  # 1小时
                limit_seconds = 3600
            hours = limit_seconds // 3600
            minutes = (limit_seconds % 3600) // 60
            SLURM_JOB_TIMELIMIT = f"{hours:02d}:{minutes:02d}:00"
        else:
            SLURM_JOB_TIMELIMIT = '01:00:00'  # 默认1小时
    except:
        SLURM_JOB_TIMELIMIT = '01:00:00'  # 默认1小时

def get_job_gpus(job_id: str) -> int:
    """使用 scontrol 获取作业的 GPU 请求数量"""
    try:
        import subprocess
        import re
        result = subprocess.run(
            ['scontrol', 'show', 'job', job_id],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            output = result.stdout
            # 方法1: 检查 ReqTRES/AllocTRES 中的 gres/gpu=N
            for line in output.split('\n'):
                if 'ReqTRES=' in line or 'AllocTRES=' in line:
                    match = re.search(r'gres/gpu=(\d+)', line)
                    if match:
                        return int(match.group(1))
            # 方法2: 检查 TresPerNode=gres/gpu:N
            for line in output.split('\n'):
                if 'TresPerNode=' in line and 'gpu' in line.lower():
                    match = re.search(r'gpu:(\d+)', line)
                    if match:
                        return int(match.group(1))
            # 方法3: 检查 GRES=gpu:N 格式
            for line in output.split('\n'):
                if 'GRES=' in line and 'gpu' in line.lower():
                    parts = line.split('GRES=')[1].split()[0]
                    if 'gpu:' in parts.lower():
                        gpu_parts = parts.split(':')
                        for i, part in enumerate(gpu_parts):
                            if part.lower() == 'gpu' and i + 1 < len(gpu_parts):
                                try:
                                    return int(gpu_parts[i + 1])
                                except ValueError:
                                    pass
    except Exception as e:
        print(f"[Prolog] 警告: 获取 GPU 信息失败: {e}", file=sys.stderr)
    return 0

# GPU信息（使用 scontrol 获取）
SLURM_JOB_GPUS = str(get_job_gpus(SLURM_JOB_ID))


def parse_time_to_hours(time_str: str) -> float:
    """将时间字符串转换为小时数"""
    try:
        # 处理格式: HH:MM:SS 或 DD-HH:MM:SS
        days = 0
        if '-' in time_str:
            day_part, time_str = time_str.split('-')
            days = int(day_part)
        
        parts = time_str.split(':')
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
        elif len(parts) == 2:
            hours, minutes = map(int, parts)
            seconds = 0
        else:
            return 1.0  # 默认1小时
        
        total_hours = days * 24 + hours + minutes / 60 + seconds / 3600
        return max(total_hours, 0.1)  # 至少0.1小时
    except:
        return 1.0  # 默认1小时


def estimate_job_cost() -> Decimal:
    """
    预估作业费用
    根据作业配置和资源请求估算费用
    """
    try:
        # 解析时间限制
        hours = parse_time_to_hours(SLURM_JOB_TIMELIMIT)
        
        # 预估内存使用（如果没有指定，假设每核2GB）
        mem_gb = SLURM_JOB_NCPUS * 2
        
        # GPU数量
        gpus = int(SLURM_JOB_GPUS)
        
        # 创建计费计算器获取费率
        calculator = BillingCalculator()
        rates = calculator.rate
        
        # 估算费用
        cpu_cost = Decimal(SLURM_JOB_NCPUS) * Decimal(str(hours)) * rates.cpu_per_hour
        mem_cost = Decimal(str(mem_gb)) * Decimal(str(hours)) * rates.memory_gb_per_hour
        gpu_cost = Decimal(gpus) * Decimal(str(hours)) * rates.gpu_per_hour
        
        # 应用分区倍率
        config = calculator.config
        partition_multiplier = Decimal(str(
            config.get('partitions', {}).get(SLURM_JOB_PARTITION, 1.0)
        ))
        
        total = (cpu_cost + mem_cost + gpu_cost) * partition_multiplier
        
        # 四舍五入
        total = total.quantize(Decimal('0.01'))
        
        return total
    except Exception as e:
        print(f"[Prolog] 费用估算错误: {e}", file=sys.stderr)
        # 如果估算失败，返回一个默认值（保守估计）
        return Decimal('1.00')


def main():
    """Prolog主函数"""
    # 记录所有环境变量到日志
    with open('/var/log/slurm-bill/prolog_debug.log', 'a') as f:
        f.write(f"\n=== Job {SLURM_JOB_ID} at {datetime.now().isoformat()} ===\n")
        for key, val in sorted(os.environ.items()):
            if key.startswith('SLURM'):
                f.write(f"{key}={val}\n")
    
    print(f"[Prolog] 检查作业 {SLURM_JOB_ID} 的余额...", file=sys.stderr)
    print(f"[Prolog] 用户: {SLURM_JOB_USER}, 账户: {SLURM_JOB_ACCOUNT}", file=sys.stderr)
    
    # 检查必要的环境变量
    if not SLURM_JOB_USER:
        print("[Prolog] 错误: 无法获取用户名", file=sys.stderr)
        return 1
    
    # 估算作业费用
    estimated_cost = estimate_job_cost()
    print(f"[Prolog] 预估费用: {estimated_cost} 元", file=sys.stderr)
    
    # 检查余额
    manager = BalanceManager()
    can_submit, msg, info = manager.check_balance(
        user=SLURM_JOB_USER,
        estimated_cost=estimated_cost,
        account=SLURM_JOB_ACCOUNT
    )
    
    if can_submit:
        print(f"[Prolog] ✓ {msg}", file=sys.stderr)
        
        # 预扣费用
        success, reserve_msg = manager.reserve_funds(
            job_id=SLURM_JOB_ID,
            user=SLURM_JOB_USER,
            estimated_cost=estimated_cost,
            account=SLURM_JOB_ACCOUNT
        )
        
        if success:
            print(f"[Prolog] ✓ {reserve_msg}", file=sys.stderr)
            
            # 输出余额信息到作业环境（供Epilog使用）
            print(f"BILLING_ESTIMATED_COST={estimated_cost}")
            print(f"BILLING_USER={SLURM_JOB_USER}")
            print(f"BILLING_ACCOUNT={SLURM_JOB_ACCOUNT}")
            
            return 0  # 允许作业运行
        else:
            print(f"[Prolog] ✗ 预扣费用失败: {reserve_msg}", file=sys.stderr)
            return 1
    else:
        print(f"[Prolog] ✗ 余额检查失败: {msg}", file=sys.stderr)
        print(f"[Prolog] 用户 {SLURM_JOB_USER} 余额不足，无法提交作业", file=sys.stderr)
        print(f"[Prolog] 请使用以下命令充值:", file=sys.stderr)
        print(f"[Prolog]   recharge user {SLURM_JOB_USER} --amount 100", file=sys.stderr)
        
        # 写入阻止信息到日志（忽略权限错误）
        try:
            with open('/var/log/slurm-bill/rejected_jobs.log', 'a') as f:
                f.write(f"{SLURM_JOB_ID}|{SLURM_JOB_USER}|{SLURM_JOB_ACCOUNT}|"
                       f"{estimated_cost}|{info.get('balance', 0)}|{datetime.now().isoformat()}\n")
        except:
            pass
        
        return 1  # 阻止作业运行


if __name__ == '__main__':
    # 捕获所有异常并记录到文件
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        import traceback
        with open('/var/log/slurm-bill/prolog_crash.log', 'a') as f:
            f.write(f"\n=== Prolog Crash {SLURM_JOB_ID} at {datetime.now().isoformat()} ===\n")
            f.write(f"Error: {e}\n")
            f.write(traceback.format_exc())
        sys.exit(1)
