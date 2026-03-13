#!/usr/bin/env python3
"""
Slurm Billing - 充值管理CLI工具

用法:
  recharge user <username> --amount 100              # 给用户充值
  recharge account <account> --amount 1000           # 给账户充值
  recharge query <username>                          # 查询余额
  recharge history <username>                        # 查询充值记录
  recharge list                                      # 列出所有余额
  recharge suspend <username>                        # 暂停用户
  recharge activate <username>                       # 激活用户
  recharge set-credit <username> --amount 50         # 设置信用额度
  recharge set-alert <username> --amount 10          # 设置预警阈值
"""

import sys
import subprocess
import argparse
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

# 开发环境下优先从当前目录加载，生产环境使用 /opt/slurm-bill
import os
_current_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.exists(os.path.join(_current_dir, 'balance_manager.py')):
    sys.path.insert(0, _current_dir)
else:
    sys.path.insert(0, '/opt/slurm-bill')

from balance_manager import BalanceManager, get_slurm_default_account
from prettytable import PrettyTable


def print_balance_table(balances):
    """打印余额表格"""
    if not balances:
        print("没有余额记录")
        return
    
    table = PrettyTable()
    table.field_names = ["用户", "账户", "余额", "信用额度", "累计充值", "累计消费", "状态"]
    table.align["用户"] = "l"
    table.align["账户"] = "l"
    
    for b in balances:
        table.add_row([
            b['user'],
            b['account'],
            f"{Decimal(b['balance']):.2f}",
            f"{Decimal(b['credit_limit']):.2f}",
            f"{Decimal(b['total_recharged']):.2f}",
            f"{Decimal(b['total_consumed']):.2f}",
            b['status']
        ])
    
    print(table)


def print_recharge_history(records):
    """打印充值记录"""
    if not records:
        print("没有充值记录")
        return
    
    table = PrettyTable()
    table.field_names = ["ID", "用户", "账户", "金额", "充值后余额", "类型", "操作人", "时间"]
    table.align["用户"] = "l"
    
    for r in records:
        table.add_row([
            r['id'],
            r['user'],
            r['account'],
            f"{Decimal(r['amount']):.2f}",
            f"{Decimal(r['balance_after']):.2f}",
            r['recharge_type'],
            r['operator'],
            r['created_at']
        ])
    
    print(table)


def cmd_recharge_user(args):
    """给用户充值"""
    manager = BalanceManager()
    
    # 如果账户是default，尝试从Slurm获取默认账户
    account = args.account
    if account == 'default':
        slurm_account = get_slurm_default_account(args.username)
        if slurm_account:
            account = slurm_account
            print(f"[Info] 使用Slurm默认账户: {account}")
    
    success, msg = manager.recharge(
        user=args.username,
        amount=Decimal(str(args.amount)),
        account=account,
        recharge_type=args.type,
        operator=args.operator,
        remark=args.remark
    )
    
    if success:
        print(f"✓ {msg}")
    else:
        print(f"✗ {msg}")
        return 1
    
    return 0


def cmd_recharge_account(args):
    """给账户下所有用户充值（平均分配）"""
    manager = BalanceManager()
    
    # 获取该账户下的所有用户
    all_balances = manager.get_all_balances()
    account_users = [b for b in all_balances if b['account'] == args.account]
    
    if not account_users:
        print(f"✗ 账户 {args.account} 下没有用户")
        return 1
    
    # 计算每人分配的金额
    total_amount = Decimal(str(args.amount))
    user_count = len(account_users)
    amount_per_user = (total_amount / user_count).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    print(f"账户充值功能：给账户 {args.account} 下的所有用户平均分配充值金额")
    print(f"总金额: {total_amount} 元, 用户数量: {user_count}, 每人分配: {amount_per_user} 元")
    print()
    
    # 给每个用户充值
    success_count = 0
    for user_balance in account_users:
        user = user_balance['user']
        success, msg = manager.recharge(
            user=user,
            amount=amount_per_user,
            account=args.account,
            recharge_type='account_recharge',
            operator='admin',
            remark=f'账户 {args.account} 充值分配'
        )
        if success:
            print(f"✓ {user}: {msg}")
            success_count += 1
        else:
            print(f"✗ {user}: {msg}")
    
    print()
    print(f"充值完成: {success_count}/{user_count} 个用户成功")
    return 0 if success_count == user_count else 1


def cmd_query(args):
    """查询余额"""
    manager = BalanceManager()
    
    if args.username:
        # 查询特定用户
        balance = manager.get_or_create_balance(args.username, args.account)
        print(f"\n用户: {balance.user}")
        print(f"账户: {balance.account}")
        print(f"当前余额: {balance.balance} 元")
        print(f"信用额度: {balance.credit_limit} 元")
        print(f"可用额度: {balance.balance + balance.credit_limit} 元")
        print(f"累计充值: {balance.total_recharged} 元")
        print(f"累计消费: {balance.total_consumed} 元")
        print(f"账户状态: {balance.status}")
        print(f"最后更新: {balance.last_updated}")
        
        # 获取最近的充值记录
        history = manager.get_recharge_history(args.username, args.account, limit=5)
        if history:
            print(f"\n最近充值记录:")
            print_recharge_history(history)
    else:
        # 查询所有
        balances = manager.get_all_balances()
        print(f"\n所有用户余额 ({len(balances)} 个):\n")
        print_balance_table(balances)
    
    return 0


def cmd_history(args):
    """查询充值历史"""
    manager = BalanceManager()
    
    # 确定要查询的账户
    account = args.account
    if account == 'default':
        if args.username:
            # 指定了用户，尝试从Slurm获取默认账户
            slurm_account = get_slurm_default_account(args.username)
            if slurm_account:
                account = slurm_account
            else:
                # 如果获取失败，查询该用户的所有充值记录（不限制账户）
                account = None
        else:
            # 未指定用户，查询所有用户的所有账户
            account = None
    
    records = manager.get_recharge_history(
        user=args.username,
        account=account,
        limit=args.limit
    )
    
    print(f"\n充值记录 ({len(records)} 条):\n")
    print_recharge_history(records)
    
    return 0


def cmd_list(args):
    """列出所有余额"""
    manager = BalanceManager()
    balances = manager.get_all_balances()
    
    print(f"\n余额列表 ({len(balances)} 个):\n")
    print_balance_table(balances)
    
    # 统计信息
    from decimal import Decimal
    total_balance = sum(Decimal(b['balance']) for b in balances)
    total_recharged = sum(Decimal(b['total_recharged']) for b in balances)
    total_consumed = sum(Decimal(b['total_consumed']) for b in balances)
    
    print(f"\n统计:")
    print(f"  总余额: {total_balance:.2f} 元")
    print(f"  累计充值: {total_recharged:.2f} 元")
    print(f"  累计消费: {total_consumed:.2f} 元")
    
    return 0


def cmd_suspend(args):
    """暂停用户"""
    manager = BalanceManager()
    success, msg = manager.suspend_user(args.username, args.account)
    print(f"{'✓' if success else '✗'} {msg}")
    return 0 if success else 1


def cmd_activate(args):
    """激活用户"""
    manager = BalanceManager()
    success, msg = manager.activate_user(args.username, args.account)
    print(f"{'✓' if success else '✗'} {msg}")
    return 0 if success else 1


def cmd_set_credit(args):
    """设置信用额度"""
    manager = BalanceManager()
    success, msg = manager.set_credit_limit(
        args.username, 
        Decimal(str(args.amount)),
        args.account
    )
    print(f"{'✓' if success else '✗'} {msg}")
    return 0 if success else 1


def cmd_set_alert(args):
    """设置预警阈值"""
    manager = BalanceManager()
    success, msg = manager.set_alert_threshold(
        args.username,
        Decimal(str(args.amount)),
        args.account
    )
    print(f"{'✓' if success else '✗'} {msg}")
    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(
        description='Slurm Billing 充值管理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  # 给用户充值 100 元
  recharge user amax --amount 100

  # 查询用户余额
  recharge query amax

  # 列出所有余额
  recharge list

  # 设置信用额度 50 元（允许欠费50元）
  recharge set-credit amax --amount 50

  # 设置余额预警 10 元（余额低于10元时预警）
  recharge set-alert amax --amount 10
        '''
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # recharge user
    user_parser = subparsers.add_parser('user', help='给用户充值')
    user_parser.add_argument('username', help='用户名')
    user_parser.add_argument('--amount', '-a', type=float, required=True, help='充值金额')
    user_parser.add_argument('--account', default='default', help='账户名（默认自动从Slurm获取用户默认账户）')
    user_parser.add_argument('--type', default='cash', choices=['cash', 'transfer', 'grant', 'adjustment'], help='充值类型')
    user_parser.add_argument('--operator', '-o', default='admin', help='操作人')
    user_parser.add_argument('--remark', '-r', default='', help='备注')
    user_parser.set_defaults(func=cmd_recharge_user)
    
    # recharge account
    account_parser = subparsers.add_parser('account', help='给账户充值')
    account_parser.add_argument('account', help='账户名')
    account_parser.add_argument('--amount', '-a', type=float, required=True, help='充值金额')
    account_parser.set_defaults(func=cmd_recharge_account)
    
    # recharge query
    query_parser = subparsers.add_parser('query', help='查询余额')
    query_parser.add_argument('username', nargs='?', help='用户名（不指定则查询所有）')
    query_parser.add_argument('--account', default='default', help='账户名')
    query_parser.set_defaults(func=cmd_query)
    
    # recharge history
    history_parser = subparsers.add_parser('history', help='查询充值历史')
    history_parser.add_argument('username', nargs='?', help='用户名')
    history_parser.add_argument('--account', default='default', help='账户名')
    history_parser.add_argument('--limit', '-l', type=int, default=50, help='限制条数')
    history_parser.set_defaults(func=cmd_history)
    
    # recharge list
    list_parser = subparsers.add_parser('list', help='列出所有余额')
    list_parser.set_defaults(func=cmd_list)
    
    # recharge suspend
    suspend_parser = subparsers.add_parser('suspend', help='暂停用户')
    suspend_parser.add_argument('username', help='用户名')
    suspend_parser.add_argument('--account', default='default', help='账户名')
    suspend_parser.set_defaults(func=cmd_suspend)
    
    # recharge activate
    activate_parser = subparsers.add_parser('activate', help='激活用户')
    activate_parser.add_argument('username', help='用户名')
    activate_parser.add_argument('--account', default='default', help='账户名')
    activate_parser.set_defaults(func=cmd_activate)
    
    # recharge set-credit
    credit_parser = subparsers.add_parser('set-credit', help='设置信用额度')
    credit_parser.add_argument('username', help='用户名')
    credit_parser.add_argument('--amount', '-a', type=float, required=True, help='信用额度')
    credit_parser.add_argument('--account', default='default', help='账户名')
    credit_parser.set_defaults(func=cmd_set_credit)
    
    # recharge set-alert
    alert_parser = subparsers.add_parser('set-alert', help='设置预警阈值')
    alert_parser.add_argument('username', help='用户名')
    alert_parser.add_argument('--amount', '-a', type=float, required=True, help='预警阈值')
    alert_parser.add_argument('--account', default='default', help='账户名')
    alert_parser.set_defaults(func=cmd_set_alert)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
