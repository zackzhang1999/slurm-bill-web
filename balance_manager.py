#!/usr/bin/env python3
"""
Slurm Billing - 余额管理模块
管理用户/账户的充值、消费、余额查询
"""

import os
import sys
import sqlite3
import json
import subprocess
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass

DB_PATH = '/var/lib/slurm-bill/billing.db'


def get_slurm_default_account(user: str) -> Optional[str]:
    """
    从Slurm获取用户的默认账户
    
    Args:
        user: 用户名
    
    Returns:
        默认账户名，如果获取失败返回None
    """
    try:
        result = subprocess.run(
            ['sacctmgr', 'show', 'user', user, 'format=defaultaccount', '--noheader'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            account = result.stdout.strip()
            if account:
                return account
    except Exception as e:
        print(f"[Warning] 获取Slurm默认账户失败: {e}")
    
    return None


@dataclass
class BalanceRecord:
    """余额记录"""
    user: str
    account: str
    balance: Decimal
    credit_limit: Decimal  # 信用额度（允许欠费额度）
    total_recharged: Decimal
    total_consumed: Decimal
    alert_threshold: Decimal = None  # 预警阈值
    last_updated: str = None
    status: str = 'active'  # active, suspended, frozen
    
    def __post_init__(self):
        if self.alert_threshold is None:
            self.alert_threshold = Decimal('0.00')
        if self.last_updated is None:
            self.last_updated = datetime.now().isoformat()


@dataclass
class RechargeRecord:
    """充值记录"""
    id: int
    user: str
    account: str
    amount: Decimal
    recharge_type: str  # cash, transfer, grant, adjustment
    operator: str
    remark: str
    created_at: str


@dataclass
class ConsumptionRecord:
    """消费记录（实时扣费记录）"""
    id: int
    job_id: str
    user: str
    account: str
    estimated_cost: Decimal  # 预估费用（提交时）
    actual_cost: Decimal     # 实际费用（作业结束时）
    status: str              # reserved(预扣), charged(已扣), refunded(已退)
    created_at: str
    charged_at: Optional[str]


class BalanceManager:
    """余额管理器"""
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_tables()
    
    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_tables(self):
        """初始化余额相关表"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 账户余额表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS account_balance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user TEXT NOT NULL,
                    account TEXT NOT NULL,
                    balance TEXT DEFAULT '0.00',           -- 当前余额
                    credit_limit TEXT DEFAULT '0.00',      -- 信用额度（允许欠费额度）
                    total_recharged TEXT DEFAULT '0.00',   -- 累计充值
                    total_consumed TEXT DEFAULT '0.00',    -- 累计消费
                    alert_threshold TEXT DEFAULT '0.00',   -- 预警阈值
                    status TEXT DEFAULT 'active',          -- active, suspended, frozen
                    last_updated TEXT,
                    UNIQUE(user, account)
                )
            ''')
            
            # 充值记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS recharge_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user TEXT NOT NULL,
                    account TEXT NOT NULL,
                    amount TEXT NOT NULL,                  -- 充值金额
                    balance_after TEXT NOT NULL,           -- 充值后余额
                    recharge_type TEXT DEFAULT 'cash',     -- cash, transfer, grant, adjustment
                    operator TEXT,                         -- 操作人
                    remark TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 实时消费记录表（用于预付费扣费）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS consumption_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL UNIQUE,
                    user TEXT NOT NULL,
                    account TEXT NOT NULL,
                    estimated_cost TEXT DEFAULT '0.00',    -- 预估费用
                    actual_cost TEXT DEFAULT '0.00',       -- 实际费用
                    status TEXT DEFAULT 'reserved',        -- reserved, charged, refunded, failed
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    charged_at TEXT
                )
            ''')
            
            # 创建索引
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_balance_user ON account_balance(user)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_balance_account ON account_balance(account)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_recharge_user ON recharge_records(user)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_consumption_job ON consumption_records(job_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_consumption_status ON consumption_records(status)
            ''')
            
            conn.commit()
            print("[Balance] 余额管理表初始化完成")
    
    def get_or_create_balance(self, user: str, account: str = 'default') -> BalanceRecord:
        """获取或创建余额记录"""
        # 如果account是default，尝试从Slurm获取默认账户
        resolved_account = account
        if account == 'default':
            slurm_account = get_slurm_default_account(user)
            if slurm_account:
                resolved_account = slurm_account
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 首先尝试精确匹配用户和解析后的账户名
            cursor.execute('''
                SELECT * FROM account_balance 
                WHERE user = ? AND account = ?
            ''', (user, resolved_account))
            
            row = cursor.fetchone()
            
            if row:
                return BalanceRecord(
                    user=row['user'],
                    account=row['account'],
                    balance=Decimal(row['balance']),
                    credit_limit=Decimal(row['credit_limit']),
                    total_recharged=Decimal(row['total_recharged']),
                    total_consumed=Decimal(row['total_consumed']),
                    alert_threshold=Decimal(row['alert_threshold']) if row['alert_threshold'] else Decimal('0.00'),
                    last_updated=row['last_updated'],
                    status=row['status']
                )
            
            # 如果没有精确匹配，尝试查询用户的任何账户记录
            cursor.execute('''
                SELECT * FROM account_balance 
                WHERE user = ?
            ''', (user,))
            
            rows = cursor.fetchall()
            
            # 如果找到记录，返回第一个（通常是默认账户）
            if rows:
                row = rows[0]
                return BalanceRecord(
                    user=row['user'],
                    account=row['account'],
                    balance=Decimal(row['balance']),
                    credit_limit=Decimal(row['credit_limit']),
                    total_recharged=Decimal(row['total_recharged']),
                    total_consumed=Decimal(row['total_consumed']),
                    alert_threshold=Decimal(row['alert_threshold']) if row['alert_threshold'] else Decimal('0.00'),
                    last_updated=row['last_updated'],
                    status=row['status']
                )
            
            # 没有记录，创建新记录（使用解析后的账户名）
            now = datetime.now().isoformat()
            cursor.execute('''
                INSERT INTO account_balance 
                (user, account, balance, credit_limit, total_recharged, total_consumed, alert_threshold, last_updated, status)
                VALUES (?, ?, '0.00', '0.00', '0.00', '0.00', '0.00', ?, 'active')
            ''', (user, resolved_account, now))
            conn.commit()
            
            return BalanceRecord(
                user=user,
                account=resolved_account,
                balance=Decimal('0.00'),
                credit_limit=Decimal('0.00'),
                total_recharged=Decimal('0.00'),
                total_consumed=Decimal('0.00'),
                last_updated=now,
                status='active'
            )
    
    def recharge(self, user: str, amount: Decimal, 
                 account: str = 'default',
                 recharge_type: str = 'cash',
                 operator: str = 'system',
                 remark: str = '') -> Tuple[bool, str]:
        """
        为用户充值
        
        Args:
            user: 用户名
            amount: 充值金额（必须为正数）
            account: 账户名
            recharge_type: 充值类型 (cash, transfer, grant, adjustment)
            operator: 操作人
            remark: 备注
        
        Returns:
            (是否成功, 消息)
        """
        if amount <= 0:
            return False, "充值金额必须大于0"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            # 获取当前余额，并获取实际账户名（如果是default会被解析）
            balance_record = self.get_or_create_balance(user, account)
            actual_account = balance_record.account
            new_balance = balance_record.balance + amount
            new_total_recharged = balance_record.total_recharged + amount
            
            # 更新余额表
            cursor.execute('''
                UPDATE account_balance 
                SET balance = ?, 
                    total_recharged = ?,
                    last_updated = ?,
                    status = 'active'
                WHERE user = ? AND account = ?
            ''', (str(new_balance), str(new_total_recharged), now, user, actual_account))
            
            # 记录充值日志
            cursor.execute('''
                INSERT INTO recharge_records 
                (user, account, amount, balance_after, recharge_type, operator, remark, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user, actual_account, str(amount), str(new_balance), 
                  recharge_type, operator, remark, now))
            
            conn.commit()
            
            return True, f"充值成功！用户 {user} 充值 {amount} 元，当前余额 {new_balance} 元"
    
    def deduct_balance(self, user: str, amount: Decimal, 
                       account: str = 'default',
                       job_id: str = None) -> Tuple[bool, str]:
        """
        从用户余额中扣费
        
        Args:
            user: 用户名
            amount: 扣费金额
            account: 账户名
            job_id: 关联的作业ID（可选）
        
        Returns:
            (是否成功, 消息)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            # 获取当前余额，并获取实际账户名（如果是default会被解析）
            balance_record = self.get_or_create_balance(user, account)
            actual_account = balance_record.account
            
            # 检查余额（考虑信用额度）
            available = balance_record.balance + balance_record.credit_limit
            if available < amount:
                return False, f"余额不足！当前余额 {balance_record.balance} 元，需要 {amount} 元"
            
            # 执行扣费
            new_balance = balance_record.balance - amount
            new_total_consumed = balance_record.total_consumed + amount
            
            cursor.execute('''
                UPDATE account_balance 
                SET balance = ?,
                    total_consumed = ?,
                    last_updated = ?
                WHERE user = ? AND account = ?
            ''', (str(new_balance), str(new_total_consumed), now, user, actual_account))
            
            conn.commit()
            
            return True, f"扣费成功！扣除 {amount} 元，当前余额 {new_balance} 元"
    
    def check_balance(self, user: str, estimated_cost: Decimal,
                      account: str = 'default') -> Tuple[bool, str, Dict]:
        """
        检查余额是否足够提交作业
        
        Args:
            user: 用户名
            estimated_cost: 预估费用
            account: 账户名
        
        Returns:
            (是否可提交, 消息, 余额信息)
        """
        balance_record = self.get_or_create_balance(user, account)
        
        available = balance_record.balance + balance_record.credit_limit
        
        info = {
            'user': user,
            'account': account,
            'balance': float(balance_record.balance),
            'credit_limit': float(balance_record.credit_limit),
            'available': float(available),
            'estimated_cost': float(estimated_cost),
            'status': balance_record.status
        }
        
        # 检查账户状态
        if balance_record.status == 'suspended':
            return False, "账户已被暂停，请联系管理员", info
        if balance_record.status == 'frozen':
            return False, "账户已被冻结，请联系管理员", info
        
        # 检查余额
        if available < estimated_cost:
            msg = (f"余额不足！当前余额 {balance_record.balance} 元，"
                   f"预估费用 {estimated_cost} 元，"
                   f"还需充值 {estimated_cost - available} 元")
            return False, msg, info
        
        # 检查是否需要预警
        if balance_record.balance < Decimal(str(balance_record.alert_threshold)):
            msg = f"余额预警：当前余额 {balance_record.balance} 元，请及时充值"
            return True, msg, info
        
        return True, f"余额充足！当前余额 {balance_record.balance} 元", info
    
    def reserve_funds(self, job_id: str, user: str, 
                      estimated_cost: Decimal,
                      account: str = 'default') -> Tuple[bool, str]:
        """
        预扣费用（作业提交时调用）
        
        Args:
            job_id: 作业ID
            user: 用户名
            estimated_cost: 预估费用
            account: 账户名
        
        Returns:
            (是否成功, 消息)
        """
        # 先检查余额
        can_submit, msg, info = self.check_balance(user, estimated_cost, account)
        if not can_submit:
            return False, msg
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            # 检查是否已存在该作业的记录
            cursor.execute('''
                SELECT * FROM consumption_records WHERE job_id = ?
            ''', (job_id,))
            if cursor.fetchone():
                return True, "该作业已预扣费用"
            
            # 创建消费记录
            cursor.execute('''
                INSERT INTO consumption_records 
                (job_id, user, account, estimated_cost, actual_cost, status, created_at)
                VALUES (?, ?, ?, ?, '0.00', 'reserved', ?)
            ''', (job_id, user, account, str(estimated_cost), now))
            
            conn.commit()
            
            return True, f"预扣费用成功！预估费用 {estimated_cost} 元"
    
    def charge_job(self, job_id: str, actual_cost: Decimal) -> Tuple[bool, str]:
        """
        实际扣费（作业结束时调用）
        
        Args:
            job_id: 作业ID
            actual_cost: 实际费用
        
        Returns:
            (是否成功, 消息)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            # 获取消费记录
            cursor.execute('''
                SELECT * FROM consumption_records WHERE job_id = ?
            ''', (job_id,))
            record = cursor.fetchone()
            
            if not record:
                return False, f"未找到作业 {job_id} 的预扣记录"
            
            if record['status'] == 'charged':
                return True, "该作业已扣费"
            
            user = record['user']
            account = record['account']
            estimated = Decimal(record['estimated_cost'])
            
            # 执行实际扣费
            success, msg = self.deduct_balance(user, actual_cost, account, job_id)
            if not success:
                # 扣费失败，标记为failed
                cursor.execute('''
                    UPDATE consumption_records 
                    SET status = 'failed', charged_at = ?
                    WHERE job_id = ?
                ''', (now, job_id))
                conn.commit()
                return False, f"扣费失败: {msg}"
            
            # 更新消费记录
            cursor.execute('''
                UPDATE consumption_records 
                SET actual_cost = ?, status = 'charged', charged_at = ?
                WHERE job_id = ?
            ''', (str(actual_cost), now, job_id))
            
            conn.commit()
            
            # 计算差异
            diff = actual_cost - estimated
            if diff > 0:
                return True, f"扣费成功！实际费用 {actual_cost} 元（比预估多 {diff} 元）"
            elif diff < 0:
                return True, f"扣费成功！实际费用 {actual_cost} 元（比预估少 {abs(diff)} 元）"
            else:
                return True, f"扣费成功！实际费用 {actual_cost} 元"
    
    def get_recharge_history(self, user: str = None, account: str = None,
                            limit: int = 50) -> List[Dict]:
        """获取充值记录"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            sql = '''
                SELECT * FROM recharge_records 
                WHERE 1=1
            '''
            params = []
            
            if user:
                sql += " AND user = ?"
                params.append(user)
            if account:
                sql += " AND account = ?"
                params.append(account)
            
            sql += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_balances(self) -> List[Dict]:
        """获取所有用户的余额"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM account_balance 
                ORDER BY balance DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]
    
    def set_credit_limit(self, user: str, credit_limit: Decimal,
                         account: str = 'default') -> Tuple[bool, str]:
        """设置信用额度"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            # 确保记录存在，并获取实际账户名（如果是default会被解析）
            balance_record = self.get_or_create_balance(user, account)
            actual_account = balance_record.account
            
            cursor.execute('''
                UPDATE account_balance 
                SET credit_limit = ?, last_updated = ?
                WHERE user = ? AND account = ?
            ''', (str(credit_limit), now, user, actual_account))
            
            conn.commit()
            
            return True, f"设置成功！用户 {user} 信用额度为 {credit_limit} 元"
    
    def set_alert_threshold(self, user: str, threshold: Decimal,
                           account: str = 'default') -> Tuple[bool, str]:
        """设置余额预警阈值"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 确保记录存在，并获取实际账户名（如果是default会被解析）
            balance_record = self.get_or_create_balance(user, account)
            actual_account = balance_record.account
            
            cursor.execute('''
                UPDATE account_balance 
                SET alert_threshold = ?
                WHERE user = ? AND account = ?
            ''', (str(threshold), user, actual_account))
            
            conn.commit()
            
            return True, f"设置成功！用户 {user} 余额预警阈值为 {threshold} 元"
    
    def suspend_user(self, user: str, account: str = 'default') -> Tuple[bool, str]:
        """暂停用户账户"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 获取实际账户名（如果是default会被解析）
            balance_record = self.get_or_create_balance(user, account)
            actual_account = balance_record.account
            
            cursor.execute('''
                UPDATE account_balance 
                SET status = 'suspended'
                WHERE user = ? AND account = ?
            ''', (user, actual_account))
            
            conn.commit()
            
            return True, f"用户 {user} 账户已暂停"
    
    def activate_user(self, user: str, account: str = 'default') -> Tuple[bool, str]:
        """激活用户账户"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 获取实际账户名（如果是default会被解析）
            balance_record = self.get_or_create_balance(user, account)
            actual_account = balance_record.account
            
            cursor.execute('''
                UPDATE account_balance 
                SET status = 'active'
                WHERE user = ? AND account = ?
            ''', (user, actual_account))
            
            conn.commit()
            
            return True, f"用户 {user} 账户已激活"


def main():
    """测试接口"""
    manager = BalanceManager()
    
    # 测试充值
    print("="*60)
    print("余额管理系统测试")
    print("="*60)
    
    # 给用户 amax 充值 200 元
    success, msg = manager.recharge(
        user='amax',
        amount=Decimal('200.00'),
        operator='admin',
        remark='首次充值'
    )
    print(f"\n[充值] {msg}")
    
    # 查询余额
    balance = manager.get_or_create_balance('amax')
    print(f"\n[余额] 用户: {balance.user}")
    print(f"       当前余额: {balance.balance} 元")
    print(f"       累计充值: {balance.total_recharged} 元")
    print(f"       累计消费: {balance.total_consumed} 元")
    print(f"       状态: {balance.status}")
    
    # 检查余额（模拟提交一个预估10元的作业）
    can_submit, msg, info = manager.check_balance('amax', Decimal('10.00'))
    print(f"\n[检查] {msg}")
    
    # 模拟预扣费用
    success, msg = manager.reserve_funds('12345', 'amax', Decimal('10.00'))
    print(f"\n[预扣] {msg}")
    
    # 模拟实际扣费
    success, msg = manager.charge_job('12345', Decimal('12.50'))
    print(f"\n[扣费] {msg}")
    
    # 再次查询余额
    balance = manager.get_or_create_balance('amax')
    print(f"\n[余额] 当前余额: {balance.balance} 元")
    
    print("\n" + "="*60)


if __name__ == '__main__':
    main()
