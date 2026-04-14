#!/usr/bin/env python3
"""
Slurm Billing Web Application - Slurm计费系统Web管理界面
精美的现代化Web界面，提供完整的计费管理功能
支持管理员和普通用户两种角色，普通用户需要密码验证

作者: Assistant
版本: 2.2.0
"""

import os
import sys
import sqlite3
import json
import yaml
import subprocess
import re
from datetime import datetime, timedelta
from decimal import Decimal
from functools import wraps
from pathlib import Path

from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, session, abort
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash

# 添加父目录到路径以导入现有模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from balance_manager import BalanceManager, get_slurm_default_account

app = Flask(__name__)
app.secret_key = os.urandom(24)

# 配置
CONFIG_PATH = '/etc/slurm-bill/config.yaml'
DB_PATH = '/var/lib/slurm-bill/billing.db'

# 确保使用真实系统数据库
if not os.path.exists(DB_PATH):
    local_db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'billing.db')
    if os.path.exists(local_db):
        DB_PATH = local_db
        print(f"[Web] 使用本地数据库: {DB_PATH}")
    else:
        print(f"[Web] 警告: 数据库不存在: {DB_PATH}")
else:
    print(f"[Web] 使用系统数据库: {DB_PATH}")

# 登录管理
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==================== 辅助函数 ====================

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_job_status_from_jobinfo(job_id):
    """
    使用 jobinfo 命令获取作业状态
    
    Args:
        job_id: 作业ID
    
    Returns:
        dict: 包含作业状态信息的字典，如果获取失败返回 None
    """
    try:
        result = subprocess.run(
            ['jobinfo', str(job_id)],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            # jobinfo 返回错误，可能是作业不存在
            return None
        
        output = result.stdout
        job_info = {}
        
        # 解析 jobinfo 输出
        for line in output.split('\n'):
            line = line.strip()
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                job_info[key] = value
        
        # 提取状态信息
        state = job_info.get('State', '')
        # 状态可能包含更多信息，如 "COMPLETED" 或 "CANCELLED by 0"
        # 只取第一个单词作为主要状态
        if state:
            state = state.split()[0]
        
        # 解析 CPU 数量 (Cores 字段)
        ncpus = job_info.get('Cores')
        if ncpus:
            try:
                ncpus = int(ncpus)
            except (ValueError, TypeError):
                ncpus = None
        
        # 解析 GPU 数量
        alloc_gpus = job_info.get('GPUs')
        if alloc_gpus:
            try:
                alloc_gpus = int(alloc_gpus)
            except (ValueError, TypeError):
                alloc_gpus = 0
        else:
            alloc_gpus = 0
        
        # 解析节点数
        nnodes = job_info.get('Nodes')
        if nnodes and nnodes != 'None assigned':
            try:
                nnodes = int(nnodes)
            except (ValueError, TypeError):
                nnodes = 1
        else:
            nnodes = 1
        
        return {
            'job_id': job_id,
            'state': state,
            'user': job_info.get('User'),
            'partition': job_info.get('Partition'),
            'account': job_info.get('Account'),
            'ncpus': ncpus,
            'nnodes': nnodes,
            'alloc_gpus': alloc_gpus,
            'start_time': job_info.get('Start') if job_info.get('Start') != 'None' else None,
            'end_time': job_info.get('End') if job_info.get('End') != 'None' else None,
            'elapsed': job_info.get('Used walltime'),
            'job_name': job_info.get('Name'),
            'exit_code': job_info.get('ExitCode'),
            'submit_time': job_info.get('Submit'),
            'waited': job_info.get('Waited'),
            'reserved_walltime': job_info.get('Reserved walltime'),
            'used_cpu_time': job_info.get('Used CPU time'),
            'max_mem_used': job_info.get('Max Mem used'),
            'raw_info': job_info
        }
    except subprocess.TimeoutExpired:
        print(f"[jobinfo] 超时: job_id={job_id}")
        return None
    except Exception as e:
        print(f"[jobinfo] 错误: job_id={job_id}, error={e}")
        return None

def init_user_passwords_table():
    """初始化用户密码表"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 创建用户密码表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_passwords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_default INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_passwords_user ON user_passwords(user)')
        
        # 为新用户初始化默认密码
        default_pwd_hash = generate_password_hash('123456')
        
        # 查找没有密码记录的用户
        cursor.execute('''
            SELECT DISTINCT user FROM job_records
            WHERE user NOT IN (SELECT user FROM user_passwords)
        ''')
        new_users = cursor.fetchall()
        
        for (user,) in new_users:
            cursor.execute('''
                INSERT INTO user_passwords (user, password_hash, is_default)
                VALUES (?, ?, 1)
            ''', (user, default_pwd_hash))
            print(f"[Web] 为新用户 {user} 初始化默认密码")
        
        conn.commit()
        conn.close()
        print("[Web] 用户密码表初始化完成")
    except Exception as e:
        print(f"[Web] 初始化密码表失败: {e}")

def verify_user_password(username, password):
    """验证用户密码"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT password_hash FROM user_passwords WHERE user = ?', (username,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return check_password_hash(row['password_hash'], password)
        return False
    except Exception as e:
        print(f"验证密码失败: {e}")
        return False

def set_user_password(username, password, is_default=False):
    """设置用户密码"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        password_hash = generate_password_hash(password)
        
        cursor.execute('''
            INSERT INTO user_passwords (user, password_hash, is_default, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user) DO UPDATE SET
                password_hash = excluded.password_hash,
                is_default = excluded.is_default,
                updated_at = excluded.updated_at
        ''', (username, password_hash, 1 if is_default else 0, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"设置密码失败: {e}")
        return False

def is_default_password(username):
    """检查是否是默认密码"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT is_default FROM user_passwords WHERE user = ?', (username,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return row['is_default'] == 1
        return True
    except:
        return True

def load_config():
    """加载配置文件"""
    default_config = {
        'billing': {
            'cpu_per_hour': 0.10,
            'memory_gb_per_hour': 0.02,
            'gpu_per_hour': 2.00,
            'currency': 'CNY'
        },
        'web': {
            'auth': {
                'admin_password': 'changeme'
            }
        }
    }
    
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r') as f:
                config = yaml.safe_load(f)
                if config:
                    default_config.update(config)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
    
    return default_config

config = load_config()

# ==================== 用户认证 ====================

class User(UserMixin):
    """用户类，支持管理员和普通用户"""
    def __init__(self, id, username, is_admin=False, need_change_password=False):
        self.id = id
        self.username = username
        self.is_admin = is_admin
        self.need_change_password = need_change_password
    
    def get_id(self):
        return self.id
    
    def is_authenticated(self):
        return True

@login_manager.user_loader
def load_user(user_id):
    """加载用户"""
    if user_id == 'admin':
        return User('admin', 'admin', is_admin=True)
    else:
        # 检查用户是否存在
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM job_records WHERE user = ? LIMIT 1', (user_id,))
            has_jobs = cursor.fetchone() is not None
            cursor.execute('SELECT 1 FROM account_balance WHERE user = ? LIMIT 1', (user_id,))
            has_balance = cursor.fetchone() is not None
            conn.close()
            
            if has_jobs or has_balance:
                need_change = is_default_password(user_id)
                return User(user_id, user_id, is_admin=False, need_change_password=need_change)
        except:
            pass
    return None

def admin_required(f):
    """管理员权限装饰器"""
    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.is_admin:
            flash('需要管理员权限', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
def login():
    """登录页面"""
    if request.method == 'POST':
        login_type = request.form.get('login_type', 'user')
        
        if login_type == 'admin':
            # 管理员登录
            password = request.form.get('password')
            admin_password = config.get('web', {}).get('auth', {}).get('admin_password', 'changeme')
            
            if password == admin_password:
                user = User('admin', 'admin', is_admin=True)
                login_user(user)
                return redirect(url_for('index'))
            else:
                flash('管理员密码错误', 'error')
        else:
            # 普通用户登录
            username = request.form.get('username', '').strip()
            password = request.form.get('user_password', '').strip()
            
            if not username:
                flash('请输入用户名', 'error')
                return render_template('login.html')
            
            if not password:
                flash('请输入密码', 'error')
                return render_template('login.html')
            
            # 验证用户是否存在
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT 1 FROM job_records WHERE user = ? LIMIT 1', (username,))
                has_jobs = cursor.fetchone() is not None
                cursor.execute('SELECT 1 FROM account_balance WHERE user = ? LIMIT 1', (username,))
                has_balance = cursor.fetchone() is not None
                conn.close()
                
                if not has_jobs and not has_balance:
                    flash(f'用户 "{username}" 不存在', 'error')
                    return render_template('login.html')
                
                # 验证密码
                if verify_user_password(username, password):
                    need_change = is_default_password(username)
                    user = User(username, username, is_admin=False, need_change_password=need_change)
                    login_user(user)
                    
                    if need_change:
                        flash('请修改默认密码', 'warning')
                        return redirect(url_for('change_password'))
                    
                    return redirect(url_for('index'))
                else:
                    flash('密码错误', 'error')
            except Exception as e:
                flash('登录失败，请稍后重试', 'error')
    
    return render_template('login.html')

@app.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    """修改密码页面"""
    if current_user.is_admin:
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        new_password = request.form.get('new_password', '').strip()
        confirm_password = request.form.get('confirm_password', '').strip()
        
        if len(new_password) < 6:
            flash('密码长度至少6位', 'error')
        elif new_password != confirm_password:
            flash('两次输入的密码不一致', 'error')
        else:
            if set_user_password(current_user.username, new_password, is_default=False):
                current_user.need_change_password = False
                flash('密码修改成功', 'success')
                return redirect(url_for('index'))
            else:
                flash('密码修改失败', 'error')
    
    return render_template('change_password.html', username=current_user.username)

@app.route('/logout')
@login_required
def logout():
    """退出登录"""
    logout_user()
    return redirect(url_for('login'))

# ==================== 页面路由 ====================

@app.route('/')
@login_required
def index():
    """仪表盘首页"""
    if not current_user.is_admin and current_user.need_change_password:
        return redirect(url_for('change_password'))
    return render_template('index.html', is_admin=current_user.is_admin)

@app.route('/users')
@login_required
def users():
    """用户管理页面"""
    if not current_user.is_admin:
        return redirect(url_for('user_self'))
    return render_template('users.html', is_admin=True)

@app.route('/user/self')
@login_required
def user_self():
    """普通用户查看自己的信息"""
    if current_user.is_admin:
        return redirect(url_for('users'))
    if current_user.need_change_password:
        return redirect(url_for('change_password'))
    return render_template('user_self.html', username=current_user.username)

@app.route('/jobs')
@login_required
def jobs():
    """作业查询页面"""
    if not current_user.is_admin and current_user.need_change_password:
        return redirect(url_for('change_password'))
    return render_template('jobs.html', is_admin=current_user.is_admin, username=current_user.username)

@app.route('/reports')
@login_required
def reports():
    """统计报表页面"""
    if not current_user.is_admin and current_user.need_change_password:
        return redirect(url_for('change_password'))
    return render_template('reports.html', is_admin=current_user.is_admin, username=current_user.username)

@app.route('/settings')
@admin_required
def settings():
    """设置页面 - 仅管理员"""
    return render_template('settings.html', is_admin=True)

# ==================== API路由 ====================

@app.route('/api/user/info')
@login_required
def api_user_info():
    """获取当前登录用户信息"""
    return jsonify({
        'success': True,
        'data': {
            'username': current_user.username,
            'is_admin': current_user.is_admin,
            'need_change_password': getattr(current_user, 'need_change_password', False)
        }
    })

@app.route('/api/user/<username>/password', methods=['POST'])
@admin_required
def api_set_user_password(username):
    """管理员设置用户密码"""
    try:
        data = request.json
        new_password = data.get('password', '').strip()
        
        if len(new_password) < 6:
            return jsonify({'success': False, 'error': '密码长度至少6位'}), 400
        
        if set_user_password(username, new_password, is_default=True):
            return jsonify({'success': True, 'message': f'用户 {username} 密码已重置为默认密码'})
        else:
            return jsonify({'success': False, 'error': '密码设置失败'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<username>/password-status')
@admin_required
def api_user_password_status(username):
    """获取用户密码状态"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT is_default, updated_at FROM user_passwords WHERE user = ?', (username,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return jsonify({
                'success': True,
                'data': {
                    'is_default': row['is_default'] == 1,
                    'updated_at': row['updated_at']
                }
            })
        else:
            return jsonify({'success': False, 'error': '用户未设置密码'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dashboard')
@login_required
def api_dashboard():
    """获取仪表盘数据"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 普通用户只能看到自己的数据
        user_filter = ''
        params = []
        if not current_user.is_admin:
            user_filter = 'AND user = ?'
            params = [current_user.username]
        
        today = datetime.now().strftime('%Y-%m-%d')
        this_month = datetime.now().strftime('%Y-%m')
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        month_ago = (datetime.now() - timedelta(days=30)).isoformat()
        
        # 今日费用和作业数
        cursor.execute(f'''
            SELECT 
                COUNT(*) as today_jobs,
                SUM(CAST(cost AS DECIMAL)) as today_cost
            FROM job_records
            WHERE DATE(end_time) = ? AND state IN ('COMPLETED', 'CD') {user_filter}
        ''', [today] + params)
        today_stats = dict(cursor.fetchone())
        
        # 本月费用
        cursor.execute(f'''
            SELECT SUM(CAST(cost AS DECIMAL)) as month_cost
            FROM job_records
            WHERE strftime('%Y-%m', end_time) = ? AND state IN ('COMPLETED', 'CD') {user_filter}
        ''', [this_month] + params)
        month_cost = cursor.fetchone()[0] or 0
        
        # 活跃用户（7天）- 普通用户固定为1
        if current_user.is_admin:
            cursor.execute('''
                SELECT COUNT(DISTINCT user) as active_users
                FROM job_records
                WHERE end_time >= ?
            ''', (week_ago,))
            active_users = cursor.fetchone()[0]
        else:
            active_users = 1
        
        # 总用户数
        if current_user.is_admin:
            cursor.execute('SELECT COUNT(DISTINCT user) as total_users FROM job_records')
            total_users = cursor.fetchone()[0]
        else:
            total_users = 1
        
        # 总作业数
        cursor.execute(f'SELECT COUNT(*) as total_jobs FROM job_records WHERE 1=1 {user_filter}', params)
        total_jobs = cursor.fetchone()[0]
        
        # 累计费用
        cursor.execute(f'''
            SELECT SUM(CAST(cost AS DECIMAL)) as total_cost
            FROM job_records
            WHERE state IN ('COMPLETED', 'CD') {user_filter}
        ''', params)
        total_cost = cursor.fetchone()[0] or 0
        
        # 近30天每日趋势
        cursor.execute(f'''
            SELECT 
                DATE(end_time) as date,
                COUNT(*) as job_count,
                SUM(CAST(cost AS DECIMAL)) as daily_cost
            FROM job_records
            WHERE DATE(end_time) >= DATE(?, '-30 days') AND state IN ('COMPLETED', 'CD') {user_filter}
            GROUP BY DATE(end_time)
            ORDER BY date
        ''', [today] + params)
        daily_trend = [dict(row) for row in cursor.fetchall()]
        
        # 消费排行 Top 10 - 普通用户看不到排行
        top_users = []
        if current_user.is_admin:
            cursor.execute('''
                SELECT 
                    user,
                    COUNT(*) as job_count,
                    SUM(CAST(cost AS DECIMAL)) as total_cost,
                    SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_hours
                FROM job_records
                WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
                GROUP BY user
                ORDER BY total_cost DESC
                LIMIT 10
            ''', (month_ago,))
            top_users = [dict(row) for row in cursor.fetchall()]
        
        # 分区统计
        cursor.execute(f'''
            SELECT 
                partition,
                COUNT(*) as job_count,
                SUM(CAST(cost AS DECIMAL)) as total_cost
            FROM job_records
            WHERE end_time >= ? AND state IN ('COMPLETED', 'CD') {user_filter}
            GROUP BY partition
            ORDER BY total_cost DESC
        ''', [month_ago] + params)
        partition_stats = [dict(row) for row in cursor.fetchall()]
        
        # 账户余额统计 - 普通用户只看自己的
        if current_user.is_admin:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_accounts,
                    SUM(CAST(balance AS DECIMAL)) as total_balance,
                    SUM(CAST(total_recharged AS DECIMAL)) as total_recharged,
                    SUM(CAST(total_consumed AS DECIMAL)) as total_consumed
                FROM account_balance
            ''')
        else:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_accounts,
                    SUM(CAST(balance AS DECIMAL)) as total_balance,
                    SUM(CAST(total_recharged AS DECIMAL)) as total_recharged,
                    SUM(CAST(total_consumed AS DECIMAL)) as total_consumed
                FROM account_balance
                WHERE user = ?
            ''', (current_user.username,))
        balance_stats = dict(cursor.fetchone())
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'today_jobs': today_stats['today_jobs'] or 0,
                'today_cost': float(today_stats['today_cost'] or 0),
                'month_cost': float(month_cost),
                'active_users': active_users,
                'total_users': total_users,
                'total_jobs': total_jobs,
                'total_cost': float(total_cost),
                'daily_trend': daily_trend,
                'top_users': top_users,
                'partition_stats': partition_stats,
                'balance_stats': {
                    'total_accounts': balance_stats['total_accounts'] or 0,
                    'total_balance': float(balance_stats['total_balance'] or 0),
                    'total_recharged': float(balance_stats['total_recharged'] or 0),
                    'total_consumed': float(balance_stats['total_consumed'] or 0)
                }
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/users')
@admin_required
def api_users():
    """获取用户列表 - 仅管理员"""
    try:
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取作业统计
        cursor.execute('''
            SELECT 
                user,
                account,
                COUNT(*) as job_count,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours,
                SUM(CAST(cost AS DECIMAL)) as total_cost
            FROM job_records
            WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
            GROUP BY user, account
            ORDER BY total_cost DESC
        ''', (start_date,))
        
        job_stats = {row['user']: dict(row) for row in cursor.fetchall()}
        
        # 获取余额信息
        cursor.execute('SELECT * FROM account_balance ORDER BY balance DESC')
        balances = [dict(row) for row in cursor.fetchall()]
        
        # 获取密码状态
        cursor.execute('SELECT user, is_default FROM user_passwords')
        pwd_status = {row['user']: row['is_default'] for row in cursor.fetchall()}
        
        # 合并数据
        users = []
        for balance in balances:
            user = balance['user']
            stats = job_stats.get(user, {})
            users.append({
                'user': user,
                'account': balance['account'],
                'balance': float(balance['balance']),
                'credit_limit': float(balance['credit_limit']),
                'total_recharged': float(balance['total_recharged']),
                'total_consumed': float(balance['total_consumed']),
                'status': balance['status'],
                'is_default_password': pwd_status.get(user, 1) == 1,
                'job_count': stats.get('job_count', 0),
                'cpu_hours': stats.get('cpu_hours', 0),
                'gpu_hours': stats.get('gpu_hours', 0),
                'total_cost': float(stats.get('total_cost', 0))
            })
        
        # 添加有作业记录但没有余额记录的用户
        for user, stats in job_stats.items():
            if not any(u['user'] == user for u in users):
                users.append({
                    'user': user,
                    'account': stats.get('account', 'default'),
                    'balance': 0,
                    'credit_limit': 0,
                    'total_recharged': 0,
                    'total_consumed': 0,
                    'status': 'active',
                    'is_default_password': pwd_status.get(user, 1) == 1,
                    'job_count': stats.get('job_count', 0),
                    'cpu_hours': stats.get('cpu_hours', 0),
                    'gpu_hours': stats.get('gpu_hours', 0),
                    'total_cost': float(stats.get('total_cost', 0))
                })
        
        conn.close()
        
        return jsonify({'success': True, 'data': users})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<username>')
@login_required
def api_user_detail(username):
    """获取用户详情"""
    # 普通用户只能查看自己的信息
    if not current_user.is_admin and current_user.username != username:
        return jsonify({'success': False, 'error': '无权查看其他用户信息'}), 403
    
    try:
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 用户汇总
        cursor.execute('''
            SELECT 
                COUNT(*) as job_count,
                SUM(CAST(cost AS DECIMAL)) as total_cost,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours
            FROM job_records
            WHERE user = ? AND end_time >= ? AND state IN ('COMPLETED', 'CD')
        ''', (username, start_date))
        summary = dict(cursor.fetchone())
        
        # 近期作业
        cursor.execute('''
            SELECT 
                job_id, job_name, account, partition, state,
                ncpus, alloc_gpus, elapsed, cost, end_time
            FROM job_records
            WHERE user = ?
            ORDER BY end_time DESC
            LIMIT 50
        ''', (username,))
        recent_jobs = [dict(row) for row in cursor.fetchall()]
        
        # 每日趋势
        cursor.execute('''
            SELECT 
                DATE(end_time) as date,
                COUNT(*) as job_count,
                SUM(CAST(cost AS DECIMAL)) as daily_cost
            FROM job_records
            WHERE user = ? AND DATE(end_time) >= DATE(?) AND state IN ('COMPLETED', 'CD')
            GROUP BY DATE(end_time)
            ORDER BY date
        ''', (username, start_date))
        daily_trend = [dict(row) for row in cursor.fetchall()]
        
        # 余额信息
        cursor.execute('SELECT * FROM account_balance WHERE user = ?', (username,))
        balance_row = cursor.fetchone()
        balance = dict(balance_row) if balance_row else None
        
        # 充值记录
        cursor.execute('''
            SELECT * FROM recharge_records 
            WHERE user = ? 
            ORDER BY created_at DESC 
            LIMIT 20
        ''', (username,))
        recharge_history = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'summary': summary,
                'recent_jobs': recent_jobs,
                'daily_trend': daily_trend,
                'balance': balance,
                'recharge_history': recharge_history
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<username>/recharge', methods=['POST'])
@admin_required
def api_recharge_user(username):
    """给用户充值 - 仅管理员"""
    try:
        data = request.json
        amount = Decimal(str(data.get('amount', 0)))
        recharge_type = data.get('type', 'cash')
        remark = data.get('remark', '')
        
        if amount <= 0:
            return jsonify({'success': False, 'error': '充值金额必须大于0'}), 400
        
        manager = BalanceManager(DB_PATH)
        success, msg = manager.recharge(
            user=username,
            amount=amount,
            recharge_type=recharge_type,
            operator='admin',
            remark=remark
        )
        
        return jsonify({'success': success, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<username>/set-credit', methods=['POST'])
@admin_required
def api_set_credit(username):
    """设置用户信用额度 - 仅管理员"""
    try:
        data = request.json
        amount = Decimal(str(data.get('amount', 0)))
        
        manager = BalanceManager(DB_PATH)
        success, msg = manager.set_credit_limit(username, amount)
        
        return jsonify({'success': success, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<username>/set-status', methods=['POST'])
@admin_required
def api_set_status(username):
    """设置用户状态 - 仅管理员"""
    try:
        data = request.json
        status = data.get('status', 'active')
        
        manager = BalanceManager(DB_PATH)
        if status == 'suspended':
            success, msg = manager.suspend_user(username)
        else:
            success, msg = manager.activate_user(username)
        
        return jsonify({'success': success, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs')
@login_required
def api_jobs():
    """获取作业列表"""
    try:
        user = request.args.get('user')
        account = request.args.get('account')
        days = request.args.get('days', 30, type=int)
        limit = request.args.get('limit', 100, type=int)
        use_jobinfo = request.args.get('use_jobinfo', 'false').lower() == 'true'
        
        # 普通用户只能查看自己的作业
        if not current_user.is_admin:
            user = current_user.username
        
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 使用子查询去重，只获取每个 job_id 的最新记录
        query = '''
            SELECT 
                j.job_id, j.job_name, j.user, j.account, j.partition, j.state,
                j.submit_time, j.start_time, j.end_time, j.elapsed,
                j.ncpus, j.nnodes, j.alloc_gpus, j.cost, j.billing_units
            FROM job_records j
            INNER JOIN (
                SELECT job_id, MAX(created_at) as max_created_at
                FROM job_records
                WHERE end_time >= ?
                GROUP BY job_id
            ) latest ON j.job_id = latest.job_id AND j.created_at = latest.max_created_at
            WHERE 1=1
        '''
        params = [start_date]
        
        if user:
            query += " AND j.user = ?"
            params.append(user)
        if account:
            query += " AND j.account = ?"
            params.append(account)
        
        query += " ORDER BY j.end_time DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        jobs = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        # 如果使用 jobinfo，则获取实时状态
        if use_jobinfo:
            for job in jobs:
                job_id = job.get('job_id')
                if job_id:
                    real_time_info = get_job_status_from_jobinfo(job_id)
                    if real_time_info and real_time_info.get('state'):
                        job['state'] = real_time_info['state']
                        job['state_source'] = 'jobinfo'  # 标记状态来源
                    else:
                        job['state_source'] = 'database'  # 使用数据库状态
        
        return jsonify({'success': True, 'data': jobs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/job/<job_id>/status')
@login_required
def api_job_status(job_id):
    """
    获取单个作业的实时状态（使用 jobinfo）
    
    Args:
        job_id: 作业ID
    
    Returns:
        JSON: 包含作业实时状态的信息
    """
    try:
        # 首先检查用户是否有权限查看此作业
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user FROM job_records WHERE job_id = ?', (job_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return jsonify({'success': False, 'error': '作业不存在'}), 404
        
        # 普通用户只能查看自己的作业
        if not current_user.is_admin and row['user'] != current_user.username:
            return jsonify({'success': False, 'error': '无权查看此作业'}), 403
        
        # 使用 jobinfo 获取实时状态
        job_info = get_job_status_from_jobinfo(job_id)
        
        if job_info:
            return jsonify({
                'success': True,
                'data': job_info,
                'source': 'jobinfo'
            })
        else:
            # 如果 jobinfo 失败，返回数据库中的状态
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT job_id, job_name, user, account, partition, state,
                       submit_time, start_time, end_time, elapsed,
                       ncpus, nnodes, alloc_gpus, cost
                FROM job_records WHERE job_id = ?
            ''', (job_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return jsonify({
                    'success': True,
                    'data': dict(row),
                    'source': 'database',
                    'message': 'jobinfo 查询失败，返回数据库状态'
                })
            else:
                return jsonify({'success': False, 'error': '无法获取作业状态'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/job/<job_id>/cost-detail')
@login_required
def api_job_cost_detail(job_id):
    """
    获取作业费用的详细计算过程
    返回与后端计算一致的明细，解决前端展示与实际费用不一致的问题
    """
    try:
        # 获取作业详细信息（包括 elapsed_seconds）
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                j.job_id, j.job_name, j.user, j.account, j.partition, j.state,
                j.submit_time, j.start_time, j.end_time, j.elapsed, j.elapsed_seconds,
                j.ncpus, j.nnodes, j.alloc_gpus, j.max_rss_mb,
                j.cost, j.billing_units
            FROM job_records j
            INNER JOIN (
                SELECT job_id, MAX(created_at) as max_created_at
                FROM job_records WHERE job_id = ?
                GROUP BY job_id
            ) latest ON j.job_id = latest.job_id AND j.created_at = latest.max_created_at
            WHERE j.job_id = ?
        ''', (job_id, job_id))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return jsonify({'success': False, 'error': '作业不存在'}), 404
        
        job = dict(row)
        
        # 普通用户只能查看自己的作业
        if not current_user.is_admin and job['user'] != current_user.username:
            return jsonify({'success': False, 'error': '无权查看此作业'}), 403
        
        # 加载配置
        config = load_config()
        billing = config.get('billing', {})
        partitions = config.get('partitions', {})
        discounts = config.get('discounts', {})
        
        # 计算参数
        elapsed_seconds = Decimal(str(job.get('elapsed_seconds', 0) or 0))
        hours = elapsed_seconds / Decimal(3600)
        
        ncpus = int(job.get('ncpus', 0) or 0)
        alloc_gpus = int(job.get('alloc_gpus', 0) or 0)
        nnodes = int(job.get('nnodes', 1) or 1)
        max_rss_mb = Decimal(str(job.get('max_rss_mb', 0) or 0))
        mem_gb = max_rss_mb / Decimal(1024)
        
        # 费率
        cpu_rate = Decimal(str(billing.get('cpu_per_hour', 0.1)))
        gpu_rate = Decimal(str(billing.get('gpu_per_hour', 2.0)))
        mem_rate = Decimal(str(billing.get('memory_gb_per_hour', 0.02)))
        node_rate = Decimal(str(billing.get('node_per_hour', 0.0)))
        
        # 计算各项费用（与后端一致）
        cpu_cost = hours * cpu_rate * ncpus
        gpu_cost = hours * gpu_rate * alloc_gpus
        mem_cost = hours * mem_rate * mem_gb
        node_cost = hours * node_rate * nnodes
        
        subtotal = cpu_cost + gpu_cost + mem_cost + node_cost
        
        # 分区倍率
        partition = job.get('partition', 'default')
        partition_multiplier = Decimal(str(partitions.get(partition, 1.0)))
        after_partition = subtotal * partition_multiplier
        
        # 折扣
        account = job.get('account', '')
        user = job.get('user', '')
        account_discount = Decimal(str(discounts.get('accounts', {}).get(account, 0)))
        user_discount = Decimal(str(discounts.get('users', {}).get(user, 0)))
        
        # 优先级：用户折扣 > 账户折扣
        discount = user_discount if user_discount > 0 else account_discount
        discount_source = 'user' if user_discount > 0 else ('account' if account_discount > 0 else None)
        
        after_discount = after_partition * (Decimal(1) - discount)
        
        # 四舍五入
        rounding = billing.get('rounding', '0.01')
        final_cost = after_discount.quantize(Decimal(rounding))
        
        # 最低消费
        min_charge = Decimal(str(billing.get('min_charge', 0.01)))
        if final_cost > 0 and final_cost < min_charge:
            final_cost = min_charge
        
        return jsonify({
            'success': True,
            'data': {
                'job_id': job_id,
                'elapsed': job.get('elapsed'),
                'elapsed_seconds': float(elapsed_seconds),
                'hours': float(hours.quantize(Decimal('0.0001'))),
                'resources': {
                    'ncpus': ncpus,
                    'alloc_gpus': alloc_gpus,
                    'nnodes': nnodes,
                    'max_rss_mb': float(max_rss_mb),
                    'mem_gb': float(mem_gb.quantize(Decimal('0.01')))
                },
                'rates': {
                    'cpu_per_hour': float(cpu_rate),
                    'gpu_per_hour': float(gpu_rate),
                    'memory_gb_per_hour': float(mem_rate),
                    'node_per_hour': float(node_rate)
                },
                'cost_breakdown': {
                    'cpu_cost': float(cpu_cost.quantize(Decimal('0.01'))),
                    'gpu_cost': float(gpu_cost.quantize(Decimal('0.01'))),
                    'mem_cost': float(mem_cost.quantize(Decimal('0.01'))),
                    'node_cost': float(node_cost.quantize(Decimal('0.01'))),
                    'subtotal': float(subtotal.quantize(Decimal('0.01')))
                },
                'partition': {
                    'name': partition,
                    'multiplier': float(partition_multiplier),
                    'cost_after': float(after_partition.quantize(Decimal('0.01')))
                },
                'discount': {
                    'rate': float(discount),
                    'percentage': float(discount * 100),
                    'source': discount_source,
                    'account_discount': float(account_discount),
                    'user_discount': float(user_discount),
                    'cost_after': float(after_discount.quantize(Decimal('0.01')))
                },
                'final_cost': float(final_cost),
                'stored_cost': float(job.get('cost', 0) or 0),
                'billing_units': float(job.get('billing_units', 0) or 0),
                'currency': billing.get('currency', 'CNY')
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'traceback': traceback.format_exc()}), 500

@app.route('/api/accounts')
@admin_required
def api_accounts():
    """获取账户统计 - 仅管理员"""
    try:
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                account,
                COUNT(*) as job_count,
                COUNT(DISTINCT user) as user_count,
                SUM(CAST(cost AS DECIMAL)) as total_cost,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours
            FROM job_records
            WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
            GROUP BY account
            ORDER BY total_cost DESC
        ''', (start_date,))
        
        accounts = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({'success': True, 'data': accounts})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/partitions')
@login_required
def api_partitions():
    """获取分区统计"""
    try:
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 普通用户只能看到自己的数据
        user_filter = ''
        params = [start_date]
        if not current_user.is_admin:
            user_filter = 'AND user = ?'
            params.append(current_user.username)
        
        cursor.execute(f'''
            SELECT 
                partition,
                COUNT(*) as job_count,
                SUM(CAST(cost AS DECIMAL)) as total_cost,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours
            FROM job_records
            WHERE end_time >= ? AND state IN ('COMPLETED', 'CD') {user_filter}
            GROUP BY partition
            ORDER BY total_cost DESC
        ''', params)
        
        partitions = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({'success': True, 'data': partitions})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/daily-stats')
@login_required
def api_daily_stats():
    """获取每日统计"""
    try:
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 普通用户只能看到自己的数据
        user_filter = ''
        params = [start_date]
        if not current_user.is_admin:
            user_filter = 'AND user = ?'
            params.append(current_user.username)
        
        cursor.execute(f'''
            SELECT 
                DATE(end_time) as date,
                COUNT(*) as job_count,
                COUNT(DISTINCT user) as active_users,
                SUM(CAST(cost AS DECIMAL)) as daily_cost,
                SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_hours,
                SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours
            FROM job_records
            WHERE DATE(end_time) >= ? AND state IN ('COMPLETED', 'CD') {user_filter}
            GROUP BY DATE(end_time)
            ORDER BY date DESC
        ''', params)
        
        daily_stats = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({'success': True, 'data': daily_stats})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    """获取或保存配置信息 - GET允许所有登录用户，POST仅管理员"""
    CONFIG_PATH = '/etc/slurm-bill/config.yaml'
    
    if request.method == 'POST':
        # POST 仅允许管理员
        if not current_user.is_admin:
            return jsonify({'success': False, 'error': '权限不足'}), 403
        try:
            data = request.json
            
            # 读取现有配置
            existing_config = {}
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r') as f:
                    existing_config = yaml.safe_load(f) or {}
            
            # 更新配置（递归合并，但字典值完全替换）
            def deep_merge(source, destination):
                for key, value in source.items():
                    if isinstance(value, dict):
                        # 如果 destination[key] 不存在或为 None，创建空字典
                        if key not in destination or destination.get(key) is None:
                            destination[key] = {}
                        # 对于特定的配置项（partitions, discounts等），完全替换子字典
                        # 而不是递归合并，这样可以支持删除操作
                        if key in ['partitions', 'account_quotas']:
                            destination[key] = value.copy()
                        elif key == 'discounts' and isinstance(value, dict):
                            # discounts 下的 accounts 和 users 也需要完全替换
                            destination[key] = destination.get(key, {})
                            for sub_key, sub_value in value.items():
                                if isinstance(sub_value, dict):
                                    destination[key][sub_key] = sub_value.copy()
                                else:
                                    destination[key][sub_key] = sub_value
                        else:
                            deep_merge(value, destination[key])
                    else:
                        destination[key] = value
                return destination
            
            # 合并新配置到现有配置
            updated_config = deep_merge(data, existing_config.copy())
            
            # 确保目录存在
            os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
            
            # 写入配置文件
            with open(CONFIG_PATH, 'w') as f:
                yaml.dump(updated_config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            
            # 重新加载全局配置
            global config
            config = load_config()
            
            return jsonify({'success': True, 'message': '配置已保存'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    else:
        # GET 请求 - 允许所有登录用户访问
        if not current_user.is_authenticated:
            return jsonify({'success': False, 'error': '请先登录'}), 401
        
        # 返回完整配置（不包含敏感信息如密码）
        try:
            # 读取完整配置
            full_config = {}
            if os.path.exists(CONFIG_PATH):
                with open(CONFIG_PATH, 'r') as f:
                    full_config = yaml.safe_load(f) or {}
            
            # 隐藏敏感字段
            safe_config = full_config.copy()
            if 'web' in safe_config and 'auth' in safe_config['web']:
                if 'password' in safe_config['web']['auth']:
                    safe_config['web']['auth']['password'] = '********'
            if 'reporting' in safe_config and 'email' in safe_config['reporting']:
                if 'password' in safe_config['reporting']['email']:
                    safe_config['reporting']['email']['password'] = '********'
            
            return jsonify({'success': True, 'data': safe_config})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

# ==================== 主程序 ====================

if __name__ == '__main__':
    # 初始化数据库表
    init_user_passwords_table()
    
    # 检查数据库连接
    try:
        conn = get_db_connection()
        conn.close()
        print(f"✓ 数据库连接成功: {DB_PATH}")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        print(f"  请确保数据库文件存在: {DB_PATH}")
        sys.exit(1)
    
    # 启动Web服务器
    print("\n" + "="*60)
    print("Slurm Billing Web Application")
    print("="*60)
    print(f"访问地址: http://localhost:5000")
    print(f"管理员密码: {config.get('web', {}).get('auth', {}).get('admin_password', 'changeme')}")
    print("="*60 + "\n")
    
    app.run(host='0.0.0.0', port=5001, debug=True)
