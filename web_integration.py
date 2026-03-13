#!/usr/bin/env python3
"""
Slurm Billing Web Integration - 与 Slurm-Web 集成模块
提供 RESTful API 供前端调用

需要在 Slurm-Web 的 app.py 中添加：
    from web_integration import register_billing_routes
    register_billing_routes(app)
"""

import sqlite3
import json
from datetime import datetime, timedelta
from decimal import Decimal
from functools import wraps
from flask import jsonify, request

DB_PATH = '/var/lib/slurm-bill/billing.db'


def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def decimal_default(obj):
    """JSON 序列化 Decimal"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def register_billing_routes(app):
    """注册计费相关路由到 Flask app"""
    
    @app.route('/api/billing/summary')
    def api_billing_summary():
        """获取计费汇总数据"""
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 总体统计
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(DISTINCT user) as active_users,
                    COUNT(DISTINCT account) as active_accounts,
                    SUM(CAST(cost AS DECIMAL)) as total_cost,
                    SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
                    SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours
                FROM job_records
                WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
            ''', (start_date,))
            
            summary = dict(cursor.fetchone())
            
            # 每日趋势
            cursor.execute('''
                SELECT 
                    DATE(end_time) as date,
                    COUNT(*) as job_count,
                    SUM(CAST(cost AS DECIMAL)) as daily_cost
                FROM job_records
                WHERE DATE(end_time) >= DATE(?) AND state IN ('COMPLETED', 'CD')
                GROUP BY DATE(end_time)
                ORDER BY date
            ''', (start_date,))
            
            daily_trend = [dict(row) for row in cursor.fetchall()]
            
            # Top 10 消费用户
            cursor.execute('''
                SELECT 
                    user,
                    COUNT(*) as job_count,
                    SUM(CAST(cost AS DECIMAL)) as total_cost
                FROM job_records
                WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
                GROUP BY user
                ORDER BY total_cost DESC
                LIMIT 10
            ''', (start_date,))
            
            top_users = [dict(row) for row in cursor.fetchall()]
            
            conn.close()
            
            return jsonify({
                'success': True,
                'data': {
                    'summary': summary,
                    'daily_trend': daily_trend,
                    'top_users': top_users
                }
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    
    @app.route('/api/billing/users')
    def api_billing_users():
        """获取用户计费列表"""
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    user,
                    account,
                    COUNT(*) as job_count,
                    SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
                    SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours,
                    SUM(CAST(cost AS DECIMAL)) as total_cost,
                    AVG(CAST(cost AS DECIMAL)) as avg_cost
                FROM job_records
                WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
                GROUP BY user, account
                ORDER BY total_cost DESC
            ''', (start_date,))
            
            users = [dict(row) for row in cursor.fetchall()]
            conn.close()
            
            return jsonify({'success': True, 'data': users})
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    
    @app.route('/api/billing/user/<username>')
    def api_billing_user_detail(username):
        """获取单个用户的计费详情"""
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 用户汇总
            cursor.execute('''
                SELECT 
                    COUNT(*) as job_count,
                    SUM(CAST(cost AS DECIMAL)) as total_cost,
                    SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
                    SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours
                FROM job_records
                WHERE user = ? AND end_time >= ? AND state IN ('COMPLETED', 'CD')
            ''', (username, start_date))
            
            summary = dict(cursor.fetchone())
            
            # 近期作业
            cursor.execute('''
                SELECT 
                    job_id,
                    job_name,
                    partition,
                    state,
                    ncpus,
                    alloc_gpus,
                    elapsed,
                    cost,
                    end_time
                FROM job_records
                WHERE user = ?
                ORDER BY end_time DESC
                LIMIT 50
            ''', (username,))
            
            recent_jobs = [dict(row) for row in cursor.fetchall()]
            
            # 每日消费趋势
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
            
            conn.close()
            
            return jsonify({
                'success': True,
                'data': {
                    'summary': summary,
                    'recent_jobs': recent_jobs,
                    'daily_trend': daily_trend
                }
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    
    @app.route('/api/billing/accounts')
    def api_billing_accounts():
        """获取账户计费列表"""
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    account,
                    COUNT(*) as job_count,
                    COUNT(DISTINCT user) as user_count,
                    SUM(CAST(cost AS DECIMAL)) as total_cost,
                    SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
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
    
    
    @app.route('/api/billing/partitions')
    def api_billing_partitions():
        """获取分区计费统计"""
        days = request.args.get('days', 30, type=int)
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    partition,
                    COUNT(*) as job_count,
                    SUM(CAST(cost AS DECIMAL)) as total_cost,
                    SUM(ncpus * elapsed_seconds) / 3600.0 as cpu_core_hours,
                    SUM(alloc_gpus * elapsed_seconds) / 3600.0 as gpu_hours
                FROM job_records
                WHERE end_time >= ? AND state IN ('COMPLETED', 'CD')
                GROUP BY partition
                ORDER BY total_cost DESC
            ''', (start_date,))
            
            partitions = [dict(row) for row in cursor.fetchall()]
            conn.close()
            
            return jsonify({'success': True, 'data': partitions})
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    
    @app.route('/api/billing/realtime')
    def api_billing_realtime():
        """获取实时计费统计（用于仪表盘）"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            today = datetime.now().strftime('%Y-%m-%d')
            this_month = datetime.now().strftime('%Y-%m')
            
            # 今日费用
            cursor.execute('''
                SELECT SUM(CAST(cost AS DECIMAL)) as today_cost
                FROM job_records
                WHERE DATE(end_time) = ? AND state IN ('COMPLETED', 'CD')
            ''', (today,))
            today_cost = cursor.fetchone()[0] or 0
            
            # 本月费用
            cursor.execute('''
                SELECT SUM(CAST(cost AS DECIMAL)) as month_cost
                FROM job_records
                WHERE strftime('%Y-%m', end_time) = ? AND state IN ('COMPLETED', 'CD')
            ''', (this_month,))
            month_cost = cursor.fetchone()[0] or 0
            
            # 今日作业数
            cursor.execute('''
                SELECT COUNT(*) as today_jobs
                FROM job_records
                WHERE DATE(end_time) = ?
            ''', (today,))
            today_jobs = cursor.fetchone()[0]
            
            # 活跃用户数（7天）
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            cursor.execute('''
                SELECT COUNT(DISTINCT user) as active_users
                FROM job_records
                WHERE end_time >= ?
            ''', (week_ago,))
            active_users = cursor.fetchone()[0]
            
            conn.close()
            
            return jsonify({
                'success': True,
                'data': {
                    'today_cost': float(today_cost),
                    'month_cost': float(month_cost),
                    'today_jobs': today_jobs,
                    'active_users': active_users
                }
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    
    print("[Billing] 计费 API 路由已注册")
    print("[Billing] 可用端点:")
    print("  - GET /api/billing/summary")
    print("  - GET /api/billing/users")
    print("  - GET /api/billing/user/<username>")
    print("  - GET /api/billing/accounts")
    print("  - GET /api/billing/partitions")
    print("  - GET /api/billing/realtime")


# 如果直接运行此脚本，启动测试服务器
if __name__ == '__main__':
    from flask import Flask
    
    app = Flask(__name__)
    register_billing_routes(app)
    
    print("\n启动计费 API 测试服务器...")
    print("访问: http://localhost:8080/api/billing/summary")
    app.run(host='0.0.0.0', port=8080, debug=True)
