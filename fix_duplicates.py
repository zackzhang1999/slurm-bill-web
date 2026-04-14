#!/usr/bin/env python3
"""
清理数据库中重复的作业记录
保留每个 job_id 的最新记录
"""

import os
import sqlite3

DB_PATH = '/var/lib/slurm-bill/billing.db'


def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fix_duplicates():
    """清理重复记录"""
    print("="*70)
    print("清理数据库中的重复作业记录")
    print("="*70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 查找重复的 job_id
    cursor.execute('''
        SELECT job_id, COUNT(*) as cnt 
        FROM job_records 
        GROUP BY job_id 
        HAVING cnt > 1 
        ORDER BY cnt DESC
    ''')
    duplicates = cursor.fetchall()
    
    if not duplicates:
        print("\n✓ 没有发现重复记录，数据库已是最新状态！")
        conn.close()
        return
    
    print(f"\n发现 {len(duplicates)} 个重复的 job_id")
    print(f"\n{'Job ID':<15} {'重复数':>10}")
    print("-"*30)
    for row in duplicates:
        print(f"{row['job_id']:<15} {row['cnt']:>10}")
    
    # 统计要删除的记录数
    cursor.execute('''
        SELECT COUNT(*) as total_duplicates
        FROM job_records
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM job_records
            GROUP BY job_id
        )
    ''')
    to_delete = cursor.fetchone()['total_duplicates']
    
    print(f"\n将删除 {to_delete} 条重复记录（保留每个 job_id 的最新记录）")
    
    # 询问确认
    print("\n是否执行清理？ (yes/no): ", end='')
    response = input().strip().lower()
    
    if response not in ('yes', 'y'):
        print("已取消清理")
        conn.close()
        return
    
    # 执行删除
    cursor.execute('''
        DELETE FROM job_records
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM job_records
            GROUP BY job_id
        )
    ''')
    
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"\n✓ 清理完成！已删除 {deleted} 条重复记录")
    
    # 验证结果
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) as total FROM job_records')
    total = cursor.fetchone()['total']
    conn.close()
    
    print(f"✓ 当前数据库共有 {total} 条作业记录")
    print("="*70)


if __name__ == '__main__':
    if not os.path.exists(DB_PATH):
        print(f"错误: 数据库不存在: {DB_PATH}")
        exit(1)
    
    fix_duplicates()
