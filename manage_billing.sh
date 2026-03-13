#!/bin/bash
# Slurm Billing 数据管理脚本

DB_PATH="/var/lib/slurm-bill/billing.db"
BACKUP_DIR="/var/backups/slurm-bill"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

show_menu() {
    echo "================================"
    echo "  Slurm Billing 数据管理"
    echo "================================"
    echo "1. 查看所有用户"
    echo "2. 查看用户详情"
    echo "3. 删除指定用户数据"
    echo "4. 删除指定账户数据"
    echo "5. 按时间范围删除"
    echo "6. 清空所有数据（危险）"
    echo "7. 备份数据库"
    echo "8. 查看数据库统计"
    echo "0. 退出"
    echo "================================"
}

backup_db() {
    mkdir -p "$BACKUP_DIR"
    local backup_file="$BACKUP_DIR/billing_$(date +%Y%m%d_%H%M%S).db"
    cp "$DB_PATH" "$backup_file"
    echo -e "${GREEN}✓ 数据库已备份到: $backup_file${NC}"
}

list_users() {
    echo "用户列表:"
    sqlite3 "$DB_PATH" "SELECT user, COUNT(*) as jobs, 
        ROUND(SUM(CAST(cost AS DECIMAL)), 2) as total_cost 
        FROM job_records GROUP BY user ORDER BY total_cost DESC;"
}

show_user_detail() {
    read -p "请输入用户名: " user
    echo "用户 $user 详情:"
    sqlite3 "$DB_PATH" "SELECT 
        COUNT(*) as jobs,
        ROUND(SUM(ncpus * elapsed_seconds)/3600.0, 2) as cpu_hours,
        ROUND(SUM(alloc_gpus * elapsed_seconds)/3600.0, 2) as gpu_hours,
        ROUND(SUM(CAST(cost AS DECIMAL)), 2) as total_cost
        FROM job_records WHERE user='$user';"
}

delete_user() {
    read -p "请输入要删除的用户名: " user
    
    # 先查询
    local count=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM job_records WHERE user='$user';")
    local cost=$(sqlite3 "$DB_PATH" "SELECT ROUND(SUM(CAST(cost AS DECIMAL)), 2) FROM job_records WHERE user='$user';")
    
    if [ "$count" -eq 0 ]; then
        echo -e "${YELLOW}⚠ 用户 $user 没有计费记录${NC}"
        return
    fi
    
    echo -e "${YELLOW}即将删除用户 $user 的 $count 条记录，费用合计 $cost 元${NC}"
    read -p "确认删除? (yes/no): " confirm
    
    if [ "$confirm" = "yes" ]; then
        backup_db
        sqlite3 "$DB_PATH" "DELETE FROM job_records WHERE user='$user';"
        echo -e "${GREEN}✓ 已删除用户 $user 的所有计费记录${NC}"
    else
        echo "已取消"
    fi
}

delete_account() {
    read -p "请输入要删除的账户名: " account
    
    local count=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM job_records WHERE account='$account';")
    
    if [ "$count" -eq 0 ]; then
        echo -e "${YELLOW}⚠ 账户 $account 没有计费记录${NC}"
        return
    fi
    
    echo -e "${YELLOW}即将删除账户 $account 的 $count 条记录${NC}"
    read -p "确认删除? (yes/no): " confirm
    
    if [ "$confirm" = "yes" ]; then
        backup_db
        sqlite3 "$DB_PATH" "DELETE FROM job_records WHERE account='$account';"
        echo -e "${GREEN}✓ 已删除账户 $account 的所有计费记录${NC}"
    else
        echo "已取消"
    fi
}

delete_by_date() {
    echo "删除时间范围:"
    echo "1. 删除某日期之前的记录"
    echo "2. 删除某月份的数据"
    read -p "选择: " choice
    
    if [ "$choice" = "1" ]; then
        read -p "输入日期 (YYYY-MM-DD): " date
        local count=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM job_records WHERE end_time < '$date';")
        echo "将删除 $count 条记录"
        read -p "确认删除? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            backup_db
            sqlite3 "$DB_PATH" "DELETE FROM job_records WHERE end_time < '$date';"
            echo -e "${GREEN}✓ 已删除${NC}"
        fi
    elif [ "$choice" = "2" ]; then
        read -p "输入月份 (YYYY-MM): " month
        local count=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM job_records WHERE strftime('%Y-%m', end_time) = '$month';")
        echo "将删除 $count 条记录"
        read -p "确认删除? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            backup_db
            sqlite3 "$DB_PATH" "DELETE FROM job_records WHERE strftime('%Y-%m', end_time) = '$month';"
            echo -e "${GREEN}✓ 已删除${NC}"
        fi
    fi
}

delete_all() {
    echo -e "${RED}⚠⚠⚠ 警告: 这将清空所有计费数据!${NC}"
    local total=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM job_records;")
    echo "当前数据库共有 $total 条记录"
    read -p "输入 'DELETE ALL' 确认清空: " confirm
    
    if [ "$confirm" = "DELETE ALL" ]; then
        backup_db
        sqlite3 "$DB_PATH" "DELETE FROM job_records; VACUUM;"
        echo -e "${GREEN}✓ 数据库已清空${NC}"
    else
        echo "已取消"
    fi
}

show_stats() {
    echo "数据库统计:"
    sqlite3 "$DB_PATH" "SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT user) as users,
        COUNT(DISTINCT account) as accounts,
        ROUND(SUM(CAST(cost AS DECIMAL)), 2) as total_cost
        FROM job_records;"
}

# 主循环
while true; do
    show_menu
    read -p "请选择操作: " choice
    
    case $choice in
        1) list_users ;;
        2) show_user_detail ;;
        3) delete_user ;;
        4) delete_account ;;
        5) delete_by_date ;;
        6) delete_all ;;
        7) backup_db ;;
        8) show_stats ;;
        0) echo "再见!"; exit 0 ;;
        *) echo "无效选择" ;;
    esac
    
    echo ""
    read -p "按回车继续..."
done
