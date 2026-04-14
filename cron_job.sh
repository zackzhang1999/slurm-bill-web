#!/bin/bash
# Slurm Billing System - 定时任务脚本
# 建议添加到 crontab: */15 * * * * /root/slurm-bill/cron_job.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/slurm-bill/cron.log"
LOCK_FILE="/var/run/slurm-bill.lock"

# 确保日志目录存在
mkdir -p "$(dirname "$LOG_FILE")"
mkdir -p /var/lib/slurm-bill
mkdir -p /var/log/slurm-bill

# 检查锁文件，防止重复执行
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "$(date): 另一个计费进程正在运行 (PID: $PID)" >> "$LOG_FILE"
        exit 1
    else
        rm -f "$LOCK_FILE"
    fi
fi

# 创建锁文件
echo $$ > "$LOCK_FILE"

# 清理锁文件的函数
cleanup() {
    rm -f "$LOCK_FILE"
}
trap cleanup EXIT

# 运行计费收集
echo "$(date): 开始计费收集..." >> "$LOG_FILE"

# 使用 sync 命令同步最近2天的作业（包含数组作业和作业步）
python3 "$SCRIPT_DIR/slurm_bill.py" sync --days 2 >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "$(date): 计费收集完成" >> "$LOG_FILE"
else
    echo "$(date): 计费收集失败 (退出码: $EXIT_CODE)" >> "$LOG_FILE"
fi

exit $EXIT_CODE
