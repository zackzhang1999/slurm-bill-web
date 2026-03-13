#!/bin/bash
# Slurm Billing System 安装脚本
# 版本: 2.0 - 支持预付费系统

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_USER="${SLURM_BILL_USER:-slurm}"

echo "============================================"
echo "Slurm Billing System 安装脚本"
echo "版本: 2.0 (支持预付费系统)"
echo "============================================"
echo ""

# 检查 root 权限
if [ "$EUID" -ne 0 ]; then
    echo "错误: 请使用 root 权限运行此脚本"
    exit 1
fi

# 检查依赖
echo "[1/9] 检查依赖..."

# 检查 Python3
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到 Python3，请先安装"
    exit 1
fi

# 检查 pip3
if ! command -v pip3 &> /dev/null; then
    echo "安装 pip3..."
    apt-get update && apt-get install -y python3-pip || yum install -y python3-pip
fi

# 检查 Slurm 命令
if ! command -v sacct &> /dev/null; then
    echo "警告: 未找到 sacct 命令，请确保 Slurm 已正确安装"
fi

echo "[2/9] 安装 Python 依赖..."
pip3 install pyyaml prettytable 2>/dev/null || pip3 install pyyaml prettytable --break-system-packages 2>/dev/null || echo "依赖可能已安装"

# 创建系统用户
echo "[3/9] 创建系统用户..."
if ! id "$INSTALL_USER" &>/dev/null; then
    useradd -r -s /bin/false -d /var/lib/slurm-bill -m "$INSTALL_USER"
    echo "✓ 创建用户: $INSTALL_USER"
else
    echo "✓ 用户已存在: $INSTALL_USER"
fi

# 创建目录结构
echo "[4/9] 创建目录结构..."
mkdir -p /etc/slurm-bill
mkdir -p /var/lib/slurm-bill
mkdir -p /var/log/slurm-bill
mkdir -p /var/backups/slurm-bill
mkdir -p /opt/slurm-bill

# 复制主程序文件
echo "[5/9] 复制主程序文件..."
cp "$SCRIPT_DIR/slurm_bill.py" /opt/slurm-bill/
cp "$SCRIPT_DIR/bill_query.py" /opt/slurm-bill/
cp "$SCRIPT_DIR/cron_job.sh" /opt/slurm-bill/
chmod +x /opt/slurm-bill/*.py
chmod +x /opt/slurm-bill/cron_job.sh

# 复制预付费系统文件
echo "[6/9] 复制预付费系统文件..."
if [ -f "$SCRIPT_DIR/balance_manager.py" ]; then
    cp "$SCRIPT_DIR/balance_manager.py" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/balance_manager.py
    echo "✓ balance_manager.py (余额管理模块)"
fi

if [ -f "$SCRIPT_DIR/recharge_cli.py" ]; then
    cp "$SCRIPT_DIR/recharge_cli.py" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/recharge_cli.py
    echo "✓ recharge_cli.py (充值管理CLI)"
fi

if [ -f "$SCRIPT_DIR/recalculate_costs.py" ]; then
    cp "$SCRIPT_DIR/recalculate_costs.py" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/recalculate_costs.py
    echo "✓ recalculate_costs.py (费用重算脚本)"
fi

if [ -f "$SCRIPT_DIR/slurm_prolog.py" ]; then
    cp "$SCRIPT_DIR/slurm_prolog.py" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/slurm_prolog.py
    echo "✓ slurm_prolog.py (Slurm Prolog脚本)"
fi

if [ -f "$SCRIPT_DIR/slurm_epilog.py" ]; then
    cp "$SCRIPT_DIR/slurm_epilog.py" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/slurm_epilog.py
    echo "✓ slurm_epilog.py (Slurm Epilog脚本)"
fi

if [ -f "$SCRIPT_DIR/web_integration.py" ]; then
    cp "$SCRIPT_DIR/web_integration.py" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/web_integration.py
    echo "✓ web_integration.py (Web API集成)"
fi

if [ -f "$SCRIPT_DIR/test_billing.py" ]; then
    cp "$SCRIPT_DIR/test_billing.py" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/test_billing.py
    echo "✓ test_billing.py (测试脚本)"
fi

# 复制管理脚本
echo "[7/9] 复制管理脚本..."
if [ -f "$SCRIPT_DIR/backup.sh" ]; then
    cp "$SCRIPT_DIR/backup.sh" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/backup.sh
    echo "✓ backup.sh (备份脚本)"
fi

if [ -f "$SCRIPT_DIR/manage_billing.sh" ]; then
    cp "$SCRIPT_DIR/manage_billing.sh" /opt/slurm-bill/
    chmod +x /opt/slurm-bill/manage_billing.sh
    echo "✓ manage_billing.sh (数据管理脚本)"
fi

# 复制配置文件
echo "[8/9] 复制配置文件..."
# 同时复制到两个位置，确保一致性
cp "$SCRIPT_DIR/config.yaml" /opt/slurm-bill/

if [ ! -f /etc/slurm-bill/config.yaml ]; then
    cp "$SCRIPT_DIR/config.yaml" /etc/slurm-bill/
    echo "✓ 配置文件已创建: /etc/slurm-bill/config.yaml"
else
    # 备份旧配置
    cp /etc/slurm-bill/config.yaml /etc/slurm-bill/config.yaml.backup.$(date +%Y%m%d_%H%M%S)
    cp "$SCRIPT_DIR/config.yaml" /etc/slurm-bill/config.yaml
    echo "✓ 配置文件已更新: /etc/slurm-bill/config.yaml (旧配置已备份)"
fi

# 创建软链接
echo "[9/9] 创建命令软链接..."
ln -sf /opt/slurm-bill/slurm_bill.py /usr/local/bin/slurm-bill
ln -sf /opt/slurm-bill/bill_query.py /usr/local/bin/bill-query

# 预付费系统命令
if [ -f /opt/slurm-bill/recharge_cli.py ]; then
    ln -sf /opt/slurm-bill/recharge_cli.py /usr/local/bin/recharge
    echo "✓ recharge 命令"
fi

# 费用重算命令
if [ -f /opt/slurm-bill/recalculate_costs.py ]; then
    ln -sf /opt/slurm-bill/recalculate_costs.py /usr/local/bin/recalc-costs
    echo "✓ recalc-costs 命令"
fi

# 设置权限
echo ""
echo "设置权限..."
chown -R "$INSTALL_USER:$INSTALL_USER" /var/lib/slurm-bill
chown -R "$INSTALL_USER:$INSTALL_USER" /var/log/slurm-bill
chown -R "$INSTALL_USER:$INSTALL_USER" /var/backups/slurm-bill
chown -R root:root /etc/slurm-bill
chown -R root:root /opt/slurm-bill
chmod 755 /etc/slurm-bill
chmod 644 /etc/slurm-bill/config.yaml

# 初始化数据库
echo ""
echo "初始化数据库..."
python3 /opt/slurm-bill/slurm_bill.py init
chown "$INSTALL_USER:$INSTALL_USER" /var/lib/slurm-bill/billing.db 2>/dev/null || true

# 初始化余额管理表
echo "初始化余额管理模块..."
python3 /opt/slurm-bill/balance_manager.py > /dev/null 2>&1 || echo "余额管理模块初始化完成"

echo ""
echo "============================================"
echo "安装完成!"
echo "============================================"
echo ""
echo "使用方法:"
echo ""
echo "【计费数据收集】"
echo "  sudo -u $INSTALL_USER /opt/slurm-bill/slurm_bill.py collect"
echo ""
echo "【账单查询】"
echo "  bill-query interactive              # 交互式模式"
echo "  bill-query user username -d 30     # 查询用户30天账单"
echo "  bill-query top 10                   # 消费排行前10"
echo ""
echo "【预付费系统 - 充值管理】"
echo "  recharge user username --amount 100    # 给用户充值100元"
echo "  recharge query username                # 查询余额"
echo "  recharge list                          # 列出所有余额"
echo "  recharge history username              # 查看充值历史"
echo "  recharge set-credit username --amount 50  # 设置信用额度50元"
echo ""
echo "【修改费率后更新数据库】"
echo "  recalc-costs --dry-run                 # 试运行，查看变化"
echo "  recalc-costs --days 30                 # 只更新最近30天"
echo "  recalc-costs                           # 更新所有历史作业"
echo ""
echo "【生成报表】"
echo "  slurm-bill report --group-by account"
echo ""
echo "【Slurm集成（可选）】"
echo "  # 启用余额检查和作业拦截:"
echo "  sudo cp /opt/slurm-bill/slurm_prolog.py /etc/slurm/"
echo "  sudo cp /opt/slurm-bill/slurm_epilog.py /etc/slurm/"
echo "  # 然后编辑 /etc/slurm/slurm.conf 添加:"
echo "  # PrologSlurmctld=/etc/slurm/slurm_prolog.py  # 注意: 使用 PrologSlurmctld 而非 Prolog"
echo "  # Epilog=/etc/slurm/slurm_epilog.py"
echo "  # PrologFlags=Alloc"
echo "  # sudo systemctl restart slurmctld"
echo ""
echo "【定时任务】"
echo "  echo '*/15 * * * * $INSTALL_USER /opt/slurm-bill/cron_job.sh' | sudo crontab -"
echo ""
echo "文件位置:"
echo "  配置文件: /etc/slurm-bill/config.yaml"
echo "  数据库:   /var/lib/slurm-bill/billing.db"
echo "  日志:     /var/log/slurm-bill/billing.log"
echo "  程序:     /opt/slurm-bill/"
echo ""
echo "建议:"
echo "  1. 编辑 /etc/slurm-bill/config.yaml 配置费率"
echo "  2. 运行测试: python3 /opt/slurm-bill/test_billing.py"
echo "  3. 测试计费收集: slurm-bill collect"
echo "  4. 如需预付费: 给用户充值 recharge user username --amount 100"
echo ""
echo "重要提示:"
echo "  - 提交作业时必须指定 --time，否则默认1年时间限制会导致预估费用极高"
echo "  - gpu 分区默认 1.5 倍费率，确保余额充足"
echo "  - 示例: sbatch --wrap='sleep 10' --gres=gpu:1 -c 2 -p gpu --time=00:10"
echo "  5. 添加 crontab 定时任务"
echo ""
