#!/bin/bash

# Slurm Billing Web Application 启动脚本

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Slurm Billing Web Application${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}错误: 未找到 Python3${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Python3 已安装${NC}"

# 检查pip
if ! command -v pip3 &> /dev/null; then
    echo -e "${RED}错误: 未找到 pip3${NC}"
    exit 1
fi

echo -e "${GREEN}✓ pip3 已安装${NC}"

# 安装依赖
echo ""
echo -e "${YELLOW}正在安装依赖...${NC}"
pip3 install -q -r requirements.txt

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ 依赖安装完成${NC}"
else
    echo -e "${RED}✗ 依赖安装失败${NC}"
    exit 1
fi

# 检查数据库
echo ""
echo -e "${YELLOW}检查数据库...${NC}"
if [ -f "/var/lib/slurm-bill/billing.db" ]; then
    echo -e "${GREEN}✓ 找到系统数据库${NC}"
    DB_PATH="/var/lib/slurm-bill/billing.db"
elif [ -f "../billing.db" ]; then
    echo -e "${GREEN}✓ 找到本地数据库${NC}"
    DB_PATH="../billing.db"
else
    echo -e "${YELLOW}! 未找到数据库，将使用测试模式${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}启动 Web 服务器...${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "访问地址: ${GREEN}http://localhost:5000${NC}"
echo -e "默认密码: ${GREEN}changeme${NC} (可在 config.yaml 中修改)"
echo ""
echo -e "按 ${YELLOW}Ctrl+C${NC} 停止服务器"
echo ""

# 启动Flask应用
export FLASK_APP=app.py
export FLASK_ENV=development
python3 -m flask run --host=0.0.0.0 --port=5000
