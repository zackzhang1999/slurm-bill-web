# Slurm Billing System 快速入门指南

## 5分钟快速上手

### 1. 安装（1分钟）

```bash
cd /root/slurm-bill
sudo bash install.sh
```

### 2. 配置费率（1分钟）

```bash
sudo vim /etc/slurm-bill/config.yaml
```

修改基础费率：
```yaml
billing:
  cpu_per_hour: 0.10          # CPU: 0.1元/核/小时
  memory_gb_per_hour: 0.02    # 内存: 0.02元/GB/小时
  gpu_per_hour: 2.00          # GPU: 2元/卡/小时
```

### 3. 收集数据（1分钟）

```bash
# 立即收集最近24小时的作业数据
slurm-bill collect
```

### 4. 查询账单（1分钟）

```bash
# 查询用户账单
bill-query user amax

# 输出示例：
# 📋 费率: CPU 0.1元/核/小时 | GPU 2.0元/卡/小时
# +------+--------+---------+---------+--------+----------+
# | 用户 | 作业数 | CPU核时 | GPU卡时 | 总费用 | 平均费用 |
# +------+--------+---------+---------+--------+----------+
# | amax |     28 |  126.20 |    0.05 |  12.74 |     0.46 |
# +------+--------+---------+---------+--------+----------+
# 💰 费用明细:
#    CPU费用: 126.20 核时 × 0.1 元/核时 = 12.62 元
#    GPU费用: 0.05 卡时 × 2.0 元/卡时 = 0.10 元
#    总计费用: 12.74 元
```

### 5. 设置自动收集（1分钟）

```bash
# 添加定时任务，每15分钟自动收集
echo '*/15 * * * * /opt/slurm-bill/cron_job.sh' | sudo crontab -u slurm -
```

---

## 预付费系统快速上手（3分钟）

### 1. 给用户充值

```bash
# 给用户充值200元（自动获取Slurm默认账户）
recharge user amax --amount 200 --operator admin

# 查询余额
recharge query amax
```

### 2. 与Slurm集成（启用作业拦截）

```bash
# 复制脚本到Slurm目录
sudo cp /opt/slurm-bill/slurm_prolog.py /etc/slurm/
sudo cp /opt/slurm-bill/slurm_epilog.py /etc/slurm/
sudo chmod 755 /etc/slurm/slurm_prolog.py
sudo chmod 755 /etc/slurm/slurm_epilog.py

# 修改slurm.conf，添加以下两行：
# Prolog=/etc/slurm/slurm_prolog.py
# Epilog=/etc/slurm/slurm_epilog.py
sudo vim /etc/slurm/slurm.conf

# 重启Slurm
sudo systemctl restart slurmctld
```

### 3. 测试余额检查

```bash
# 切换到测试用户
su - amax

# 提交作业（余额充足，应该成功）
sbatch --wrap="sleep 10" --cpus-per-task=2 --time=0:10

# 查看作业状态
squeue

# 作业结束后查看扣费
recharge query amax
```

### 4. 测试余额不足拦截

```bash
# 充值少量金额
recharge user testuser --amount 1

# 提交大作业（应该被拒绝）
su - testuser
sbatch --wrap="sleep 60" --cpus-per-task=100 --time=10:00
# 输出: 余额不足！当前余额1元，预估费用XX元...
```

---

## 常用命令速查

### 数据收集
```bash
slurm-bill collect                          # 收集最近24小时
slurm-bill collect --start 2024-01-01       # 指定开始时间
slurm-bill init                             # 初始化数据库
```

### 账单查询
```bash
bill-query user amax                        # 查询用户（默认30天）
bill-query user amax -d 7                   # 查询最近7天
bill-query user amax -d 365                 # 查询最近一年
bill-query account research                 # 查询账户
bill-query top 10                           # 消费排行Top 10
bill-query jobs amax -d 7                   # 查看作业详情
bill-query                                  # 交互式模式
```

### 报表生成
```bash
slurm-bill report --group-by user           # 按用户分组
slurm-bill report --group-by account        # 按账户分组
slurm-bill report --group-by partition      # 按分组分组
slurm-bill report --format csv -o bill.csv  # 导出CSV
slurm-bill report --format json -o bill.json # 导出JSON
```

### 数据管理
```bash
# 删除指定用户的计费记录
delete-billing user amax

# 删除指定账户的计费记录  
delete-billing account research

# 删除某日期之前的数据
delete-billing before 2024-01-01

# 清空所有数据（危险！）
delete-billing all

# 备份数据库
sudo /opt/slurm-bill/backup.sh

# 交互式管理
sudo /opt/slurm-bill/manage_billing.sh
```

### 预付费系统（充值管理）
```bash
# 给用户充值（自动检测Slurm默认账户）
recharge user amax --amount 200
recharge user amax --amount 100 --type cash --operator admin --remark "月度充值"

# 查询余额
recharge query amax
recharge list

# 查看充值历史
recharge history amax
recharge history

# 设置信用额度（允许欠费额度）
recharge set-credit amax --amount 50

# 设置余额预警阈值
recharge set-alert amax --amount 20

# 暂停/激活用户
recharge suspend amax
recharge activate amax
```

---

## 配置文件示例

### 基础配置
```yaml
billing:
  cpu_per_hour: 0.10
  memory_gb_per_hour: 0.02
  gpu_per_hour: 2.00
  currency: "CNY"
  min_charge: 0.01
```

### 分区倍率
```yaml
partitions:
  normal: 1.0        # 标准分区原价
  gpu: 1.5           # GPU分区1.5倍
  debug: 0.5         # Debug分区半价
```

### 折扣设置
```yaml
discounts:
  accounts:
    admin: 1.0       # admin账户免费
    student: 0.5     # student账户5折
  users:
    vip_user: 0.2    # 特定用户8折
```

---

## 费用计算示例

| 场景 | 配置 | 时长 | 费用计算 | 结果 |
|-----|------|-----|---------|-----|
| CPU计算 | 4核, 8GB | 2小时 | 4×2×0.1 + 8×2×0.02 | **1.12元** |
| GPU训练 | 8核, 32GB, 2GPU | 10小时 | 8×10×0.1 + 32×10×0.02 + 2×10×2.0 | **54.4元** |
| 大作业 | 22核, 64GB | 3小时 | 22×3×0.1 + 64×3×0.02 | **10.44元** |

---

## 目录结构

```
/etc/slurm-bill/
└── config.yaml              # 配置文件

/var/lib/slurm-bill/
├── billing.db               # SQLite数据库
└── reports/                 # 报表导出目录

/var/log/slurm-bill/
├── billing.log              # 主日志
└── cron.log                 # 定时任务日志

/opt/slurm-bill/
├── slurm_bill.py            # 主计费程序
├── bill_query.py            # 查询工具
├── balance_manager.py       # 余额管理模块
├── recharge_cli.py          # 充值管理CLI
├── slurm_prolog.py          # Slurm Prolog脚本（余额检查）
├── slurm_epilog.py          # Slurm Epilog脚本（扣费）
├── cron_job.sh              # 定时任务脚本
├── backup.sh                # 备份脚本
└── manage_billing.sh        # 交互式管理脚本
```

---

## 故障排查

### 问题1: 收集不到数据
```bash
# 检查sacct是否可用
sacct -a | head -5

# 检查slurmdbd服务
systemctl status slurmdbd
```

### 问题2: 费用计算错误
```bash
# 检查费率配置
cat /etc/slurm-bill/config.yaml

# 查看数据库中的原始数据
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT job_id, ncpus, alloc_gpus, elapsed, cost FROM job_records LIMIT 5;"
```

### 问题3: 权限错误
```bash
# 修复权限
sudo chown -R slurm:slurm /var/lib/slurm-bill
sudo chown -R slurm:slurm /var/log/slurm-bill
```

### 问题4: 如何清空用户计费数据
```bash
# 方法1: 使用工具（推荐）
delete-billing user <用户名>

# 方法2: 直接SQL
sqlite3 /var/lib/slurm-bill/billing.db \
  "DELETE FROM job_records WHERE user='用户名';"

# ⚠️ 删除前建议先备份
cp /var/lib/slurm-bill/billing.db \
   /backup/billing.db.$(date +%Y%m%d)
```

---

## 下一步

- 📖 阅读完整文档：[GUIDE.md](GUIDE.md)
- 🔧 修改配置：`sudo vim /etc/slurm-bill/config.yaml`
- 📊 查看报表：`slurm-bill report --group-by user`
- 🌐 Web集成：参考 [web_integration.py](web_integration.py)

---

**遇到问题？** 查看 [GUIDE.md](GUIDE.md) 的"故障排除"章节
