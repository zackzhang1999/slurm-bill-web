# Slurm Billing System 完整使用指南

## 目录
1. [系统概述](#系统概述)
2. [安装部署](#安装部署)
3. [配置详解](#配置详解)
4. [日常使用](#日常使用)
5. [计费原理](#计费原理)
6. [预付费系统](#预付费系统)
7. [数据管理](#数据管理)
8. [API接口](#API接口)
9. [故障排除](#故障排除)

---

## 系统概述

### 什么是 Slurm Billing System？

Slurm Billing System 是一个专为 Slurm 工作负载管理器设计的**资源计费系统**，能够：

- ✅ **自动收集**作业资源使用数据（CPU、内存、GPU、运行时间）
- ✅ **精确计算**每个作业的费用
- ✅ **多维度统计**（按用户、账户、分区、时间）
- ✅ **灵活配置**费率、折扣、分区倍率
- ✅ **实时查询**账单和消费排行
- ✅ **Web集成**与 Slurm-Web 无缝对接

### 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Slurm Billing System                      │
├─────────────────────────────────────────────────────────────┤
│  数据收集层  │  sacct → 解析 → 数据库 (SQLite)              │
├─────────────────────────────────────────────────────────────┤
│  计费引擎    │  CPU/内存/GPU费用计算 → 折扣 → 分区倍率      │
├─────────────────────────────────────────────────────────────┤
│  查询接口    │  CLI工具 / Web API / SQL直连                 │
├─────────────────────────────────────────────────────────────┤
│  展示层      │  命令行报表 / Web仪表盘 / CSV导出            │
└─────────────────────────────────────────────────────────────┘
```

### 核心特性

| 特性 | 说明 |
|------|------|
| **多维度计费** | CPU核时、内存GB时、GPU卡时独立计费 |
| **阶梯定价** | 支持用量越大单价越低的阶梯模式 |
| **灵活折扣** | 账户级别、用户级别自定义折扣 |
| **分区倍率** | 不同分区设置不同计费倍率 |
| **实时查询** | 交互式命令行，支持历史数据回溯 |
| **零侵入** | 不修改 Slurm 配置，独立运行 |

---

## 安装部署

### 环境要求

- **操作系统**: Linux (CentOS 7/8, Ubuntu 18.04/20.04/22.04)
- **Python**: 3.6+
- **Slurm**: 已安装并配置 accounting (slurmdbd)
- **依赖包**: PyYAML, prettytable

### 快速安装

```bash
# 1. 进入项目目录
cd /root/slurm-bill

# 2. 运行安装脚本
sudo bash install.sh
```

安装脚本会自动完成：
- ✅ 安装 Python 依赖
- ✅ 创建系统用户 `slurm`
- ✅ 创建目录结构
- ✅ 复制程序文件到 `/opt/slurm-bill/`
- ✅ 创建命令软链接
- ✅ 初始化数据库

### 手动安装

如果自动安装失败，可以手动安装：

```bash
# 1. 安装依赖
pip3 install pyyaml prettytable

# 2. 创建目录
sudo mkdir -p /etc/slurm-bill /var/lib/slurm-bill /var/log/slurm-bill

# 3. 复制文件
sudo cp slurm_bill.py bill_query.py cron_job.sh /opt/slurm-bill/
sudo cp config.yaml /etc/slurm-bill/
sudo chmod +x /opt/slurm-bill/*.py /opt/slurm-bill/*.sh

# 4. 创建软链接
sudo ln -sf /opt/slurm-bill/slurm_bill.py /usr/local/bin/slurm-bill
sudo ln -sf /opt/slurm-bill/bill_query.py /usr/local/bin/bill-query

# 5. 初始化数据库
sudo /opt/slurm-bill/slurm_bill.py init
```

### 验证安装

```bash
# 测试计费系统
slurm-bill --help

# 测试查询工具
bill-query --help

# 运行测试脚本
python3 /root/slurm-bill/test_billing.py
```

---

## 配置详解

### 配置文件位置

```
/etc/slurm-bill/config.yaml
```

### 基础费率配置

```yaml
billing:
  # 基础费率（单位：元）
  cpu_per_hour: 0.10          # CPU 核心每小时费用
  memory_gb_per_hour: 0.02    # 内存每GB每小时费用
  gpu_per_hour: 2.00          # GPU 卡每小时费用
  node_per_hour: 0.00         # 整节点每小时费用（可选）
  
  # 计费选项
  currency: "CNY"             # 货币单位
  min_charge: 0.01            # 最低消费
  rounding: "0.01"            # 四舍五入精度
  
  # 计费模式: linear(线性) / tiered(阶梯)
  mode: "linear"
```

**示例计算**：
- 4核CPU运行1小时：4 × 0.1 = **0.4元**
- 8GB内存运行1小时：8 × 0.02 = **0.16元**
- 2张GPU运行1小时：2 × 2.0 = **4.0元**

### 分区倍率配置

```yaml
partitions:
  default_multiplier: 1.0     # 默认倍率
  
  # 特定分区倍率
  normal: 1.0                 # 标准分区原价
  gpu: 1.5                    # GPU分区1.5倍
  debug: 0.5                  # Debug分区半价
  high_mem: 1.2               # 大内存分区1.2倍
```

**应用场景**：
- GPU分区设备成本高，设置1.5倍费率
- Debug分区用于测试，设置0.5倍优惠

**费用计算示例**（假设 cpu_per_hour=100, gpu_per_hour=200）：

| 分区 | 配置 | CPU费用 | GPU费用 | 倍率 | 总费用 |
|------|------|---------|---------|------|--------|
| cpu | -c 2 | 200元 | 0元 | 1.0 | **200元** |
| gpu | -c 2 --gres=gpu:1 | 200元 | 200元 | 1.5 | **600元** |
| debug | -c 2 | 200元 | 0元 | 0.5 | **100元** |

**重要提示**：
- 提交到 `gpu` 分区的作业费用 = 基础费用 × 1.5
- 使用 `-p gpu` 时必须确保余额充足（特别是多核+GPU组合）
- 未指定分区时，Slurm 使用默认分区（可能是 `cpu` 或 `gpu`）

### 折扣配置

```yaml
discounts:
  # 账户级别折扣 (0.0-1.0，1.0表示免费)
  accounts:
    admin: 1.0                # admin账户免费
    research: 0.2             # research账户8折(20%折扣)
    student: 0.5              # student账户5折
    
  # 用户级别折扣（优先级高于账户折扣）
  users:
    professor_zhang: 0.3      # 特定用户7折
    vip_user: 0.2             # VIP用户8折
```

**折扣计算**：
```
最终费用 = 原始费用 × 分区倍率 × (1 - 折扣)
```

### 阶梯计费配置

```yaml
billing:
  mode: "tiered"              # 启用阶梯计费
  
  tiers:
    - limit: 1000             # 0-1000核时
      multiplier: 1.0         # 原价
    - limit: 5000             # 1001-5000核时
      multiplier: 0.9         # 9折
    - limit: 10000            # 5001-10000核时
      multiplier: 0.8         # 8折
    - limit: 999999999        # 10000核时以上
      multiplier: 0.7         # 7折
```

---

## 日常使用

### 1. 数据收集

#### 手动收集

```bash
# 收集最近24小时的作业数据
slurm-bill collect

# 收集指定时间范围
slurm-bill collect --start 2024-01-01 --end 2024-01-31
```

#### 自动收集（推荐）

```bash
# 添加定时任务，每15分钟收集一次
echo '*/15 * * * * /opt/slurm-bill/cron_job.sh' | sudo crontab -u slurm -

# 查看定时任务日志
tail -f /var/log/slurm-bill/cron.log
```

### 2. 账单查询

#### 查询用户账单

```bash
# 查询指定用户（默认最近30天）
bill-query user amax

# 查询最近7天
bill-query user amax -d 7

# 查询最近一年
bill-query user amax -d 365
```

**输出示例**：
```
=====================================================================================
用户消费汇总
=====================================================================================
📋 费率: CPU 0.1元/核/小时 | GPU 2.0元/卡/小时 | 内存 0.02元/GB/小时
-------------------------------------------------------------------------------------
+------+--------+---------+---------+--------+----------+
| 用户 | 作业数 | CPU核时 | GPU卡时 | 总费用 | 平均费用 |
+------+--------+---------+---------+--------+----------+
| amax |     28 |  126.20 |    0.05 |  12.74 |     0.46 |
+------+--------+---------+---------+--------+----------+
-------------------------------------------------------------------------------------
💰 费用明细:
   CPU费用: 126.20 核时 × 0.1 元/核时 = 12.62 元
   GPU费用: 0.05 卡时 × 2.0 元/卡时 = 0.10 元
   ──────────────────────────────────────────────────
   总计费用: 12.74 元
=====================================================================================
```

#### 查询账户账单

```bash
# 查询指定账户
bill-query account research

# 查询所有账户（最近90天）
bill-query account -d 90
```

#### 消费排行榜

```bash
# Top 10 消费用户
bill-query top 10

# Top 20（最近7天）
bill-query top 20 -d 7
```

#### 查看用户作业详情

```bash
# 查看用户最近作业
bill-query jobs amax -d 7

# 查看更多条数
bill-query jobs amax -d 30 -l 100
```

#### 交互式查询模式

```bash
# 进入交互模式
bill-query

# 交互命令：
billing> user amax -d 30      # 查询用户30天账单
billing> account research     # 查询账户
billing> top 10               # 消费排行
billing> daily -d 30          # 每日统计
billing> partition            # 分区统计
billing> help                 # 显示帮助
billing> quit                 # 退出
```

### 3. 生成报表

#### 命令行报表

```bash
# 按用户分组（默认上月）
slurm-bill report --group-by user

# 按账户分组（指定时间范围）
slurm-bill report --group-by account --start 2024-01-01 --end 2024-01-31

# 按分区分组
slurm-bill report --group-by partition
```

#### 导出报表

```bash
# 导出 CSV 格式
slurm-bill report --group-by user --format csv --output bill_2024_01.csv

# 导出 JSON 格式
slurm-bill report --group-by user --format json --output bill_2024_01.json
```

### 4. 数据库直接查询

```bash
# 连接数据库
sqlite3 /var/lib/slurm-bill/billing.db

# 常用查询：
-- 用户消费汇总
SELECT user, COUNT(*), SUM(CAST(cost AS DECIMAL)) 
FROM job_records GROUP BY user;

-- 月度消费趋势
SELECT strftime('%Y-%m', end_time) as month, 
       SUM(CAST(cost AS DECIMAL)) as cost
FROM job_records GROUP BY month ORDER BY month;

-- 高费用作业TOP 10
SELECT job_id, user, elapsed, ncpus, alloc_gpus, cost
FROM job_records ORDER BY CAST(cost AS DECIMAL) DESC LIMIT 10;
```

---

## 计费原理

### 费用计算公式

```
作业费用 = (CPU费用 + 内存费用 + GPU费用 + 节点费用) 
         × 分区倍率 
         × (1 - 折扣)

其中：
- CPU费用 = CPU核数 × 运行小时数 × cpu_per_hour
- 内存费用 = 内存GB × 运行小时数 × memory_gb_per_hour  
- GPU费用 = GPU卡数 × 运行小时数 × gpu_per_hour
- 节点费用 = 节点数 × 运行小时数 × node_per_hour
```

### 计算示例

**示例1：标准计算作业**
```
作业配置: 4核CPU, 8GB内存, 无GPU
运行时长: 2小时
分区: normal (倍率1.0)
账户: research (折扣20%)

计算过程:
CPU费用 = 4核 × 2小时 × 0.1元 = 0.8元
内存费用 = 8GB × 2小时 × 0.02元 = 0.32元
GPU费用 = 0
小计 = 0.8 + 0.32 = 1.12元
应用分区倍率 = 1.12 × 1.0 = 1.12元
应用折扣 = 1.12 × (1 - 0.2) = 0.896元 ≈ 0.90元

最终费用: 0.90元
```

**示例2：GPU训练作业**
```
作业配置: 8核CPU, 32GB内存, 2张GPU
运行时长: 10小时
分区: gpu (倍率1.5)
账户: student (折扣50%)

计算过程:
CPU费用 = 8 × 10 × 0.1 = 8.0元
内存费用 = 32 × 10 × 0.02 = 6.4元
GPU费用 = 2 × 10 × 2.0 = 40.0元
小计 = 8.0 + 6.4 + 40.0 = 54.4元
应用分区倍率 = 54.4 × 1.5 = 81.6元
应用折扣 = 81.6 × (1 - 0.5) = 40.8元

最终费用: 40.8元
```

### 数据收集原理

系统通过 `sacct` 命令从 Slurm 获取作业数据：

```bash
sacct -a -P -D --format=JobID,JobName,User,Account,Partition,State,Submit,Start,End,Elapsed,NCPUS,NNodes,ReqMem,MaxRSS,AllocTRES
```

关键字段解析：
- **NCPUS**: 分配的CPU核心数
- **AllocTRES**: 分配的资源，从中解析GPU数量 (`gres/gpu=N`)
- **MaxRSS**: 实际使用的最大内存
- **Elapsed**: 运行时长

---

## 预付费系统

### 概述

预付费系统实现了**充值 → 消费 → 余额管理**的完整闭环：

- ✅ **充值管理** - 用户/账户充值、余额查询
- ✅ **余额检查** - 作业提交前自动检查余额是否充足
- ✅ **作业拦截** - 余额不足时阻止作业提交
- ✅ **自动扣费** - 作业结束后按实际使用扣费
- ✅ **信用额度** - 允许设置透支额度（允许欠费一定金额）
- ✅ **余额预警** - 低余额时提醒用户充值
- ✅ **账户控制** - 管理员可暂停/激活用户账户

### 工作流程

```
用户充值 → 提交作业 → Prolog检查余额 → 预扣费用 → 作业运行 → Epilog实际扣费
                                              ↓
                                       余额不足 → 拒绝作业
```

### 充值管理

#### 给用户充值

```bash
# 基本充值（自动获取Slurm默认账户）
recharge user amax --amount 200

# 完整参数充值
recharge user amax --amount 200 --type cash --operator admin --remark "月度充值"

# 指定特定账户充值
recharge user amax --amount 100 --account research
```

**参数说明：**
- `--amount`: 充值金额（必填，单位：元）
- `--type`: 充值类型（cash现金/transfer转账/grant拨款/adjustment调整）
- `--operator`: 操作人
- `--remark`: 备注信息
- `--account`: 指定账户（默认自动从Slurm获取用户默认账户）

#### 查询余额

```bash
# 查询特定用户
recharge query amax

# 查询所有用户
recharge list
```

**输出示例：**
```
用户: amax
账户: user
当前余额: 400.00 元
信用额度: 0.00 元
可用额度: 400.00 元
累计充值: 400.00 元
累计消费: 0.00 元
账户状态: active
```

#### 查看充值历史

```bash
# 查看特定用户的充值记录
recharge history amax

# 查看所有充值记录
recharge history

# 限制显示条数
recharge history amax --limit 10
```

**输出示例：**
```
+----+------+--------+--------+------------+------+--------+---------------------------+
| ID | 用户 |  账户  |  金额  | 充值后余额 | 类型 | 操作人 |           时间            |
+----+------+--------+--------+------------+------+--------+---------------------------+
| 6  | amax |  user  | 200.00 |   500.00   | cash | admin  | 2026-03-03T00:47:53.860838|
| 5  | amax |  user  | 200.00 |   300.00   | cash | admin  | 2026-03-03T00:47:44.463082|
| 4  | amax |  user  | 100.00 |   100.00   | cash | admin  | 2026-03-03T00:46:50.888654|
+----+------+--------+--------+------------+------+--------+---------------------------+
```

### 账户控制

#### 设置信用额度

允许用户在余额为0时仍可提交作业，最多欠费指定金额。

```bash
# 给用户设置50元信用额度（允许欠费50元）
recharge set-credit amax --amount 50

# 示例：
# - 余额0元时仍可提交作业
# - 最多可欠费50元
# - 欠费达到50元后才会被阻止
```

#### 设置余额预警

```bash
# 余额低于20元时预警
recharge set-alert amax --amount 20

# 用户提交作业时会看到：
# ⚠ 余额预警：当前余额 15.00 元，请及时充值
```

#### 暂停/激活用户

```bash
# 暂停用户（禁止提交作业）
recharge suspend amax

# 激活用户
recharge activate amax
```

### 与Slurm集成

#### 集成步骤

1. **复制脚本到Slurm目录**
```bash
sudo cp /opt/slurm-bill/slurm_prolog.py /etc/slurm/
sudo cp /opt/slurm-bill/slurm_epilog.py /etc/slurm/
sudo chmod 755 /etc/slurm/slurm_prolog.py
sudo chmod 755 /etc/slurm/slurm_epilog.py
```

2. **修改slurm.conf**
```bash
sudo vim /etc/slurm/slurm.conf
# 添加以下配置：
# 注意：使用 PrologSlurmctld（在 slurmctld 上运行）
# 而不是 Prolog（在计算节点上运行）
PrologSlurmctld=/etc/slurm/slurm_prolog.py
Epilog=/etc/slurm/slurm_epilog.py
PrologFlags=Alloc
```

3. **重启Slurm服务**
```bash
sudo systemctl restart slurmctld
sudo systemctl restart slurmd
```

#### 余额检查机制

**作业提交时（Prolog）：**
1. 估算作业费用：`预估费用 = (CPU费用 + 内存费用 + GPU费用) × 分区倍率`
2. 检查余额：`可用额度 = 当前余额 + 信用额度`
3. 判断：
   - 可用额度 ≥ 预估费用 → 允许提交，预扣费用
   - 可用额度 < 预估费用 → 拒绝提交，提示充值

**重要提示**：
- 未指定 `--time` 时，Slurm 默认给 1 年时间限制，会导致预估费用极高
- **必须**使用 `--time` 指定合理的时间限制，例如：`--time=00:30:00`

**作业结束时（Epilog）：**
1. 获取实际运行时间和资源使用
2. 计算实际费用
3. 多退少补：
   - 实际 < 预估：退还差额到余额
   - 实际 > 预估：补扣差额
   - 实际 = 预估：正常扣费

#### 余额不足时的提示

```bash
$ sbatch --wrap="sleep 60" --cpus-per-task=4
sbatch: error: Batch job submission failed: 
余额不足！当前余额 5.00 元，预估费用 8.00 元，还需充值 3.00 元
请使用以下命令充值:
  recharge user username --amount 100
```

### 数据库表结构

#### account_balance 表（余额表）

| 字段 | 类型 | 说明 |
|------|------|------|
| user | TEXT | 用户名 |
| account | TEXT | 账户名（自动从Slurm获取） |
| balance | TEXT | 当前余额 |
| credit_limit | TEXT | 信用额度（允许欠费额度） |
| total_recharged | TEXT | 累计充值金额 |
| total_consumed | TEXT | 累计消费金额 |
| alert_threshold | TEXT | 余额预警阈值 |
| status | TEXT | 状态：active/suspended/frozen |

#### recharge_records 表（充值记录表）

| 字段 | 说明 |
|------|------|
| user | 用户名 |
| account | 账户名 |
| amount | 充值金额 |
| balance_after | 充值后余额 |
| recharge_type | 充值类型 |
| operator | 操作人 |
| remark | 备注 |
| created_at | 充值时间 |

#### consumption_records 表（消费记录表）

| 字段 | 说明 |
|------|------|
| job_id | 作业ID |
| user | 用户名 |
| estimated_cost | 预估费用（Prolog时计算） |
| actual_cost | 实际费用（Epilog时计算） |
| status | 状态：reserved/charged/refunded/failed |
| created_at | 创建时间 |
| charged_at | 扣费时间 |

### 监控和日志

#### 查看被拒绝的作业

```bash
# 查看拒绝日志
cat /var/log/slurm-bill/rejected_jobs.log

# 格式: job_id|user|account|estimated_cost|balance|timestamp
```

#### 查看实时消费记录

```bash
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT * FROM consumption_records WHERE user='amax';"
```

#### 查看余额不足的用户

```bash
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT user, balance FROM account_balance WHERE balance < 20;"
```

---

## 数据管理

### 查看计费数据详情

查询输出会显示费率信息和费用明细：

```
=====================================================================================
用户消费汇总
=====================================================================================
📋 费率: CPU 0.1元/核/小时 | GPU 2.0元/卡/小时 | 内存 0.02元/GB/小时
-------------------------------------------------------------------------------------
+------+--------+---------+---------+--------+----------+
| 用户 | 作业数 | CPU核时 | GPU卡时 | 总费用 | 平均费用 |
+------+--------+---------+---------+--------+----------+
| amax |     28 |  126.20 |    0.05 |  12.74 |     0.46 |
+------+--------+---------+---------+--------+----------+
-------------------------------------------------------------------------------------
💰 费用明细:
   CPU费用: 126.20 核时 × 0.1 元/核时 = 12.62 元
   GPU费用: 0.05 卡时 × 2.0 元/卡时 = 0.10 元
   ──────────────────────────────────────────────────
   总计费用: 12.74 元
=====================================================================================
```

### 重新计算费用（修改费率后）

当修改 `config.yaml` 中的费率后，历史作业的费用不会自动更新。使用 `recalc-costs` 命令重新计算：

```bash
# 试运行（查看变化但不更新）
recalc-costs --dry-run

# 更新最近30天的作业
recalc-costs --days 30

# 更新所有历史作业
recalc-costs
```

**使用场景**：
- 调整 CPU/GPU 费率后，同步更新历史账单
- 修复计费错误后重新计算

### 删除计费数据

#### 方法1: 使用 delete-billing 工具（推荐）

```bash
# 删除指定用户的所有计费记录
delete-billing user amax

# 删除指定账户的所有记录
delete-billing account research

# 删除某日期之前的记录
delete-billing before 2024-01-01

# 清空所有数据（危险！）
delete-billing all
```

#### 方法2: 使用管理脚本（交互式）

```bash
sudo /opt/slurm-bill/manage_billing.sh
```

菜单功能：
- 查看所有用户
- 删除指定用户数据
- 删除指定账户数据
- 按时间范围删除
- 清空所有数据

#### 方法3: 直接 SQL 操作

```bash
# 删除特定用户
sqlite3 /var/lib/slurm-bill/billing.db \
  "DELETE FROM job_records WHERE user='amax';"

# 删除多个用户
sqlite3 /var/lib/slurm-bill/billing.db \
  "DELETE FROM job_records WHERE user IN ('user1', 'user2');"

# 删除整个账户
sqlite3 /var/lib/slurm-bill/billing.db \
  "DELETE FROM job_records WHERE account='research';"

# 清空所有数据（危险！）
sqlite3 /var/lib/slurm-bill/billing.db \
  "DELETE FROM job_records;"
```

**⚠️ 注意**: 
- 删除前建议先备份数据库
- 删除后数据无法恢复，但可以重新运行 `slurm-bill collect` 重新收集
- 删除只影响计费数据库，不影响 Slurm 本身的作业记录

### 数据库备份

#### 自动备份脚本

```bash
# 运行备份脚本
sudo /opt/slurm-bill/backup.sh

# 备份文件保存在: /var/backups/slurm-bill/billing_YYYYMMDD_HHMMSS.db.gz
```

#### 手动备份

```bash
# 备份数据库
cp /var/lib/slurm-bill/billing.db \
   /backup/billing.db.$(date +%Y%m%d)

# 或使用 sqlite3 备份
sqlite3 /var/lib/slurm-bill/billing.db \
  ".backup /backup/billing_backup.db"
```

#### 定期自动备份（crontab）

```bash
# 每天凌晨2点自动备份
0 2 * * * /opt/slurm-bill/backup.sh
```

---

## API接口

### 启动 Web API 服务

```bash
# 方式1: 独立运行
python3 /root/slurm-bill/web_integration.py

# 方式2: 集成到 Slurm-Web
# 在 Slurm-Web 的 app.py 中添加：
from web_integration import register_billing_routes
register_billing_routes(app)
```

### API 端点

#### 1. 获取计费汇总

```bash
GET /api/billing/summary?days=30
```

**响应示例**：
```json
{
  "success": true,
  "data": {
    "summary": {
      "total_jobs": 156,
      "active_users": 12,
      "total_cost": 1280.50
    },
    "daily_trend": [...],
    "top_users": [...]
  }
}
```

#### 2. 获取用户列表

```bash
GET /api/billing/users?days=30
```

#### 3. 获取用户详情

```bash
GET /api/billing/user/{username}?days=30
```

#### 4. 获取实时统计

```bash
GET /api/billing/realtime
```

**响应示例**：
```json
{
  "success": true,
  "data": {
    "today_cost": 45.20,
    "month_cost": 1280.50,
    "today_jobs": 12,
    "active_users": 8
  }
}
```

---

## 故障排除

### 常见问题

#### Q1: 收集不到作业数据

**症状**：`slurm-bill collect` 显示 "成功收集 0 条作业记录"

**排查步骤**：
```bash
# 1. 检查 sacct 是否可用
sacct --version

# 2. 检查是否有作业数据
sacct -a -X --format=JobID,User,State | head -10

# 3. 检查时间范围
sacct -a -X --starttime=2024-01-01 | head -10

# 4. 检查 Slurm accounting 配置
scontrol show config | grep AccountingStorageType
# 应该显示: accounting_storage/slurmdbd
```

**解决方案**：
- 确保 `slurmdbd` 服务正在运行
- 检查作业状态是否包含 `COMPLETED`, `FAILED`, `CANCELLED` 等

#### Q2: 费用计算明显偏高/偏低

**症状**：费用与预期不符

**排查步骤**：
```bash
# 检查费率配置
cat /etc/slurm-bill/config.yaml

# 检查分区倍率
grep -A10 "partitions:" /etc/slurm-bill/config.yaml

# 检查折扣配置
grep -A10 "discounts:" /etc/slurm-bill/config.yaml

# 查看单个作业费用详情
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT job_id, ncpus, alloc_gpus, elapsed, cost FROM job_records WHERE job_id='123';"
```

#### Q3: 报表数据不完整

**症状**：某些作业未出现在报表中

**原因**：默认报表只统计 `COMPLETED` 状态作业

**解决方案**：
```sql
-- 查看所有状态的作业费用
SELECT state, COUNT(*), SUM(CAST(cost AS DECIMAL)) 
FROM job_records GROUP BY state;
```

#### Q4: 数据库锁定或损坏

**症状**：查询时报错 `database is locked`

**解决方案**：
```bash
# 备份数据库
cp /var/lib/slurm-bill/billing.db /backup/billing.db.bak

# 修复数据库
sqlite3 /var/lib/slurm-bill/billing.db ".recover" | sqlite3 /var/lib/slurm-bill/billing.db.fixed
mv /var/lib/slurm-bill/billing.db.fixed /var/lib/slurm-bill/billing.db

# 重启服务
```

#### Q5: 节点状态为 DRAIN (Prolog error)

**症状**：`sinfo` 显示节点状态为 `DRAIN`，Reason 为 `Prolog error`

**原因**：Prolog 脚本执行失败，Slurm 自动将节点置为 DRAIN 状态

**解决方案**：
```bash
# 1. 检查 Prolog 脚本权限和语法
ls -la /etc/slurm/slurm_prolog.py
python3 -m py_compile /etc/slurm/slurm_prolog.py

# 2. 检查脚本是否能正常执行
sudo /etc/slurm/slurm_prolog.py
echo $?  # 应该返回 0 或 1

# 3. 检查 slurmctld 日志
sudo grep "PrologSlurmctld" /var/log/slurm/slurmctld.log

# 4. 修复后恢复节点
sudo scontrol update nodename=amax state=resume

# 5. 重启 slurmctld
sudo systemctl restart slurmctld
```

#### Q6: 作业一直 PD（挂起）不运行

**症状**：作业提交后一直处于 `PD` 状态，Reason 为 `BeginTime` 或无

**排查步骤**：
```bash
# 1. 检查作业状态
scontrol show job <job_id>

# 2. 检查余额是否充足（注意分区倍率）
recharge query <username>

# 3. 检查预估费用
# gpu 分区默认 1.5 倍费率
# 2核CPU + 1GPU = (2×100 + 1×200) × 1.5 = 600元/小时

# 4. 检查是否指定了时间限制
scontrol show job <job_id> | grep TimeLimit
```

**常见原因**：

1. **余额不足** - 特别是 gpu 分区有 1.5 倍倍率
2. **未指定 --time** - Slurm 默认给 1 年时间，预估费用极高
3. **Prolog 脚本失败** - 检查 slurmctld 日志

**正确提交示例**：
```bash
# ✅ 正确：指定时间限制和分区
sbatch --wrap="sleep 10" --gres=gpu:1 -c 2 -p gpu --time=00:10

# ❌ 错误：未指定时间，默认1年
sbatch --wrap="sleep 10" --gres=gpu:1 -c 2 -p gpu
```

### 日志查看

```bash
# 计费系统日志
tail -f /var/log/slurm-bill/billing.log

# 定时任务日志
tail -f /var/log/slurm-bill/cron.log
```

### 数据备份

```bash
#!/bin/bash
# 备份脚本 /opt/slurm-bill/backup.sh

BACKUP_DIR="/backup/slurm-bill"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

# 备份数据库
sqlite3 /var/lib/slurm-bill/billing.db ".backup ${BACKUP_DIR}/billing_${DATE}.db"

# 备份配置
cp /etc/slurm-bill/config.yaml ${BACKUP_DIR}/config_${DATE}.yaml

# 保留最近30个备份
ls -t ${BACKUP_DIR}/billing_*.db | tail -n +31 | xargs -r rm
ls -t ${BACKUP_DIR}/config_*.yaml | tail -n +31 | xargs -r rm

echo "备份完成: ${DATE}"
```

---

## 最佳实践

### 1. 费率设置建议

```yaml
# 高校/科研机构推荐费率
billing:
  cpu_per_hour: 0.05          # 5分钱/核/小时
  memory_gb_per_hour: 0.01    # 1分钱/GB/小时
  gpu_per_hour: 1.00          # 1元/卡/小时

# 商业环境推荐费率
billing:
  cpu_per_hour: 0.20
  memory_gb_per_hour: 0.05
  gpu_per_hour: 5.00
```

### 2. 定期维护

```bash
# 每周清理旧数据（保留2年）
sqlite3 /var/lib/slurm-bill/billing.db \
  "DELETE FROM job_records WHERE end_time < date('now', '-2 years');"

# 每月生成月度报表
slurm-bill report --group-by user --start $(date -d "last month" +%Y-%m-01) \
  --format csv --output /var/lib/slurm-bill/reports/monthly_$(date +%Y%m).csv
```

### 3. 安全建议

```bash
# 设置数据库权限
chmod 640 /var/lib/slurm-bill/billing.db
chown slurm:slurm /var/lib/slurm-bill/billing.db

# 配置文件权限
chmod 644 /etc/slurm-bill/config.yaml
```

---

## 联系支持

如有问题，请：
1. 查看日志文件 `/var/log/slurm-bill/billing.log`
2. 运行测试脚本 `python3 test_billing.py`
3. 检查数据库状态 `sqlite3 /var/lib/slurm-bill/billing.db ".tables"`

---

**文档版本**: 1.0.0  
**最后更新**: 2026-03-03
