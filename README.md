# Slurm Billing System - Slurm 计费系统

一个功能完善的 Slurm 作业计费系统，支持多维度资源计费、折扣配置、报表生成和查询功能。

## 功能特性

- ✅ **多维度计费**：CPU、内存、GPU、节点级别计费
- ✅ **灵活费率**：支持分区倍率、账户折扣、用户折扣
- ✅ **阶梯计费**：支持阶梯价格（用量越大，单价越低）
- ✅ **费用明细**：查询结果显示费率信息和分项费用明细
- ✅ **预付费系统**：充值管理、余额检查、作业拦截、自动扣费
- ✅ **信用额度**：支持设置透支额度，允许欠费一定金额
- ✅ **余额预警**：低余额时自动提醒用户充值
- ✅ **费用重算**：修改费率后可重新计算历史作业费用
- ✅ **数据存储**：SQLite 数据库存储，轻量高效
- ✅ **报表生成**：支持 Table/JSON/CSV 格式导出
- ✅ **交互查询**：交互式命令行查询工具
- ✅ **数据管理**：支持删除指定用户/账户/时间的计费数据
- ✅ **自动备份**：内置备份脚本，支持定时备份
- ✅ **定时任务**：自动收集和计费
- ✅ **Web 集成**：可与 Slurm-Web 集成显示

## 快速开始

### 1. 安装

```bash
cd /root/slurm-bill
sudo bash install.sh
```

### 2. 配置费率

编辑 `/etc/slurm-bill/config.yaml`：

```yaml
billing:
  cpu_per_hour: 0.10          # CPU 核心每小时 0.1 元
  memory_gb_per_hour: 0.02    # 内存每GB每小时 0.02 元
  gpu_per_hour: 2.00          # GPU 每小时 2 元

discounts:
  accounts:
    research: 0.2             # research 账户 8 折
    student: 0.5              # student 账户 5 折
  
  users:
    admin: 1.0                # admin 用户免费
```

### 3. 测试运行

```bash
# 立即收集一次作业数据
sudo slurm-bill collect

# 查询用户账单
bill-query user username -d 7

# 生成报表
slurm-bill report --group-by account
```

### 4. 设置定时任务

```bash
# 添加 crontab（每15分钟执行一次）
echo '*/15 * * * * /opt/slurm-bill/cron_job.sh' | sudo crontab -u slurm -
```

### 5. 启用预付费系统（可选）

```bash
# 1. 给用户充值
recharge user amax --amount 200

# 2. 查询余额
recharge query amax

# 3. 集成到Slurm（启用余额检查和作业拦截）
sudo cp /opt/slurm-bill/slurm_prolog.py /etc/slurm/
sudo cp /opt/slurm-bill/slurm_epilog.py /etc/slurm/
sudo chmod 755 /etc/slurm/slurm_prolog.py /etc/slurm/slurm_epilog.py

# 4. 编辑 slurm.conf 添加：
# PrologSlurmctld=/etc/slurm/slurm_prolog.py  # 注意：使用 PrologSlurmctld
# Epilog=/etc/slurm/slurm_epilog.py
# PrologFlags=Alloc
sudo vim /etc/slurm/slurm.conf

# 5. 重启 Slurm
sudo systemctl restart slurmctld

# 6. 测试：余额不足时作业会被拒绝
# 注意：必须指定 --time，否则默认1年时间限制会导致预估费用极高
sbatch --wrap="sleep 60" --cpus-per-task=4 --time=00:10  # 如果余额不足会被拒绝
```

## 命令使用指南

### slurm-bill 命令

```bash
# 收集计费数据
slurm-bill collect
slurm-bill collect --start 2024-01-01 --end 2024-01-31

# 生成报表
slurm-bill report
slurm-bill report --group-by account --start 2024-01-01 --format csv
slurm-bill report --group-by user --format json --output bill.json

# 查询详细记录
slurm-bill query --user username --start 2024-01-01
slurm-bill query --account research --limit 100
```

### bill-query 命令

```bash
# 交互式模式
bill-query

# 查询用户消费
bill-query user username
c bill-query user username -d 30

# 查询账户消费
bill-query account research -d 30

# 查看消费排行
bill-query top 10
bill-query top 20 -d 7

# 每日统计
bill-query daily -d 30

# 分区统计
bill-query partition

# 查看用户作业详情
bill-query jobs username -d 7
```

### 预付费系统命令 (recharge)

```bash
# 给用户充值（自动获取Slurm默认账户）
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

### 数据管理命令

```bash
# 删除指定用户的计费记录
delete-billing user amax

# 删除指定账户的计费记录
delete-billing account research

# 删除某日期之前的数据
delete-billing before 2024-01-01

# 清空所有计费数据（危险！）
delete-billing all

# 交互式数据管理
sudo /opt/slurm-bill/manage_billing.sh

# 备份数据库
sudo /opt/slurm-bill/backup.sh
```

## 计费计算公式

```
费用 = (CPU费用 + 内存费用 + GPU费用 + 节点费用) × 分区倍率 × (1 - 折扣)

其中：
- CPU费用 = CPU核数 × 运行小时数 × CPU单价
- 内存费用 = 内存GB × 运行小时数 × 内存单价
- GPU费用 = GPU卡数 × 运行小时数 × GPU单价
- 节点费用 = 节点数 × 运行小时数 × 节点单价（可选）
```

## 查询输出示例

查询结果会显示费率信息和费用明细：

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

## 配置文件详解

### 基础计费配置

```yaml
billing:
  cpu_per_hour: 0.10          # CPU 单价（元/核/小时）
  memory_gb_per_hour: 0.02    # 内存单价（元/GB/小时）
  gpu_per_hour: 2.00          # GPU 单价（元/卡/小时）
  node_per_hour: 0.00         # 节点单价（可选）
  currency: "CNY"             # 货币单位
  min_charge: 0.01            # 最低消费
  rounding: "0.01"            # 四舍五入精度
```

### 分区倍率

```yaml
partitions:
  default_multiplier: 1.0
  normal: 1.0
  gpu: 1.5          # GPU 分区 1.5 倍
  debug: 0.5        # Debug 分区半价
  high_mem: 1.2     # 大内存分区 1.2 倍
```

### 折扣配置

```yaml
discounts:
  accounts:         # 账户级别折扣
    admin: 1.0      # 免费
    research: 0.2   # 8 折
    student: 0.5    # 5 折
  
  users:            # 用户级别折扣（优先级更高）
    vip_user: 0.3   # 7 折
```

### 阶梯计费

```yaml
billing:
  mode: "tiered"    # 启用阶梯计费
  tiers:
    - limit: 1000
      multiplier: 1.0
    - limit: 5000
      multiplier: 0.9    # 9 折
    - limit: 10000
      multiplier: 0.8    # 8 折
    - limit: 999999999
      multiplier: 0.7    # 7 折
```

## 数据库结构

### job_records 表

| 字段 | 类型 | 说明 |
|-----|-----|-----|
| job_id | TEXT | 作业 ID |
| user | TEXT | 用户名 |
| account | TEXT | 账户名 |
| partition | TEXT | 分区 |
| state | TEXT | 作业状态 |
| elapsed_seconds | INTEGER | 运行秒数 |
| ncpus | INTEGER | CPU 核数 |
| alloc_gpus | INTEGER | GPU 数量 |
| billing_units | TEXT | 原始计费单位 |
| cost | TEXT | 最终费用 |

### 其他表

- `billing_cycles`：计费周期
- `account_balance`：账户余额

## 与 Slurm-Web 集成

在 `config.yaml` 中启用 Web 集成：

```yaml
web:
  slurm_web_integration:
    enabled: true
    api_endpoint: "http://localhost:5000/api/billing"
```

然后在 Slurm-Web 中添加计费显示模块（需要额外开发前端组件）。

## 常见问题

### Q: 如何修改已计费作业的费用？

直接操作数据库：

```bash
sqlite3 /var/lib/slurm-bill/billing.db
UPDATE job_records SET cost='100.00' WHERE job_id='12345';
```

### Q: 如何清空测试数据？

```bash
# 方法1: 使用删除工具（推荐）
delete-billing all

# 方法2: 重新初始化数据库
sudo rm /var/lib/slurm-bill/billing.db
sudo slurm-bill init

# 方法3: 只删除特定用户的数据
delete-billing user testuser
```

### Q: 支持其他数据库吗？

目前仅支持 SQLite。如需 MySQL/PostgreSQL，需要修改 `DatabaseManager` 类。

### Q: 如何备份数据？

```bash
# 方法1: 使用备份脚本（推荐）
sudo /opt/slurm-bill/backup.sh

# 方法2: 手动备份
cp /var/lib/slurm-bill/billing.db /backup/billing-$(date +%Y%m%d).db

# 方法3: SQLite备份
sqlite3 /var/lib/slurm-bill/billing.db ".backup /backup/billing_backup.db"

# 设置定时自动备份（每天凌晨2点）
0 2 * * * /opt/slurm-bill/backup.sh
```

备份文件保存在 `/var/backups/slurm-bill/`，自动保留最近30个备份。

## 文件结构

```
/root/slurm-bill/
├── slurm_bill.py          # 主计费程序
├── bill_query.py          # 查询工具
├── balance_manager.py     # 余额管理模块（预付费系统核心）
├── recharge_cli.py        # 充值管理CLI
├── slurm_prolog.py        # Slurm Prolog脚本（余额检查/作业拦截）
├── slurm_epilog.py        # Slurm Epilog脚本（自动扣费）
├── delete_billing.py      # 数据删除工具
├── web_integration.py     # Web API集成
├── test_billing.py        # 测试脚本
├── cron_job.sh            # 定时任务脚本
├── backup.sh              # 备份脚本
├── manage_billing.sh      # 数据管理脚本
├── config.yaml            # 示例配置文件
├── install.sh             # 安装脚本
├── README.md              # 使用文档
├── GUIDE.md               # 完整使用指南
└── QUICKSTART.md          # 快速入门指南

安装后：
/etc/slurm-bill/
└── config.yaml        # 配置文件

/var/lib/slurm-bill/
├── billing.db         # SQLite 数据库
└── reports/           # 报表导出目录

/var/log/slurm-bill/
├── billing.log        # 主日志
└── cron.log           # 定时任务日志

/opt/slurm-bill/
├── slurm_bill.py
├── bill_query.py
└── cron_job.sh
```

## 开发计划

- [ ] Web 管理界面
- [ ] 邮件账单通知
- [ ] 配额限制和预警
- [ ] 多币种支持
- [ ] 与财务系统对接 API
- [ ] 图表可视化

## 许可证

MIT License

## 技术支持

如有问题，请提交 Issue 或联系系统管理员。

---

## Web 管理界面 (New!)

现已提供精美的Web管理界面，支持可视化操作所有计费功能。

### 快速启动 Web 界面

```bash
cd web
bash start.sh
```

访问 http://localhost:5000，默认密码为 `changeme`

### Web 界面功能

- **仪表盘**: 实时费用统计、趋势图表、消费排行
- **用户管理**: 余额查询、充值、信用额度设置、账户状态管理
- **作业查询**: 多条件筛选、作业详情查看、费用明细
- **统计报表**: 每日/账户/分区多维度统计，支持图表展示
- **系统设置**: 费率配置查看、系统信息

### Web 界面截图

*精美的现代化界面设计，包含数据可视化图表*

### Web 界面技术栈

- **后端**: Flask + Flask-Login
- **前端**: Tailwind CSS + Chart.js + Font Awesome
- **特性**: 响应式设计、数据可视化、实时搜索、导出功能

更多详情请查看 `web/README.md`

