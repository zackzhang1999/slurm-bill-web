# Slurm 预付费计费系统完整指南

## 系统概述

这是一个完整的**预付费计费系统**，实现了：

- ✅ **充值管理** - 用户/账户充值、余额查询
- ✅ **余额检查** - 作业提交前自动检查余额
- ✅ **作业拦截** - 余额不足时阻止作业提交
- ✅ **自动扣费** - 作业结束后按实际使用扣费
- ✅ **信用额度** - 允许设置透支额度
- ✅ **余额预警** - 低余额时提醒用户
- ✅ **账户控制** - 暂停/激活用户账户

## 工作流程

```
┌─────────────────────────────────────────────────────────────┐
│                      预付费工作流程                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. 用户充值                                                │
│     $ recharge user amax --amount 200                       │
│                                                             │
│  2. 提交作业                                                │
│     $ sbatch --cpus-per-task=4 --time=1:00 job.sh           │
│                                                             │
│  3. Slurm Prolog 检查余额                                   │
│     - 估算费用: 4核 × 1小时 × 0.1元 = 0.4元                │
│     - 检查余额: 200元 > 0.4元 ✓                            │
│     - 预扣费用: 冻结0.4元                                   │
│                                                             │
│  4. 作业运行                                                │
│     - 正常运行...                                           │
│                                                             │
│  5. 作业结束                                                │
│     $ Slurm Epilog 实际扣费                                 │
│     - 实际运行: 1小时                                       │
│     - 实际费用: 0.4元                                       │
│     - 确认扣费: 余额 199.6元                                │
│                                                             │
│  6. 余额不足时                                              │
│     $ sbatch job.sh                                         │
│     sbatch: error: 余额不足！当前余额 5元，还需充值...      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 安装新模块

```bash
# 复制新文件到安装目录
sudo cp /root/slurm-bill/balance_manager.py /opt/slurm-bill/
sudo cp /root/slurm-bill/slurm_prolog.py /opt/slurm-bill/
sudo cp /root/slurm-bill/slurm_epilog.py /opt/slurm-bill/
sudo cp /root/slurm-bill/recharge_cli.py /opt/slurm-bill/

# 创建命令软链接
sudo ln -sf /opt/slurm-bill/recharge_cli.py /usr/local/bin/recharge

# 设置权限
sudo chmod +x /opt/slurm-bill/*.py
```

### 2. 集成到 Slurm

```bash
# 复制脚本到 Slurm 目录
sudo cp /opt/slurm-bill/slurm_prolog.py /etc/slurm/
sudo cp /opt/slurm-bill/slurm_epilog.py /etc/slurm/
sudo chmod 755 /etc/slurm/slurm_prolog.py
sudo chmod 755 /etc/slurm/slurm_epilog.py

# 修改 slurm.conf
sudo vim /etc/slurm/slurm.conf
# 添加:
Prolog=/etc/slurm/slurm_prolog.py
Epilog=/etc/slurm/slurm_epilog.py

# 重启 Slurm
sudo systemctl restart slurmctld
```

### 3. 给用户充值

```bash
# 给用户充值 200 元
recharge user amax --amount 200 --operator admin

# 查询余额
recharge query amax
```

### 4. 测试作业提交

```bash
# 切换到测试用户
su - amax

# 提交作业（应该成功，因为有余额）
sbatch --wrap="sleep 10" --cpus-per-task=1 --time=0:10

# 查看作业状态
squeue

# 作业结束后查看扣费
recharge query amax
```

## 命令详解

### 充值管理 `recharge`

```bash
# 给用户充值
recharge user <username> --amount <金额>

# 示例
recharge user amax --amount 100
recharge user amax --amount 100 --type cash --operator admin --remark "月度充值"
```

**参数说明：**
- `--amount`: 充值金额（必填）
- `--type`: 充值类型（cash现金, transfer转账, grant拨款, adjustment调整）
- `--operator`: 操作人
- `--remark`: 备注

### 余额查询

```bash
# 查询特定用户
recharge query amax

# 查询所有用户
recharge list
```

**输出示例：**
```
用户: amax
账户: default
当前余额: 400.00 元
信用额度: 0.00 元
可用额度: 400.00 元
累计充值: 400.00 元
累计消费: 0.00 元
账户状态: active
```

### 充值历史

```bash
recharge history amax
```

**输出：**
```
+----+------+---------+--------+------------+------+--------+----------------------------+
| ID | 用户 |   账户  |  金额  | 充值后余额 | 类型 | 操作人 |            时间            |
+----+------+---------+--------+------------+------+--------+----------------------------+
| 1  | amax | default | 200.00 |   200.00   | cash | admin  | 2026-03-03T00:32:00.608286 |
+----+------+---------+--------+------------+------+--------+----------------------------+
```

### 账户控制

```bash
# 暂停用户（禁止提交作业）
recharge suspend amax

# 激活用户
recharge activate amax

# 设置信用额度（允许欠费额度）
recharge set-credit amax --amount 50

# 设置余额预警阈值
recharge set-alert amax --amount 20
```

## 高级功能

### 1. 信用额度

允许用户在余额为0时仍可提交作业，最多欠费指定金额。

```bash
# 给用户设置 50 元信用额度
recharge set-credit amax --amount 50

# 这样：
# - 余额 0 元时仍可提交作业
# - 最多可欠费 50 元
# - 欠费达到 50 元后才会被阻止
```

### 2. 余额预警

当余额低于阈值时，用户提交作业会看到预警信息。

```bash
# 设置预警阈值 20 元
recharge set-alert amax --amount 20

# 当余额低于 20 元时：
# ⚠ 余额预警：当前余额 15.00 元，请及时充值
```

### 3. 批量充值

```bash
# 给多个用户充值
for user in user1 user2 user3; do
    recharge user $user --amount 100 --operator admin
done
```

## 计费规则

### 费用计算

```
预估费用 = (CPU核数 × 小时数 × CPU单价) +
          (内存GB × 小时数 × 内存单价) +
          (GPU卡数 × 小时数 × GPU单价)

实际费用 = 根据实际运行时间和资源使用计算
```

### 预扣机制

1. **作业提交时**：预扣预估费用
2. **作业运行时**：费用被冻结
3. **作业结束时**：
   - 实际费用 < 预估费用：退还差额
   - 实际费用 > 预估费用：补扣差额
   - 实际费用 = 预估费用：正常扣费

### 余额检查逻辑

```python
可用额度 = 当前余额 + 信用额度

if 可用额度 >= 预估费用:
    允许提交作业
else:
    拒绝作业，提示充值
```

## 监控和日志

### 查看被拒绝的作业

```bash
# 查看拒绝日志
cat /var/log/slurm-bill/rejected_jobs.log

# 格式: job_id|user|account|estimated_cost|balance|timestamp
12345|amax|default|10.00|5.00|2026-03-03T10:00:00
```

### 查看消费记录

```bash
# 查看实时消费记录（预扣和实际扣费）
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT * FROM consumption_records WHERE user='amax';"
```

### 查看所有余额

```bash
recharge list
```

## 故障排查

### 问题1: 作业提交时说余额不足，但查询有余额

**原因**：可能是预估费用计算错误或表未初始化

**解决**：
```bash
# 检查费率配置
cat /etc/slurm-bill/config.yaml

# 重新初始化余额表
python3 /opt/slurm-bill/balance_manager.py

# 再次查询
recharge query amax
```

### 问题2: Prolog脚本不执行

**检查**：
```bash
# 检查脚本是否存在
ls -la /etc/slurm/slurm_prolog.py

# 检查权限
sudo chmod 755 /etc/slurm/slurm_prolog.py

# 检查日志
tail -f /var/log/slurm/slurmctld.log

# 手动测试
sudo SLURM_JOB_ID=123 SLURM_JOB_USER=amax /etc/slurm/slurm_prolog.py
echo $?  # 应该返回0或1
```

### 问题3: 扣费不准确

**检查**：
```bash
# 查看作业实际使用
sacct -j <job_id> --format=JobID,Elapsed,CPUTime,MaxRSS

# 查看消费记录
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT * FROM consumption_records WHERE job_id='12345';"
```

## 数据库表结构

### account_balance 表

| 字段 | 说明 |
|------|------|
| user | 用户名 |
| account | 账户名 |
| balance | 当前余额 |
| credit_limit | 信用额度 |
| total_recharged | 累计充值 |
| total_consumed | 累计消费 |
| alert_threshold | 预警阈值 |
| status | 状态(active/suspended/frozen) |

### recharge_records 表

| 字段 | 说明 |
|------|------|
| user | 用户名 |
| amount | 充值金额 |
| balance_after | 充值后余额 |
| recharge_type | 充值类型 |
| operator | 操作人 |
| remark | 备注 |

### consumption_records 表

| 字段 | 说明 |
|------|------|
| job_id | 作业ID |
| user | 用户名 |
| estimated_cost | 预估费用 |
| actual_cost | 实际费用 |
| status | 状态(reserved/charged/refunded) |

## 最佳实践

### 1. 定期充值提醒

```bash
# 查看余额不足的用户
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT user, balance FROM account_balance WHERE balance < 20;"

# 发送邮件提醒（需要配置邮件）
```

### 2. 自动充值（可选）

可以对接财务系统自动充值：

```python
# 示例：从财务系统同步充值记录
from balance_manager import BalanceManager

manager = BalanceManager()
manager.recharge(
    user='amax',
    amount=Decimal('100'),
    recharge_type='transfer',
    operator='finance_system',
    remark='财务系统自动充值'
)
```

### 3. 月末结算

```bash
# 导出月度消费报表
slurm-bill report --start 2026-02-01 --end 2026-02-28 --format csv -o feb_bill.csv

# 清空余额（如果需要）
# 注意：这会清空所有余额，谨慎操作！
```

## 安全注意事项

1. **数据库安全**: 确保数据库文件权限正确
   ```bash
   chmod 644 /var/lib/slurm-bill/billing.db
   chown slurm:slurm /var/lib/slurm-bill/billing.db
   ```

2. **脚本安全**: Prolog/Epilog 以 root 运行，确保脚本安全
   - 不要允许用户修改脚本
   - 定期检查脚本完整性

3. **备份**: 定期备份数据库
   ```bash
   /opt/slurm-bill/backup.sh
   ```

## 联系支持

如有问题，请检查：
1. 日志文件 `/var/log/slurm-bill/billing.log`
2. Slurm 日志 `/var/log/slurm/slurmctld.log`
3. 数据库状态 `sqlite3 /var/lib/slurm-bill/billing.db ".tables"`

---

**文档版本**: 1.0  
**最后更新**: 2026-03-03
