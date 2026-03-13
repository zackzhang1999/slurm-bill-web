# Slurm 计费系统集成指南

## 概述

本指南说明如何将计费系统与 Slurm 集成，实现：**余额检查 → 作业拦截 → 自动扣费** 的完整流程。

## 集成架构

```
用户提交作业
    ↓
[Slurm Prolog] ← 检查余额
    ↓ 余额充足？
   是 → 预扣费用 → 作业运行
   否 → 拒绝作业 → 提示充值
    ↓
[Slurm Epilog] ← 作业结束
    ↓
实际扣费（多退少补）
```

## 集成步骤

### 1. 复制脚本到 Slurm 目录

```bash
# 复制 Prolog/Epilog 脚本
sudo cp /opt/slurm-bill/slurm_prolog.py /etc/slurm/
sudo cp /opt/slurm-bill/slurm_epilog.py /etc/slurm/

# 设置权限（必须root执行，因为需要访问所有用户的作业）
sudo chmod 755 /etc/slurm/slurm_prolog.py
sudo chmod 755 /etc/slurm/slurm_epilog.py
sudo chown root:root /etc/slurm/slurm_prolog.py
sudo chown root:root /etc/slurm/slurm_epilog.py
```

### 2. 修改 Slurm 配置

编辑 `/etc/slurm/slurm.conf`：

```bash
# 添加或修改以下配置
# 注意：使用 PrologSlurmctld（在 slurmctld 上运行），而不是 Prolog（在计算节点上运行）
# 这是因为计费脚本需要访问 /opt/slurm-bill/ 的 Python 模块
PrologSlurmctld=/etc/slurm/slurm_prolog.py
Epilog=/etc/slurm/slurm_epilog.py

# 确保 Prolog 在分配资源后运行，传递环境变量
PrologFlags=Alloc
```

### 3. 重启 Slurm 服务

```bash
sudo systemctl restart slurmctld
sudo systemctl restart slurmd

# 验证配置
scontrol show config | grep -E "Prolog|Epilog"
```

### 4. 测试集成

```bash
# 1. 给测试用户充值
recharge user testuser --amount 10

# 2. 查询余额
recharge query testuser

# 3. 提交一个小作业测试
su - testuser
sbatch --wrap="sleep 10" --cpus-per-task=1 -t 1:00

# 4. 查看作业状态
squeue -u testuser

# 5. 作业结束后检查扣费
recharge query testuser
```

## 余额不足时的行为

### 作业提交时（Prolog）

```
$ sbatch --wrap="sleep 60" --cpus-per-task=4
sbatch: error: Batch job submission failed: 
余额不足！当前余额 5.00 元，预估费用 8.00 元，还需充值 3.00 元
请使用以下命令充值:
  recharge user username --amount 100
```

### 查看被拒绝的作业

```bash
# 查看拒绝日志
cat /var/log/slurm-bill/rejected_jobs.log

# 格式: job_id|user|account|estimated_cost|balance|timestamp

### 调试日志

如果 Prolog 脚本运行失败，可以查看调试日志：

```bash
# Prolog 环境变量调试日志
cat /var/log/slurm-bill/prolog_debug.log

# Prolog 错误日志
cat /var/log/slurm-bill/prolog_errors.log

# Slurm 控制器日志
sudo tail -f /var/log/slurm/slurmctld.log | grep -i prolog
```
```

## 高级配置

### 1. 设置信用额度（允许欠费）

```bash
# 给用户设置 50 元信用额度（允许欠费50元）
recharge set-credit username --amount 50

# 这样余额为0时仍可提交作业，最多欠费50元
```

### 2. 设置余额预警

```bash
# 余额低于 20 元时预警
recharge set-alert username --amount 20

# 用户提交作业时会看到:
# ⚠ 余额预警：当前余额 15.00 元，请及时充值
```

### 3. 暂停/激活用户

```bash
# 暂停用户（禁止提交作业）
recharge suspend username

# 激活用户
recharge activate username
```

## 故障排查

### Prolog 脚本不执行

```bash
# 检查日志
tail -f /var/log/slurm/slurmctld.log | grep -i prolog

# 检查脚本权限
ls -la /etc/slurm/slurm_prolog.py

# 手动测试脚本
sudo SLURM_JOB_ID=12345 SLURM_JOB_USER=testuser /etc/slurm/slurm_prolog.py
echo $?  # 应该返回 0 或 1
```

### 余额检查失败

```bash
# 检查数据库
sqlite3 /var/lib/slurm-bill/billing.db ".tables"

# 检查用户余额记录是否存在
recharge query username

# 如果不存在，需要初始化
# 系统会自动创建，但余额为0
```

### 扣费不准确

```bash
# 查看作业实际使用
sacct -j <job_id> --format=JobID,Elapsed,CPUTime,MaxRSS,AllocTRES

# 查看计费记录
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT * FROM consumption_records WHERE job_id='12345';"
```

### 作业一直 PD（挂起）不运行

**现象**: 作业提交后一直处于 `PD` 状态，Reason 为 `BeginTime` 或无

**可能原因及解决**:

1. **余额不足**
   ```bash
   # 检查余额
   recharge query username
   
   # 预估费用计算（含分区倍率）
   # gpu 分区默认 1.5 倍费率
   # 2核CPU + 1GPU = (2×100 + 1×200) × 1.5 = 600元/小时
   ```

2. **时间限制过长**
   ```bash
   # 未指定 --time 时，Slurm 默认给 1 年时间限制
   # 这会导致预估费用极高（数十万），超过余额
   # 
   # ✅ 正确：指定合理的时间限制
   sbatch --wrap="sleep 10" --gres=gpu:1 -c 2 -p gpu --time=00:10
   
   # ❌ 错误：未指定时间，默认1年
   sbatch --wrap="sleep 10" --gres=gpu:1 -c 2 -p gpu
   ```

3. **Prolog 脚本执行失败**
   ```bash
   # 检查 slurmctld 日志
   sudo grep "PrologSlurmctld" /var/log/slurm/slurmctld.log
   
   # 测试脚本执行
   sudo /etc/slurm/slurm_prolog.py
   ```

## 安全注意事项

1. **脚本权限**: Prolog/Epilog 必须以 root 运行，因为需要访问所有用户的作业信息
2. **数据库权限**: 确保脚本有权限读取/写入计费数据库
3. **日志权限**: 确保日志目录可写

```bash
# 修复权限
sudo chown -R slurm:slurm /var/lib/slurm-bill
sudo chmod 755 /var/lib/slurm-bill
sudo chmod 644 /var/lib/slurm-bill/billing.db
```

## 备选方案：使用 QOS 限制

如果不想使用 Prolog/Epilog，也可以通过 QOS 实现类似功能：

```bash
# 创建特殊的 QOS
sacctmgr add qos limited
sacctmgr modify qos limited set MaxSubmitJobsPerUser=0

# 当用户余额不足时，将其 QOS 改为 limited
# 这不会阻止正在运行的作业，但会阻止新作业提交
```

但 Prolog/Epilog 方案更加精确和实时。

## 监控和告警

建议设置监控：

```bash
# 检查被拒绝的作业数量
wc -l /var/log/slurm-bill/rejected_jobs.log

# 检查余额不足的用户
sqlite3 /var/lib/slurm-bill/billing.db \
  "SELECT user, balance FROM account_balance WHERE balance < 10;"
```

可以结合邮件或钉钉告警通知管理员和用户。
