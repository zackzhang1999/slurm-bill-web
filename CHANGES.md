# 更新记录

## 2026-03-03 重要更新

### 1. Prolog 脚本修复
**问题**: Prolog 脚本在计算节点上运行时无法访问 Python 模块  
**解决**: 改用 `PrologSlurmctld`（在 slurmctld 上运行）

**配置变更**:
```bash
# 旧配置（已废弃）
Prolog=/etc/slurm/slurm_prolog.py

# 新配置（推荐）
PrologSlurmctld=/etc/slurm/slurm_prolog.py
PrologFlags=Alloc
```

### 2. GPU 检测修复
**问题**: PrologSlurmctld 环境缺少 GPU 相关环境变量  
**解决**: 使用 `scontrol show job` 动态获取 GPU 数量

### 3. 时间限制处理
**问题**: 未指定 `--time` 时 Slurm 默认给 1 年时间，导致预估费用极高  
**解决**: 
- Prolog 脚本限制最大预估时间为 1 小时
- **用户必须**使用 `--time` 指定合理时间限制

### 4. 新增 recalc-costs 命令
**功能**: 修改费率后重新计算历史作业费用

```bash
# 试运行（查看变化但不更新）
recalc-costs --dry-run

# 更新最近30天
recalc-costs --days 30

# 更新所有历史作业
recalc-costs
```

### 5. 分区倍率说明
**重要**: `gpu` 分区默认 1.5 倍费率

| 分区 | 倍率 | 说明 |
|------|------|------|
| cpu | 1.0 | 标准费率 |
| gpu | 1.5 | GPU分区溢价 |
| debug | 0.5 | 测试分区半价 |

### 6. 调试日志
新增调试日志位置：
- `/var/log/slurm-bill/prolog_debug.log` - Prolog 环境变量
- `/var/log/slurm-bill/prolog_errors.log` - Prolog 错误信息
- `/var/log/slurm-bill/prolog_crash.log` - Prolog 崩溃日志

### 7. 文档更新
- GUIDE.md: 添加故障排除 Q5/Q6，费用重算说明
- INTEGRATION.md: 修正 PrologSlurmctld 配置
- README.md: 更新快速开始指南
- install.sh: 添加重要提示

## 使用建议

1. **提交作业必须指定时间**：
   ```bash
   sbatch --wrap="sleep 10" --gres=gpu:1 -c 2 -p gpu --time=00:10
   ```

2. **检查余额时注意分区倍率**：
   ```bash
   # gpu 分区 1.5 倍费率
   # 2核 + 1GPU = (200 + 200) × 1.5 = 600元/小时
   recharge query username
   ```

3. **修改费率后更新历史数据**：
   ```bash
   recalc-costs --dry-run  # 先试运行
   recalc-costs             # 正式更新
   ```
