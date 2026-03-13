# Slurm Billing System 文档索引

## 📚 文档列表

| 文档 | 说明 | 适用读者 |
|------|------|---------|
| [README.md](README.md) | 项目概述、快速开始、功能特性 | 所有用户 |
| [QUICKSTART.md](QUICKSTART.md) | 5分钟快速上手指南 | 新手用户 |
| [GUIDE.md](GUIDE.md) | 完整使用指南（最详细） | 系统管理员 |
| [PREPAID_GUIDE.md](PREPAID_GUIDE.md) | 预付费系统完整指南 | 系统管理员 |
| [INTEGRATION.md](INTEGRATION.md) | Slurm集成配置说明 | 系统管理员 |

## 🚀 快速导航

### 我是新手，想快速体验
👉 阅读 [QUICKSTART.md](QUICKSTART.md)

### 我要部署生产环境
👉 按顺序阅读：
1. [README.md](README.md) - 了解项目
2. [GUIDE.md](GUIDE.md) - 完整配置指南
3. [PREPAID_GUIDE.md](PREPAID_GUIDE.md) - 启用预付费系统
4. [INTEGRATION.md](INTEGRATION.md) - 集成到Slurm

### 我要启用预付费计费（充值+余额管理）
👉 阅读 [PREPAID_GUIDE.md](PREPAID_GUIDE.md)

### 我要排查问题
👉 查看 [GUIDE.md](GUIDE.md) 的"故障排除"章节

### 我要开发/扩展功能
👉 查看源代码注释和 [INTEGRATION.md](INTEGRATION.md)

## 📖 文档内容概要

### README.md
- 项目概述和功能特性
- 快速安装和配置
- 基本命令使用
- 常见问题解答

### QUICKSTART.md
- 5分钟快速上手
- 常用命令速查表
- 配置文件示例
- 故障排查快速指南

### GUIDE.md（最详细）
- 系统架构和组件说明
- 详细安装部署步骤
- 完整配置详解（费率、折扣、分区倍率）
- 日常使用指南（数据收集、查询、报表）
- **预付费系统**（充值、余额管理、作业拦截）
- 数据管理（删除、备份）
- API接口说明
- 详细故障排查

### PREPAID_GUIDE.md
- 预付费系统概述
- 充值管理详解
- 与Slurm集成步骤
- 余额检查机制
- 账户控制（信用额度、预警、暂停/激活）
- 监控和日志

### INTEGRATION.md
- Slurm Prolog/Epilog配置
- 作业拦截原理
- 安全注意事项
- 监控和告警设置

## 🔍 按功能查找文档

| 功能 | 参考文档 |
|------|---------|
| 安装部署 | README.md, GUIDE.md |
| 费率配置 | GUIDE.md |
| 数据收集 | QUICKSTART.md, GUIDE.md |
| 账单查询 | QUICKSTART.md, GUIDE.md |
| **充值管理** | **PREPAID_GUIDE.md** |
| **余额检查** | **PREPAID_GUIDE.md, INTEGRATION.md** |
| **作业拦截** | **INTEGRATION.md** |
| 报表导出 | QUICKSTART.md, GUIDE.md |
| 数据备份 | GUIDE.md |
| 故障排查 | GUIDE.md |

## 💡 使用建议

1. **首次使用**：从 [QUICKSTART.md](QUICKSTART.md) 开始，快速体验系统
2. **正式部署**：详细阅读 [GUIDE.md](GUIDE.md)，确保配置正确
3. **启用预付费**：仔细阅读 [PREPAID_GUIDE.md](PREPAID_GUIDE.md) 和 [INTEGRATION.md](INTEGRATION.md)
4. **日常维护**：收藏 [QUICKSTART.md](QUICKSTART.md) 的命令速查表
5. **遇到问题**：先查 [GUIDE.md](GUIDE.md) 的故障排除章节

## 📞 获取帮助

如果在阅读文档后仍有疑问：
1. 检查日志文件 `/var/log/slurm-bill/billing.log`
2. 运行测试脚本 `python3 /opt/slurm-bill/test_billing.py`
3. 检查数据库状态 `sqlite3 /var/lib/slurm-bill/billing.db ".tables"`

---

**文档版本**: 1.0  
**最后更新**: 2026-03-03
