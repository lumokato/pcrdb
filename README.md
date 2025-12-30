# pcrdb - 公主连结渠道服数据采集系统

从公主连结游戏 API 采集公会、成员、竞技场等数据的 ETL 工具。

## 快速开始

### 方式一：Docker 部署 (推荐)

最简单的方式是使用 Docker Compose 一键启动数据库和应用。

1.  **准备配置**:
    复制 `.env.example` 为 `.env` 并填入数据库密码：
    ```bash
    cp .env.example .env
    ```

2.  **启动服务**:
    ```bash
    docker-compose up -d
    ```

### 方式二：本地运行

1.  **安装依赖**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **配置数据库**:
    确保本地安装了 PostgreSQL，并在 `.env` 中配置连接信息。

3.  **运行任务**:
    ```bash
    python cli.py task clan_sync
    ```

## 目录结构

```
pcrdb/
├── cli.py              # 命令行入口
├── scheduler.py        # 任务调度器
├── docker-compose.yml  # Docker 编排配置
├── src/pcrdb/          # 源代码
│   ├── api/            # 游戏 API 客户端
│   ├── models/         # 数据库模型
│   ├── tasks/          # 采集任务逻辑
│   └── analysis/       # 数据分析模块
├── config/             # 配置文件 (需自行创建/配置)
│   ├── accounts.json   # 游戏账号配置 (敏感信息，不上传)
│   ├── schedule.yaml   # 任务调度配置
│   └── unit_id.json    # 角色 ID 映射
└── docs/               # 文档和示例文件
```

## 配置说明

本项目依赖 `config/` 目录下的配置文件运行。

1.  **账号配置** (`config/accounts.json`):
    包含游戏账号的认证信息。**请勿提交此文件到版本控制。**

2.  **调度配置** (`config/schedule.yaml`):
    定义定时任务的执行规则。可参考 `docs/schedule.yaml` (如果存在) 或创建新文件。

3.  **环境变量** (`.env`):
    定义数据库连接信息和访问密钥。参考 `.env.example`。

## CLI 命令

使用 `cli.py` 手动运行采集任务。

```bash
# 查看帮助
python cli.py --help

# 运行特定任务
python cli.py task <task_name> [args]
```

### 可用任务

| 任务名称 | 描述 | 参数示例 |
| :--- | :--- | :--- |
| `clan_sync` | 同步公会及成员信息 | (无) |
| `grand_sync` | 同步公主竞技场(PJJC)排名 | (无) |
| `arena_deck_sync` | 同步竞技场防守阵容 | (无) |
| `player_profile_sync` | 同步玩家详细档案 | `mode=top_clans rank_limit=30` |

### 示例

```bash
# 采集前30名公会的成员档案
python cli.py task player_profile_sync --args mode=top_clans rank_limit=30

# 如果配置了月度全量模式
python cli.py task player_profile_sync --args mode=active_all
```

## 任务调度

本项目包含一个基于 Python 的调度器 `scheduler.py`，用于按计划自动执行上述任务。

```bash
python scheduler.py
```

调度规则在 `config/schedule.yaml` 中配置。

## 文档列表

- [数据库管理](docs/DATABASE.md): Schema 执行、验证与修复
- [开发指南](docs/DEVELOPMENT.md): 如何添加新功能
- [API 规范](docs/ANALYSIS_API.md): 查询接口定义
- [功能特性](docs/FEATURES.md): 功能清单与优先级
