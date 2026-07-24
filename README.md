# pcrdb

公主连结渠道服数据采集、分析与会战排名查询服务。PCRDB 与 ClanRank 已合并为一个 GitOps 项目。

## 运行结构

- `db`: PostgreSQL 17，复用外部卷 `pcrdb-data`，启动时应用 schema migration 和角色授权。
- `web`: 单个 FastAPI 进程，同时提供 `/api/*` 和 `frontend/` 静态站点。
- `worker`: APScheduler 进程，运行原 PCRDB 任务和会战状态机采集。
- `backup`: 每 24 小时生成一次 PostgreSQL 自校验逻辑备份，写入外部卷 `pcrdb-backups`。

浏览器直接访问 `/api/clan-battle/*`，不再使用 `/proxy` 或独立 ClanRank 服务。

## GitOps 部署

Dokploy Compose 使用仓库根目录的 `docker-compose.yml`。先从 `.env.example` 配置环境变量；所有密码和密钥只保存在 Dokploy 环境变量或授权的秘密存储中。

首次切换时保持：

```text
CLAN_BATTLE_COLLECTION_ENABLED=false
```

历史 CSV 导入和校验通过后，再改为 `true`。

## 本地命令

```bash
python cli.py task clan_sync
python cli.py task player_profile_sync --args mode=top_clans rank_limit=30
python -m pcrdb.worker
```

本地运行模块时将 `src` 加入 `PYTHONPATH`，容器镜像已预先配置。

## 会战 CSV 导入

导入器接受多个不可变来源，先干跑检查时间冲突：

```bash
python -m pcrdb.clan_battle.importer \
  --source local=/migration/local/qd \
  --source uk03=/migration/uk03/qd \
  --source kr01=/migration/kr01/qd \
  --dry-run
```

确认 `conflicts` 为空后去掉 `--dry-run`。重复文件、相同时间的前缀文件、源哈希、规范化内容哈希、行数和目标快照都会写入 `clan_battle.import_files`。

导入后执行：

```bash
python -m pcrdb.clan_battle.verify \
  --manifest /migration/verification/clanrank-import-manifest.jsonl
```

只有验证结果 `ok: true`、逻辑备份恢复抽检通过、原生 API 可查询且新 Worker 影子运行正常后，才能删除旧 CSV 和旧服务。

## 会战调度

Worker 每个北京时间 `00/30` 分运行一次：

1. 每月 20 日后，对比第一页与上期最终榜指纹，直到新非空榜出现。
2. 会战中每 30 分钟保存前 300 名。
3. 连续成功空响应后进入结算等待；网络或协议错误不计为空响应。
4. 同月内数据重新出现时恢复为会战中，避免短暂空榜被误判成最终榜。
5. 次月最终榜出现后抓取全部排名，连续两次内容一致才标记为 final。
6. 到次月 20 日仍无可靠最终榜时，旧月份保留为 settlement，并自动等待新会战。
7. 状态保存在 PostgreSQL，并使用 advisory lock 防止重复 Worker。

普通 PCRDB 任务继续读取 `config/schedule.yaml`，由 APScheduler 执行完整 cron 表达式；`L-N` 仍表示当月倒数第 `N+1` 天。

## 竞技场提醒

Worker 可监控一个玩家的 JJC 与 PJJC 排名，并在名次变差时通过钉钉群机器人发送提醒。当前排名基线保存在 PostgreSQL；首次启用时只建立基线、不发通知，Worker 重启后继续使用已有基线。

```text
ARENA_ALERT_ENABLED=true
ARENA_ALERT_TARGET_VIEWER_ID=<viewer_id>
ARENA_ALERT_POLL_SECONDS=30
ARENA_ALERT_DINGTALK_WEBHOOK=<webhook URL>
ARENA_ALERT_DINGTALK_SECRET=<optional signing secret>
```

竞技场提醒、会战榜、玩家档案、公会明细和分场任务统一从 `accounts` 的农场账号池短租账号。账号池通过 PostgreSQL advisory lock 避免并发任务重复使用同一账号，并按最近使用时间轮转；`pool_enabled=false` 的专用账号不会参与。Webhook 和加签密钥只能放在部署环境中，不得提交到仓库。

批量查询并发由 `PCRDB_SYNC_NUM` 控制，会战分页并发由 `CLAN_BATTLE_QUERY_CONCURRENCY` 控制。JJC/PJJC 分场任务仍只租用属于目标分场的账号。

钉钉机器人使用安全关键词时需允许 `pcrjjc`，所有正式提醒都会在标题中包含该关键词。

## 数据库备份

`backup` 服务启动后立即运行 `pg_dump`，并在写入正式文件前使用 `pg_restore --list` 校验。卷内默认只保留最新一份完整 dump 和对应 SHA256；Dokploy Volume Backup 再将这个已完成的文件卷上传到对象存储，不直接在线复制 PostgreSQL 数据目录。

首次部署前需要创建外部卷：

```bash
docker volume create pcrdb-backups
```

备份恢复验收必须包含两层：先由 Dokploy 恢复 `pcrdb-backups` 到未挂载临时卷，再将其中的 dump 恢复到临时 PostgreSQL，核对旧 PCRDB 表和 `clan_battle` 总量。
