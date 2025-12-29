# pcrdb 功能概览

公主连结渠道服数据采集与分析系统 (PostgreSQL 版)

---

## 数据采集任务 (tasks/)

| 任务 | 说明 | 写入表 |
|------|------|--------|
| `clan_sync` | 同步公会数据 | `clan_snapshots`, `player_clan_snapshots` |
| `grand_sync` | 同步PJJC排名数据 | `grand_rankings` |
| `arena_deck_sync` | 同步JJC防守阵容 | `arena_deck_snapshots` |
| `player_profile_sync` | 同步玩家档案 | `player_profile_snapshots` |

---

## 数据分析功能 (analysis/)

### 优先实现清单

| # | 需求描述 | 函数名 | 状态 |
|:-:|----------|--------|:----:|
| 1 | 公会按月历史查询（排名，间隔20-35天） | `clan.get_clan_history` | ✅ |
| 2 | 成员所在公会查询 + 当期公会排名 | `player.get_player_clan_history` | ✅ |
| 3 | 最近一期公会按平均战力排名 | `clan.get_clan_power_ranking` | ✅ |
| 4 | PJJC 按胜场排名（关联名字） | `grand.get_winning_ranking` | ✅ |

---

### 完整功能清单（来自 _refer/）

#### clan.py - 公会分析

| 函数 | 说明 | 优先级 | 状态 |
|------|------|:------:|:----:|
| `query_member_history` | 玩家公会历史（按月） | P0 | ⬜ |
| `query_clan_members_timeline` | 公会成员加入时间线 | P1 | ⬜ |
| `query_average_power` | 指定月份公会平均战力 | P1 | ⬜ |
| `query_members_now` | 成员当前所在公会 | P1 | ⬜ |
| `query_clan_same` | 两人同公会历史 | P2 | ⬜ |

#### arena.py - JJC 分析

| 函数 | 说明 | 优先级 | 状态 |
|------|------|:------:|:----:|
| `find_group` | 分场人员流动分析 | P1 | ⬜ |
| `get_avatar` | 头像使用率统计 | P2 | ⬜ |
| `arena_chara_stats` | JJC 防守角色使用率 | P2 | ⬜ |

#### grand.py - PJJC 分析

| 函数 | 说明 | 优先级 | 状态 |
|------|------|:------:|:----:|
| `count_winning_num` | 胜场超过阈值人数统计 | P0 | ⬜ |
| `count_top_clan` | 前10名公会分布 | P1 | ⬜ |
| `power_clan` | 分场平均战力 | P1 | ⬜ |

#### member_stats.py - 成员统计

| 函数 | 说明 | 优先级 | 状态 |
|------|------|:------:|:----:|
| `query_talent_quest` | 天赋关卡通关统计 | P1 | ⬜ |
| `compute_clan_averages` | 公会平均值并导出CSV | P1 | ⬜ |

---

## 参考代码

旧代码位于 `src/pcrdb/analysis/_refer/`，可作为实现参考。

---

## 定时调度

- 配置：`config/schedule.yaml`
- 入口：`scheduler.py`
