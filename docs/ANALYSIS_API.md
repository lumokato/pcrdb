# Analysis API 接口规范

开发用文档，定义每个分析功能的接口规范。

---

## 优先实现清单

### 1. 公会按月历史查询

**需求**：查询公会在每个会战期的排名历史（间隔20-35天视为不同期）

**函数**：`clan.get_clan_history(clan_id: int = None, clan_name: str = None)`

**输入**（二选一）：
| 参数 | 类型 | 说明 |
|------|------|------|
| clan_id | int | 公会 ID（优先使用） |
| clan_name | str | 公会名（同名选最高排名且≠0的） |

**返回**：`Dict`
```python
{
    "clan_id": 42877,
    "clan_name": "3388",  # 最新的名字
    "history": [
        {"period": "2024-12", "ranking": 1, "is_estimate": False, "member_num": 30},
        ...
    ]
}
```

| 字段（顶层） | 类型 | 说明 |
|-------------|------|------|
| clan_id | int | 公会 ID |
| clan_name | str | 最新公会名 |
| history | list | 历史列表 |

| 字段（history） | 类型 | 说明 |
|----------------|------|------|
| period | str | 期间，如 "2024-12" 或 "2024-12/2" |
| ranking | int | 最终排名 |
| is_estimate | bool | True=使用 current_period_ranking |
| member_num | int | 成员数 |
| leader_name | str | 会长名 |
| leader_viewer_id | int | 会长 ID |
---

### 2. 成员所在公会查询 + 当期公会排名

**需求**：查询玩家的公会归属历史，并显示每期所在公会的排名

**函数**：`player.get_player_clan_history(viewer_id: int)`

**输入**：
| 参数 | 类型 | 说明 |
|------|------|------|
| viewer_id | int | 玩家 ID |

**返回**：`Dict`
```python
{
    "viewer_id": 1079252523537,
    "user_name": "玩家名",
    "history": [
        {"period": "2024-12", "clan_id": 42877, "clan_name": "3388", "clan_ranking": 1, "level": 300, "total_power": 50000000},
        ...
    ]
}
```

| 字段（顶层） | 类型 | 说明 |
|-------------|------|------|
| viewer_id | int | 玩家 ID |
| user_name | str | 最新玩家名 |
| history | list | 历史列表 |

| 字段（history） | 类型 | 说明 |
|----------------|------|------|
| period | str | 期间 |
| clan_id | int | 公会 ID |
| clan_name | str | 公会名 |
| clan_ranking | int | 该期公会排名 |
| level | int | 玩家等级 |
| total_power | int | 玩家战力 |

**状态**：⬜ 待实现

---

### 3. 最近一期公会按平均战力排名

**需求**：基于最近采集的数据，计算各公会的平均战力排名

**函数**：`clan.get_clan_power_ranking(limit: int = 50)`

**输入**：
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| limit | int | 50 | 返回数量 |

**返回**：`List[Dict]`
| 字段 | 类型 | 说明 |
|------|------|------|
| rank | int | 排名 |
| clan_id | int | 公会 ID |
| clan_name | str | 公会名 |
| avg_power | int | 平均战力 |
| member_count | int | 统计成员数 |

**状态**：⬜ 待实现

---

### 4. PJJC 按胜场排名（关联名字）

**需求**：显示 PJJC 各分场的胜场排名，前50名需要从其他表关联获取名字

**函数**：`grand.get_winning_ranking(group: int, limit: int = 100)`

**输入**：
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| group | int | - | 分场编号 |
| limit | int | 100 | 返回数量 |

**返回**：`List[Dict]`
| 字段 | 类型 | 说明 |
|------|------|------|
| rank | int | 排名 |
| viewer_id | int | 玩家 ID |
| user_name | str | 玩家名（从其他表关联） |
| winning_number | int | 胜场数 |
| grand_arena_rank | int | PJJC 排名 |

**状态**：⬜ 待实现

---

### 5. 玩家名模糊搜索

**需求**：在指定月份数据中，模糊搜索玩家名，按战力倒序排列

**函数**：`player.search_players_by_name(name_pattern: str, period: str = None, limit: int = 50)`

**输入**：
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| name_pattern | str | - | 玩家名模糊匹配（支持 ILIKE） |
| period | str | None | 月份，格式 "YYYY-MM"，None 为最近一期 |
| limit | int | 50 | 返回数量 |

**返回**：`List[Dict]`
| 字段 | 类型 | 说明 |
|------|------|------|
| viewer_id | int | 玩家 ID |
| name | str | 玩家名 |
| level | int | 等级 |
| total_power | int | 战力 |
| clan_name | str | 所属公会 |

**状态**：⬜ 待实现

---

## 备注

- 所有返回都是 `List[Dict]` 格式，方便前端处理
- 时间相关字段统一用 ISO 格式字符串或保持 datetime
- 涉及的表：`clan_snapshots`, `player_clan_snapshots`, `grand_rankings`
