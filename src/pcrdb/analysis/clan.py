"""
公会分析模块
"""
from typing import Dict, List, Optional

from ..db.connection import get_connection


def get_clan_history(clan_id: int = None, clan_name: str = None, limit: int = 10) -> Dict:
    """
    获取公会的月度历史（排名、成员数、会长）
    
    Args:
        clan_id: 公会 ID（优先）
        clan_name: 公会名（同名选最高排名且≠0的）
        
    Returns:
        {clan_id, clan_name, history: [{period, ranking, is_estimate, member_num, leader_name, leader_viewer_id}, ...]}
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 如果传入 clan_name，先查找对应的 clan_id
    if clan_id is None and clan_name:
        cursor.execute("""
            SELECT DISTINCT ON (clan_id) clan_id, clan_name, current_period_ranking
            FROM clan_snapshots
            WHERE clan_name = %s AND exist = TRUE
            ORDER BY clan_id, collected_at DESC
        """, (clan_name,))
        rows = cursor.fetchall()
        
        if not rows:
            return {"error": f"未找到公会: {clan_name}"}
        
        # 选排名最高且≠0的
        valid = [(r[0], r[1], r[2]) for r in rows if r[2] and r[2] > 0]
        if valid:
            valid.sort(key=lambda x: x[2])  # 按排名升序
            clan_id = valid[0][0]
        else:
            clan_id = rows[0][0]
    
    if clan_id is None:
        return {"error": "请提供 clan_id 或 clan_name"}
    
    # 获取该公会所有快照，按时间排序
    cursor.execute("""
        SELECT 
            collected_at,
            current_period_ranking,
            grade_rank,
            member_num,
            clan_name,
            leader_name,
            leader_viewer_id
        FROM clan_snapshots
        WHERE clan_id = %s AND exist = TRUE
        ORDER BY collected_at ASC
    """, (clan_id,))
    
    rows = cursor.fetchall()
    if not rows:
        return {"clan_id": clan_id, "clan_name": None, "history": []}
    
    # 按会战周期分组（相隔 20 天以上视为不同期）
    periods = []
    last_date = None
    month_count = {}  # 记录每月出现次数
    
    for row in rows:
        collected_at = row[0]
        
        if last_date is None or (collected_at - last_date).days >= 20:
            month_str = collected_at.strftime('%Y-%m')
            
            # 处理同月多次的情况
            if month_str in month_count:
                month_count[month_str] += 1
                period_str = f"{month_str}/{month_count[month_str]}"
            else:
                month_count[month_str] = 1
                period_str = month_str
            
            periods.append({
                'period': period_str,
                'collected_at': collected_at,
                'current_period_ranking': row[1],
                'grade_rank': row[2],
                'member_num': row[3],
                'clan_name': row[4],
                'leader_name': row[5],
                'leader_viewer_id': row[6]
            })
            last_date = collected_at
    
    # 计算最终排名：用下一期的 grade_rank
    history = []
    for i, p in enumerate(periods):
        if i + 1 < len(periods):
            # 有下一期，用下一期的 grade_rank
            ranking = periods[i + 1]['grade_rank']
            is_estimate = False
        else:
            # 最近一期，用 current_period_ranking
            ranking = p['current_period_ranking']
            is_estimate = True
        
        history.append({
            'period': p['period'],
            'ranking': ranking,
            'is_estimate': is_estimate,
            'member_num': p['member_num'],
            'leader_name': p['leader_name'],
            'leader_viewer_id': p['leader_viewer_id'],
            'clan_name': p['clan_name']
        })
    
    # 按倒序排列（最新的在前面）
    history.reverse()

    # 获取最新的公会名
    # latest_name = periods_raw[-1]['clan_name'] if periods_raw else None # This line is no longer needed as clan_name is taken from the first item in reversed history
    
    
    # Apply limit if specified
    if limit > 0:
        history = history[:limit]

    return {
        'clan_id': clan_id,
        'clan_name': history[0]['clan_name'] if history else (clan_name or "Unknown"),
        'history': history
    }


def get_clan_power_ranking(limit: int = 50) -> List[Dict]:
    """
    获取最近一期公会按平均战力排名
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 基于最近的 player_clan_snapshots 计算平均战力
    cursor.execute("""
        WITH latest_data AS (
            SELECT DISTINCT ON (viewer_id)
                viewer_id,
                join_clan_id,
                join_clan_name,
                total_power
            FROM player_clan_snapshots
            WHERE collected_at > NOW() - INTERVAL '7 days'
              AND join_clan_id IS NOT NULL
              AND total_power > 0
            ORDER BY viewer_id, collected_at DESC
        )
        SELECT 
            join_clan_id,
            join_clan_name,
            ROUND(AVG(total_power)) as avg_power,
            COUNT(*) as member_count
        FROM latest_data
        GROUP BY join_clan_id, join_clan_name
        HAVING COUNT(*) >= 10  -- 至少10人才统计
        ORDER BY avg_power DESC
        LIMIT %s
    """, (limit,))
    
    result = []
    for i, row in enumerate(cursor.fetchall(), 1):
        result.append({
            'rank': i,
            'clan_id': row[0],
            'clan_name': row[1],
            'avg_power': int(row[2]),
            'member_count': row[3]
        })
    
    return result


def get_clan_members(clan_id: int = None, clan_name: str = None, period: str = None) -> Dict:
    """
    获取指定月份的公会成员列表
    
    Args:
        clan_id: 公会 ID
        clan_name: 公会名 (如果 clan_id 为 None)
        period: 月份 (YYYY-MM)，默认为最新月份
        
    Returns:
        {clan_id, clan_name, member_count, members: [{viewer_id, name, level, ...}]}
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. 确定 period
    if not period:
        cursor.execute("""
            SELECT DISTINCT to_char(collected_at, 'YYYY-MM') as period
            FROM player_clan_snapshots
            ORDER BY period DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if not row:
            return {"error": "暂无数据"}
        period = row[0]
        
    # 2. 如果只给了 clan_name，先找 clan_id (在指定月份存在)
    if clan_id is None and clan_name:
        cursor.execute("""
            SELECT DISTINCT join_clan_id
            FROM player_clan_snapshots
            WHERE to_char(collected_at, 'YYYY-MM') = %s
              AND join_clan_name = %s
            LIMIT 1
        """, (period, clan_name))
        row = cursor.fetchone()
        if not row:
            return {"error": f"未找到公会: {clan_name} 在 {period}"}
        clan_id = row[0]
        
    if clan_id is None:
        return {"error": "请提供 clan_id 或 clan_name"}

    # 3. 查询成员列表 (每个成员取该月最新的快照)
    # role: 40=会长, 30=副会长
    cursor.execute("""
        SELECT DISTINCT ON (viewer_id)
            viewer_id,
            name,
            level,
            total_power,
            role,
            join_clan_name
        FROM player_clan_snapshots
        WHERE to_char(collected_at, 'YYYY-MM') = %s
          AND join_clan_id = %s
        ORDER BY viewer_id, collected_at DESC
    """, (period, clan_id))
    
    rows = cursor.fetchall()
    
    members = []
    clan_name_actual = None
    
    for row in rows:
        vid, name, level, power, role_val, cname = row
        if clan_name_actual is None:
            clan_name_actual = cname
            
        role_str = ""
        if role_val == 40:
            role_str = "会长"
        elif role_val == 30:
            role_str = "副会长"
            
        members.append({
            "viewer_id": vid,
            "name": name,
            "level": level,
            "total_power": power,
            "role": role_str,
            "role_val": role_val or 0
        })
        
    # 按职务排序 (会长>副会长>普通)，再按战力降序
    members.sort(key=lambda x: (x['role_val'], x['total_power']), reverse=True)

    return {
        "clan_id": clan_id,
        "clan_name": clan_name_actual or clan_name,
        "period": period,
        "member_count": len(members),
        "members": members
    }


def _exp_to_knight_level(exp: int) -> str:
    """
    Convert princess_knight_rank_total_exp to knight level.
    Level 1-125: Linear, slope 53235
    Level 126-201: Linear, slope 53236
    Level 202-251: Quadratic, acceleration 505
    Returns "251+" if exceeds level 251.
    """
    if exp is None or exp <= 0:
        return "0"
    
    # Cumulative exp at level boundaries
    # Level 125: 125 * 53235 = 6654375
    # Level 201: 6654375 + 76 * 53236 = 6654375 + 4045936 = 10700311
    exp_at_125 = 125 * 53235  # 6654375
    exp_at_201 = exp_at_125 + 76 * 53236  # 10700311
    
    if exp <= exp_at_125:
        level = exp // 53235
        return str(max(1, level))
    elif exp <= exp_at_201:
        level = 125 + (exp - exp_at_125) // 53236
        return str(level)
    else:
        # Quadratic phase 202-251
        # Each level requires: base + (level - 202) * 505
        # Approximate by iterating
        remaining = exp - exp_at_201
        level = 201
        step = 53236  # Starting step for level 202
        while remaining > 0 and level < 251:
            step += 505
            if remaining >= step:
                remaining -= step
                level += 1
            else:
                break
        
        if level >= 251:
            return "251+"
        return str(level)


def _count_talent_quest(talent_data) -> int:
    """
    Count completed talent quest stages from JSON data.
    
    Supports multiple data formats:
    - List format: [49, 39, 49, 39, 50] - 每个元素代表一个大区的完成数量
    - Dict format: {stage_id: difficulty_flags, ...}
    """
    if not talent_data:
        return 0
    
    # 列表格式：每个元素是一个大区的完成数量
    if isinstance(talent_data, list):
        return sum(v for v in talent_data if isinstance(v, (int, float)))
    
    # 字典格式：{stage_id: difficulty_flags, ...}
    if isinstance(talent_data, dict):
        count = 0
        for stage_id, flags in talent_data.items():
            if isinstance(flags, int):
                # Count bits set (each bit = one difficulty)
                count += bin(flags).count('1')
            elif isinstance(flags, list):
                count += len(flags)
        return count
    
    return 0


def get_top_clans(period: str = None, limit: int = 30) -> Dict:
    """
    Get top clans (by ranking) for a given period from clan_snapshots.
    
    Args:
        period: Month (YYYY-MM), defaults to latest
        limit: Number of clans to return (default 30)
        
    Returns:
        {period, clans: [{clan_id, clan_name, ranking}, ...]}
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Determine period
    if not period:
        cursor.execute("""
            SELECT DISTINCT to_char(collected_at, 'YYYY-MM') as period
            FROM clan_snapshots
            ORDER BY period DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if not row:
            return {"error": "暂无数据"}
        period = row[0]
    
    # Get top clans by ranking for the period
    cursor.execute("""
        SELECT DISTINCT ON (clan_id)
            clan_id,
            clan_name,
            current_period_ranking
        FROM clan_snapshots
        WHERE to_char(collected_at, 'YYYY-MM') = %s
          AND current_period_ranking > 0
          AND current_period_ranking <= %s
          AND exist = TRUE
        ORDER BY clan_id, collected_at DESC
    """, (period, limit))
    
    rows = cursor.fetchall()
    
    clans = []
    for row in rows:
        clans.append({
            "clan_id": row[0],
            "clan_name": row[1],
            "ranking": row[2]
        })
    
    # Sort by ranking
    clans.sort(key=lambda x: x['ranking'])
    
    return {
        "period": period,
        "clans": clans
    }


def get_top_clan_profiles(date: str = None, clan_id: int = None) -> Dict:
    """
    Get player profiles for top 30 clans (synced daily by player_profile_sync).
    
    Args:
        date: Date (YYYY-MM-DD), defaults to latest
        clan_id: Clan ID to filter by
        
    Returns:
        {date, talent_total, players: [{viewer_id, user_name, join_clan_name, ...}]}
    """
    import os
    talent_total = int(os.getenv('TALENT_QUEST_TOTAL', 250))
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Determine date
    if not date:
        cursor.execute("""
            SELECT DISTINCT to_char(collected_at, 'YYYY-MM-DD') as date
            FROM player_profile_snapshots
            ORDER BY date DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if not row:
            return {"error": "暂无数据"}
        date = row[0]
    
    # Get players for the date
    if clan_id:
        cursor.execute("""
            SELECT DISTINCT ON (viewer_id)
                viewer_id,
                user_name,
                join_clan_name,
                team_level,
                unit_num,
                total_power,
                princess_knight_rank_total_exp,
                talent_quest_clear,
                arena_rank,
                grand_arena_rank
            FROM player_profile_snapshots
            WHERE to_char(collected_at, 'YYYY-MM-DD') = %s
              AND join_clan_id = %s
            ORDER BY viewer_id, collected_at DESC
        """, (date, clan_id))
    else:
        cursor.execute("""
            SELECT DISTINCT ON (viewer_id)
                viewer_id,
                user_name,
                join_clan_name,
                team_level,
                unit_num,
                total_power,
                princess_knight_rank_total_exp,
                talent_quest_clear,
                arena_rank,
                grand_arena_rank
            FROM player_profile_snapshots
            WHERE to_char(collected_at, 'YYYY-MM-DD') = %s
            ORDER BY viewer_id, collected_at DESC
        """, (date,))
    
    rows = cursor.fetchall()
    
    players = []
    for row in rows:
        (viewer_id, user_name, clan_name, team_level, unit_num, 
         total_power, knight_exp, talent_data, arena_rank, grand_rank) = row
        
        players.append({
            "viewer_id": viewer_id,
            "user_name": user_name,
            "join_clan_name": clan_name,
            "team_level": team_level or 0,
            "unit_num": unit_num or 0,
            "total_power": total_power or 0,
            "knight_level": _exp_to_knight_level(knight_exp),
            "talent_done": _count_talent_quest(talent_data),
            "arena_rank": arena_rank or 0,
            "grand_arena_rank": grand_rank or 0
        })
    
    # Default sort by total_power descending
    players.sort(key=lambda x: x['total_power'], reverse=True)
    
    # 获取公会名（如果指定了 clan_id）
    clan_name_result = None
    if clan_id and players:
        clan_name_result = players[0].get('join_clan_name')
    
    return {
        "date": date,
        "talent_total": talent_total,
        "count": len(players),
        "clan_id": clan_id,  # 可能为 None
        "clan_name": clan_name_result,  # 可能为 None
        "players": players
    }
