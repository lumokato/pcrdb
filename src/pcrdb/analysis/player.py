"""
玩家分析模块
"""
from typing import Dict, List

from ..db.connection import get_connection


def get_available_periods() -> List[str]:
    """
    获取数据库中有玩家数据的月份列表
    
    Returns:
        ["2024-12", "2024-11", ...] 按时间倒序
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT to_char(collected_at, 'YYYY-MM') as period
        FROM player_clan_snapshots
        ORDER BY period DESC
    """)
    
    rows = cursor.fetchall()
    return [row[0] for row in rows]


def get_player_clan_history(viewer_id: int) -> Dict:
    """
    获取玩家的公会归属历史 + 当期公会排名
    
    Args:
        viewer_id: 玩家 ID
        
    Returns:
        {viewer_id, user_name, history: [{period, clan_id, clan_name, clan_ranking, level, total_power}, ...]}
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 获取玩家公会历史，按月分组
    cursor.execute("""
        SELECT DISTINCT ON (to_char(collected_at, 'YYYY-MM'))
            to_char(collected_at, 'YYYY-MM') as period,
            join_clan_id,
            join_clan_name,
            level,
            total_power,
            collected_at,
            name
        FROM player_clan_snapshots
        WHERE viewer_id = %s AND join_clan_id IS NOT NULL
        ORDER BY to_char(collected_at, 'YYYY-MM') ASC, collected_at DESC
    """, (viewer_id,))
    
    player_history = cursor.fetchall()
    
    if not player_history:
        return {"viewer_id": viewer_id, "user_name": None, "history": []}
    
    # 获取最新的玩家名
    latest_name = player_history[-1][6] if player_history else None
    
    history = []
    for row in player_history:
        period, clan_id, clan_name, level, total_power, collected_at, name = row
        
        # 获取该时期公会的排名（取最接近的快照的下一期 grade_rank）
        cursor.execute("""
            SELECT grade_rank, current_period_ranking
            FROM clan_snapshots
            WHERE clan_id = %s 
              AND collected_at > %s
            ORDER BY collected_at ASC
            LIMIT 1
        """, (clan_id, collected_at))
        
        ranking_row = cursor.fetchone()
        if ranking_row and ranking_row[0]:
            # 有下一期，用 grade_rank
            clan_ranking = ranking_row[0]
        else:
            # 没有下一期，用最近的 current_period_ranking
            cursor.execute("""
                SELECT current_period_ranking
                FROM clan_snapshots
                WHERE clan_id = %s
                ORDER BY collected_at DESC
                LIMIT 1
            """, (clan_id,))
            r = cursor.fetchone()
            clan_ranking = r[0] if r else None
        
        history.append({
            'period': period,
            'clan_id': clan_id,
            'clan_name': clan_name,
            'clan_ranking': clan_ranking,
            'level': level,
            'total_power': total_power,
            'player_name': name
        })
    
    # 按倒序排列（最新的在前面）
    history.reverse()

    return {
        'viewer_id': viewer_id,
        'user_name': latest_name,
        'history': history
    }


def search_players_by_name(name_pattern: str, period: str = None, limit: int = 50) -> List[Dict]:
    """
    模糊搜索玩家名，按战力倒序排列
    
    Args:
        name_pattern: 玩家名模糊匹配
        period: 月份 (YYYY-MM)，None 为最近一期
        limit: 返回数量
    
    Returns:
        [{viewer_id, name, level, total_power, clan_name}, ...]
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 如果没有指定 period，获取最近的月份
    if not period:
        cursor.execute("""
            SELECT DISTINCT to_char(collected_at, 'YYYY-MM') as period
            FROM player_clan_snapshots
            ORDER BY period DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if not row:
            return []
        period = row[0]
    
    # 搜索匹配的玩家，每个玩家只返回该月份最新的一条记录
    # 使用 DISTINCT ON 去重，按战力倒序排列
    cursor.execute("""
        SELECT DISTINCT ON (viewer_id)
            viewer_id,
            name,
            level,
            total_power,
            join_clan_name
        FROM player_clan_snapshots
        WHERE to_char(collected_at, 'YYYY-MM') = %s
          AND name ILIKE %s
        ORDER BY viewer_id, collected_at DESC
    """, (period, f'%{name_pattern}%'))
    
    rows = cursor.fetchall()
    
    # 按战力倒序排序
    results = []
    for row in rows:
        results.append({
            'viewer_id': row[0],
            'name': row[1],
            'level': row[2],
            'total_power': row[3],
            'clan_name': row[4]
        })
    
    # 按战力倒序排列
    results.sort(key=lambda x: x['total_power'] or 0, reverse=True)
    
    return results[:limit]

