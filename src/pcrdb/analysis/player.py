"""
玩家分析模块
"""
from typing import Dict, List, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_connection


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
