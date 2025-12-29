"""
PJJC 分析模块
"""
from typing import Dict, List

from ..db.connection import get_connection


def get_winning_ranking(group: int, limit: int = 100) -> List[Dict]:
    """
    获取 PJJC 胜场排名（关联玩家名字）
    
    Args:
        group: 分场编号
        limit: 返回数量
        
    Returns:
        [{rank, viewer_id, user_name, winning_number, grand_arena_rank}, ...]
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 获取最新的胜场排名
    # 注意：grand_rankings 表中没有 user_name，需要关联 player_profile_snapshots 或 player_clan_snapshots
    # 优先关联 player_profile (更可能包含准确名字)，其次 player_clan
    

    
    if group == 0:
        # 查询所有分场
        # 使用全服最近一次采集时间作为基准
        # 或者每个分场各自取最新？考虑到可能不在同一时间更新，最好是取每个分场的最新快照
        
        query = """
        WITH latest_per_group AS (
            SELECT grand_arena_group, MAX(collected_at) as max_time
            FROM grand_arena_snapshots
            GROUP BY grand_arena_group
        ),
        latest_grand AS (
            SELECT DISTINCT ON (viewer_id)
                viewer_id,
                winning_number,
                grand_arena_rank,
                t.grand_arena_group
            FROM grand_arena_snapshots t
            JOIN latest_per_group l ON t.grand_arena_group = l.grand_arena_group 
                                   AND t.collected_at = l.max_time
            ORDER BY viewer_id, collected_at DESC
        ),
        player_names AS (
            SELECT DISTINCT ON (viewer_id) viewer_id, user_name
            FROM player_profile_snapshots
            ORDER BY viewer_id, collected_at DESC
        ),
        clan_names AS (
            SELECT DISTINCT ON (viewer_id) viewer_id, name as user_name
            FROM player_clan_snapshots
            ORDER BY viewer_id, collected_at DESC
        )
        SELECT 
            g.viewer_id,
            COALESCE(p.user_name, c.user_name, 'Unknown') as user_name,
            g.winning_number,
            g.grand_arena_rank,
            g.grand_arena_group
        FROM latest_grand g
        LEFT JOIN player_names p ON g.viewer_id = p.viewer_id
        LEFT JOIN clan_names c ON g.viewer_id = c.viewer_id
        ORDER BY g.winning_number DESC, g.grand_arena_rank ASC
        LIMIT %s
        """
        params = (limit,)
    
    else:
        # 查询单个分场
        query = """
        WITH latest_time AS (
            SELECT MAX(collected_at) as max_time
            FROM grand_arena_snapshots
            WHERE grand_arena_group = %s
        ),
        latest_grand AS (
            SELECT DISTINCT ON (viewer_id)
                viewer_id,
                winning_number,
                grand_arena_rank,
                grand_arena_group
            FROM grand_arena_snapshots, latest_time
            WHERE grand_arena_group = %s
              AND collected_at = latest_time.max_time
            ORDER BY viewer_id, collected_at DESC
        ),
        player_names AS (
            SELECT DISTINCT ON (viewer_id) viewer_id, user_name
            FROM player_profile_snapshots
            ORDER BY viewer_id, collected_at DESC
        ),
        clan_names AS (
            SELECT DISTINCT ON (viewer_id) viewer_id, name as user_name
            FROM player_clan_snapshots
            ORDER BY viewer_id, collected_at DESC
        )
        SELECT 
            g.viewer_id,
            COALESCE(p.user_name, c.user_name, 'Unknown') as user_name,
            g.winning_number,
            g.grand_arena_rank,
            g.grand_arena_group
        FROM latest_grand g
        LEFT JOIN player_names p ON g.viewer_id = p.viewer_id
        LEFT JOIN clan_names c ON g.viewer_id = c.viewer_id
        ORDER BY g.winning_number DESC, g.grand_arena_rank ASC
        LIMIT %s
        """
        params = (group, group, limit)

    cursor.execute(query, params)
    
    result = []
    for i, row in enumerate(cursor.fetchall(), 1):
        result.append({
            'rank': i,
            'viewer_id': row[0],
            'user_name': row[1],
            'winning_number': row[2],
            'grand_arena_rank': row[3],
            'grand_arena_group': row[4]
        })
    
    return result
