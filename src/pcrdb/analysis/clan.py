"""
公会分析模块
"""
from typing import Dict, List, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_connection


def get_clan_history(clan_id: int = None, clan_name: str = None) -> Dict:
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
    latest_name = periods[-1]['clan_name'] if periods else None
    
    
    return {
        'clan_id': clan_id,
        'clan_name': latest_name,
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
