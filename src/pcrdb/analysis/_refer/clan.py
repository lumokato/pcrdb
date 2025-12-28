```
"""
公会数据分析模块
提供公会历史追溯、成员查询等功能
"""
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import sys
from pathlib import Path

# Add src to path to allow imports from db.connection
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_connection

def query_member_history(viewer_id: int) -> List[Dict]:
    """
    查询成员的公会历史
    
    Args:
        viewer_id: 玩家 ID
        
    Returns:
        历史记录列表 [{month, clan_id, clan_name}, ...]
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 查找该成员在每个月的第一条记录（或最后一条）
    # 这里我们按月聚合，取每个月最后一次出现的记录
    query = """
        SELECT DISTINCT ON (to_char(collected_at, 'YYMM'))
            to_char(collected_at, 'YYMM') as month,
            join_clan_id,
            join_clan_name
        FROM player_clan_snapshots
        WHERE viewer_id = %s AND join_clan_id IS NOT NULL
        ORDER BY to_char(collected_at, 'YYMM') ASC, collected_at DESC
    """
    cursor.execute(query, (viewer_id,))
    
    history = []
    for row in cursor.fetchall():
        history.append({
            'month': row[0],
            'clan_id': row[1],
            'clan_name': row[2]
        })
    return history


def query_clan_members_timeline(clan_id: int) -> Dict[str, List[str]]:
    """
    查询公会成员的加入时间线
    
    Args:
        clan_id: 公会 ID
        
    Returns:
        {month: [成员名称列表], ...}
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 获取该公会所有历史快照成员
    query = """
        SELECT viewer_id, name, to_char(collected_at, 'YYMM') as month
        FROM player_clan_snapshots
        WHERE join_clan_id = %s
        ORDER BY collected_at ASC
    """
    cursor.execute(query, (clan_id,))
    
    timeline = {}
    seen_members = set()
    
    # 模拟 legacy 逻辑：第一次出现在该公会的月份即为加入月份
    # 注意：如果数据是从中间开始采集的，第一个月会被视为所有人都加入
    
    for vid, name, month in cursor.fetchall():
        if vid not in seen_members:
            seen_members.add(vid)
            if month not in timeline:
                timeline[month] = []
            timeline[month].append(name)
            
    return timeline


def query_clan_same(id1: int, id2: int) -> List[Dict]:
    """
    查询两个成员在同一公会的历史
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 简单的 join 查询
    query = """
        SELECT DISTINCT ON (to_char(t1.collected_at, 'YYMM'))
            to_char(t1.collected_at, 'YYMM') as month,
            t1.join_clan_name
        FROM player_clan_snapshots t1
        JOIN player_clan_snapshots t2 ON 
            t1.join_clan_id = t2.join_clan_id AND 
            to_char(t1.collected_at, 'YYMM') = to_char(t2.collected_at, 'YYMM')
        WHERE t1.viewer_id = %s AND t2.viewer_id = %s
          AND t1.join_clan_id IS NOT NULL
        ORDER BY to_char(t1.collected_at, 'YYMM') ASC
    """
    cursor.execute(query, (id1, id2))
    
    same_records = []
    for row in cursor.fetchall():
        same_records.append({
            'month': row[0],
            'clan_name': row[1]
        })
    return same_records


def query_average_power(month: str) -> Dict[str, int]:
    """
    查询指定月份各公会的平均战力
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT join_clan_name, CAST(AVG(total_power) AS INTEGER) as avg_power
        FROM player_clan_snapshots
        WHERE to_char(collected_at, 'YYMM') = %s
          AND total_power > 0
          AND join_clan_name IS NOT NULL
        GROUP BY join_clan_name
        ORDER BY avg_power DESC
    """
    cursor.execute(query, (month,))
    
    result = {}
    for row in cursor.fetchall():
        result[row[0]] = row[1]
    return result


def query_members_now(clan_id: int, month: str) -> Dict[str, List[str]]:
    """
    查询指定月份公会成员的当前所在公会
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. 找出指定月份在该公会的成员
    members_then_query = """
        SELECT DISTINCT viewer_id
        FROM player_clan_snapshots
        WHERE join_clan_id = %s AND to_char(collected_at, 'YYMM') = %s
    """
    cursor.execute(members_then_query, (clan_id, month))
    target_vids = [r[0] for r in cursor.fetchall()]
    
    if not target_vids:
        return {}
        
    # 2. 找出这些成员现在的状态 (最新的 snapshot)
    vids_tuple = tuple(target_vids)
    current_status_query = """
        SELECT DISTINCT ON (viewer_id)
            join_clan_name, name
        FROM player_clan_snapshots
        WHERE viewer_id IN %s
        ORDER BY viewer_id, collected_at DESC
    """
    cursor.execute(current_status_query, (vids_tuple,))
    
    result = {}
    for row in cursor.fetchall():
        clan_name = row[0] or "Unknown/No Clan"
        member_name = row[1]
        
        if clan_name not in result:
            result[clan_name] = []
        result[clan_name].append(member_name)
    
    return result


# CLI 接口
def run_history(viewer_id: int):
    """运行历史查询"""
    print(f"查询玩家 {viewer_id} 的公会历史：")
    history = query_member_history(viewer_id)
    for record in history:
        print(f"  {record['month']}: {record['clan_name']}")


def run_members(clan_id: int):
    """运行成员时间线查询"""
    print(f"查询公会 {clan_id} 的成员加入时间线：")
    timeline = query_clan_members_timeline(clan_id)
    for month, names in timeline.items():
        print(f"  {month} 加入: {', '.join(names)}")


def run_power(month: str):
    """运行战力统计"""
    print(f"查询 {month} 公会平均战力：")
    powers = query_average_power(month)
    for name, power in list(powers.items())[:20]:
        print(f"  {name}: {power}")


def run_members_now(clan_id: int, month: str):
    """运行成员当前公会查询"""
    print(f"查询 {month} 公会 {clan_id} 成员的当前所在公会：")
    result = query_members_now(clan_id, month)
    for clan_name, members in result.items():
        print(f"  {clan_name}: {members}")
