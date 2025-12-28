"""
PJJC 分场分析模块
提供胜场统计、排名分析等功能
"""
from typing import Dict, List, Tuple
from collections import defaultdict
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_connection

def count_winning_num(filter_num: int = 4000) -> Dict[int, int]:
    """
    统计各分组胜场超过阈值的人数
    (使用最近一次快照)
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 获取最近 7 天的数据
    query = """
        SELECT grand_arena_group, COUNT(*)
        FROM grand_arena_snapshots
        WHERE winning_number > %s 
          AND collected_at > NOW() - INTERVAL '7 days'
        GROUP BY grand_arena_group
        ORDER BY grand_arena_group
    """
    cursor.execute(query, (filter_num,))
    
    result = {}
    print(f"\n胜场超过 {filter_num} 人数 (近7天):")
    for row in cursor.fetchall():
        result[row[0]] = row[1]
        print(f"  第 {row[0]} 组: {row[1]}")
    
    return result


def count_top_clan() -> Dict[str, int]:
    """统计 PJJC 前 10 名的公会分布"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # 关联 PJJC 排名和公会信息
    # 取最近 3 天的数据
    query = """
        SELECT c.join_clan_name, COUNT(*)
        FROM grand_arena_snapshots g
        JOIN player_clan_snapshots c ON g.viewer_id = c.viewer_id
        WHERE g.grand_arena_rank <= 10
          AND g.collected_at > NOW() - INTERVAL '3 days'
          AND c.collected_at > NOW() - INTERVAL '7 days' -- 公会信息可能更新慢一点
          AND c.join_clan_name IS NOT NULL
        GROUP BY c.join_clan_name
        ORDER BY COUNT(*) DESC
    """
    cursor.execute(query)
    
    result = {}
    print("\nPJJC 前 10 名公会分布 (近3天):")
    for row in cursor.fetchall():
        result[row[0]] = row[1]
        print(f"  {row[0]}: {row[1]}")
        
    return result


def power_clan(filter_rank: int = 50) -> Dict[str, Dict[int, float]]:
    """
    统计各分场前 N 名的平均战力
    由于 grand_arena_snapshots 没有战力数据，需关联 player_profile_snapshots
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # PJJC 平均战力
    query = """
        SELECT g.grand_arena_group, AVG(p.total_power)
        FROM grand_arena_snapshots g
        JOIN player_profile_snapshots p ON g.viewer_id = p.viewer_id
        WHERE g.grand_arena_rank <= %s
          AND g.collected_at > NOW() - INTERVAL '3 days'
          AND p.collected_at > NOW() - INTERVAL '7 days'
        GROUP BY g.grand_arena_group
        ORDER BY g.grand_arena_group
    """
    cursor.execute(query, (filter_rank,))
    
    grand_avg = {}
    print(f"\nPJJC 前 {filter_rank} 平均战力:")
    for row in cursor.fetchall():
        grp = row[0]
        avg = round(row[1])
        grand_avg[grp] = avg
        print(f"  第 {grp} 组: {avg}")
    
    # JJC 平均战力 (直接查 profile)
    query_arena = """
        SELECT arena_group, AVG(total_power)
        FROM player_profile_snapshots
        WHERE arena_rank <= %s
          AND collected_at > NOW() - INTERVAL '3 days'
          AND arena_group > 0
        GROUP BY arena_group
        ORDER BY arena_group
    """
    cursor.execute(query_arena, (filter_rank,))
    
    arena_avg = {}
    print(f"\nJJC 前 {filter_rank} 平均战力:")
    for row in cursor.fetchall():
        grp = row[0]
        avg = round(row[1])
        arena_avg[grp] = avg
        print(f"  第 {grp} 组: {avg}")
        
    return {'arena': arena_avg, 'grand': grand_avg}


# CLI 接口
def run_winning(filter_num: int = 4000):
    """运行胜场统计"""
    count_winning_num(filter_num)

def run_top_clan():
    """运行公会排名统计"""
    count_top_clan()

def run_power(filter_rank: int = 50):
    """运行战力统计"""
    power_clan(filter_rank)
