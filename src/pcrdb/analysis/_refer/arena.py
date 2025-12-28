"""
JJC 分场分析模块
提供分场人员分布、角色使用率等功能
"""
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
from pathlib import Path
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_connection

CONFIG_DIR = Path(__file__).parent.parent.parent.parent / 'config'


def find_group(group: int = 4, reverse: bool = False, days_diff: int = 1) -> Tuple[Dict[int, int], List[Dict]]:
    """
    分析分场人员分布 (比较最新数据和 N 天前的数据)
    
    Args:
        group: 目标分场编号
        reverse: True=分析原场去向，False=分析新场来源
        days_diff: 时间差（天）
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 获取最近两个时间点的快照 (简化逻辑: 获取最近一次和 24h 前的)
    # 实战中可能需要指定具体日期，这里取最近的两个不同日期的 distinct 集合可能比较慢。
    # 我们假设查询的是 player_profile_snapshots 中的 arena_group 变化。
    
    # 简单起见，我们只查当前状态和历史状态的对比。
    # 或者让用户传入两个日期? 这里的接口没有日期参数。
    # 我们尝试自动查找最近的两次大规模更新。
    
    # 这里简化为：查询当前在该分场的用户，查找他们在 N 天前的分场。
    
    # 1. 目标用户：当前在 group 的用户 (reverse=False) 或 N 天前在 group 的用户 (reverse=True)
    
    if not reverse:
        # 分析新场来源: 谁现在在 group? 他们以前在哪?
        print(f"分析 JJC {group} 场来源:")
        
        # 今天的用户
        cursor.execute("""
            SELECT DISTINCT viewer_id
            FROM player_profile_snapshots
            WHERE arena_group = %s AND collected_at > NOW() - INTERVAL '1 day'
        """, (group,))
        target_vids = [r[0] for r in cursor.fetchall()]
        
        if not target_vids:
            print("最近 1 天没有该分场数据")
            return {}, []
            
        # 查这些用户 N 天前的状态
        vids_tuple = tuple(target_vids)
        cursor.execute("""
            SELECT DISTINCT ON (viewer_id) viewer_id, arena_group, user_name, arena_rank
            FROM player_profile_snapshots
            WHERE viewer_id IN %s AND collected_at < NOW() - INTERVAL '%s days'
            ORDER BY viewer_id, collected_at DESC
        """, (vids_tuple, days_diff))
        
        source_groups = defaultdict(int)
        active_list = []
        
        for row in cursor.fetchall():
            prev_group = row[1]
            if prev_group and prev_group != group:
                source_groups[prev_group] += 1
                if row[3] < 21: # 前20名
                    active_list.append({'user_name': row[2], 'prev_group': prev_group, 'rank': row[3]})

        print(f"来源分布: {dict(source_groups)}")
        return dict(source_groups), active_list

    else:
        # 分析原场去向: 以前在 group 的人，现在去哪了?
        print(f"分析 JJC {group} 场去向:")
        
        cursor.execute("""
            SELECT DISTINCT viewer_id
            FROM player_profile_snapshots
            WHERE arena_group = %s AND collected_at < NOW() - INTERVAL '%s days' 
              AND collected_at > NOW() - INTERVAL '%s days'
        """, (group, days_diff, days_diff + 2)) # 限定一个历史时间窗口
        target_vids = [r[0] for r in cursor.fetchall()]

        if not target_vids:
            print(f"{days_diff} 天前没有该分场数据")
            return {}, []

        vids_tuple = tuple(target_vids)
        cursor.execute("""
            SELECT DISTINCT ON (viewer_id) viewer_id, arena_group, user_name, arena_rank
            FROM player_profile_snapshots
            WHERE viewer_id IN %s AND collected_at > NOW() - INTERVAL '1 day'
            ORDER BY viewer_id, collected_at DESC
        """, (vids_tuple,))
        
        dest_groups = defaultdict(int)
        active_list = []
        
        for row in cursor.fetchall():
            curr_group = row[1]
            if curr_group and curr_group != group:
                dest_groups[curr_group] += 1
                if row[3] < 21:
                    active_list.append({'user_name': row[2], 'curr_group': curr_group, 'rank': row[3]})
        
        print(f"去向分布: {dict(dest_groups)}")
        return dict(dest_groups), active_list


def get_avatar():
    """统计头像使用率 (基于最新数据)"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # 假设 emblem_id 在 API 响应中但我们没有在 schema 中显式存储为列?
    # 检查 schema: table player_profile_snapshots 没有 emblem_id。
    # 但是 grand_arena_snapshots 也没有 table definition 里 (legacy 有)。
    # 我们的 schema.sql 里 player_profile_snapshots 没有 emblem_id。
    # 既然数据库没有存，在这个版本里无法统计 emblem_id。
    # 除非它在 JSON 字段里? 也没有大的 JSON 字段。
    # 暂时跳过 emblem_id 统计，或者提示不支持。
    print("提示: 当前数据库架构未存储头像信息 (emblem_id)，无法统计头像使用率。")
    return


def arena_chara_stats() -> Dict[str, float]:
    """
    统计 JJC 防守角色使用率
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 从 arena_deck_snapshots 统计
    # arena_deck 是 JSONB。结构假设: user -> arena_deck list -> {unit_id: ...}
    # 或者 user -> defense_deck -> ...
    # 我们需要根据实际存储的数据结构来写查询。
    # 假设存储的是整个 API 返回的 UserInfo 对象。
    # 通常结构: arena_deck: [{unit_id: 1001, ...}, ...]
    
    # 尝试查询
    # 使用 jsonb_path_query 或 jsonb_array_elements
    # 假设 json 根目录下有 'arena_deck' 键，值为数组
    query = """
        SELECT value->>'unit_id'
        FROM arena_deck_snapshots,
             jsonb_array_elements(arena_deck->'arena_deck') as value
        WHERE collected_at > NOW() - INTERVAL '7 days'
    """
    
    # 如果根目录就是数组 (存储时如果是 Json(api_result['arena_deck']))
    # query = ... jsonb_array_elements(arena_deck) ...
    # 在 arena_deck_sync.py 中我们存的是: 'arena_deck': Json(user)
    # 所以应该是 arena_deck->'arena_deck'
    
    try:
        cursor.execute(query)
    except Exception as e:
        print(f"查询失败，可能是 JSON 结构不匹配: {e}")
        conn.rollback()
        return {}
        
    unit_count = defaultdict(int)
    total_entries = 0
    
    for row in cursor.fetchall():
        unit_id = row[0]
        if unit_id:
            unit_count[int(unit_id)] += 1
            total_entries += 1
            
    # 注意 total_entries 是角色总数 / 5 才是队伍数? 
    # 计算使用率通常是: 出现次数 / 总队伍数
    # 获取总队伍数
    cursor.execute("SELECT COUNT(*) FROM arena_deck_snapshots WHERE collected_at > NOW() - INTERVAL '7 days'")
    total_decks = cursor.fetchone()[0]
    
    if total_decks == 0:
        return {}
    
    # 加载角色名称
    unit_id_path = CONFIG_DIR / 'unit_id.json'
    unit_names = {}
    if unit_id_path.exists():
        with open(unit_id_path, encoding='utf-8') as f:
            unit_names = json.load(f)
            
    results = {}
    for unit_id, count in sorted(unit_count.items(), key=lambda x: x[1], reverse=True):
        name = unit_names.get(str(unit_id), f"ID:{unit_id}")
        pct = count / total_decks * 100
        results[name] = pct
        
    return results


# CLI 接口
def run_find_group(group: int = 4, reverse: bool = False):
    """运行分场分析"""
    find_group(group, reverse)

def run_avatar():
    """运行头像统计"""
    get_avatar()

def run_chara():
    """运行 JJC 防守角色使用率统计"""
    print("\nJJC 防守角色使用率 (近7天)：")
    stats = arena_chara_stats()
    for name, pct in list(stats.items())[:30]:
        print(f"  {name}: {pct:.2f}%")
