"""
玩家档案同步任务
采集目标玩家的完整档案信息
"""
import time
from typing import Dict, Any, List, Tuple
from datetime import datetime
from psycopg2.extras import Json

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.base import TaskQueue
from db.connection import get_connection, insert_snapshots_batch, get_config


def get_target_players(mode: str = 'top_clans', rank_limit: int = 30) -> Tuple[List[int], Dict[int, Dict]]:
    """
    获取目标玩家列表
    
    Args:
        mode: 'top_clans' 获取前N公会成员, 'active_all' 获取所有活跃高战力玩家
        rank_limit: 公会排名限制（仅 top_clans 模式）
    
    Returns:
        (viewer_ids, member_info_dict)
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if mode == 'top_clans':
        # 获取最新一次快照中排名前N的公会
        cursor.execute("""
            WITH latest_date AS (
                SELECT DATE(MAX(collected_at)) as max_date
                FROM clan_snapshots
                WHERE collected_at > NOW() - INTERVAL '30 days'
            )
            SELECT DISTINCT clan_id
            FROM clan_snapshots
            WHERE current_period_ranking > 0 
              AND current_period_ranking <= %s
              AND exist = TRUE
              AND DATE(collected_at) = (SELECT max_date FROM latest_date)
            ORDER BY clan_id
        """, (rank_limit,))
        
        top_clans = [r[0] for r in cursor.fetchall()]
        
        if not top_clans:
            print(f"未找到最新快照中排名前 {rank_limit} 的公会，尝试使用评级...")
            cursor.execute("""
                WITH latest_date AS (
                    SELECT DATE(MAX(collected_at)) as max_date
                    FROM clan_snapshots
                    WHERE collected_at > NOW() - INTERVAL '30 days'
                )
                SELECT DISTINCT clan_id
                FROM clan_snapshots
                WHERE grade_rank > 0 AND grade_rank <= 3
                  AND exist = TRUE
                  AND DATE(collected_at) = (SELECT max_date FROM latest_date)
                ORDER BY clan_id
            """)
            top_clans = [r[0] for r in cursor.fetchall()]
        
        if not top_clans:
            return [], {}
        
        # 获取这些公会的成员（仅最近30天的记录）
        clan_ids_tuple = tuple(top_clans)
        cursor.execute("""
            SELECT DISTINCT ON (viewer_id) 
                viewer_id, join_clan_id, join_clan_name
            FROM player_clan_snapshots
            WHERE join_clan_id IN %s
              AND collected_at > NOW() - INTERVAL '30 days'
            ORDER BY viewer_id, collected_at DESC
        """, (clan_ids_tuple,))
        
        rows = cursor.fetchall()
        viewer_ids = []
        member_info = {}
        
        for r in rows:
            vid = r[0]
            viewer_ids.append(vid)
            member_info[vid] = {
                'join_clan_id': r[1],
                'join_clan_name': r[2]
            }
        
        return viewer_ids, member_info
    
    else:  # mode == 'active_all'
        # 获取所有活跃高战力玩家
        cursor.execute("""
            SELECT DISTINCT ON (viewer_id) 
                viewer_id, join_clan_id, join_clan_name
            FROM player_clan_snapshots
            WHERE total_power > 1000000 
              AND last_login_time > NOW() - INTERVAL '30 days'
            ORDER BY viewer_id, collected_at DESC
        """)
        
        rows = cursor.fetchall()
        viewer_ids = []
        member_info = {}
        
        for r in rows:
            vid = r[0]
            viewer_ids.append(vid)
            member_info[vid] = {
                'join_clan_id': r[1],
                'join_clan_name': r[2]
            }
        
        return viewer_ids, member_info


def process_profile(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理玩家档案数据
    提取所有字段（合并原 arena_sync 和 member_stats_sync 的字段）
    """
    if 'user_info' not in profile_data:
        return None
    
    user = profile_data['user_info']
    vid = user.get('viewer_id')
    
    # 提取天赋关卡通关信息
    talent_quest = profile_data.get('quest_info', {}).get('talent_quest', {})
    talent_clear = [0, 0, 0, 0, 0]
    for idx, tq in enumerate(talent_quest):
        talent_clear[idx] = tq.get('clear_count', 0)
    
    # 提取骑士经验
    princess_knight_exp = user.get('princess_knight_rank_total_exp', 0)

    favorite_unit_id = profile_data['favorite_unit']['id']

    user_comment = user.get('user_comment')
    
    return {
        'viewer_id': vid,
        'user_name': user.get('user_name', ''),
        'team_level': user.get('team_level', 0),
        'unit_num': user.get('unit_num', 0),
        'total_power': user.get('total_power', 0),
        'arena_rank': user.get('arena_rank', 0),
        'arena_group': user.get('arena_group', 0),
        'grand_arena_rank': user.get('grand_arena_rank', 0),
        'grand_arena_group': user.get('grand_arena_group', 0),
        'favorite_unit': favorite_unit_id,
        'user_comment': user_comment,
        'princess_knight_rank_total_exp': princess_knight_exp,
        'talent_quest_clear': talent_clear
    }


def insert_profile_batch(data_batch: List[Dict], member_info: Dict):
    """批量插入玩家档案数据"""
    records = []
    now = datetime.now()
    
    for data in data_batch:
        if not data:
            continue
            
        vid = data['viewer_id']
        info = member_info.get(vid, {})
        
        record = {
            'viewer_id': vid,
            'user_name': data['user_name'],
            'team_level': data['team_level'],
            'total_power': data['total_power'],
            'unit_num': data['unit_num'],
            'arena_rank': data['arena_rank'],
            'arena_group': data['arena_group'],
            'grand_arena_rank': data['grand_arena_rank'],
            'grand_arena_group': data['grand_arena_group'],
            'favorite_unit': data['favorite_unit'],
            'user_comment': data.get('user_comment', ''),
            'join_clan_id': info.get('join_clan_id'),
            'join_clan_name': info.get('join_clan_name'),
            'princess_knight_rank_total_exp': data['princess_knight_rank_total_exp'],
            'talent_quest_clear': Json(data['talent_quest_clear'])
        }
        records.append(record)
    
    if records:
        insert_snapshots_batch('player_profile_snapshots', records, collected_at=now)


def run(mode: str = 'top_clans', rank_limit: int = 30):
    """
    运行玩家档案同步任务
    
    Args:
        mode: 'top_clans' 每日模式（前N公会）, 'active_all' 月度模式（所有活跃玩家）
        rank_limit: 公会排名限制
    """
    from db.task_logger import TaskLogger
    
    print("=" * 60)
    print("玩家档案同步任务 (PostgreSQL)")
    print("=" * 60)
    
    config = get_config()
    print(f"DEBUG Config: DB={config['database']}")
    print(f"运行模式: {mode}")
    
    viewer_ids, member_info = get_target_players(mode, rank_limit)
    records_expected = len(viewer_ids)
    
    if mode == 'top_clans':
        print(f"待查询成员: {records_expected} 人 (前 {rank_limit} 公会)")
    else:
        print(f"待查询成员: {records_expected} 人 (所有活跃高战力)")
    
    # 用于累计实际获取的记录数
    fetch_counter = {'count': 0}
    
    # 根据mode确定task_name
    task_name = 'player_profile_sync_monthly' if mode == 'active_all' else 'player_profile_sync'
    task_logger = TaskLogger(task_name)
    task_logger.start(
        records_expected=records_expected,
        details={'mode': mode, 'rank_limit': rank_limit}
    )
    
    if not viewer_ids:
        print("没有待查询的成员")
        task_logger.finish_success(records_fetched=0)
        return
    
    try:
        # 使用闭包传递 member_info 和计数
        def inserter_with_count(batch):
            fetch_counter['count'] += len(batch)
            insert_profile_batch(batch, member_info)
        
        queue = TaskQueue(
            query_list=viewer_ids,
            data_processor=process_profile,
            pg_inserter=inserter_with_count,
            sync_num=config['sync_num'],
            batch_size=config['batch_size']
        )
        
        queue.run()
        task_logger.finish_success(records_fetched=fetch_counter['count'])
    except Exception as e:
        task_logger.finish_failed(str(e), records_fetched=fetch_counter['count'])
        raise


if __name__ == '__main__':
    # 默认每日模式：前30公会
    run(mode='top_clans', rank_limit=30)
