"""
公会信息同步任务
每月同步所有公会和成员信息
"""
import time
from typing import Dict, Any, List
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.base import TaskQueue
from db.connection import get_connection, insert_snapshots_batch, get_config


def build_query_list(new_clan_add: int = 100) -> List[int]:
    """
    构建待查询的公会 ID 列表
    定义活跃公会: 成员中最后一次登录时间在快照时间一个月之内
    """
    start_time = time.time()
    print("正在构建待查询公会列表...")
    
    conn = get_connection()
    # Debug: Print current database
    print(f"Connected to Database: {get_config()['database']}")
    
    cursor = conn.cursor()
    
    # SQL: 查找活跃公会
    # 逻辑: 按可以 join_clan_id 分组，如果该公会最新快照里有成员登录时间 > 快照时间 - 30天，则视为活跃
    # 注意: player_clan_snapshots 可能很大，这个查询可能慢，需关注性能
    query_active_sql = """
        SELECT join_clan_id 
        FROM player_clan_snapshots
        WHERE join_clan_id IS NOT NULL
        GROUP BY join_clan_id
        HAVING MAX(last_login_time) > MAX(collected_at) - INTERVAL '30 days'
        ORDER BY join_clan_id
    """
    
    cursor.execute(query_active_sql)
    active_clans = [r[0] for r in cursor.fetchall()]
    
    now = datetime.now()
    is_full_scan_month = (now.month == 1 or now.month == 7)
    
    # 如果是空库 (无历史数据) 且不是生产库，尝试从生产库获取种子列表 (仅用于测试验证)
    if not active_clans:
        current_db = get_config()['database']
        if current_db != 'pcrdb':
            print(f"当前库 {current_db} 活跃公会为空，尝试从生产库 pcrdb 获取...")
            try:
                import psycopg2
                prod_cfg = get_config().copy()
                prod_cfg['database'] = 'pcrdb'
                
                with psycopg2.connect(
                    host=prod_cfg['host'], port=prod_cfg['port'], 
                    user=prod_cfg['user'], password=prod_cfg['password'], 
                    database='pcrdb'
                ) as prod_conn:
                    with prod_conn.cursor() as prod_cur:
                        prod_cur.execute(query_active_sql)
                        active_clans = [r[0] for r in prod_cur.fetchall()]
                print(f"从生产库获取到 {len(active_clans)} 个活跃公会")
            except Exception as e:
                print(f"从生产库获取失败: {e}")

    query_cost = time.time() - start_time
    print(f"构建列表耗时: {query_cost:.2f} 秒")

    if not active_clans:
        print("无活跃历史数据，执行默认初始化全量范围 1-5000")
        return list(range(1, 5001))

    max_id = max(active_clans)
    
    if is_full_scan_month:
        print(f"当前是 {now.month} 月，执行全量扫描 (1-{max_id + 500})")
        return list(range(1, max_id + 500))
    else:
        # 添加新公会 ID
        print(f"当前是 {now.month} 月，执行活跃扫描 (活跃: {len(active_clans)} + 新增探测: {new_clan_add})")
        extra_clans = list(range(max_id + 1, max_id + new_clan_add + 1))
        # 合并去重
        final_list = sorted(list(set(active_clans + extra_clans)))
        return final_list


def process_clan_data(clan_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理公会 API 返回数据
    
    Returns:
        处理后的数据，或 None 表示需要重试
    """
    if 'clan' in clan_data:
        # 成功获取数据
        return {
            "type": "data",
            "content": clan_data,
            "clan_id": clan_data['clan']['detail']['clan_id']
        }
    
    elif 'server_error' in clan_data:
        msg = clan_data.get('server_error', {}).get('message', '')
        if '此行会已解散' in msg:
            # 标记为解散
            # query_clan 失败时没办法直接从 API 得到 ID，通常需要调用者知道 ID
            # 但在这里 process_clan_data 只接收结果。
            # 这里的 TaskQueue 实现是将 query_id 作为 key 传递不好传，
            # 实际上 base.py 中的 _worker 并没有把 query_id 传给 data_processor。
            # 我们需要让 processed data 包含 ID。
            # 修改: 如果是解散，我们无法从 resp 中拿到 ID (API error通常不带请求参数)
            # 不过我们可以在 data_processor 闭包里或者修改 base.py 传 ID。
            # 暂时忽略解散的具体 ID 记录？
            # 不，base.py 里我们可以看到 query_id。
            # 让我们简化：如果解散，返回 None 或者特定标记，但缺少 ID。
            # 现在的 base.py 实现： processed = self.data_processor(result)
            # 并没有传递 query_id。
            # 这是一个问题。不过通常 search result 会包含 ID。
            # 如果是解散，API 返回可能不包含 ID。
            return None # 暂时跳过解散处理，或者需要修改 base.py 传入 query_id
            
        elif '连接中断' in msg:
            return None  # 需要重试
    
    return None


def insert_clan_batch(data_batch: List[Dict]):
    """批量插入公会数据"""
    clan_records = []
    member_records = []
    
    now = datetime.now()
    
    for item in data_batch:
        if item.get('type') != 'data':
            continue
            
        content = item['content']
        clan = content['clan']
        detail = clan['detail']
        members = clan['members']
        
        # 1. 准备公会快照
        clan_records.append({
            'clan_id': detail['clan_id'],
            'clan_name': detail['clan_name'],
            'leader_viewer_id': detail['leader_viewer_id'],
            'leader_name': detail['leader_name'],
            'join_condition': detail['join_condition'],
            'activity': detail['activity'],
            'clan_battle_mode': detail['clan_battle_mode'],
            'member_num': detail['member_num'],
            'current_period_ranking': detail['current_period_ranking'],
            'grade_rank': detail['grade_rank'],
            'description': detail['description'],
            'exist': True
        })
        
        # 2. 准备成员快照
        for m in members:
            # 转换 last_login_time (int timestamp) -> datetime
            login_ts = m['last_login_time']
            login_time = datetime.fromtimestamp(login_ts) if login_ts else None
            
            member_records.append({
                'viewer_id': m['viewer_id'],
                'name': m['name'],
                'level': m['level'],
                'role': m['role'],
                'total_power': m['total_power'],
                'join_clan_id': detail['clan_id'],
                'join_clan_name': detail['clan_name'],
                'last_login_time': login_time
            })
    
    # 批量插入
    if clan_records:
        insert_snapshots_batch('clan_snapshots', clan_records, collected_at=now)
        
    if member_records:
        insert_snapshots_batch('player_clan_snapshots', member_records, collected_at=now)


def run(new_clan_add: int = 100):
    """运行公会信息同步任务"""
    from db.task_logger import TaskLogger
    
    print("=" * 60)
    print(f"公会信息同步任务 (PostgreSQL)")
    print("=" * 60)
    
    config = get_config()
    
    query_list = build_query_list(new_clan_add)
    query_count = len(query_list)
    print(f"待查询公会: {query_count} 个")
    
    # 预估获取数：使用上次入库数或查询数 × 30（估计每公会30成员）
    # 简化：用查询数 × 30 作为预估
    records_expected = query_count * 31  # 1条公会 + 约30条成员
    
    # 用于累计实际获取的记录数
    fetch_counter = {'count': 0}
    
    def insert_with_count(data_batch):
        """带计数的插入函数"""
        fetch_counter['count'] += len(data_batch)
        insert_clan_batch(data_batch)
    
    # 初始化日志记录
    task_logger = TaskLogger('clan_sync')
    task_logger.start(
        records_expected=records_expected, 
        details={'new_clan_add': new_clan_add, 'query_count': query_count}
    )
    
    try:
        queue = TaskQueue(
            query_list=query_list,
            data_processor=process_clan_data,
            pg_inserter=insert_with_count,
            sync_num=config['sync_num'],
            batch_size=config['batch_size']
        )
        
        queue.run()
        task_logger.finish_success(records_fetched=fetch_counter['count'])
    except Exception as e:
        task_logger.finish_failed(str(e), records_fetched=fetch_counter['count'])
        raise


if __name__ == '__main__':
    run()
