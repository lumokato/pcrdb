"""
JJC 防守阵容采集任务
采集各分场 JJC 前 100 名排名及防守阵容
"""
import time
import asyncio
import os
from typing import Dict, Any, List

from pcrdb.account_pool import available_groups, lease_account
from pcrdb.api.endpoints import PCRApi, create_client
from pcrdb.db.connection import insert_snapshots_batch, utc_now
from psycopg2.extras import Json

# 用于统计实际获取的记录数
_fetch_counter = {'count': 0}


async def query_and_save_deck(client: PCRApi, group: int, pages: int = 2):
    """查询单个分组的排名并保存"""
    all_users = []
    
    for page in range(1, pages + 1):
        try:
            result = await client.query_arena_ranking(page)
            ranking = result.get('ranking', [])
            
            # 过滤 NPC (vid <= 1000000000 通常是 NPC)
            valid_users = [u for u in ranking if u.get('viewer_id', 0) > 1000000000]
            
            if valid_users:
                all_users.extend(valid_users)
                
            if page % 10 == 0:
                print(f"第 {group} 组第 {page} 页完成")
                
        except Exception as e:
            print(f"查询第 {group} 组第 {page} 页失败: {e}")
            
    if all_users:
        insert_deck_batch(all_users, group)
        print(f"第 {group} 组完成，共 {len(all_users)} 条记录")
    return len(all_users)


def insert_deck_batch(user_list: List[Dict], group: int):
    """批量插入防守阵容数据"""
    global _fetch_counter
    records = []
    now = utc_now()
    
    for user in user_list:
        # 提取阵容: 紧凑格式 [id, rarity, level, power]
        arena_deck = user.get('arena_deck', [])
        deck_compact = [
            [u['id'], u.get('unit_rarity', 0), u.get('unit_level', 0), u.get('power', 0)]
            for u in arena_deck
        ] if arena_deck else []
        
        record = {
            'viewer_id': user['viewer_id'],
            'team_level': user.get('team_level', 0),
            'arena_group': group,
            'arena_rank': user.get('rank', 0),
            'arena_deck': Json(deck_compact)
        }
        records.append(record)
    
    if records:
        insert_snapshots_batch('arena_deck_snapshots', records, collected_at=now)
        _fetch_counter['count'] += len(records)


async def _run_group(group_id: int) -> int:
    lease = lease_account("arena_deck_sync", arena_group=group_id)
    try:
        client = await create_client(lease.client_data)
        count = await query_and_save_deck(client, group_id)
    except BaseException as exc:
        try:
            lease.release(False, type(exc).__name__)
        except Exception as release_exc:
            print(f"分场 {group_id} 账号租约释放失败: {release_exc}")
        raise
    else:
        if count <= 0:
            lease.release(False, "EmptyResult")
            raise RuntimeError(f"JJC group {group_id} returned no usable rows")
        lease.release(True)
        return count


async def run_async(groups: List[int]):
    """异步运行"""
    if not groups:
        print("没有找到配置了 JJC 分场的账号。请确保 accounts 表中 arena_group 已正确设置。")
        return

    print(f"将采集以下分场: {groups}")
    results = await asyncio.gather(
        *(_run_group(group_id) for group_id in groups),
        return_exceptions=True,
    )
    errors = [result for result in results if isinstance(result, BaseException)]
    if errors:
        raise RuntimeError(f"{len(errors)} JJC group task(s) failed") from errors[0]


def run():
    """运行 JJC 防守阵容采集任务"""
    from pcrdb.db.task_logger import TaskLogger
    global _fetch_counter
    
    print("=" * 60)
    print("JJC 防守阵容采集任务 (PostgreSQL)")
    print("=" * 60)
    
    # 重置计数器
    _fetch_counter = {'count': 0}
    
    # 获取分场数以计算预期获取数
    groups = available_groups('arena')
    num_groups = len(groups)
    pages_per_group = 2  # query_and_save_deck 默认 pages=2
    records_per_page = 50  # 每页约50条
    records_expected = num_groups * pages_per_group * records_per_page
    
    task_logger = TaskLogger('arena_deck_sync')
    task_logger.start(
        records_expected=records_expected,
        details={'groups': groups, 'pages_per_group': pages_per_group}
    )
    
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        start = time.time()
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(run_async(groups))
        finally:
            loop.close()
        
        elapsed = time.time() - start
        print(f"任务完成，耗时 {elapsed:.2f} 秒")
        task_logger.finish_success(records_fetched=_fetch_counter['count'])
    except Exception as e:
        task_logger.finish_failed(str(e), records_fetched=_fetch_counter['count'])
        raise


if __name__ == '__main__':
    run()
