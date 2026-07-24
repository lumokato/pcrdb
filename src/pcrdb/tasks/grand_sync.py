"""
PJJC 排名同步任务
采集各分场 PJJC 前 200 名排名
"""
import time
import asyncio
import os
from datetime import datetime
from typing import Dict, Any, List

from pcrdb.account_pool import available_groups, lease_account
from pcrdb.api.endpoints import PCRApi, create_client
from pcrdb.db.connection import insert_snapshots_batch

# 用于统计实际获取的记录数
_fetch_counter = {'count': 0}


async def query_and_save_ranking(client: PCRApi, group: int, pages: int = 10):
    """查询单个分组的排名并保存"""
    all_rankings = []
    
    for page in range(1, pages + 1):
        try:
            result = await client.query_grand_arena_ranking(page)
            ranking = result.get('ranking', [])
            if ranking:
                all_rankings.extend(ranking)
                print(f"完成第 {group} 组第 {page} 页 (获取 {len(ranking)} 条)")
            else:
                print(f"第 {group} 组第 {page} 页为空")
        except Exception as e:
            print(f"查询第 {group} 组第 {page} 页失败: {e}")
            # 简单的错误处理，继续下一页
    
    if all_rankings:
        insert_grand_ranking(all_rankings, group)
    return len(all_rankings)


def insert_grand_ranking(ranking_list: List[Dict], group: int):
    """插入 PJJC 排名数据"""
    global _fetch_counter
    records = []
    now = datetime.now()
    
    for user in ranking_list:
        # favorite_unit 可能是字典或直接是 id
        fav_unit = user.get('favorite_unit')
        
        favorite_unit_id = fav_unit.get('id', 0)

 
        
        record = {
            'viewer_id': user['viewer_id'],
            'user_name': user.get('user_name', ''),
            'team_level': user.get('team_level', 0),
            'grand_arena_rank': user.get('rank', 0),
            'grand_arena_group': group,
            'winning_number': user.get('winning_number', 0),
            'favorite_unit': favorite_unit_id
        }
        records.append(record)
    
    if records:
        insert_snapshots_batch('grand_arena_snapshots', records, collected_at=now)
        _fetch_counter['count'] += len(records)
        print(f"已保存第 {group} 组数据: {len(records)} 条")


async def _run_group(group_id: int) -> int:
    lease = lease_account("grand_sync", grand_arena_group=group_id)
    try:
        client = await create_client(lease.client_data)
        count = await query_and_save_ranking(client, group_id)
    except BaseException as exc:
        try:
            lease.release(False, type(exc).__name__)
        except Exception as release_exc:
            print(f"分场 {group_id} 账号租约释放失败: {release_exc}")
        raise
    else:
        if count <= 0:
            lease.release(False, "EmptyResult")
            raise RuntimeError(f"PJJC group {group_id} returned no usable rows")
        lease.release(True)
        return count


async def run_async(groups: List[int]):
    """异步运行"""
    if not groups:
        print("没有找到配置了 PJJC 分场的账号。请确保 accounts 表中 grand_arena_group 已正确设置。")
        return

    print(f"将采集以下分场: {groups}")
    results = await asyncio.gather(
        *(_run_group(group_id) for group_id in groups),
        return_exceptions=True,
    )
    errors = [result for result in results if isinstance(result, BaseException)]
    if errors:
        raise RuntimeError(f"{len(errors)} PJJC group task(s) failed") from errors[0]


def run():
    """运行 PJJC 排名同步任务"""
    from pcrdb.db.task_logger import TaskLogger
    global _fetch_counter
    
    print("=" * 60)
    print("PJJC 排名同步任务 (PostgreSQL)")
    print("=" * 60)
    
    # 重置计数器
    _fetch_counter = {'count': 0}
    
    # 获取分场数以计算预期获取数
    groups = available_groups('grand_arena')
    num_groups = len(groups)
    pages_per_group = 10
    records_per_page = 20
    records_expected = num_groups * pages_per_group * records_per_page
    
    task_logger = TaskLogger('grand_sync')
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
