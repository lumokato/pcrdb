"""
PJJC 排名同步任务
采集各分场 PJJC 前 200 名排名
"""
import time
import asyncio
import os
from datetime import datetime
from typing import Dict, Any, List

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.endpoints import PCRApi, create_client
from db.connection import get_accounts_by_group, insert_snapshots_batch


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


def insert_grand_ranking(ranking_list: List[Dict], group: int):
    """插入 PJJC 排名数据"""
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
        print(f"已保存第 {group} 组数据: {len(records)} 条")


async def run_async():
    """异步运行"""
    # 获取每个分场的查询账号
    # {group_id: Account}
    accounts_map = get_accounts_by_group('grand_arena')
    
    if not accounts_map:
        print("没有找到配置了 PJJC 分场的账号。请确保 accounts 表中 grand_arena_group 已正确设置。")
        return

    print(f"将采集以下分场: {list(accounts_map.keys())}")
    
    tasks = []
    for group_id, account in accounts_map.items():
        # 创建客户端
        acc_dict = {
            'vid': account.viewer_id,
            'uid': str(account.uid),
            'access_key': account.access_key
        }
        
        try:
            client = await create_client(acc_dict)
            task = asyncio.create_task(query_and_save_ranking(client, group_id))
            tasks.append(task)
        except Exception as e:
            print(f"分场 {group_id} (账号 {account.uid}) 初始化失败: {e}")
    
    if tasks:
        await asyncio.gather(*tasks)


def run():
    """运行 PJJC 排名同步任务"""
    print("=" * 60)
    print("PJJC 排名同步任务 (PostgreSQL)")
    print("=" * 60)
    
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    start = time.time()
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_async())
    finally:
        loop.close()
    
    elapsed = time.time() - start
    print(f"任务完成，耗时 {elapsed:.2f} 秒")


if __name__ == '__main__':
    run()
