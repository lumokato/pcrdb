"""
JJC 防守阵容采集任务
采集各分场 JJC 前 100 名排名及防守阵容
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
from psycopg2.extras import Json


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


def insert_deck_batch(user_list: List[Dict], group: int):
    """批量插入防守阵容数据"""
    records = []
    now = datetime.now()
    
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


async def run_async():
    """异步运行"""
    # 获取每个分场的查询账号
    accounts_map = get_accounts_by_group('arena')
    
    if not accounts_map:
        print("没有找到配置了 JJC 分场的账号。请确保 accounts 表中 arena_group 已正确设置。")
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
            task = asyncio.create_task(query_and_save_deck(client, group_id))
            tasks.append(task)
        except Exception as e:
            print(f"分场 {group_id} (账号 {account.uid}) 初始化失败: {e}")
    
    if tasks:
        await asyncio.gather(*tasks)


def run():
    """运行 JJC 防守阵容采集任务"""
    print("=" * 60)
    print("JJC 防守阵容采集任务 (PostgreSQL)")
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
