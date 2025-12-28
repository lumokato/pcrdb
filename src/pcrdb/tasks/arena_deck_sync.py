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


async def query_and_save_deck(client: PCRApi, group: int, pages: int = 5):
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
        # Schema: viewer_id, collected_at, user_name, team_level, 
        # arena_group, arena_rank, arena_deck
        
        # 提取阵容信息，通常 API 返回中包含阵容数据
        # 假设 user 结构包含 deck 信息，或者我们存储整个 user 对象的有效部分
        # 如果 user 里面有 specifically 'arena_deck' 字段最好，没有就把整个 user 存入 JSONB?
        # schema 中 arena_deck 是 JSONB。
        # 即使 API 返回的结构是扁平的，我们也尽量结构化。
        # 通常 leaderboard 返回包含 'unit_ids' 列表或类似结构。
        # 为了兼容性，我们将整个 user 对象中可能的 deck 相关字段存入 arena_deck。
        
        # 简单的 deck 提取：
        deck_info = {}
        if 'arena_deck' in user:
            deck_info = user['arena_deck']
        else:
            # 尝试提取单位列表
            # 具体字段取决于 API 响应，这里作为 fallback 把整个 user 存进去供后续分析
            # 但为了节省空间，只存关键的 units
            # 如果不确定字段，暂时存空或者只存 user
            pass
            
        # 如果完全不确定 deck 字段，可以将 user 字典作为 json 存入，
        # 但要注意 user 可能包含不需要的大字段。
        # 鉴于这是 snapshots，存整个 dict 也是可以的，PostgreSQL JSONB 压缩不错。
        deck_json = user 
        
        record = {
            'viewer_id': user['viewer_id'],
            'user_name': user.get('user_name', ''),
            'team_level': user.get('team_level', 0),
            'arena_group': group,
            'arena_rank': user.get('rank', 0),
            'arena_deck': Json(deck_json) 
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
