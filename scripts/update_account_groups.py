"""
更新账号的分场信息
登录每个账号，查询其 JJC/PJJC 分场，并更新到 accounts 表
"""
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'pcrdb'))

from api.endpoints import create_client
from db.connection import get_accounts, update_account


async def update_account_worker(account):
    """更新单个账号信息"""
    print(f"[{account.uid}] 正在登录...")
    try:
        acc_dict = {
            'vid': account.viewer_id,
            'uid': str(account.uid),
            'access_key': account.access_key
        }
        client = await create_client(acc_dict)
        
        # 尝试激活/刷新场次信息 (通过查询 info 接口)
        try:
            await client.query_arena_info()
        except Exception:
            pass
            
        try:
            await client.query_grand_arena_info()
        except Exception:
            pass

        # 查询自己档案
        print(f"[{account.uid}] 查询档案...")
        target_vid = client.viewer_id
        profile = await client.query_profile(target_vid)
        
        if 'user_info' in profile:
            info = profile['user_info']
            
            updates = {
                'name': info.get('user_name', ''),
                'viewer_id': info.get('viewer_id'),
                'arena_group': info.get('arena_group', 0),
                'grand_arena_group': info.get('grand_arena_group', 0)
            }
            
            update_account(str(account.uid), **updates)
            print(f"[{account.uid}] 更新成功: {updates}")
        else:
            print(f"[{account.uid}] 无法获取档案信息")
            
    except Exception as e:
        print(f"[{account.uid}] 失败: {e}")


async def main():
    accounts = get_accounts(active_only=True)
    print(f"共 {len(accounts)} 个活跃账号")
    
    tasks = []
    # 限制并发
    semaphore = asyncio.Semaphore(10)
    
    async def bounded_worker(acc):
        async with semaphore:
            await update_account_worker(acc)
            
    for acc in accounts:
        tasks.append(asyncio.create_task(bounded_worker(acc)))
        
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
