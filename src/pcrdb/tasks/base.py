"""
任务队列基类
提供并发数据采集的基础设施
"""
import os
import time
import math
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Callable, Optional
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.endpoints import PCRApi, create_client
from db.connection import get_accounts, Account


class TaskQueue:
    """
    并发任务队列
    支持多客户端并行采集，直接写入 PostgreSQL
    """
    
    def __init__(
        self,
        query_list: List[int],
        data_processor: Callable[[Dict], Any],
        pg_inserter: Callable[[List[Dict]], None],
        sync_num: int = 10,
        batch_size: int = 30
    ):
        """
        初始化任务队列
        
        Args:
            query_list: 查询 ID 列表
            data_processor: 数据处理函数，返回 None 表示失败需重试
            pg_inserter: PostgreSQL 插入函数 (接收 list of dict)
            sync_num: 并发客户端数量 (最大)
            batch_size: 每批处理数量
        """
        self.query_list = query_list
        # 去重query_list，防止重复查询
        if query_list:
             self.query_list = sorted(list(set(query_list)))
             
        self.data_processor = data_processor
        self.pg_inserter = pg_inserter
        self.sync_num = sync_num
        self.batch_size = batch_size
        
        # 自动判断查询类型：viewer_id > 1万亿
        self.query_type = 'profile' if self.query_list and self.query_list[0] > 1000000000000 else 'clan'
    
    async def _monitor(self):
        """进度监控协程"""
        last_log_time = 0
        while True:
            if self.processed_count >= self.total_tasks:
                break
                
            now = time.time()
            if now - last_log_time >= 0.2: # 刷新频率提高
                pct = self.processed_count / self.total_tasks if self.total_tasks > 0 else 0
                elapsed = now - self.start_time
                rate = self.processed_count / elapsed if elapsed > 0 else 0
                eta = (self.total_tasks - self.processed_count) / rate if rate > 0 else 0
                
                # ASCII 进度条
                # [██████████--------] 50.0% 500/1000 [10.5it/s] ETA: 00:45
                bar_len = 30
                filled_len = int(bar_len * pct)
                bar = '█' * filled_len + '-' * (bar_len - filled_len)
                
                eta_str = time.strftime("%M:%S", time.gmtime(eta))
                
                sys.stdout.write(f"\r|{bar}| {pct:.1%} {self.processed_count}/{self.total_tasks} [{rate:.1f}it/s] ETA: {eta_str}")
                sys.stdout.flush()
                last_log_time = now
            
            await asyncio.sleep(0.1)
            
        elapsed = time.time() - self.start_time
        sys.stdout.write(f"\r|{'█'*30}| 100.0% {self.total_tasks}/{self.total_tasks} [{self.total_tasks/elapsed:.1f}it/s] Time: {elapsed:.1f}s\n")
        sys.stdout.flush()

    async def _worker(self, account_dict: Dict, client_index: int):
        """单个客户端工作协程"""
        
        # 1. 登录
        client = None
        try:
            client = await create_client(account_dict)
        except Exception as e:
            return

        # 2. 消费队列
        while True:
            batch = []
            try:
                for _ in range(self.batch_size):
                    if self.queue.empty():
                        break
                    query_id = self.queue.get_nowait()
                    batch.append(query_id)
            except asyncio.QueueEmpty:
                pass
            
            if not batch:
                break
                
            data_batch = []
            
            for query_id in batch:
                success = False
                for retry in range(4):
                    try:
                        if self.query_type == 'clan':
                            result = await client.query_clan(query_id)
                        else:
                            result = await client.query_profile(query_id)
                        
                        processed = self.data_processor(result)
                        if processed:
                            data_batch.append(processed)
                            success = True
                            break
                        else:
                            print(f"\n[DEBUG] Processed returned None for {query_id}")
                    except Exception as e:
                        print(f"\n[DEBUG] Query error for {query_id}: {e}")
                    
                if not success and retry < 3:
                     # 必须使用 await asyncio.sleep，否则会阻塞整个线程
                     await asyncio.sleep(2)  # 减少等待时间加快重试
                     try:
                         await client.login()
                     except Exception as e:
                         pass
                
                self.processed_count += 1
                self.queue.task_done()
            
            if self.pg_inserter and data_batch:
                try:
                    print(f"\n[DEBUG] Inserting {len(data_batch)} records...")
                    self.pg_inserter(data_batch)
                    print(f"[DEBUG] Insert done.")
                except Exception as e:
                    print(f"\nDB Error: {e}")

    async def _run_async(self):
        """异步主函数"""
        # 从数据库获取活跃账号
        accounts = get_accounts(active_only=True)
        if not accounts:
            print("错误: 没有找到活跃的采集账号 (is_active=True)")
            return

        # 限制并发数不超过账号数
        actual_sync_num = min(self.sync_num, len(accounts))
        print(f"启动 {actual_sync_num} 个采集客户端...")
        
        # 初始化队列
        self.queue = asyncio.Queue()
        for qid in self.query_list:
            self.queue.put_nowait(qid)
        
        # 进度追踪
        self.total_tasks = len(self.query_list)
        self.processed_count = 0
        self.start_time = time.time()
        
        # 启动监控协程
        monitor_task = asyncio.create_task(self._monitor())

        tasks = []
        for i in range(actual_sync_num):
            account_data = accounts[i]
            
            # 转换为 create_client 需要的字典格式
            acc_dict = {
                'vid': account_data.viewer_id,
                'uid': str(account_data.uid),
                'access_key': account_data.access_key
            }
            
            # 直接传递 dict 给 worker
            task = asyncio.create_task(self._worker(acc_dict, i))
            tasks.append(task)
            # 错峰启动，避免并发登录拥堵
            await asyncio.sleep(0.5)
        
        if tasks:
            await asyncio.gather(*tasks)
            # 等待监控结束 (tasks done -> monitor loop break)
            await monitor_task
        else:
            print("没有成功启动任何客户端任务")
    
    def run(self):
        """运行任务队列"""
        start = time.time()
        
        # 在 Windows 上使用 WindowsSelectorEventLoopPolicy
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._run_async())
        finally:
            loop.close()
        
        elapsed = time.time() - start
        print(f"任务完成，耗时 {elapsed:.2f} 秒")
