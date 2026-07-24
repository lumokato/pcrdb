"""
任务队列基类
提供并发数据采集的基础设施
"""
import os
import sys
import time
import math
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Callable, Optional
from pcrdb.api.endpoints import PCRApi, create_client
from pcrdb.account_pool import AccountLease, lease_accounts


class TaskQueueError(RuntimeError):
    """The query queue could not complete with the available accounts."""


class RetryableResultError(RuntimeError):
    """The API responded, but the response asks the caller to retry."""


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
        batch_size: int = 30,
        purpose: str = "bulk_sync",
    ):
        """
        初始化任务队列
        
        Args:
            query_list: 查询 ID 列表
            data_processor: 返回处理后数据；None 表示正常空结果，需重试时抛 RetryableResultError
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
        self.purpose = purpose
        
        # 自动判断查询类型：viewer_id > 1万亿
        self.query_type = 'profile' if self.query_list and self.query_list[0] > 1000000000000 else 'clan'
    
    async def _monitor(self):
        """进度监控协程"""
        last_log_time = 0
        while not self.workers_done:
                
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
                
                eta_str = time.strftime("%H:%M:%S", time.gmtime(eta))
                
                sys.stdout.write(f"\r|{bar}| {pct:.1%} {self.processed_count}/{self.total_tasks} [{rate:.1f}it/s] ETA: {eta_str}")
                sys.stdout.flush()
                last_log_time = now
            
            await asyncio.sleep(0.1)
            
        elapsed = time.time() - self.start_time
        pct = self.processed_count / self.total_tasks if self.total_tasks else 1
        sys.stdout.write(
            f"\r|{'█' * int(30 * pct)}{'-' * (30 - int(30 * pct))}| "
            f"{pct:.1%} {self.processed_count}/{self.total_tasks} "
            f"[{self.processed_count / elapsed if elapsed else 0:.1f}it/s] "
            f"Time: {elapsed:.1f}s\n"
        )
        sys.stdout.flush()

    async def _worker(self, lease: AccountLease, client_index: int) -> Dict[str, int]:
        """单个客户端工作协程"""
        succeeded = 0
        failed = 0
        result = {"succeeded": 0, "failed": 0, "login_failed": 0}
        lease_success = False
        lease_error_type: str | None = "UnknownError"
        try:
            try:
                client = await create_client(lease.client_data)
            except Exception as exc:
                lease_error_type = type(exc).__name__
                result["login_failed"] = 1
            else:
                while True:
                    batch = []
                    try:
                        for _ in range(self.batch_size):
                            if self.queue.empty():
                                break
                            batch.append(self.queue.get_nowait())
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
                                    response = await client.query_clan(query_id)
                                else:
                                    response = await client.query_profile(query_id)
                            except Exception as exc:
                                print(f"\n[DEBUG] Query error for {query_id}: {exc}")
                            else:
                                try:
                                    processed = self.data_processor(response)
                                except RetryableResultError as exc:
                                    print(
                                        f"\n[DEBUG] Retryable result for {query_id}: {exc}"
                                    )
                                except Exception as exc:
                                    raise TaskQueueError(
                                        f"data processing failed for {query_id}: "
                                        f"{type(exc).__name__}"
                                    ) from exc
                                else:
                                    if processed:
                                        data_batch.append(processed)
                                        success = True
                                        succeeded += 1
                                    # Empty data is a normal miss in sparse clan-ID scans.
                                    break

                            if not success and retry < 3:
                                await asyncio.sleep(2)
                                try:
                                    await client.login()
                                except Exception:
                                    pass

                        if not success:
                            failed += 1
                        self.processed_count += 1
                        self.queue.task_done()

                    if self.pg_inserter and data_batch:
                        try:
                            print(f"\n[DEBUG] Inserting {len(data_batch)} records...")
                            self.pg_inserter(data_batch)
                            print("[DEBUG] Insert done.")
                        except Exception as exc:
                            raise TaskQueueError(
                                f"database insert failed: {type(exc).__name__}"
                            ) from exc

                lease_success = succeeded > 0 or failed == 0
                lease_error_type = None if lease_success else "EmptyResult"
                result = {
                    "succeeded": succeeded,
                    "failed": failed,
                    "login_failed": 0,
                }
        except BaseException as exc:
            try:
                lease.release(False, type(exc).__name__)
            except Exception as release_exc:
                print(f"\n账号租约释放失败: {release_exc}")
            raise
        else:
            lease.release(lease_success, lease_error_type)
        return result

    async def _run_async(self):
        """异步主函数"""
        if not self.query_list:
            return {"succeeded": 0, "failed": 0, "login_failed": 0}

        requested_clients = min(self.sync_num, len(self.query_list))
        leases = lease_accounts(requested_clients, self.purpose)
        if not leases:
            print("错误: 共享农场账号池当前没有可用账号")
            raise TaskQueueError("shared farm account pool has no available account")

        actual_sync_num = len(leases)
        if actual_sync_num < requested_clients:
            print(f"账号池仅租到 {actual_sync_num}/{requested_clients} 个账号，降低并发继续")
        if actual_sync_num <= 0:
            return
        print(f"启动 {actual_sync_num} 个采集客户端...")
        
        # 初始化队列
        self.queue = asyncio.Queue()
        for qid in self.query_list:
            self.queue.put_nowait(qid)
        
        # 进度追踪
        self.total_tasks = len(self.query_list)
        self.processed_count = 0
        self.start_time = time.time()
        self.workers_done = False
        
        # 启动监控协程
        monitor_task = asyncio.create_task(self._monitor())

        tasks: list[asyncio.Task] = []
        cleanup_errors: list[Exception] = []
        try:
            for i, lease in enumerate(leases):
                task = asyncio.create_task(self._worker(lease, i))
                tasks.append(task)
                # 错峰启动，避免并发登录拥堵
                await asyncio.sleep(0.5)

            worker_results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            for lease in leases:
                if lease.released:
                    continue
                try:
                    lease.release(False, "TaskCleanup")
                except Exception as exc:
                    cleanup_errors.append(exc)
            self.workers_done = True
            await monitor_task

        worker_errors = [
            result for result in worker_results if isinstance(result, BaseException)
        ]
        if worker_errors:
            raise TaskQueueError(
                f"{len(worker_errors)} account worker(s) failed"
            ) from worker_errors[0]
        if cleanup_errors:
            raise TaskQueueError(
                f"{len(cleanup_errors)} account lease(s) failed to release"
            ) from cleanup_errors[0]

        result = {
            key: sum(item[key] for item in worker_results)
            for key in ("succeeded", "failed", "login_failed")
        }
        if result["succeeded"] == 0 and self.total_tasks > 0:
            raise TaskQueueError(
                "all game API queries failed or returned no usable data"
            )
        return result
    
    def run(self) -> Dict[str, int]:
        """运行任务队列"""
        start = time.time()
        
        # 在 Windows 上使用 WindowsSelectorEventLoopPolicy
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self._run_async())
        finally:
            loop.close()
        
        elapsed = time.time() - start
        print(f"任务完成，耗时 {elapsed:.2f} 秒")
        return result
