import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from pcrdb.tasks.base import RetryableResultError, TaskQueue
from pcrdb.tasks.clan_sync import process_clan_data


class FakeLease:
    def __init__(self, account_id):
        self.account = type("FakeAccount", (), {"id": account_id})()
        self.client_data = {
            "vid": 1000 + account_id,
            "uid": f"uid-{account_id}",
            "access_key": "key",
        }
        self.released = False
        self.releases = []

    def release(self, success=True, error_type=None):
        if self.released:
            return
        self.released = True
        self.releases.append((success, error_type))


class TaskQueueLeaseTests(IsolatedAsyncioTestCase):
    async def test_cancellation_releases_started_and_unstarted_leases(self):
        leases = [FakeLease(1), FakeLease(2)]
        login_started = asyncio.Event()
        never_complete = asyncio.Event()

        async def blocking_client(_account):
            login_started.set()
            await never_complete.wait()

        queue = TaskQueue(
            query_list=[1, 2],
            data_processor=lambda value: value,
            pg_inserter=lambda values: None,
            sync_num=2,
        )

        with (
            patch("pcrdb.tasks.base.lease_accounts", return_value=leases),
            patch("pcrdb.tasks.base.create_client", side_effect=blocking_client),
        ):
            task = asyncio.create_task(queue._run_async())
            await login_started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(all(lease.released for lease in leases))
        self.assertEqual(leases[0].releases, [(False, "CancelledError")])
        self.assertEqual(leases[1].releases, [(False, "TaskCleanup")])


class TaskQueueRetryTests(IsolatedAsyncioTestCase):
    @staticmethod
    def make_queue(processor):
        queue = TaskQueue(
            query_list=[1],
            data_processor=processor,
            pg_inserter=lambda values: None,
            sync_num=1,
        )
        queue.queue = asyncio.Queue()
        queue.queue.put_nowait(1)
        queue.processed_count = 0
        return queue

    async def test_empty_processed_result_is_not_retried(self):
        lease = FakeLease(1)
        client = AsyncMock()
        client.query_clan.return_value = {"server_error": {"message": "not found"}}
        queue = self.make_queue(lambda response: None)

        with patch(
            "pcrdb.tasks.base.create_client",
            new=AsyncMock(return_value=client),
        ):
            result = await queue._worker(lease, 0)

        self.assertEqual(
            result,
            {"succeeded": 0, "failed": 0, "empty": 1, "login_failed": 0},
        )
        client.query_clan.assert_awaited_once_with(1)
        client.login.assert_not_awaited()
        self.assertEqual(lease.releases, [(True, None)])

    async def test_transport_error_retries_and_reauthenticates(self):
        lease = FakeLease(1)
        client = AsyncMock()
        client.query_clan.side_effect = [RuntimeError("network"), {"clan": {}}]
        queue = self.make_queue(
            lambda response: {"ok": True} if "clan" in response else None
        )

        with (
            patch(
                "pcrdb.tasks.base.create_client",
                new=AsyncMock(return_value=client),
            ),
            patch("pcrdb.tasks.base.asyncio.sleep", new=AsyncMock()),
        ):
            result = await queue._worker(lease, 0)

        self.assertEqual(
            result,
            {"succeeded": 1, "failed": 0, "empty": 0, "login_failed": 0},
        )
        self.assertEqual(client.query_clan.await_count, 2)
        client.login.assert_awaited_once()
        self.assertEqual(lease.releases, [(True, None)])

    async def test_retryable_response_uses_the_same_retry_path(self):
        lease = FakeLease(1)
        client = AsyncMock()
        client.query_clan.return_value = {"server_error": {"message": "连接中断"}}
        attempts = 0

        def processor(response):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RetryableResultError("连接中断")
            return {"clan": {}}

        queue = self.make_queue(processor)
        with (
            patch(
                "pcrdb.tasks.base.create_client",
                new=AsyncMock(return_value=client),
            ),
            patch("pcrdb.tasks.base.asyncio.sleep", new=AsyncMock()),
        ):
            result = await queue._worker(lease, 0)

        self.assertEqual(result["succeeded"], 1)
        self.assertEqual(client.query_clan.await_count, 2)
        client.login.assert_awaited_once()

    async def test_clan_connection_interruption_is_declared_retryable(self):
        with self.assertRaises(RetryableResultError):
            process_clan_data({"server_error": {"message": "连接中断"}})
