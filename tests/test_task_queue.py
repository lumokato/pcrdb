import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from pcrdb.tasks.base import TaskQueue


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
