from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from pcrdb.tasks.arena_deck_sync import _run_group as run_arena_group
from pcrdb.tasks.grand_sync import _run_group as run_grand_group


class FakeLease:
    def __init__(self):
        self.client_data = {"vid": 1, "uid": "uid", "access_key": "key"}
        self.releases = []

    def release(self, success=True, error_type=None):
        self.releases.append((success, error_type))


class EmptyGroupResultTests(IsolatedAsyncioTestCase):
    async def test_empty_arena_group_is_a_failure(self):
        lease = FakeLease()
        with (
            patch("pcrdb.tasks.arena_deck_sync.lease_account", return_value=lease),
            patch(
                "pcrdb.tasks.arena_deck_sync.create_client",
                new=AsyncMock(return_value=object()),
            ),
            patch(
                "pcrdb.tasks.arena_deck_sync.query_and_save_deck",
                new=AsyncMock(return_value=0),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "returned no usable rows"):
                await run_arena_group(1)

        self.assertEqual(lease.releases, [(False, "EmptyResult")])
    async def test_empty_grand_arena_group_is_a_failure(self):
        lease = FakeLease()
        with (
            patch("pcrdb.tasks.grand_sync.lease_account", return_value=lease),
            patch(
                "pcrdb.tasks.grand_sync.create_client",
                new=AsyncMock(return_value=object()),
            ),
            patch(
                "pcrdb.tasks.grand_sync.query_and_save_ranking",
                new=AsyncMock(return_value=0),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "returned no usable rows"):
                await run_grand_group(1)

        self.assertEqual(lease.releases, [(False, "EmptyResult")])
