import asyncio
from contextlib import nullcontext
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from pcrdb.clan_battle.collector import (
    _fetch_snapshot,
    _is_final_data_window,
    _lease_clients,
    _prepare_phase,
    collect_tick,
)
from pcrdb.clan_battle.models import RankingRow


BEIJING = ZoneInfo("Asia/Shanghai")


class PhasePreparationTests(unittest.TestCase):
    def test_enters_waiting_start_after_twentieth(self):
        state = {
            "phase": "final",
            "active_period": date(2026, 6, 1),
            "reference_probe_sha256": "old-final",
        }

        with patch("pcrdb.clan_battle.collector.update_worker_state") as update:
            result = _prepare_phase(
                state,
                datetime(2026, 7, 20, 4, 0, tzinfo=BEIJING),
            )

        self.assertEqual(result["phase"], "waiting_start")
        self.assertEqual(result["active_period"], date(2026, 7, 1))
        update.assert_called_once_with(
            phase="waiting_start",
            active_period=date(2026, 7, 1),
            candidate_content_sha256=None,
            candidate_seen_count=0,
            successful_empty_count=0,
        )

    def test_recovers_previous_month_final_after_long_outage(self):
        state = {
            "phase": "final",
            "active_period": date(2026, 6, 1),
            "reference_probe_sha256": "old-final",
        }

        with patch("pcrdb.clan_battle.collector.update_worker_state") as update:
            result = _prepare_phase(
                state,
                datetime(2026, 9, 10, 15, 0, tzinfo=BEIJING),
            )

        self.assertEqual(result["phase"], "settlement")
        self.assertEqual(result["active_period"], date(2026, 8, 1))
        update.assert_called_once_with(
            phase="settlement",
            active_period=date(2026, 8, 1),
            candidate_content_sha256=None,
            candidate_seen_count=0,
            successful_empty_count=0,
        )

    def test_rolls_unresolved_settlement_into_next_battle_window(self):
        last_nonempty = datetime(2026, 6, 30, 23, 30, tzinfo=BEIJING)
        state = {
            "phase": "settlement",
            "active_period": date(2026, 6, 1),
            "reference_probe_sha256": "older-final",
            "candidate_content_sha256": "candidate",
            "candidate_seen_count": 1,
            "successful_empty_count": 2,
            "last_nonempty_at": last_nonempty,
        }

        with (
            patch("pcrdb.clan_battle.collector.mark_period_settlement") as settle,
            patch("pcrdb.clan_battle.collector.update_worker_state") as update,
        ):
            result = _prepare_phase(
                state,
                datetime(2026, 7, 20, 4, 0, tzinfo=BEIJING),
            )

        self.assertEqual(result["phase"], "waiting_start")
        self.assertEqual(result["active_period"], date(2026, 7, 1))
        self.assertIsNone(result["reference_probe_sha256"])
        settle.assert_called_once_with(date(2026, 6, 1), last_nonempty)
        update.assert_called_once_with(
            phase="waiting_start",
            active_period=date(2026, 7, 1),
            reference_probe_sha256=None,
            candidate_content_sha256=None,
            candidate_seen_count=0,
            successful_empty_count=0,
        )

    def test_final_data_is_only_accepted_after_period_month(self):
        period = date(2026, 7, 1)
        self.assertFalse(
            _is_final_data_window(period, datetime(2026, 7, 31, 23, 30, tzinfo=BEIJING))
        )
        self.assertTrue(
            _is_final_data_window(period, datetime(2026, 8, 1, 0, 0, tzinfo=BEIJING))
        )


class FakeRankingApi:
    def __init__(self, empty_page=None):
        self.empty_page = empty_page
        self.pages = []

    async def query_clan_battle_ranking(self, page):
        self.pages.append(page)
        if page == self.empty_page:
            return []
        return [
            {
                "rank": page + 1,
                "clan_name": f"clan-{page + 1}",
                "leader_name": "leader",
                "member_num": 30,
                "damage": page + 100,
                "grade_rank": 1,
            }
        ]


class ConcurrentPageTests(unittest.IsolatedAsyncioTestCase):
    async def test_distributes_pages_across_logged_in_accounts(self):
        first = RankingRow.from_mapping(
            {
                "rank": 1,
                "clan_name": "clan-1",
                "leader_name": "leader",
                "member_num": 30,
                "damage": 100,
                "grade_rank": 1,
            }
        )
        api_one = FakeRankingApi()
        api_two = FakeRankingApi()

        rows, pages_fetched = await _fetch_snapshot(
            [api_one, api_two],
            page_limit=5,
            first_page=[first],
        )

        self.assertEqual([row.rank for row in rows], [1, 2, 3, 4, 5])
        self.assertEqual(pages_fetched, 5)
        self.assertEqual(api_one.pages, [1, 3])
        self.assertEqual(api_two.pages, [2, 4])

    async def test_stops_at_the_first_empty_page(self):
        first = RankingRow.from_mapping(
            {
                "rank": 1,
                "clan_name": "clan-1",
                "leader_name": "leader",
                "member_num": 30,
                "damage": 100,
                "grade_rank": 1,
            }
        )
        api_one = FakeRankingApi(empty_page=3)
        api_two = FakeRankingApi()

        rows, pages_fetched = await _fetch_snapshot(
            [api_one, api_two],
            page_limit=8,
            first_page=[first],
        )

        self.assertEqual([row.rank for row in rows], [1, 2, 3])
        self.assertEqual(pages_fetched, 4)


class FakeLease:
    def __init__(self, account_id=1, release_error=None):
        self.account = type(
            "FakeAccount",
            (),
            {
                "id": account_id,
                "viewer_id": 1000 + account_id,
                "uid": f"uid-{account_id}",
                "access_key": "key",
            },
        )()
        self.release_error = release_error
        self.releases = []
        self.disables = []

    def release(self, success=True, error_type=None):
        self.releases.append((success, error_type))
        if self.release_error:
            raise self.release_error

    def disable(self, reason):
        self.disables.append(reason)


class AccountLeaseCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_without_clan_is_disabled_and_not_used(self):
        lease = FakeLease(1)

        class NoClanApi:
            current_clan_id = None

            async def login(self):
                return None

        with (
            patch("pcrdb.account_pool.lease_accounts", return_value=[lease]),
            patch("pcrdb.clan_battle.collector.PCRApi", return_value=NoClanApi()),
        ):
            with self.assertRaisesRegex(RuntimeError, "valid clan membership"):
                await _lease_clients(1)

        self.assertEqual(lease.disables, ["NotInClan"])
        self.assertEqual(lease.releases, [])

    async def test_cancelled_login_releases_all_leases(self):
        leases = [FakeLease(1), FakeLease(2)]
        login_started = asyncio.Event()
        never_complete = asyncio.Event()

        class BlockingApi:
            async def login(self):
                login_started.set()
                await never_complete.wait()

        with (
            patch("pcrdb.account_pool.lease_accounts", return_value=leases),
            patch(
                "pcrdb.clan_battle.collector.PCRApi",
                side_effect=[BlockingApi(), BlockingApi()],
            ),
        ):
            task = asyncio.create_task(_lease_clients(2))
            await login_started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        for lease in leases:
            self.assertEqual(lease.releases, [(False, "CancelledError")])

    async def test_release_failure_does_not_hide_collection_log(self):
        lease = FakeLease(release_error=RuntimeError("bookkeeping failed"))
        record_run = MagicMock()
        state = {
            "phase": "waiting_start",
            "active_period": date(2026, 7, 1),
            "reference_probe_sha256": None,
        }

        with (
            patch.dict(
                "os.environ",
                {"CLAN_BATTLE_COLLECTION_ENABLED": "true"},
                clear=False,
            ),
            patch(
                "pcrdb.clan_battle.collector.advisory_lock",
                return_value=nullcontext(object()),
            ),
            patch("pcrdb.clan_battle.collector.get_worker_state", return_value=state),
            patch("pcrdb.clan_battle.collector._prepare_phase", return_value=state),
            patch(
                "pcrdb.clan_battle.collector._lease_clients",
                new=AsyncMock(return_value=[(lease, object())]),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_page",
                new=AsyncMock(return_value=[]),
            ),
            patch("pcrdb.clan_battle.collector.update_worker_state"),
            patch("pcrdb.clan_battle.collector.record_collection_run", record_run),
        ):
            await collect_tick("test")

        record_run.assert_called_once()
        self.assertEqual(
            record_run.call_args.kwargs["details"]["account_release_errors"],
            1,
        )

    async def test_cancelled_collection_is_logged_and_releases_the_account(self):
        lease = FakeLease()
        record_run = MagicMock()
        state = {
            "phase": "waiting_start",
            "active_period": date(2026, 7, 1),
            "reference_probe_sha256": None,
        }

        with (
            patch.dict(
                "os.environ",
                {"CLAN_BATTLE_COLLECTION_ENABLED": "true"},
                clear=False,
            ),
            patch(
                "pcrdb.clan_battle.collector.advisory_lock",
                return_value=nullcontext(object()),
            ),
            patch("pcrdb.clan_battle.collector.get_worker_state", return_value=state),
            patch("pcrdb.clan_battle.collector._prepare_phase", return_value=state),
            patch(
                "pcrdb.clan_battle.collector._lease_clients",
                new=AsyncMock(return_value=[(lease, object())]),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_page",
                new=AsyncMock(side_effect=asyncio.CancelledError()),
            ),
            patch("pcrdb.clan_battle.collector.record_collection_run", record_run),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await collect_tick("test")

        self.assertEqual(lease.releases, [(False, "cancelled")])
        self.assertEqual(record_run.call_args.kwargs["result_type"], "cancelled")


if __name__ == "__main__":
    unittest.main()
