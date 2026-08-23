import asyncio
from contextlib import nullcontext
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from pcrdb.api.endpoints import ClanBattleRankingPage, ClanBattleRuntime
from pcrdb.clan_battle.collector import (
    FetchedRankingPage,
    _fetch_snapshot,
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

    def test_waits_for_current_identity_after_long_outage(self):
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

        self.assertEqual(result["phase"], "waiting_start")
        self.assertEqual(result["active_period"], date(2026, 9, 1))
        update.assert_called_once_with(
            phase="waiting_start",
            active_period=date(2026, 9, 1),
            candidate_content_sha256=None,
            candidate_seen_count=0,
            successful_empty_count=0,
        )

    def test_does_not_roll_unresolved_settlement_by_calendar_date(self):
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

        with patch("pcrdb.clan_battle.collector.update_worker_state") as update:
            result = _prepare_phase(
                state,
                datetime(2026, 7, 20, 4, 0, tzinfo=BEIJING),
            )

        self.assertEqual(result["phase"], "settlement")
        self.assertEqual(result["active_period"], date(2026, 6, 1))
        update.assert_not_called()


class FakeRankingApi:
    def __init__(self, empty_page=None, clan_battle_id=1090):
        self.empty_page = empty_page
        self.clan_battle_id = clan_battle_id
        self.pages = []

    async def query_clan_battle_ranking_page(self, page):
        self.pages.append(page)
        if page == self.empty_page:
            rankings = []
        else:
            rankings = [
                {
                    "rank": page + 1,
                    "clan_name": f"clan-{page + 1}",
                    "leader_name": "leader",
                    "member_num": 30,
                    "damage": page + 100,
                    "grade_rank": 1,
                }
            ]
        return ClanBattleRankingPage(
            clan_battle_id=self.clan_battle_id,
            period=1,
            clan_battle_mode=0,
            rankings=rankings,
        )


class ConcurrentPageTests(unittest.IsolatedAsyncioTestCase):
    async def test_distributes_pages_across_logged_in_accounts(self):
        first = FetchedRankingPage(
            clan_battle_id=1090,
            period=1,
            clan_battle_mode=0,
            rows=[RankingRow.from_mapping(
                {
                    "rank": 1,
                    "clan_name": "clan-1",
                    "leader_name": "leader",
                    "member_num": 30,
                    "damage": 100,
                    "grade_rank": 1,
                }
            )],
        )
        api_one = FakeRankingApi()
        api_two = FakeRankingApi()

        rows, pages_fetched = await _fetch_snapshot(
            [api_one, api_two],
            page_limit=5,
            first_page=first,
        )

        self.assertEqual([row.rank for row in rows], [1, 2, 3, 4, 5])
        self.assertEqual(pages_fetched, 5)
        self.assertEqual(api_one.pages, [1, 3])
        self.assertEqual(api_two.pages, [2, 4])

    async def test_stops_at_the_first_empty_page(self):
        first = FetchedRankingPage(
            clan_battle_id=1090,
            period=1,
            clan_battle_mode=0,
            rows=[RankingRow.from_mapping(
                {
                    "rank": 1,
                    "clan_name": "clan-1",
                    "leader_name": "leader",
                    "member_num": 30,
                    "damage": 100,
                    "grade_rank": 1,
                }
            )],
        )
        api_one = FakeRankingApi(empty_page=3)
        api_two = FakeRankingApi()

        rows, pages_fetched = await _fetch_snapshot(
            [api_one, api_two],
            page_limit=8,
            first_page=first,
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


class RuntimePhaseTests(unittest.IsolatedAsyncioTestCase):
    def ranking_page(self, clan_battle_id=1090):
        return FetchedRankingPage(
            clan_battle_id=clan_battle_id,
            period=1,
            clan_battle_mode=0,
            rows=[
                RankingRow.from_mapping(
                    {
                        "rank": 1,
                        "clan_name": "changed-old-ranking",
                        "leader_name": "leader",
                        "member_num": 30,
                        "damage": 999,
                        "grade_rank": 1,
                    }
                )
            ],
        )

    async def test_interval_ranking_change_does_not_start_new_month(self):
        lease = FakeLease()
        state = {
            "phase": "waiting_start",
            "active_period": date(2026, 8, 1),
            "active_clan_battle_id": 1090,
            "last_snapshot_id": 4060,
        }
        record_run = MagicMock()

        with (
            patch.dict("os.environ", {"CLAN_BATTLE_COLLECTION_ENABLED": "true"}),
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
                "pcrdb.clan_battle.collector._runtime_state",
                return_value=ClanBattleRuntime(now_open=True, is_interval=True),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_page",
                new=AsyncMock(return_value=self.ranking_page()),
            ),
            patch("pcrdb.clan_battle.collector.save_snapshot") as save_snapshot,
            patch("pcrdb.clan_battle.collector.update_worker_state"),
            patch("pcrdb.clan_battle.collector.record_collection_run", record_run),
        ):
            await collect_tick("test")

        save_snapshot.assert_not_called()
        self.assertEqual(
            record_run.call_args.kwargs["result_type"],
            "waiting_interval",
        )

    async def test_new_identity_starts_only_after_interval_ends(self):
        lease = FakeLease()
        state = {
            "phase": "waiting_start",
            "active_period": date(2026, 8, 1),
            "active_clan_battle_id": 1090,
            "last_snapshot_id": 4060,
        }
        page = self.ranking_page(clan_battle_id=1091)
        record_run = MagicMock()

        with (
            patch.dict("os.environ", {"CLAN_BATTLE_COLLECTION_ENABLED": "true"}),
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
                "pcrdb.clan_battle.collector._runtime_state",
                return_value=ClanBattleRuntime(now_open=True, is_interval=False),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_page",
                new=AsyncMock(return_value=page),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_snapshot",
                new=AsyncMock(return_value=(page.rows, 1)),
            ),
            patch("pcrdb.clan_battle.collector.save_snapshot", return_value=5000) as save_snapshot,
            patch("pcrdb.clan_battle.collector.update_worker_state"),
            patch("pcrdb.clan_battle.collector.record_collection_run", record_run),
        ):
            await collect_tick("test")

        self.assertEqual(save_snapshot.call_args.kwargs["clan_battle_id"], 1091)
        self.assertEqual(
            record_run.call_args.kwargs["result_type"],
            "battle_started",
        )

    async def test_active_battle_starts_when_legacy_state_has_no_identity(self):
        lease = FakeLease()
        state = {
            "phase": "waiting_start",
            "active_period": date(2026, 8, 1),
            "active_clan_battle_id": None,
            "last_snapshot_id": 4060,
        }
        page = self.ranking_page(clan_battle_id=1091)
        record_run = MagicMock()

        with (
            patch.dict("os.environ", {"CLAN_BATTLE_COLLECTION_ENABLED": "true"}),
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
                "pcrdb.clan_battle.collector._runtime_state",
                return_value=ClanBattleRuntime(now_open=True, is_interval=False),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_page",
                new=AsyncMock(return_value=page),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_snapshot",
                new=AsyncMock(return_value=(page.rows, 1)),
            ),
            patch(
                "pcrdb.clan_battle.collector.save_snapshot",
                return_value=5000,
            ) as save_snapshot,
            patch("pcrdb.clan_battle.collector.update_worker_state"),
            patch("pcrdb.clan_battle.collector.record_collection_run", record_run),
        ):
            await collect_tick("test")

        self.assertEqual(save_snapshot.call_args.kwargs["clan_battle_id"], 1091)
        self.assertEqual(
            record_run.call_args.kwargs["result_type"],
            "battle_started",
        )

    async def test_second_stable_final_is_confirmed(self):
        lease = FakeLease()
        page = self.ranking_page(clan_battle_id=1090)
        state = {
            "phase": "settlement",
            "active_period": date(2026, 7, 1),
            "active_clan_battle_id": 1090,
            "candidate_content_sha256": "candidate-hash",
            "candidate_seen_count": 1,
            "reference_probe_sha256": "progress-probe",
            "successful_empty_count": 0,
        }
        record_run = MagicMock()

        with (
            patch.dict(
                "os.environ",
                {
                    "CLAN_BATTLE_COLLECTION_ENABLED": "true",
                    "CLAN_BATTLE_FINAL_STABLE_COUNT": "2",
                },
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
                "pcrdb.clan_battle.collector._runtime_state",
                return_value=ClanBattleRuntime(now_open=True, is_interval=True),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_page",
                new=AsyncMock(return_value=page),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_snapshot",
                new=AsyncMock(return_value=(page.rows, 1)),
            ),
            patch(
                "pcrdb.clan_battle.collector.rows_sha256",
                return_value="candidate-hash",
            ),
            patch(
                "pcrdb.clan_battle.collector.probe_sha256",
                return_value="final-probe",
            ),
            patch(
                "pcrdb.clan_battle.collector.save_snapshot",
                return_value=5001,
            ) as save_snapshot,
            patch("pcrdb.clan_battle.collector.update_worker_state") as update_state,
            patch("pcrdb.clan_battle.collector.record_collection_run", record_run),
        ):
            await collect_tick("test")

        self.assertTrue(save_snapshot.call_args.kwargs["is_final"])
        self.assertEqual(
            update_state.call_args.kwargs["reference_probe_sha256"],
            "final-probe",
        )
        self.assertEqual(update_state.call_args.kwargs["phase"], "final")
        self.assertEqual(
            record_run.call_args.kwargs["result_type"],
            "final_confirmed",
        )


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
                "pcrdb.clan_battle.collector._runtime_state",
                return_value=ClanBattleRuntime(now_open=True, is_interval=True),
            ),
            patch(
                "pcrdb.clan_battle.collector._fetch_page",
                new=AsyncMock(
                    return_value=FetchedRankingPage(
                        clan_battle_id=1090,
                        period=1,
                        clan_battle_mode=0,
                        rows=[],
                    )
                ),
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
                "pcrdb.clan_battle.collector._runtime_state",
                return_value=ClanBattleRuntime(now_open=True, is_interval=True),
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
