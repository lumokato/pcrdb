import unittest
from datetime import date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from pcrdb.clan_battle.collector import _is_final_data_window, _prepare_phase


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


if __name__ == "__main__":
    unittest.main()
