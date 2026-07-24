from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from pcrdb.tasks.player_profile_sync import run


class EmptyTargetTests(TestCase):
    def test_stale_clan_details_are_logged_as_failure(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (
            datetime(2026, 1, 28, 4, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        task_logger = MagicMock()

        with (
            patch(
                "pcrdb.tasks.player_profile_sync.get_target_players",
                return_value=([], {}),
            ),
            patch(
                "pcrdb.tasks.player_profile_sync.get_config",
                return_value={"database": "pcrdb"},
            ),
            patch(
                "pcrdb.tasks.player_profile_sync.get_connection",
                return_value=connection,
            ),
            patch(
                "pcrdb.db.task_logger.TaskLogger",
                return_value=task_logger,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "没有近30天公会明细"):
                run(mode="top_clans", rank_limit=30)

        task_logger.start.assert_called_once()
        task_logger.finish_success.assert_not_called()
        task_logger.finish_failed.assert_called_once()
