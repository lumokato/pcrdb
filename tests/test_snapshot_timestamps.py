from unittest import TestCase
from unittest.mock import MagicMock, patch

from pcrdb.db.connection import insert_snapshots_batch, utc_now


class SnapshotTimestampTests(TestCase):
    def test_utc_now_is_timezone_aware(self):
        value = utc_now()

        self.assertIsNotNone(value.tzinfo)
        self.assertEqual(value.utcoffset().total_seconds(), 0)

    def test_default_batch_timestamp_is_timezone_aware(self):
        connection = MagicMock()
        record = {"clan_id": 1, "clan_name": "test"}

        with patch("pcrdb.db.connection.get_connection", return_value=connection):
            insert_snapshots_batch("clan_snapshots", [record])

        collected_at = record["collected_at"]
        self.assertIsNotNone(collected_at.tzinfo)
        self.assertEqual(collected_at.utcoffset().total_seconds(), 0)
        connection.commit.assert_called_once()
