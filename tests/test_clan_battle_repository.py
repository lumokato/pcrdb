from datetime import date, datetime, timezone
from unittest import TestCase
from unittest.mock import patch

from pcrdb.clan_battle.repository import list_snapshots, save_snapshot


def ranking(rank: int) -> dict[str, object]:
    return {
        "rank": rank,
        "clan_name": f"Clan {rank}",
        "leader_name": f"Leader {rank}",
        "member_num": 30,
        "damage": 1_000_000 - rank,
        "lap": 1,
        "boss_id": 1,
        "remain": 100,
        "grade_rank": rank,
    }


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, params=()):
        normalized = " ".join(query.split())
        self.connection.queries.append((normalized, params))
        if normalized.startswith("SELECT snapshot_id, content_sha256, row_count"):
            self.row = self.connection.existing_snapshot

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.connection.rows


class FakeConnection:
    def __init__(self, *, existing_snapshot=None, rows=()):
        self.existing_snapshot = existing_snapshot
        self.rows = list(rows)
        self.queries = []
        self.closed = False

    def cursor(self, **kwargs):
        return FakeCursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


class SnapshotVisibilityTests(TestCase):
    def test_list_snapshots_hides_unconfirmed_final_candidates(self):
        connection = FakeConnection(rows=[])

        with patch(
            "pcrdb.clan_battle.repository.create_connection",
            return_value=connection,
        ):
            snapshots = list_snapshots("2026-07")

        self.assertEqual(snapshots, [])
        query, params = connection.queries[0]
        self.assertIn("snapshot_type <> 'final_candidate' OR is_final", query)
        self.assertEqual(params, (date(2026, 7, 1),))

    def test_confirming_final_deletes_other_candidates_for_period(self):
        captured_at = datetime(2026, 8, 8, 15, 0, tzinfo=timezone.utc)
        rows = [ranking(1)]

        from pcrdb.clan_battle.models import normalize_rows, rows_sha256

        content_hash = rows_sha256(normalize_rows(rows))
        connection = FakeConnection(
            existing_snapshot={
                "snapshot_id": 4209,
                "content_sha256": content_hash,
                "row_count": 1,
            }
        )

        snapshot_id = save_snapshot(
            date(2026, 7, 1),
            captured_at,
            "final_candidate",
            "api",
            rows,
            is_final=True,
            connection=connection,
        )

        self.assertEqual(snapshot_id, 4209)
        cleanup_queries = [
            (query, params)
            for query, params in connection.queries
            if query.startswith("DELETE FROM clan_battle.snapshots")
        ]
        self.assertEqual(len(cleanup_queries), 1)
        self.assertIn("snapshot_id <> %s", cleanup_queries[0][0])
        self.assertEqual(cleanup_queries[0][1], (date(2026, 7, 1), 4209))

