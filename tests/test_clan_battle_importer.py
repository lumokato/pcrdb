import csv
from datetime import date
from pathlib import Path
import tempfile
import unittest

from pcrdb.clan_battle.importer import (
    KNOWN_JULY_2025_FILE_SHA256,
    Source,
    _apply_known_corrections,
    build_plan,
    infer_identity,
    scan_sources,
)
from pcrdb.clan_battle.models import normalize_rows


FIELDS = [
    "",
    "rank",
    "clan_name",
    "leader_name",
    "member_num",
    "damage",
    "lap",
    "boss_id",
    "remain",
    "grade_rank",
]


def ranking(rank: int, damage: int | None = None) -> dict[str, object]:
    return {
        "": rank - 1,
        "rank": rank,
        "clan_name": f"Clan {rank}",
        "leader_name": f"Leader {rank}",
        "member_num": 30,
        "damage": damage if damage is not None else 1_000_000 - rank,
        "lap": 1,
        "boss_id": 1,
        "remain": 100,
        "grade_rank": rank,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


class IdentityTests(unittest.TestCase):
    def test_active_and_late_final_periods(self):
        active_period, _, active_type = infer_identity("202601252300.csv")
        final_period, _, final_type = infer_identity("202603161500.csv")
        self.assertEqual(active_period, date(2026, 1, 1))
        self.assertEqual(active_type, "progress")
        self.assertEqual(final_period, date(2026, 2, 1))
        self.assertEqual(final_type, "final_candidate")

    def test_monthly_final(self):
        period, captured_at, snapshot_type = infer_identity("2025年11月.csv")
        self.assertEqual(period, date(2025, 11, 1))
        self.assertIsNone(captured_at)
        self.assertEqual(snapshot_type, "monthly_final")


class RowTests(unittest.TestCase):
    def test_tied_ranks_are_preserved(self):
        rows = normalize_rows([ranking(970, 20), ranking(970, 10)])
        self.assertEqual([row.rank for row in rows], [970, 970])
        self.assertEqual([row.damage for row in rows], [20, 10])

    def test_rank_gaps_are_preserved(self):
        rows = normalize_rows([ranking(1, 20), ranking(3, 10)])
        self.assertEqual([row.rank for row in rows], [1, 3])

    def test_known_july_2025_rank_offset_is_repaired_and_audited(self):
        values = [ranking(rank) for rank in range(1, 1237)]
        for source_index in range(970, 1236):
            values[source_index]["rank"] = source_index
            values[source_index]["grade_rank"] = source_index + 1
        values[1023]["grade_rank"] = 0
        values[1222]["grade_rank"] = 0

        corrected, corrections = _apply_known_corrections(
            values,
            KNOWN_JULY_2025_FILE_SHA256,
        )
        rows = normalize_rows(corrected)

        self.assertEqual([row.rank for row in rows], list(range(1, 1237)))
        self.assertEqual(len(corrections), 1)
        self.assertEqual(corrections[0].source_row_start, 971)
        self.assertEqual(corrections[0].source_row_end, 1236)

    def test_same_rank_pattern_is_not_repaired_for_an_unknown_file(self):
        values = [ranking(rank) for rank in range(1, 1237)]
        for source_index in range(970, 1236):
            values[source_index]["rank"] = source_index
            values[source_index]["grade_rank"] = source_index + 1

        unchanged, corrections = _apply_known_corrections(values, "0" * 64)

        self.assertIs(unchanged, values)
        self.assertEqual(corrections, ())
        self.assertEqual(
            [row.rank for row in normalize_rows(unchanged)][969:972],
            [970, 970, 971],
        )


class PlanTests(unittest.TestCase):
    def test_shorter_same_timestamp_file_is_a_prefix_duplicate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            second = root / "second"
            write_csv(first / "202510252200.csv", [ranking(1), ranking(2)])
            write_csv(second / "202510252200.csv", [ranking(1), ranking(2), ranking(3)])

            plan = build_plan(scan_sources([Source("first", first), Source("second", second)]))
            self.assertEqual(plan.conflicts, [])
            target_records = next(iter(plan.target_records.values()))
            self.assertEqual(target_records[0].row_count, 3)
            self.assertEqual(target_records[1].row_count, 2)

    def test_same_timestamp_divergence_is_a_conflict(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            second = root / "second"
            write_csv(first / "202510252200.csv", [ranking(1, 100), ranking(2, 90)])
            write_csv(second / "202510252200.csv", [ranking(1, 100), ranking(2, 80)])

            plan = build_plan(scan_sources([Source("first", first), Source("second", second)]))
            self.assertEqual(len(plan.conflicts), 1)

    def test_monthly_file_links_to_matching_timestamp_and_marks_final(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            rows = [ranking(1), ranking(2)]
            write_csv(source / "202501252000.csv", rows)
            write_csv(source / "202502071500.csv", rows)
            write_csv(source / "2025年01月.csv", rows)

            plan = build_plan(scan_sources([Source("source", source)]))
            final_target = plan.final_targets[date(2025, 1, 1)]
            self.assertIsNotNone(final_target[1])
            self.assertEqual(final_target[1].strftime("%Y%m%d%H%M"), "202502071500")

    def test_mislabeled_monthly_file_links_to_exact_cross_period_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            rows = [ranking(1), ranking(2)]
            write_csv(source / "202511061500.csv", rows)
            write_csv(source / "2025年11月.csv", rows)

            plan = build_plan(scan_sources([Source("source", source)]))
            monthly_key = ("source", "2025年11月.csv")

            self.assertEqual(plan.record_targets[monthly_key][0], date(2025, 10, 1))
            self.assertIn(date(2025, 10, 1), plan.final_targets)
            self.assertNotIn("2025-11", plan.summary()["periods"])
            self.assertEqual(plan.summary()["corrected_source_files"], 1)

    def test_mislabeled_monthly_progress_copy_is_not_marked_final(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            rows = [ranking(1), ranking(2)]
            write_csv(source / "202510252200.csv", rows)
            write_csv(source / "2025年09月.csv", rows)

            plan = build_plan(scan_sources([Source("source", source)]))

            self.assertEqual(plan.conflicts, [])
            self.assertNotIn(date(2025, 10, 1), plan.final_targets)
            self.assertEqual(plan.summary()["periods_without_final"], ["2025-10"])


if __name__ == "__main__":
    unittest.main()
