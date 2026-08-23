from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from psycopg2.extras import RealDictCursor

from pcrdb.clan_battle.models import RankingRow, normalize_rows, rows_sha256
from pcrdb.db.connection import create_connection


def collect_verification() -> dict[str, Any]:
    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT status, COUNT(*)::INTEGER AS count
                FROM clan_battle.import_files
                GROUP BY status
                ORDER BY status
                """
            )
            manifest_status = {row["status"]: row["count"] for row in cursor.fetchall()}

            cursor.execute(
                """
                SELECT COUNT(*)::INTEGER AS count
                FROM clan_battle.import_files f
                JOIN clan_battle.snapshots s ON s.snapshot_id = f.snapshot_id
                WHERE f.status IN ('imported', 'duplicate')
                  AND (f.row_count <> s.row_count OR f.content_sha256 <> s.content_sha256)
                """
            )
            manifest_mismatches = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT import_file_id, snapshot_id, row_count, content_sha256
                FROM clan_battle.import_files
                WHERE status = 'prefix_duplicate'
                ORDER BY import_file_id
                """
            )
            prefix_mismatches = 0
            for source_file in cursor.fetchall():
                cursor.execute(
                    """
                    SELECT rank, clan_name, leader_name, member_num,
                           damage, lap, boss_id, remain, grade_rank, bili_rank
                    FROM clan_battle.rankings
                    WHERE snapshot_id = %s
                    ORDER BY rank, row_number
                    LIMIT %s
                    """,
                    (source_file["snapshot_id"], source_file["row_count"]),
                )
                rows = normalize_rows(RankingRow.from_mapping(row) for row in cursor.fetchall())
                if rows_sha256(rows) != source_file["content_sha256"].strip():
                    prefix_mismatches += 1

            cursor.execute(
                """
                SELECT COUNT(*)::INTEGER AS count
                FROM (
                    SELECT s.snapshot_id, s.row_count,
                           COUNT(r.rank)::INTEGER AS stored_rows,
                           COUNT(DISTINCT r.row_number)::INTEGER AS distinct_rows,
                           MIN(r.rank) AS min_rank,
                           MAX(r.rank) AS max_rank
                    FROM clan_battle.snapshots s
                    LEFT JOIN clan_battle.rankings r ON r.snapshot_id = s.snapshot_id
                    GROUP BY s.snapshot_id
                    HAVING s.row_count <> COUNT(r.rank)
                        OR COUNT(r.rank) <> COUNT(DISTINCT r.row_number)
                ) broken
                """
            )
            broken_snapshots = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT COUNT(*)::INTEGER AS count
                FROM clan_battle.import_files
                WHERE corrections <> '[]'::jsonb
                """
            )
            corrected_files = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT COUNT(*)::INTEGER AS snapshot_count,
                       COALESCE(SUM(row_count), 0)::BIGINT AS ranking_rows,
                       COUNT(DISTINCT period)::INTEGER AS period_count,
                       MIN(period) AS first_period,
                       MAX(period) AS last_period
                FROM clan_battle.snapshots
                """
            )
            totals = dict(cursor.fetchone())

            cursor.execute(
                """
                SELECT p.period, p.status, p.final_snapshot_id,
                       s.row_count AS final_rows, s.max_rank AS final_max_rank,
                       s.content_sha256
                FROM clan_battle.periods p
                LEFT JOIN clan_battle.snapshots s ON s.snapshot_id = p.final_snapshot_id
                ORDER BY p.period
                """
            )
            periods = [dict(row) for row in cursor.fetchall()]
            periods_without_final = [
                period["period"]
                for period in periods
                if period["final_snapshot_id"] is None
            ]
            unresolved_periods = [
                period["period"]
                for period in periods
                if period["final_snapshot_id"] is None
                and period["status"] != "settlement"
            ]

            cursor.execute(
                """
                SELECT source_name, COUNT(*)::INTEGER AS file_count,
                       COALESCE(SUM(row_count), 0)::BIGINT AS source_rows
                FROM clan_battle.import_files
                GROUP BY source_name
                ORDER BY source_name
                """
            )
            sources = [dict(row) for row in cursor.fetchall()]

            ok = (
                manifest_status.get("invalid", 0) == 0
                and manifest_status.get("conflict", 0) == 0
                and manifest_mismatches == 0
                and prefix_mismatches == 0
                and broken_snapshots == 0
                and bool(periods)
                and not unresolved_periods
            )
            return {
                "ok": ok,
                "manifest_status": manifest_status,
                "manifest_mismatches": manifest_mismatches,
                "prefix_mismatches": prefix_mismatches,
                "broken_snapshots": broken_snapshots,
                "corrected_files": corrected_files,
                "periods_without_final": periods_without_final,
                "unresolved_periods": unresolved_periods,
                "totals": totals,
                "sources": sources,
                "periods": periods,
            }
    finally:
        conn.close()


def export_manifest(path: Path) -> None:
    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor, path.open("w", encoding="utf-8") as output:
            cursor.execute(
                """
                SELECT source_name, relative_path, source_size, source_mtime,
                       encoding, header, file_sha256, content_sha256, row_count,
                       inferred_period, inferred_captured_at, snapshot_id, status,
                       duplicate_of, corrections, error_message, imported_at
                FROM clan_battle.import_files
                ORDER BY source_name, relative_path
                """
            )
            for row in cursor:
                output.write(json.dumps(dict(row), ensure_ascii=False, default=str) + "\n")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify imported ClanRank data")
    parser.add_argument("--manifest")
    args = parser.parse_args()
    result = collect_verification()
    if args.manifest:
        export_manifest(Path(args.manifest))
        result["manifest_path"] = args.manifest
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
