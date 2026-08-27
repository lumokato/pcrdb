from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
import json
from typing import Any, Iterator, Sequence

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

from pcrdb.clan_battle.models import RankingRow, normalize_rows, probe_sha256, rows_sha256
from pcrdb.db.connection import create_connection


ADVISORY_LOCK_ID = 0x434C414E52414E4B


@contextmanager
def advisory_lock() -> Iterator[psycopg2.extensions.connection | None]:
    conn = create_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            acquired = cursor.fetchone()[0]
        yield conn if acquired else None
    finally:
        if not conn.closed:
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
            conn.close()


def _period_start(period: date | str) -> date:
    if isinstance(period, str):
        parsed = datetime.strptime(period, "%Y-%m").date()
    else:
        parsed = period
    return parsed.replace(day=1)


def save_snapshot(
    period: date,
    captured_at: datetime | None,
    snapshot_type: str,
    source: str,
    rows: Sequence[RankingRow | dict[str, Any]],
    *,
    clan_battle_id: int | None = None,
    is_final: bool = False,
    connection=None,
) -> int:
    normalized = normalize_rows(rows)
    content_hash = rows_sha256(normalized)
    probe_hash = probe_sha256(normalized)
    if is_final:
        period_status = "final"
    elif snapshot_type == "final_candidate":
        period_status = "settlement"
    else:
        period_status = "active"
    min_rank = normalized[0].rank if normalized else None
    max_rank = normalized[-1].rank if normalized else None
    owns_connection = connection is None
    conn = connection or create_connection()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO clan_battle.periods(
                    period, status, started_at, clan_battle_id, updated_at
                )
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (period) DO UPDATE
                SET status = CASE
                        WHEN clan_battle.periods.status = 'final'
                            THEN clan_battle.periods.status
                        ELSE EXCLUDED.status
                    END,
                    started_at = COALESCE(clan_battle.periods.started_at, EXCLUDED.started_at),
                    clan_battle_id = COALESCE(
                        clan_battle.periods.clan_battle_id,
                        EXCLUDED.clan_battle_id
                    ),
                    ended_at = CASE
                        WHEN clan_battle.periods.status = 'final'
                            THEN clan_battle.periods.ended_at
                        WHEN EXCLUDED.status = 'active'
                            THEN NULL
                        ELSE clan_battle.periods.ended_at
                    END,
                    updated_at = NOW()
                """,
                (period, period_status, captured_at, clan_battle_id),
            )
            cursor.execute(
                """
                SELECT snapshot_id, content_sha256, row_count
                FROM clan_battle.snapshots
                WHERE period = %s AND captured_at IS NOT DISTINCT FROM %s
                FOR UPDATE
                """,
                (period, captured_at),
            )
            existing = cursor.fetchone()

            if existing:
                if existing["content_sha256"].strip() != content_hash or existing["row_count"] != len(normalized):
                    raise ValueError(
                        f"snapshot conflict for {period} at {captured_at}: existing content differs"
                    )
                snapshot_id = existing["snapshot_id"]
            else:
                cursor.execute(
                    """
                    INSERT INTO clan_battle.snapshots(
                        period, captured_at, snapshot_type, source, row_count,
                        min_rank, max_rank, content_sha256, probe_sha256, is_final
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                    RETURNING snapshot_id
                    """,
                    (
                        period,
                        captured_at,
                        snapshot_type,
                        source,
                        len(normalized),
                        min_rank,
                        max_rank,
                        content_hash,
                        probe_hash,
                    ),
                )
                snapshot_id = cursor.fetchone()["snapshot_id"]
                if normalized:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO clan_battle.rankings(
                            snapshot_id, row_number, rank,
                            clan_name, leader_name, member_num,
                            damage, lap, boss_id, remain, grade_rank, bili_rank
                        ) VALUES %s
                        """,
                        [
                            (
                                snapshot_id,
                                row_number,
                                row.rank,
                                row.clan_name,
                                row.leader_name,
                                row.member_num,
                                row.damage,
                                row.lap,
                                row.boss_id,
                                row.remain,
                                row.grade_rank,
                                row.bili_rank,
                            )
                            for row_number, row in enumerate(normalized, start=1)
                        ],
                        page_size=1000,
                    )

            if is_final:
                cursor.execute(
                    "UPDATE clan_battle.snapshots SET is_final = FALSE WHERE period = %s",
                    (period,),
                )
                cursor.execute(
                    "UPDATE clan_battle.snapshots SET is_final = TRUE WHERE snapshot_id = %s",
                    (snapshot_id,),
                )
                cursor.execute(
                    """
                    UPDATE clan_battle.periods
                    SET status = 'final', final_snapshot_id = %s,
                        finalized_at = COALESCE(%s, NOW()), updated_at = NOW()
                    WHERE period = %s
                    """,
                    (snapshot_id, captured_at, period),
                )
                cursor.execute(
                    """
                    DELETE FROM clan_battle.snapshots
                    WHERE period = %s
                      AND snapshot_type = 'final_candidate'
                      AND snapshot_id <> %s
                    """,
                    (period, snapshot_id),
                )
        if owns_connection:
            conn.commit()
        return snapshot_id
    except Exception:
        if owns_connection:
            conn.rollback()
        raise
    finally:
        if owns_connection:
            conn.close()


def get_worker_state(connection=None) -> dict[str, Any]:
    owns_connection = connection is None
    conn = connection or create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM clan_battle.worker_state WHERE singleton = TRUE")
            state = cursor.fetchone()
            if not state:
                raise RuntimeError("clan battle worker state is missing")
            return dict(state)
    finally:
        if owns_connection:
            conn.close()


def update_worker_state(connection=None, **values: Any) -> None:
    if not values:
        return
    owns_connection = connection is None
    conn = connection or create_connection()
    assignments = [f"{key} = %s" for key in values]
    assignments.append("updated_at = NOW()")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE clan_battle.worker_state SET {', '.join(assignments)} WHERE singleton = TRUE",
                list(values.values()),
            )
        if owns_connection:
            conn.commit()
    except Exception:
        if owns_connection:
            conn.rollback()
        raise
    finally:
        if owns_connection:
            conn.close()


def mark_period_settlement(period: date, ended_at: datetime | None = None) -> None:
    conn = create_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE clan_battle.periods
                SET status = 'settlement',
                    ended_at = COALESCE(ended_at, %s, NOW()),
                    updated_at = NOW()
                WHERE period = %s
                  AND status <> 'final'
                """,
                (ended_at, period),
            )
        conn.commit()
    finally:
        conn.close()


def latest_final_snapshot() -> dict[str, Any] | None:
    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT s.snapshot_id, s.period, s.captured_at,
                       s.content_sha256, s.probe_sha256, p.clan_battle_id
                FROM clan_battle.snapshots s
                JOIN clan_battle.periods p ON p.period = s.period
                WHERE s.is_final
                ORDER BY s.period DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def record_collection_run(
    *,
    trigger_name: str,
    phase_before: str,
    phase_after: str,
    result_type: str,
    started_at: datetime,
    finished_at: datetime,
    pages_fetched: int = 0,
    records_fetched: int = 0,
    snapshot_id: int | None = None,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    conn = create_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO clan_battle.collection_runs(
                    trigger_name, phase_before, phase_after, result_type,
                    started_at, finished_at, pages_fetched, records_fetched,
                    snapshot_id, error_message, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    trigger_name,
                    phase_before,
                    phase_after,
                    result_type,
                    started_at,
                    finished_at,
                    pages_fetched,
                    records_fetched,
                    snapshot_id,
                    error_message,
                    Json(details or {}),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_status() -> dict[str, Any]:
    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM clan_battle.worker_state WHERE singleton = TRUE")
            state = dict(cursor.fetchone())
            cursor.execute(
                """
                SELECT snapshot_id, period, captured_at, snapshot_type,
                       row_count, max_rank, is_final
                FROM clan_battle.snapshots
                ORDER BY period DESC, captured_at DESC NULLS LAST
                LIMIT 1
                """
            )
            latest = cursor.fetchone()
            cursor.execute(
                """
                SELECT run_id, result_type, started_at, finished_at, error_message
                FROM clan_battle.collection_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            )
            last_run = cursor.fetchone()
            return {
                "worker": state,
                "latest_snapshot": dict(latest) if latest else None,
                "last_run": dict(last_run) if last_run else None,
            }
    finally:
        conn.close()


def list_periods(limit: int = 60, *, final_only: bool = False) -> list[dict[str, Any]]:
    final_filter = (
        "WHERE p.status = 'final' AND p.final_snapshot_id IS NOT NULL"
        if final_only
        else ""
    )
    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT p.period, p.status, p.clan_battle_id,
                       p.started_at, p.finalized_at,
                       p.final_snapshot_id, COUNT(s.snapshot_id)::INTEGER AS snapshot_count,
                       MIN(s.captured_at) AS first_captured_at,
                       MAX(s.captured_at) AS last_captured_at
                FROM clan_battle.periods p
                LEFT JOIN clan_battle.snapshots s ON s.period = p.period
                {final_filter}
                GROUP BY p.period
                ORDER BY p.period DESC
                LIMIT %s
                """,
                (max(1, min(limit, 240)),),
            )
            return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def list_snapshots(period: date | str) -> list[dict[str, Any]]:
    period_date = _period_start(period)
    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT snapshot_id, period, captured_at, snapshot_type, source,
                       row_count, min_rank, max_rank, is_final
                FROM clan_battle.snapshots
                WHERE period = %s
                  AND (snapshot_type <> 'final_candidate' OR is_final)
                ORDER BY captured_at ASC NULLS LAST, snapshot_id ASC
                """,
                (period_date,),
            )
            return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def resolve_snapshot_id(snapshot_id: int | None, period: date | str | None) -> int:
    conn = create_connection()
    try:
        with conn.cursor() as cursor:
            if snapshot_id is not None:
                cursor.execute(
                    "SELECT snapshot_id FROM clan_battle.snapshots WHERE snapshot_id = %s",
                    (snapshot_id,),
                )
            elif period is not None:
                period_date = _period_start(period)
                cursor.execute(
                    """
                    SELECT snapshot_id
                    FROM clan_battle.snapshots
                    WHERE period = %s
                    ORDER BY is_final DESC, captured_at DESC NULLS LAST, snapshot_id DESC
                    LIMIT 1
                    """,
                    (period_date,),
                )
            else:
                raise ValueError("snapshot_id or period is required")
            row = cursor.fetchone()
            if not row:
                raise LookupError("snapshot not found")
            return row[0]
    finally:
        conn.close()


def query_rankings(
    *,
    snapshot_id: int | None,
    period: date | str | None,
    search: str = "",
    page: int = 0,
    limit: int = 10,
) -> dict[str, Any]:
    resolved_id = resolve_snapshot_id(snapshot_id, period)
    page = max(0, page)
    limit = max(1, min(limit, 100))
    clauses = ["r.snapshot_id = %s"]
    params: list[Any] = [resolved_id]
    term = search.strip()
    if term:
        clauses.append(
            "(r.clan_name ILIKE %s OR r.leader_name ILIKE %s OR r.rank::TEXT = %s)"
        )
        params.extend((f"%{term}%", f"%{term}%", term))
    where = " AND ".join(clauses)

    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"SELECT COUNT(*) AS total FROM clan_battle.rankings r WHERE {where}", params)
            total = cursor.fetchone()["total"]
            cursor.execute(
                f"""
                SELECT r.*
                FROM clan_battle.rankings r
                WHERE {where}
                ORDER BY r.rank, r.row_number
                LIMIT %s OFFSET %s
                """,
                [*params, limit, page * limit],
            )
            return {
                "snapshot_id": resolved_id,
                "items": [dict(row) for row in cursor.fetchall()],
                "total": total,
                "page": page,
                "limit": limit,
            }
    finally:
        conn.close()


def query_scorelines(
    *,
    snapshot_id: int | None,
    period: date | str | None,
    rank: int | None,
) -> dict[str, Any]:
    resolved_id = resolve_snapshot_id(snapshot_id, period)
    ranks = [rank] if rank else [2, 10, 30, 50, 100, 200, 400, 800, 1500, 2500]
    conn = create_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT *
                FROM clan_battle.rankings
                WHERE snapshot_id = %s AND rank = ANY(%s)
                ORDER BY rank, row_number
                """,
                (resolved_id, ranks),
            )
            items = [dict(row) for row in cursor.fetchall()]
            return {"snapshot_id": resolved_id, "items": items, "total": len(items)}
    finally:
        conn.close()
