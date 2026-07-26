"""Shared farm-account leasing coordinated through PostgreSQL advisory locks."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Iterable

from psycopg2.extras import RealDictCursor

from pcrdb.db.connection import Account, create_connection


ACCOUNT_LOCK_NAMESPACE = 1_347_279_341


class AccountPoolUnavailable(RuntimeError):
    """No compatible farm account is currently available."""


@dataclass
class AccountLease:
    account: Account
    purpose: str
    connection: Any
    released: bool = False

    @property
    def client_data(self) -> dict[str, Any]:
        return self.account.as_client_dict()

    def release(self, success: bool = True, error_type: str | None = None) -> None:
        if self.released:
            return

        try:
            cooldown_seconds = max(
                0,
                int(os.getenv("PCRDB_ACCOUNT_FAILURE_COOLDOWN_SECONDS", "300")),
            )
        except ValueError:
            cooldown_seconds = 300
        try:
            with self.connection.cursor() as cursor:
                if success:
                    cursor.execute(
                        """
                        UPDATE account_pool_state
                        SET last_released_at = NOW(),
                            success_count = success_count + 1,
                            consecutive_failures = 0,
                            cooldown_until = NULL,
                            last_error_type = NULL
                        WHERE account_id = %s
                        """,
                        (self.account.id,),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE account_pool_state
                        SET last_released_at = NOW(),
                            failure_count = failure_count + 1,
                            consecutive_failures = consecutive_failures + 1,
                            cooldown_until = NOW() + make_interval(secs => %s),
                            last_error_type = %s
                        WHERE account_id = %s
                        """,
                        (cooldown_seconds, (error_type or "unknown")[:120], self.account.id),
                    )
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (ACCOUNT_LOCK_NAMESPACE, self.account.id),
                )
        finally:
            self.released = True
            self.connection.close()

    def disable(self, reason: str) -> None:
        """Remove an ineligible account from the shared pool and release its lock."""
        if self.released:
            return

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE accounts
                    SET pool_enabled = FALSE,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (self.account.id,),
                )
                cursor.execute(
                    """
                    UPDATE account_pool_state
                    SET last_released_at = NOW(),
                        consecutive_failures = 0,
                        cooldown_until = NULL,
                        last_error_type = %s
                    WHERE account_id = %s
                    """,
                    ((reason or "Ineligible")[:120], self.account.id),
                )
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (ACCOUNT_LOCK_NAMESPACE, self.account.id),
                )
        finally:
            self.released = True
            self.connection.close()

    def __enter__(self) -> "AccountLease":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.release(
            success=exc_type is None,
            error_type=exc_type.__name__ if exc_type else None,
        )
        return False


def _candidate_accounts(
    *,
    arena_group: int | None = None,
    grand_arena_group: int | None = None,
    excluded_ids: Iterable[int] = (),
) -> list[dict[str, Any]]:
    clauses = [
        "a.is_active = TRUE",
        "a.pool_enabled = TRUE",
        "a.viewer_id IS NOT NULL",
        "a.access_key <> ''",
        "(s.cooldown_until IS NULL OR s.cooldown_until <= NOW())",
    ]
    params: list[Any] = []
    if arena_group is not None:
        clauses.append("a.arena_group = %s")
        params.append(arena_group)
    if grand_arena_group is not None:
        clauses.append("a.grand_arena_group = %s")
        params.append(grand_arena_group)
    excluded = tuple(excluded_ids)
    if excluded:
        clauses.append("NOT (a.id = ANY(%s))")
        params.append(list(excluded))

    connection = create_connection(application_name="pcrdb-account-pool-list")
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT a.id, a.uid, a.access_key, a.viewer_id, a.name,
                       a.arena_group, a.grand_arena_group, a.is_active,
                       a.note, a.pool_enabled
                FROM accounts a
                LEFT JOIN account_pool_state s ON s.account_id = a.id
                WHERE {' AND '.join(clauses)}
                ORDER BY
                    CASE
                        WHEN a.arena_group = 0 AND a.grand_arena_group = 0 THEN 0
                        WHEN a.arena_group = 0 OR a.grand_arena_group = 0 THEN 1
                        ELSE 2
                    END,
                    s.last_acquired_at NULLS FIRST,
                    a.id
                """,
                params,
            )
            return [dict(row) for row in cursor.fetchall()]
    finally:
        connection.close()


def lease_accounts(
    count: int,
    purpose: str,
    *,
    arena_group: int | None = None,
    grand_arena_group: int | None = None,
    excluded_ids: Iterable[int] = (),
) -> list[AccountLease]:
    requested = max(0, count)
    if requested == 0:
        return []

    leases: list[AccountLease] = []
    for row in _candidate_accounts(
        arena_group=arena_group,
        grand_arena_group=grand_arena_group,
        excluded_ids=excluded_ids,
    ):
        connection = create_connection(
            application_name=f"pcrdb-account:{purpose}"[:63]
        )
        connection.autocommit = True
        locked = False
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s, %s)",
                    (ACCOUNT_LOCK_NAMESPACE, row["id"]),
                )
                locked = bool(cursor.fetchone()[0])
                if not locked:
                    continue
                cursor.execute(
                    """
                    INSERT INTO account_pool_state (
                        account_id, last_acquired_at, last_purpose, acquire_count
                    ) VALUES (%s, NOW(), %s, 1)
                    ON CONFLICT (account_id) DO UPDATE SET
                        last_acquired_at = EXCLUDED.last_acquired_at,
                        last_purpose = EXCLUDED.last_purpose,
                        acquire_count = account_pool_state.acquire_count + 1
                    """,
                    (row["id"], purpose[:120]),
                )

            leases.append(
                AccountLease(
                    account=Account(
                        id=row["id"],
                        uid=row["uid"],
                        access_key=row["access_key"],
                        viewer_id=row["viewer_id"],
                        name=row["name"],
                        arena_group=row["arena_group"] or 0,
                        grand_arena_group=row["grand_arena_group"] or 0,
                        is_active=row["is_active"],
                        note=row["note"],
                        pool_enabled=row["pool_enabled"],
                    ),
                    purpose=purpose,
                    connection=connection,
                )
            )
            connection = None
            if len(leases) >= requested:
                break
        finally:
            if connection is not None:
                if locked:
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                "SELECT pg_advisory_unlock(%s, %s)",
                                (ACCOUNT_LOCK_NAMESPACE, row["id"]),
                            )
                    except Exception:
                        pass
                connection.close()
    return leases


def lease_account(
    purpose: str,
    *,
    arena_group: int | None = None,
    grand_arena_group: int | None = None,
) -> AccountLease:
    leases = lease_accounts(
        1,
        purpose,
        arena_group=arena_group,
        grand_arena_group=grand_arena_group,
    )
    if not leases:
        raise AccountPoolUnavailable(
            f"no compatible farm account is available for {purpose}"
        )
    return leases[0]


def available_groups(group_type: str) -> list[int]:
    if group_type not in {"arena", "grand_arena"}:
        raise ValueError("group_type must be arena or grand_arena")
    column = "arena_group" if group_type == "arena" else "grand_arena_group"
    connection = create_connection(application_name="pcrdb-account-groups")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT DISTINCT {column}
                FROM accounts
                WHERE is_active = TRUE AND pool_enabled = TRUE AND {column} > 0
                ORDER BY {column}
                """
            )
            return [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
