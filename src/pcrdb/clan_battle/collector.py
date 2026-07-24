from __future__ import annotations

import asyncio
from datetime import date, datetime
import logging
import os
from typing import Any
from zoneinfo import ZoneInfo

from pcrdb.api.client import PCRClientError, PCRProtocolError, PCRTransportError
from pcrdb.api.endpoints import PCRApi
from pcrdb.clan_battle.models import RankingRow, normalize_rows, probe_sha256, rows_sha256
from pcrdb.clan_battle.repository import (
    advisory_lock,
    get_worker_state,
    latest_final_snapshot,
    mark_period_settlement,
    record_collection_run,
    save_snapshot,
    update_worker_state,
)


logger = logging.getLogger(__name__)
BEIJING = ZoneInfo("Asia/Shanghai")


def _month_start(value: datetime | date) -> date:
    return value.date().replace(day=1) if isinstance(value, datetime) else value.replace(day=1)


def _previous_month(value: date) -> date:
    if value.month == 1:
        return date(value.year - 1, 12, 1)
    return date(value.year, value.month - 1, 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _capture_time(now: datetime) -> datetime:
    return now.replace(second=0, microsecond=0)


def _is_final_data_window(active_period: date, now: datetime) -> bool:
    return _month_start(now) > active_period


async def _fetch_page(api: PCRApi, page: int) -> list[RankingRow]:
    values = await api.query_clan_battle_ranking(page)
    return normalize_rows(values)


async def _fetch_snapshot(
    apis: list[PCRApi],
    page_limit: int,
    first_page: list[RankingRow],
) -> tuple[list[RankingRow], int]:
    if not apis:
        raise RuntimeError("no logged-in account is available for clan battle collection")

    rows = list(first_page)
    pages_fetched = 1
    for start in range(1, page_limit, len(apis)):
        page_numbers = list(range(start, min(start + len(apis), page_limit)))
        page_results = await asyncio.gather(
            *(
                _fetch_page(apis[index % len(apis)], page)
                for index, page in enumerate(page_numbers)
            )
        )
        for page_rows in page_results:
            pages_fetched += 1
            if not page_rows:
                return normalize_rows(rows), pages_fetched
            rows.extend(page_rows)
    return normalize_rows(rows), pages_fetched


async def _lease_clients(count: int) -> list[tuple[Any, PCRApi]]:
    from pcrdb.account_pool import lease_accounts

    leases = lease_accounts(count, "clan_battle")
    if not leases:
        raise RuntimeError("shared farm account pool has no account for clan battle")

    clients = [
        PCRApi(
            lease.account.viewer_id or 0,
            str(lease.account.uid),
            lease.account.access_key,
        )
        for lease in leases
    ]
    try:
        login_results = await asyncio.gather(
            *(client.login() for client in clients),
            return_exceptions=True,
        )
    except BaseException as exc:
        for lease in leases:
            try:
                lease.release(False, type(exc).__name__)
            except Exception:
                logger.exception("Failed to release a cancelled clan battle account lease")
        raise

    ready: list[tuple[Any, PCRApi]] = []
    for lease, client, result in zip(leases, clients, login_results):
        if isinstance(result, BaseException):
            try:
                lease.release(False, type(result).__name__)
            except Exception:
                logger.exception("Failed to release a clan battle account after login failure")
        else:
            ready.append((lease, client))
    if not ready:
        raise RuntimeError("all leased clan battle accounts failed to log in")
    return ready


def _prepare_phase(state: dict, now: datetime) -> dict:
    current_month = _month_start(now)
    active_period = state.get("active_period")

    if state["phase"] != "final":
        if now.day >= 20 and active_period and current_month > active_period:
            if state["phase"] in {"active", "settlement"}:
                mark_period_settlement(active_period, state.get("last_nonempty_at"))
            state.update(
                phase="waiting_start",
                active_period=current_month,
                reference_probe_sha256=None,
                candidate_content_sha256=None,
                candidate_seen_count=0,
                successful_empty_count=0,
            )
            update_worker_state(
                phase="waiting_start",
                active_period=current_month,
                reference_probe_sha256=None,
                candidate_content_sha256=None,
                candidate_seen_count=0,
                successful_empty_count=0,
            )
        return state

    if active_period is None:
        latest = latest_final_snapshot()
        if latest:
            active_period = latest["period"]
            state["active_period"] = active_period
            state["reference_probe_sha256"] = latest["probe_sha256"].strip()
            update_worker_state(
                active_period=active_period,
                reference_probe_sha256=latest["probe_sha256"],
                last_snapshot_id=latest["snapshot_id"],
            )

    if active_period and current_month > _next_month(active_period):
        # The worker was offline for a whole battle. Recover the previous month's final first.
        recovery_period = _previous_month(current_month)
        state.update(phase="settlement", active_period=recovery_period)
        update_worker_state(
            phase="settlement",
            active_period=recovery_period,
            candidate_content_sha256=None,
            candidate_seen_count=0,
            successful_empty_count=0,
        )
    elif now.day >= 20 and (active_period is None or current_month > active_period):
        state.update(phase="waiting_start", active_period=current_month)
        update_worker_state(
            phase="waiting_start",
            active_period=current_month,
            candidate_content_sha256=None,
            candidate_seen_count=0,
            successful_empty_count=0,
        )
    return state


async def collect_tick(trigger_name: str = "cron") -> None:
    started_at = datetime.now(BEIJING)
    phase_before = "unknown"
    phase_after = "unknown"
    result_type = "unknown"
    pages_fetched = 0
    records_fetched = 0
    snapshot_id = None
    error_message = None
    details: dict[str, object] = {}
    leased_clients: list[tuple[Any, PCRApi]] = []

    if os.getenv("CLAN_BATTLE_COLLECTION_ENABLED", "false").lower() != "true":
        logger.info("Clan battle collection is disabled")
        return

    with advisory_lock() as lock_connection:
        if lock_connection is None:
            logger.info("Another clan battle worker holds the advisory lock")
            return

        state = _prepare_phase(get_worker_state(), started_at)
        phase_before = state["phase"]
        phase_after = phase_before
        active_period = state.get("active_period")

        if phase_before == "final":
            logger.info("No clan battle work is due before the monthly start window")
            return

        try:
            concurrency = max(
                1,
                min(16, int(os.getenv("CLAN_BATTLE_QUERY_CONCURRENCY", "3"))),
            )
            leased_clients = await _lease_clients(concurrency)
            apis = [client for _, client in leased_clients]
            details["account_count"] = len(apis)
            first_page = await _fetch_page(apis[0], 0)
            pages_fetched = 1

            if phase_before == "waiting_start":
                if not first_page:
                    result_type = "waiting_empty"
                    update_worker_state(
                        last_probe_at=started_at,
                        last_error_type=None,
                        last_error_message=None,
                    )
                else:
                    first_hash = probe_sha256(first_page)
                    reference_hash = (state.get("reference_probe_sha256") or "").strip()
                    if not reference_hash:
                        result_type = "waiting_reference_initialized"
                        update_worker_state(
                            reference_probe_sha256=first_hash,
                            last_probe_at=started_at,
                            last_nonempty_at=started_at,
                        )
                    elif first_hash == reference_hash:
                        result_type = "waiting_old_final"
                        update_worker_state(
                            last_probe_at=started_at,
                            last_nonempty_at=started_at,
                            last_error_type=None,
                            last_error_message=None,
                        )
                    else:
                        rows, pages_fetched = await _fetch_snapshot(
                            apis,
                            int(os.getenv("CLAN_BATTLE_PROGRESS_PAGES", "30")),
                            first_page,
                        )
                        records_fetched = len(rows)
                        snapshot_id = save_snapshot(
                            period=active_period,
                            captured_at=_capture_time(started_at),
                            snapshot_type="progress",
                            source="worker",
                            rows=rows,
                        )
                        phase_after = "active"
                        result_type = "battle_started"
                        update_worker_state(
                            phase="active",
                            successful_empty_count=0,
                            last_probe_at=started_at,
                            last_nonempty_at=started_at,
                            last_snapshot_id=snapshot_id,
                            last_error_type=None,
                            last_error_message=None,
                        )

            elif phase_before == "active":
                if not first_page:
                    empty_count = state["successful_empty_count"] + 1
                    threshold = int(os.getenv("CLAN_BATTLE_EMPTY_THRESHOLD", "2"))
                    phase_after = "settlement" if empty_count >= threshold else "active"
                    result_type = "settlement_empty" if phase_after == "settlement" else "active_empty"
                    if phase_after == "settlement":
                        mark_period_settlement(active_period, started_at)
                    update_worker_state(
                        phase=phase_after,
                        successful_empty_count=empty_count,
                        last_probe_at=started_at,
                        last_error_type=None,
                        last_error_message=None,
                    )
                else:
                    rows, pages_fetched = await _fetch_snapshot(
                        apis,
                        int(os.getenv("CLAN_BATTLE_PROGRESS_PAGES", "30")),
                        first_page,
                    )
                    records_fetched = len(rows)
                    snapshot_id = save_snapshot(
                        period=active_period,
                        captured_at=_capture_time(started_at),
                        snapshot_type="progress",
                        source="worker",
                        rows=rows,
                    )
                    result_type = "progress_saved"
                    update_worker_state(
                        successful_empty_count=0,
                        last_probe_at=started_at,
                        last_nonempty_at=started_at,
                        last_snapshot_id=snapshot_id,
                        last_error_type=None,
                        last_error_message=None,
                    )

            elif phase_before == "settlement":
                if not first_page:
                    result_type = "settlement_waiting"
                    update_worker_state(
                        last_probe_at=started_at,
                        last_error_type=None,
                        last_error_message=None,
                    )
                elif not _is_final_data_window(active_period, started_at):
                    rows, pages_fetched = await _fetch_snapshot(
                        apis,
                        int(os.getenv("CLAN_BATTLE_PROGRESS_PAGES", "30")),
                        first_page,
                    )
                    records_fetched = len(rows)
                    snapshot_id = save_snapshot(
                        period=active_period,
                        captured_at=_capture_time(started_at),
                        snapshot_type="progress",
                        source="worker",
                        rows=rows,
                    )
                    phase_after = "active"
                    result_type = "battle_resumed"
                    update_worker_state(
                        phase="active",
                        candidate_content_sha256=None,
                        candidate_seen_count=0,
                        successful_empty_count=0,
                        last_probe_at=started_at,
                        last_nonempty_at=started_at,
                        last_snapshot_id=snapshot_id,
                        last_error_type=None,
                        last_error_message=None,
                    )
                else:
                    rows, pages_fetched = await _fetch_snapshot(
                        apis,
                        int(os.getenv("CLAN_BATTLE_FINAL_PAGES", "300")),
                        first_page,
                    )
                    records_fetched = len(rows)
                    content_hash = rows_sha256(rows)
                    previous_hash = (state.get("candidate_content_sha256") or "").strip()
                    seen_count = state["candidate_seen_count"] + 1 if content_hash == previous_hash else 1
                    stable_threshold = int(os.getenv("CLAN_BATTLE_FINAL_STABLE_COUNT", "2"))
                    is_final = seen_count >= stable_threshold
                    snapshot_id = save_snapshot(
                        period=active_period,
                        captured_at=_capture_time(started_at),
                        snapshot_type="final_candidate",
                        source="worker",
                        rows=rows,
                        is_final=is_final,
                    )
                    phase_after = "final" if is_final else "settlement"
                    result_type = "final_confirmed" if is_final else "final_candidate_saved"
                    update_worker_state(
                        phase=phase_after,
                        reference_probe_sha256=probe_sha256(rows) if is_final else state.get("reference_probe_sha256"),
                        candidate_content_sha256=None if is_final else content_hash,
                        candidate_seen_count=0 if is_final else seen_count,
                        successful_empty_count=0,
                        last_probe_at=started_at,
                        last_nonempty_at=started_at,
                        last_snapshot_id=snapshot_id,
                        last_error_type=None,
                        last_error_message=None,
                    )

            details["period"] = str(active_period)
        except asyncio.CancelledError:
            result_type = "cancelled"
            error_message = "clan battle collection was cancelled"
            raise
        except PCRTransportError as exc:
            result_type = "network_error"
            error_message = str(exc)
            update_worker_state(
                last_probe_at=started_at,
                last_error_type="network",
                last_error_message=error_message,
            )
            logger.warning("Clan battle network error: %s", exc)
        except PCRProtocolError as exc:
            result_type = "protocol_error"
            error_message = str(exc)
            update_worker_state(
                last_probe_at=started_at,
                last_error_type="protocol",
                last_error_message=error_message,
            )
            logger.error("Clan battle protocol error: %s", exc)
        except PCRClientError as exc:
            result_type = "api_error"
            error_message = str(exc)
            update_worker_state(
                last_probe_at=started_at,
                last_error_type="api",
                last_error_message=error_message,
            )
            logger.error("Clan battle API error: %s", exc)
        except Exception as exc:
            result_type = "worker_error"
            error_message = str(exc)
            update_worker_state(
                last_probe_at=started_at,
                last_error_type="worker",
                last_error_message=error_message,
            )
            logger.exception("Clan battle worker failed")
        finally:
            lease_success = error_message is None
            release_errors = 0
            for lease, _ in leased_clients:
                try:
                    lease.release(
                        success=lease_success,
                        error_type=None if lease_success else result_type,
                    )
                except Exception:
                    release_errors += 1
                    logger.exception("Failed to release a clan battle account lease")
            if release_errors:
                details["account_release_errors"] = release_errors
            finished_at = datetime.now(BEIJING)
            record_collection_run(
                trigger_name=trigger_name,
                phase_before=phase_before,
                phase_after=phase_after,
                result_type=result_type,
                started_at=started_at,
                finished_at=finished_at,
                pages_fetched=pages_fetched,
                records_fetched=records_fetched,
                snapshot_id=snapshot_id,
                error_message=error_message,
                details=details,
            )
