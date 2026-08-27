from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from pcrdb.clan_battle.repository import (
    get_status,
    list_periods,
    list_snapshots,
    query_rankings,
    query_scorelines,
)


router = APIRouter(prefix="/api/clan-battle", tags=["clan-battle"])


@router.get("/status")
def status():
    return get_status()


@router.get("/periods")
def periods(
    limit: int = Query(60, ge=1, le=240),
    final_only: bool = Query(False),
):
    return {"items": list_periods(limit, final_only=final_only)}


@router.get("/snapshots")
def snapshots(period: str = Query(..., pattern=r"^\d{4}-\d{2}$")):
    return {"period": period, "items": list_snapshots(period)}


@router.get("/rankings")
def rankings(
    snapshot_id: int | None = Query(None, ge=1),
    period: str | None = Query(None, pattern=r"^\d{4}-\d{2}$"),
    search: str = Query("", max_length=100),
    page: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
):
    try:
        return query_rankings(
            snapshot_id=snapshot_id,
            period=period,
            search=search,
            page=page,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/scorelines")
def scorelines(
    snapshot_id: int | None = Query(None, ge=1),
    period: str | None = Query(None, pattern=r"^\d{4}-\d{2}$"),
    rank: int | None = Query(None, ge=1),
):
    try:
        return query_scorelines(snapshot_id=snapshot_id, period=period, rank=rank)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
