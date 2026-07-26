"""Versioned game progression data used by profile analysis."""

import json
import os
from bisect import bisect_right
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional


_DATA_PATH = Path(__file__).with_name("master_data") / "game_progress.json"


@dataclass(frozen=True)
class GameProgress:
    master_version: int
    knight_rank_total_exp: tuple[int, ...]
    talent_quest_counts: tuple[int, ...]


@lru_cache(maxsize=1)
def load_game_progress() -> GameProgress:
    raw = json.loads(_DATA_PATH.read_text(encoding="utf-8"))

    master_version = raw.get("master_version")
    if type(master_version) is not int or master_version <= 0:
        raise RuntimeError("game progress master_version must be a positive integer")

    rows = raw.get("knight_rank_thresholds")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("game progress knight rank thresholds are missing")

    thresholds = []
    for expected_rank, row in enumerate(rows, start=1):
        if not isinstance(row, list) or len(row) != 2:
            raise RuntimeError("invalid knight rank threshold row")
        rank, total_exp = row
        if type(rank) is not int or rank != expected_rank:
            raise RuntimeError("knight ranks must be contiguous and start at 1")
        if type(total_exp) is not int or total_exp < 0:
            raise RuntimeError("knight rank EXP thresholds must be non-negative integers")
        if thresholds and total_exp <= thresholds[-1]:
            raise RuntimeError("knight rank EXP thresholds must be strictly increasing")
        thresholds.append(total_exp)

    if thresholds[0] != 0:
        raise RuntimeError("knight rank 1 EXP threshold must be zero")

    counts = raw.get("talent_quest_counts")
    if (
        not isinstance(counts, list)
        or len(counts) != 5
        or any(type(count) is not int or count <= 0 for count in counts)
    ):
        raise RuntimeError("talent quest counts must contain five positive integers")

    return GameProgress(
        master_version=master_version,
        knight_rank_total_exp=tuple(thresholds),
        talent_quest_counts=tuple(counts),
    )


def exp_to_knight_level(total_exp: Optional[int]) -> str:
    """Convert cumulative princess-knight EXP to the exact current rank."""
    if total_exp is None or total_exp <= 0:
        return "0"

    thresholds = load_game_progress().knight_rank_total_exp
    if total_exp > thresholds[-1]:
        return f"{len(thresholds)}+"
    return str(bisect_right(thresholds, total_exp))


def get_talent_quest_total() -> int:
    """Return the current total stage count, allowing an explicit override."""
    override = os.getenv("TALENT_QUEST_TOTAL")
    if override is not None:
        try:
            total = int(override)
        except ValueError as exc:
            raise ValueError("TALENT_QUEST_TOTAL must be a positive integer") from exc
        if total <= 0:
            raise ValueError("TALENT_QUEST_TOTAL must be a positive integer")
        return total

    return sum(load_game_progress().talent_quest_counts)
