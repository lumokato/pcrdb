from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import math
from typing import Any, Iterable, Mapping


BOSS_LIFE = (
    (12_000_000, 15_000_000, 20_000_000, 23_000_000, 30_000_000),
    (35_000_000, 40_000_000, 45_000_000, 50_000_000, 58_000_000),
    (540_000_000, 560_000_000, 600_000_000, 620_000_000, 640_000_000),
)
BOSS_MULTIPLIER = (
    (1.6, 1.6, 1.8, 1.9, 2.0),
    (2.0, 2.0, 2.1, 2.1, 2.2),
    (4.5, 4.5, 4.7, 4.8, 5.0),
)
LAP_UPGRADE = (7, 23)


def _clean_text(value: Any, default: str) -> str:
    if value is None:
        return default
    text = str(value).replace("\x00", "").replace("\r", "").strip()
    return text or default


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(float(str(value)))


def boss_status(damage: int) -> tuple[int, int, int]:
    remaining_score = max(0, damage)
    lap = 1
    boss_index = 0
    tier = 0

    while True:
        scored_health = int(BOSS_LIFE[tier][boss_index] * BOSS_MULTIPLIER[tier][boss_index])
        if remaining_score < scored_health:
            remaining = int(
                BOSS_LIFE[tier][boss_index]
                - remaining_score / BOSS_MULTIPLIER[tier][boss_index]
            )
            return lap, boss_index + 1, max(0, remaining)

        remaining_score -= scored_health
        boss_index += 1
        if boss_index == 5:
            boss_index = 0
            lap += 1
            if tier < 2 and lap >= LAP_UPGRADE[tier]:
                tier += 1


@dataclass(frozen=True, slots=True)
class RankingRow:
    rank: int
    clan_name: str
    leader_name: str
    member_num: int
    damage: int
    lap: int
    boss_id: int
    remain: int
    grade_rank: int
    bili_rank: int | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RankingRow":
        damage = _as_int(value.get("damage"))
        calculated_lap, calculated_boss, calculated_remain = boss_status(damage)
        bili_value = value.get("bili_rank")
        return cls(
            rank=_as_int(value.get("rank"), -1),
            clan_name=_clean_text(value.get("clan_name"), "此行会已解散"),
            leader_name=_clean_text(value.get("leader_name"), "unknown"),
            member_num=_as_int(value.get("member_num")),
            damage=damage,
            lap=_as_int(value.get("lap"), calculated_lap),
            boss_id=_as_int(value.get("boss_id"), calculated_boss),
            remain=_as_int(value.get("remain"), calculated_remain),
            grade_rank=_as_int(value.get("grade_rank")),
            bili_rank=None if bili_value in (None, "") else _as_int(bili_value),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_rows(values: Iterable[Mapping[str, Any] | RankingRow]) -> list[RankingRow]:
    rows = [value if isinstance(value, RankingRow) else RankingRow.from_mapping(value) for value in values]
    rows.sort(key=lambda row: row.rank)
    if any(row.rank <= 0 for row in rows):
        raise ValueError("ranking rows contain a non-positive rank")
    return rows


def rows_sha256(rows: Iterable[RankingRow]) -> str:
    payload = [row.as_dict() for row in rows]
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def probe_sha256(rows: Iterable[RankingRow]) -> str:
    ordered = sorted(rows, key=lambda row: row.rank)
    return rows_sha256(ordered[:10])
