from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import io
import json
from pathlib import Path
import re
import sys
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

from pcrdb.clan_battle.models import RankingRow, normalize_rows, probe_sha256, rows_sha256


BEIJING = ZoneInfo("Asia/Shanghai")
TIMESTAMP_FILE = re.compile(r"^(\d{12})\.csv$")
MONTHLY_FILE = re.compile(r"^(\d{4})年(\d{2})月\.csv$")
REQUIRED_FIELDS = {
    "rank",
    "clan_name",
    "leader_name",
    "member_num",
    "damage",
    "lap",
    "boss_id",
    "remain",
    "grade_rank",
}
KNOWN_JULY_2025_FILE_SHA256 = "81dfb371b5008b308d70ea2a1e8be307f8ab290f5ea1eb4f95b4f18a34d0d3ec"


@dataclass(frozen=True, slots=True)
class Source:
    name: str
    root: Path


@dataclass(frozen=True, slots=True)
class ImportCorrection:
    field: str
    source_row_start: int
    source_row_end: int
    original_value_start: int
    original_value_end: int
    corrected_value_start: int
    corrected_value_end: int
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "field": self.field,
            "source_row_start": self.source_row_start,
            "source_row_end": self.source_row_end,
            "original_value_start": self.original_value_start,
            "original_value_end": self.original_value_end,
            "corrected_value_start": self.corrected_value_start,
            "corrected_value_end": self.corrected_value_end,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class FileRecord:
    source: Source
    path: Path
    relative_path: str
    source_size: int
    source_mtime: datetime
    encoding: str
    header: tuple[str, ...]
    file_sha256: str
    content_sha256: str
    probe_sha256: str
    row_count: int
    period: date
    captured_at: datetime | None
    snapshot_type: str
    corrections: tuple[ImportCorrection, ...]

    @property
    def source_key(self) -> tuple[str, str]:
        return self.source.name, self.relative_path

    @property
    def target_key(self) -> tuple[date, datetime | None]:
        return self.period, self.captured_at


@dataclass(slots=True)
class ImportPlan:
    records: list[FileRecord]
    target_records: dict[tuple[date, datetime | None], list[FileRecord]]
    record_targets: dict[tuple[str, str], tuple[date, datetime | None]]
    final_targets: dict[date, tuple[date, datetime | None]]
    conflicts: list[str]

    def summary(self) -> dict[str, object]:
        period_dates = sorted({target[0] for target in self.target_records})
        periods = [period.isoformat()[:7] for period in period_dates]
        corrected_files: list[dict[str, object]] = []
        for record in sorted(self.records, key=lambda item: item.source_key):
            target = self.record_targets.get(record.source_key, record.target_key)
            if record.corrections or target[0] != record.period:
                fields = [correction.field for correction in record.corrections]
                if target[0] != record.period:
                    fields.append("period")
                corrected_files.append(
                    {
                        "source": record.source.name,
                        "relative_path": record.relative_path,
                        "fields": fields,
                        "source_period": record.period.isoformat(),
                        "target_period": target[0].isoformat(),
                    }
                )
        return {
            "source_files": len(self.records),
            "snapshot_targets": len(self.target_records),
            "periods": periods,
            "period_count": len(periods),
            "final_period_count": len(self.final_targets),
            "periods_without_final": [
                period.isoformat()[:7]
                for period in period_dates
                if period not in self.final_targets
            ],
            "corrected_source_files": len(corrected_files),
            "corrected_files": corrected_files,
            "conflicts": self.conflicts,
        }


def _previous_month(value: date) -> date:
    if value.month == 1:
        return date(value.year - 1, 12, 1)
    return date(value.year, value.month - 1, 1)


def infer_identity(filename: str) -> tuple[date, datetime | None, str]:
    timestamp_match = TIMESTAMP_FILE.match(filename)
    if timestamp_match:
        captured_at = datetime.strptime(timestamp_match.group(1), "%Y%m%d%H%M").replace(tzinfo=BEIJING)
        captured_month = captured_at.date().replace(day=1)
        if captured_at.day >= 20:
            return captured_month, captured_at, "progress"
        return _previous_month(captured_month), captured_at, "final_candidate"

    monthly_match = MONTHLY_FILE.match(filename)
    if monthly_match:
        return date(int(monthly_match.group(1)), int(monthly_match.group(2)), 1), None, "monthly_final"

    raise ValueError(f"unsupported CSV filename: {filename}")


def _decode_csv(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError("CSV is neither UTF-8 nor GB18030")


def _apply_known_corrections(
    values: list[dict[str, object]],
    file_sha256: str,
) -> tuple[list[dict[str, object]], tuple[ImportCorrection, ...]]:
    if file_sha256 != KNOWN_JULY_2025_FILE_SHA256:
        return values, ()

    expected_ranks = list(range(970, 1236))
    actual_ranks = [int(row.get("rank") or -1) for row in values[970:]]
    expected_grade_ranks = list(range(971, 1237))
    actual_grade_ranks = [
        int(row["grade_rank"]) if row.get("grade_rank") is not None else -1
        for row in values[970:]
    ]
    grade_ranks_match = all(
        actual in (0, expected)
        for actual, expected in zip(actual_grade_ranks, expected_grade_ranks, strict=True)
    )
    source_indexes = [int(row.get("") or 0) for row in values]
    if (
        len(values) != 1236
        or int(values[969].get("rank") or -1) != 970
        or actual_ranks != expected_ranks
        or not grade_ranks_match
        or source_indexes != list(range(1236))
    ):
        raise ValueError("known July 2025 rank correction no longer matches the source file")

    corrected = [dict(row) for row in values]
    for row in corrected[970:]:
        row["rank"] = int(row[""] or 0) + 1

    correction = ImportCorrection(
        field="rank",
        source_row_start=971,
        source_row_end=1236,
        original_value_start=970,
        original_value_end=1235,
        corrected_value_start=971,
        corrected_value_end=1236,
        reason=(
            "Isolated source ranking anomaly consistent with a cached offset: rank repeats 970 and "
            "remains one place behind, while the source index remains contiguous and grade_rank "
            "corroborates it when present."
        ),
    )
    return corrected, (correction,)


def read_rows(
    path: Path,
) -> tuple[list[RankingRow], tuple[str, ...], str, str, tuple[ImportCorrection, ...]]:
    raw = path.read_bytes()
    text, encoding = _decode_csv(raw)
    file_hash = sha256(raw).hexdigest()
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if not reader.fieldnames:
        raise ValueError("CSV has no header")

    header = tuple((name or "").strip().lstrip("\ufeff") for name in reader.fieldnames)
    if missing := REQUIRED_FIELDS.difference(header):
        raise ValueError(f"CSV is missing columns: {', '.join(sorted(missing))}")

    values: list[dict[str, object]] = [
        dict(row)
        for row in reader
        if any(value not in (None, "") for value in row.values())
    ]
    corrected_values, corrections = _apply_known_corrections(values, file_hash)
    rows = normalize_rows(corrected_values)
    return rows, header, encoding, file_hash, corrections


def scan_file(source: Source, path: Path) -> FileRecord:
    rows, header, encoding, file_hash, corrections = read_rows(path)
    period, captured_at, snapshot_type = infer_identity(path.name)
    stat = path.stat()
    return FileRecord(
        source=source,
        path=path,
        relative_path=path.relative_to(source.root).as_posix(),
        source_size=stat.st_size,
        source_mtime=datetime.fromtimestamp(stat.st_mtime, timezone.utc),
        encoding=encoding,
        header=header,
        file_sha256=file_hash,
        content_sha256=rows_sha256(rows),
        probe_sha256=probe_sha256(rows),
        row_count=len(rows),
        period=period,
        captured_at=captured_at,
        snapshot_type=snapshot_type,
        corrections=corrections,
    )


def scan_sources(sources: Sequence[Source]) -> list[FileRecord]:
    records: list[FileRecord] = []
    for source in sources:
        if not source.root.is_dir():
            raise FileNotFoundError(f"source directory does not exist: {source.root}")
        for path in sorted(source.root.rglob("*.csv")):
            try:
                records.append(scan_file(source, path))
            except Exception as exc:
                relative = path.relative_to(source.root).as_posix()
                raise ValueError(f"{source.name}/{relative}: {exc}") from exc
    return records


def build_plan(records: Sequence[FileRecord]) -> ImportPlan:
    conflicts: list[str] = []
    target_records: dict[tuple[date, datetime | None], list[FileRecord]] = {}
    record_targets: dict[tuple[str, str], tuple[date, datetime | None]] = {}
    timestamp_by_period_hash: dict[tuple[date, str], list[FileRecord]] = {}
    timestamp_by_hash: dict[str, list[FileRecord]] = {}
    monthly_by_period: dict[date, list[FileRecord]] = {}

    for record in records:
        if record.captured_at is None:
            monthly_by_period.setdefault(record.period, []).append(record)
            continue
        target_records.setdefault(record.target_key, []).append(record)
        timestamp_by_period_hash.setdefault((record.period, record.content_sha256), []).append(record)
        timestamp_by_hash.setdefault(record.content_sha256, []).append(record)
        record_targets[record.source_key] = record.target_key

    for target, grouped in target_records.items():
        hashes = {record.content_sha256 for record in grouped}
        if len(hashes) > 1:
            canonical = max(grouped, key=lambda record: record.row_count)
            canonical_rows, _, _, _, _ = read_rows(canonical.path)
            is_prefix_group = True
            for record in grouped:
                rows, _, _, _, _ = read_rows(record.path)
                if len(rows) > len(canonical_rows) or rows != canonical_rows[:len(rows)]:
                    is_prefix_group = False
                    break
            if not is_prefix_group:
                conflicts.append(
                    f"timestamp conflict for {target[0]} at {target[1]}: {len(hashes)} different contents"
                )

    unmatched_monthly: dict[date, dict[str, list[FileRecord]]] = {}
    explicit_targets: dict[date, list[tuple[date, datetime | None]]] = {}
    for period, monthly_records in monthly_by_period.items():
        for record in monthly_records:
            same_period_matches = timestamp_by_period_hash.get((period, record.content_sha256), [])
            all_matches = timestamp_by_hash.get(record.content_sha256, [])
            matches = same_period_matches
            matched_periods = {match.period for match in all_matches}
            if not matches and len(matched_periods) == 1:
                matches = all_matches
            elif not matches and len(matched_periods) > 1:
                conflicts.append(
                    f"ambiguous monthly content for {record.source.name}/{record.relative_path}: "
                    f"matches {len(matched_periods)} periods"
                )
            if matches:
                matched = max(matches, key=lambda item: item.captured_at or datetime.min.replace(tzinfo=BEIJING))
                target = matched.target_key
            else:
                target = (period, None)
                unmatched_monthly.setdefault(period, {}).setdefault(record.content_sha256, []).append(record)
                target_records.setdefault(target, []).append(record)
            record_targets[record.source_key] = target
            if record not in target_records.setdefault(target, []):
                target_records[target].append(record)
            if target[1] is None or target[1].day < 20:
                explicit_targets.setdefault(target[0], []).append(target)

    for period, grouped_by_hash in unmatched_monthly.items():
        if len(grouped_by_hash) > 1:
            conflicts.append(
                f"monthly final conflict for {period}: {len(grouped_by_hash)} unmatched contents"
            )

    final_targets: dict[date, tuple[date, datetime | None]] = {}
    periods = sorted({target[0] for target in target_records})
    for period in periods:
        candidates = explicit_targets.get(period, [])
        if candidates:
            final_targets[period] = max(
                set(candidates),
                key=lambda target: target[1] or datetime.min.replace(tzinfo=BEIJING),
            )
            continue

        settled = [
            target
            for target in target_records
            if target[0] == period and target[1] is not None and target[1].day < 20
        ]
        if settled:
            final_targets[period] = max(settled, key=lambda target: target[1])

    for target in target_records:
        target_records[target].sort(key=lambda record: (-record.row_count, record.source_key))

    return ImportPlan(
        records=list(records),
        target_records=target_records,
        record_targets=record_targets,
        final_targets=final_targets,
        conflicts=conflicts,
    )


def _manifest_row(
    cursor,
    record: FileRecord,
    snapshot_id: int,
    status: str,
    duplicate_of: int | None,
    target_period: date,
) -> int:
    from psycopg2.extras import Json

    corrections = [correction.as_dict() for correction in record.corrections]
    if target_period != record.period:
        corrections.append(
            {
                "field": "period",
                "original_value": record.period.isoformat(),
                "corrected_value": target_period.isoformat(),
                "reason": (
                    "The legacy monthly filename is mislabeled and exactly matches a "
                    "timestamped snapshot from the corrected period."
                ),
            }
        )

    cursor.execute(
        """
        SELECT import_file_id, file_sha256
        FROM clan_battle.import_files
        WHERE source_name = %s AND relative_path = %s
        FOR UPDATE
        """,
        record.source_key,
    )
    existing = cursor.fetchone()
    if existing and existing["file_sha256"].strip() != record.file_sha256:
        raise ValueError(f"source file changed after import: {record.source.name}/{record.relative_path}")

    values = (
        record.source.name,
        record.relative_path,
        record.source_size,
        record.source_mtime,
        record.encoding,
        Json(list(record.header)),
        record.file_sha256,
        record.content_sha256,
        record.row_count,
        record.period,
        record.captured_at,
        snapshot_id,
        status,
        duplicate_of,
        Json(corrections),
    )
    if existing:
        cursor.execute(
            """
            UPDATE clan_battle.import_files
            SET source_size = %s, source_mtime = %s, encoding = %s, header = %s,
                content_sha256 = %s, row_count = %s, inferred_period = %s,
                inferred_captured_at = %s, snapshot_id = %s, status = %s,
                duplicate_of = %s, corrections = %s,
                error_message = NULL, imported_at = NOW()
            WHERE import_file_id = %s
            RETURNING import_file_id
            """,
            (
                record.source_size,
                record.source_mtime,
                record.encoding,
                Json(list(record.header)),
                record.content_sha256,
                record.row_count,
                record.period,
                record.captured_at,
                snapshot_id,
                status,
                duplicate_of,
                Json(corrections),
                existing["import_file_id"],
            ),
        )
    else:
        cursor.execute(
            """
            INSERT INTO clan_battle.import_files(
                source_name, relative_path, source_size, source_mtime, encoding,
                header, file_sha256, content_sha256, row_count, inferred_period,
                inferred_captured_at, snapshot_id, status, duplicate_of, corrections
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING import_file_id
            """,
            values,
        )
    return cursor.fetchone()["import_file_id"]


def import_plan(plan: ImportPlan) -> dict[str, object]:
    from psycopg2.extras import RealDictCursor

    from pcrdb.clan_battle.repository import save_snapshot
    from pcrdb.db.connection import create_connection

    if plan.conflicts:
        raise ValueError("import plan contains conflicts; no database changes were made")

    conn = create_connection()
    imported_targets = 0
    duplicate_files = 0
    try:
        for target in sorted(
            plan.target_records,
            key=lambda value: (value[0], value[1] or datetime.max.replace(tzinfo=BEIJING)),
        ):
            records = plan.target_records[target]
            canonical = records[0]
            rows, _, _, file_hash, _ = read_rows(canonical.path)
            if file_hash != canonical.file_sha256 or rows_sha256(rows) != canonical.content_sha256:
                raise ValueError(f"source changed during import: {canonical.path}")
            is_final = plan.final_targets.get(target[0]) == target
            snapshot_type = "monthly_final" if target[1] is None else (
                "final_candidate" if is_final else canonical.snapshot_type
            )
            snapshot_id = save_snapshot(
                period=target[0],
                captured_at=target[1],
                snapshot_type=snapshot_type,
                source=f"csv:{canonical.source.name}",
                rows=rows,
                is_final=is_final,
                connection=conn,
            )
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                canonical_id = _manifest_row(
                    cursor,
                    canonical,
                    snapshot_id,
                    "imported",
                    None,
                    target[0],
                )
                for duplicate in records[1:]:
                    duplicate_status = (
                        "duplicate"
                        if duplicate.content_sha256 == canonical.content_sha256
                        else "prefix_duplicate"
                    )
                    _manifest_row(
                        cursor,
                        duplicate,
                        snapshot_id,
                        duplicate_status,
                        canonical_id,
                        target[0],
                    )
                    duplicate_files += 1
            conn.commit()
            imported_targets += 1

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            periods_without_final = sorted(
                {target[0] for target in plan.target_records}
                - set(plan.final_targets)
            )
            if periods_without_final:
                cursor.execute(
                    """
                    UPDATE clan_battle.periods
                    SET status = 'settlement', final_snapshot_id = NULL,
                        finalized_at = NULL, updated_at = NOW()
                    WHERE period = ANY(%s)
                    """,
                    (periods_without_final,),
                )

            cursor.execute(
                """
                SELECT period, probe_sha256, snapshot_id
                FROM clan_battle.snapshots
                WHERE is_final
                ORDER BY period DESC
                LIMIT 1
                """
            )
            latest = cursor.fetchone()
            if latest:
                cursor.execute(
                    """
                    UPDATE clan_battle.worker_state
                    SET phase = 'final', active_period = %s,
                        reference_probe_sha256 = %s,
                        candidate_content_sha256 = NULL,
                        candidate_seen_count = 0,
                        successful_empty_count = 0,
                        last_snapshot_id = %s,
                        last_error_type = NULL,
                        last_error_message = NULL,
                        updated_at = NOW()
                    WHERE singleton = TRUE
                    """,
                    (latest["period"], latest["probe_sha256"], latest["snapshot_id"]),
                )
        conn.commit()
        return {
            **plan.summary(),
            "imported_targets": imported_targets,
            "duplicate_files": duplicate_files,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_source(value: str) -> Source:
    try:
        name, raw_path = value.split("=", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("source must use NAME=PATH") from exc
    if not name.strip():
        raise argparse.ArgumentTypeError("source name cannot be empty")
    return Source(name=name.strip(), root=Path(raw_path).resolve())


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import legacy ClanRank CSV snapshots")
    parser.add_argument("--source", action="append", required=True, type=parse_source)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    names = [source.name for source in args.source]
    if len(names) != len(set(names)):
        parser.error("source names must be unique")

    try:
        records = scan_sources(args.source)
        plan = build_plan(records)
        result = plan.summary() if args.dry_run else import_plan(plan)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 2 if plan.conflicts else 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
