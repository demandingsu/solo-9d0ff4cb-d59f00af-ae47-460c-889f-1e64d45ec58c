"""Strict-ish CSV parsing for AIS point uploads.

Accepted header names (case-insensitive, surrounding whitespace ignored):

* vessel / 船舶号 / mmsi / ship_id
* time / 时间 / timestamp / utc_time
* lat / latitude / 纬度
* lon / lng / longitude / 经度

A header row is mandatory.  Rows are parsed independently so that one bad row
rejects the whole batch with a precise row number instead of half-importing it.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable

from .errors import AppError

VESSEL_KEYS = ("vessel", "船舶号", "mmsi", "ship_id", "ship")
TIME_KEYS = ("time", "时间", "timestamp", "utc_time", "utc")
LAT_KEYS = ("lat", "latitude", "纬度")
LON_KEYS = ("lon", "lng", "longitude", "经度")

# WGS84 bounds, in integer micro-degrees (1e-6 deg).
MIN_LAT_E6, MAX_LAT_E6 = -90_000_000, 90_000_000
MIN_LON_E6, MAX_LON_E6 = -180_000_000, 180_000_000

# Timestamps are stored as signed 64-bit epoch seconds; constrain them to the
# range Python's datetime supports so downstream formatting never overflows.
MIN_EPOCH = -62_135_596_800  # 0001-01-01T00:00:00Z
MAX_EPOCH = 253_402_300_799  # 9999-12-31T23:59:59Z


@dataclass(frozen=True)
class RawPointRow:
    row_number: int
    vessel_id: str
    event_time: int  # epoch seconds, UTC
    lat_e6: int
    lon_e6: int


@dataclass(frozen=True)
class ParsedUpload:
    rows: list[RawPointRow]
    rows_received: int
    in_file_duplicates: int


def _normalise_header(name: str) -> str:
    return name.strip().lower().replace("-", "_").replace(" ", "_")


def _find_column(headers: list[str], candidates: Iterable[str]) -> int | None:
    normalised = [_normalise_header(h) for h in headers]
    for candidate in candidates:
        if candidate in normalised:
            return normalised.index(candidate)
    return None


def _parse_time(value: str) -> int:
    text = value.strip()
    if not text:
        raise ValueError("empty timestamp")
    # Accept an epoch (seconds or milliseconds) or ISO-8601.
    try:
        numeric = Decimal(text)
    except InvalidOperation:
        numeric = None
    if numeric is not None:
        if not numeric.is_finite():
            raise ValueError("non-finite timestamp")
        # Heuristic: values beyond year 2286 in seconds are milliseconds.
        if abs(numeric) >= Decimal("10000000000"):
            numeric = numeric / Decimal(1000)
        if numeric != numeric.to_integral_value():
            raise ValueError("timestamp must resolve to whole seconds")
        epoch = int(numeric)
        if not (MIN_EPOCH <= epoch <= MAX_EPOCH):
            raise ValueError(f"timestamp out of range: {value!r}")
        return epoch
    normalised = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _parse_coordinate_e6(
    value: str, label: str, low: int, high: int
) -> int:
    text = value.strip()
    if not text:
        raise ValueError(f"empty {label}")
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"invalid {label} {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"non-finite {label}")
    scaled = number * Decimal(1_000_000)
    # Reject absurd precision only in so far as it is outside the range; round
    # to six decimals (about 0.1 m) so equality comparisons are deterministic.
    as_e6 = int(scaled.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    if as_e6 < low or as_e6 > high:
        raise ValueError(f"{label} out of range: {value!r}")
    return as_e6


def parse_upload(content: bytes, *, max_rows: int) -> ParsedUpload:
    """Parse a CSV upload.

    ``rows`` holds the unique rows in file order.  A repeated
    ``(vessel, time)`` with identical coordinates is counted in
    ``in_file_duplicates``; with different coordinates the whole upload is
    rejected, so the caller never sees a partial batch.
    """
    if not content:
        raise AppError(400, "empty_upload", "CSV upload is empty")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise AppError(400, "invalid_encoding", "CSV must be UTF-8 encoded") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise AppError(400, "empty_upload", "CSV upload is empty") from exc
    if not header or not any(cell.strip() for cell in header):
        raise AppError(400, "missing_header", "CSV header row is required")

    vessel_idx = _find_column(header, VESSEL_KEYS)
    time_idx = _find_column(header, TIME_KEYS)
    lat_idx = _find_column(header, LAT_KEYS)
    lon_idx = _find_column(header, LON_KEYS)
    missing = [
        label
        for label, idx in (
            ("vessel", vessel_idx),
            ("time", time_idx),
            ("lat", lat_idx),
            ("lon", lon_idx),
        )
        if idx is None
    ]
    if missing:
        raise AppError(
            400,
            "missing_columns",
            f"CSV is missing required column(s): {', '.join(missing)}",
        )

    rows: list[RawPointRow] = []
    seen_in_batch: dict[tuple[str, int], tuple[int, int]] = {}
    in_file_duplicates = 0
    rows_received = 0
    for row_number, cells in enumerate(reader, start=2):
        if not cells or all(not cell.strip() for cell in cells):
            continue
        rows_received += 1
        if len(cells) <= max(vessel_idx, time_idx, lat_idx, lon_idx):
            raise AppError(
                400,
                "malformed_csv",
                f"row {row_number} has too few columns",
                {"row": row_number},
            )
        vessel = cells[vessel_idx].strip()
        try:
            if not vessel:
                raise ValueError("empty vessel id")
            if len(vessel) > 128:
                raise ValueError("vessel id is longer than 128 characters")
            event_time = _parse_time(cells[time_idx])
            lat_e6 = _parse_coordinate_e6(
                cells[lat_idx], "latitude", MIN_LAT_E6, MAX_LAT_E6
            )
            lon_e6 = _parse_coordinate_e6(
                cells[lon_idx], "longitude", MIN_LON_E6, MAX_LON_E6
            )
        except ValueError as exc:
            raise AppError(
                400,
                "invalid_row",
                f"row {row_number}: {exc}",
                {"row": row_number},
            ) from exc

        key = (vessel, event_time)
        previous = seen_in_batch.get(key)
        if previous is not None and previous != (lat_e6, lon_e6):
            # The same rule as duplicated rows in the database: one vessel
            # cannot be in two places at the same instant.
            raise AppError(
                409,
                "conflicting_coordinates",
                (
                    f"rows for vessel {vessel!r} at {event_time} have different "
                    "coordinates"
                ),
                {
                    "vessel": vessel,
                    "time": event_time,
                    "row": row_number,
                },
            )
        if previous is None:
            seen_in_batch[key] = (lat_e6, lon_e6)
            rows.append(RawPointRow(row_number, vessel, event_time, lat_e6, lon_e6))
        else:
            in_file_duplicates += 1
        if len(rows) > max_rows:
            raise AppError(
                413,
                "too_many_rows",
                f"upload exceeds the {max_rows} row limit",
            )

    if not rows:
        raise AppError(400, "no_data_rows", "CSV contains no data rows")
    return ParsedUpload(
        rows=rows,
        rows_received=rows_received,
        in_file_duplicates=in_file_duplicates,
    )
