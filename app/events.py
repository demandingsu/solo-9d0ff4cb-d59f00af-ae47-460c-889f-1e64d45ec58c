"""Port-stay event derivation.

Rules (fixed by the operational contract):

* Points are ordered by UTC time for each vessel.
* Two consecutive in-port points belong to the same stay when their gap is at
  most 10 minutes (600 s).  A gap over 10 minutes closes the event.
* An out-of-port point is an exit marker: it always closes the current stay and
  never belongs to one.
* An event is only kept when ``end - start >= 30 minutes`` (1800 s).
* Points are the only input.  When new points arrive, events are recomputed for
  the affected vessel.  Unchanged events keep their database id; changed
  events are retracted under their old id and re-created under a new id via the
  change log.

Event identity: a stay is keyed by ``(port, vessel, start_time)``.  A late point
that moves the start is therefore a different event, while a late point that
only shifts the end or point count is a payload change of the same key.  Both
cases are emitted as retract(old id) + upsert(new id) so consumers always see
complete snapshots and never have to patch an existing id in place.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

MAX_GAP_SECONDS = 10 * 60
MIN_STAY_SECONDS = 30 * 60


@dataclass(frozen=True)
class Point:
    id: int
    vessel_id: str
    event_time: int
    lat_e6: int
    lon_e6: int


@dataclass(frozen=True)
class Stay:
    vessel_id: str
    start_time: int
    end_time: int
    start_lat_e6: int
    start_lon_e6: int
    end_lat_e6: int
    end_lon_e6: int
    point_count: int

    @property
    def key(self) -> tuple[str, int]:
        return (self.vessel_id, self.start_time)


def derive_stays(
    points: Sequence[Point],
    contains: Callable[[int, int], bool],
) -> list[Stay]:
    """Derive all qualifying stays for one vessel.

    ``points`` must contain every stored point for the vessel in ``port``,
    including out-of-port exit markers, sorted by ``(event_time, id)``.
    """
    stays: list[Stay] = []
    current: list[Point] = []

    def close(run: list[Point]) -> None:
        if not run:
            return
        if run[-1].event_time - run[0].event_time < MIN_STAY_SECONDS:
            return
        stays.append(
            Stay(
                vessel_id=run[0].vessel_id,
                start_time=run[0].event_time,
                end_time=run[-1].event_time,
                start_lat_e6=run[0].lat_e6,
                start_lon_e6=run[0].lon_e6,
                end_lat_e6=run[-1].lat_e6,
                end_lon_e6=run[-1].lon_e6,
                point_count=len(run),
            )
        )

    for point in points:
        if not contains(point.lat_e6, point.lon_e6):
            # Exit point: break the stay unconditionally.
            close(current)
            current = []
            continue
        if current and point.event_time - current[-1].event_time > MAX_GAP_SECONDS:
            close(current)
            current = []
        current.append(point)
    close(current)
    return stays
