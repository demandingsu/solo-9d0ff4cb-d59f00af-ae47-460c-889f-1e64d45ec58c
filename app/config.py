from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Runtime configuration.

    The port-stay rules themselves are deliberately constants (see
    :mod:`app.events`): the operational contract is a 10 minute maximum gap and
    a 30 minute minimum stay.
    """

    db_path: str = "./data/ais.sqlite3"
    demo_enabled: bool = False
    max_upload_bytes: int = 10 * 1024 * 1024
    max_rows_per_upload: int = 100_000
    sqlite_busy_timeout_ms: int = 30_000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.getenv("AIS_DB_PATH", "./data/ais.sqlite3"),
            demo_enabled=os.getenv("AIS_DEMO_ENABLED", "0").lower()
            in {"1", "true", "yes", "on"},
            max_upload_bytes=int(
                os.getenv("AIS_MAX_UPLOAD_BYTES", str(10 * 1024 * 1024))
            ),
            max_rows_per_upload=int(os.getenv("AIS_MAX_ROWS", "100000")),
            sqlite_busy_timeout_ms=int(
                os.getenv("AIS_SQLITE_BUSY_TIMEOUT_MS", "30000")
            ),
        )
