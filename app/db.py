"""SQLite schema and connection helpers.

A single writer transaction covers ``ingest_batches``, ``raw_points``,
``events`` and ``event_changes``.  WAL mode lets readers continue to observe
the previous committed state while a batch is being applied, and
``BEGIN IMMEDIATE`` serialises writers so two service instances sharing the
database cannot lose updates.
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Iterator

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS ports (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    code          TEXT    NOT NULL UNIQUE COLLATE NOCASE,
    name          TEXT    NOT NULL,
    min_lat_e6    INTEGER NOT NULL,
    max_lat_e6    INTEGER NOT NULL,
    min_lon_e6    INTEGER NOT NULL,
    max_lon_e6    INTEGER NOT NULL,
    created_at    TEXT    NOT NULL,
    CHECK (min_lat_e6 < max_lat_e6),
    CHECK (min_lon_e6 < max_lon_e6),
    CHECK (min_lat_e6 >= -90000000 AND max_lat_e6 <= 90000000),
    CHECK (min_lon_e6 >= -180000000 AND max_lon_e6 <= 180000000)
);

CREATE TABLE IF NOT EXISTS ingest_batches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    port_id           INTEGER NOT NULL REFERENCES ports(id),
    upload_key        TEXT    NOT NULL,
    content_sha256    TEXT    NOT NULL,
    rows_received     INTEGER NOT NULL,
    rows_new          INTEGER NOT NULL,
    rows_duplicate    INTEGER NOT NULL,
    rows_outside_port INTEGER NOT NULL,
    received_at       TEXT    NOT NULL,
    UNIQUE (port_id, upload_key)
);

CREATE TABLE IF NOT EXISTS raw_points (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    port_id        INTEGER NOT NULL REFERENCES ports(id),
    batch_id       INTEGER NOT NULL REFERENCES ingest_batches(id),
    vessel_id      TEXT    NOT NULL,
    event_time     INTEGER NOT NULL,
    lat_e6         INTEGER NOT NULL,
    lon_e6         INTEGER NOT NULL,
    UNIQUE (port_id, vessel_id, event_time),
    CHECK (lat_e6 >= -90000000 AND lat_e6 <= 90000000),
    CHECK (lon_e6 >= -180000000 AND lon_e6 <= 180000000)
);

CREATE INDEX IF NOT EXISTS idx_raw_points_port_vessel_time
    ON raw_points (port_id, vessel_id, event_time);

CREATE TABLE IF NOT EXISTS events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    port_id           INTEGER NOT NULL REFERENCES ports(id),
    vessel_id         TEXT    NOT NULL,
    start_time        INTEGER NOT NULL,
    end_time          INTEGER NOT NULL,
    start_lat_e6      INTEGER NOT NULL,
    start_lon_e6      INTEGER NOT NULL,
    end_lat_e6        INTEGER NOT NULL,
    end_lon_e6        INTEGER NOT NULL,
    point_count       INTEGER NOT NULL,
    created_batch_id  INTEGER NOT NULL REFERENCES ingest_batches(id),
    updated_batch_id  INTEGER NOT NULL REFERENCES ingest_batches(id),
    CHECK (end_time - start_time >= 1800)
);

CREATE INDEX IF NOT EXISTS idx_events_port_vessel
    ON events (port_id, vessel_id, start_time);
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_port_vessel_start
    ON events (port_id, vessel_id, start_time);
CREATE INDEX IF NOT EXISTS idx_events_vessel
    ON events (vessel_id, start_time);

CREATE TABLE IF NOT EXISTS event_changes (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL REFERENCES ingest_batches(id),
    port_id      INTEGER NOT NULL REFERENCES ports(id),
    change_type  TEXT    NOT NULL CHECK (change_type IN ('upsert', 'retract')),
    event_id     INTEGER NOT NULL,
    vessel_id    TEXT    NOT NULL,
    start_time   INTEGER NOT NULL,
    end_time     INTEGER NOT NULL,
    start_lat_e6 INTEGER,
    start_lon_e6 INTEGER,
    end_lat_e6   INTEGER,
    end_lon_e6   INTEGER,
    point_count  INTEGER,
    created_at   TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_changes_port_seq
    ON event_changes (port_id, seq);

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _configure(conn: sqlite3.Connection, busy_timeout_ms: int) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
    # Durability without the fsync cost on every page for demo workloads while
    # still being crash-safe in WAL mode.
    conn.execute("PRAGMA synchronous = NORMAL")


def connect(db_path: str, *, busy_timeout_ms: int = 30_000) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=busy_timeout_ms / 1000,
        isolation_level=None,  # explicit transactions
        # FastAPI may execute a dependency and a sync endpoint on different
        # worker threads.  Each request owns its connection exclusively, so
        # cross-thread use is safe here; writes are serialised by SQLite.
        check_same_thread=False,
    )
    _configure(conn, busy_timeout_ms)
    return conn


def initialise(db_path: str, *, busy_timeout_ms: int = 30_000) -> None:
    conn = connect(db_path, busy_timeout_ms=busy_timeout_ms)
    try:
        # WAL is a persistent database property.  Set it once at startup rather
        # than on every request connection, where it would need an extra lock.
        # Concurrent container starts can race here; retry briefly rather than
        # failing startup.
        deadline = time.monotonic() + busy_timeout_ms / 1000
        while True:
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                break
            except sqlite3.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        # Apply the whole schema in one write transaction.  ``executescript``
        # would implicitly commit first, so split the idempotent DDL statements
        # and run them one by one.  Concurrent starts serialise on the write
        # lock; IF NOT EXISTS makes the second one a no-op.
        statements = [
            statement.strip()
            for statement in SCHEMA.split(";")
            if statement.strip()
        ]
        with write_transaction(conn):
            for statement in statements:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(SCHEMA_VERSION),),
            )
    finally:
        conn.close()


@contextmanager
def write_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialise writers with BEGIN IMMEDIATE; roll back on any exception.

    ``BEGIN IMMEDIATE`` acquires the write lock up front, so two service
    instances cannot both read-modify-write the event tables without one of them
    waiting.  Any Python-level failure (validation, conflict, crash) rolls the
    whole batch back.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            # The failed statement may already have ended the transaction.
            pass
        raise
    else:
        # A nested/child transaction may already have committed (see
        # app.service._transaction_if_needed); committing again is a no-op.
        if conn.in_transaction:
            conn.execute("COMMIT")


@contextmanager
def read_snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """A consistent read snapshot (WAL lets readers run during a write)."""
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("COMMIT")
