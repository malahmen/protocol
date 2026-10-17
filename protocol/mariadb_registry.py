"""The registry on MariaDB (Kamino item 69; decision 17, To decide 8).

    MariaDBRegistry   ports.Registry on two tables, `documents` and `conversions`
    SCHEMA            their DDL - the fields of contract.Document and
                      contract.Conversion, one column each
    create_schema     apply it (CREATE TABLE IF NOT EXISTS)

memory.Registry is the reference for the behaviour. Where MariaDB has to be
told something the reference gets for free:

- **One live document per content hash.** That is what makes submission
  idempotent, and a removed document keeps its hash in its tombstone, so the
  unique key must ignore removed rows. MariaDB has no partial index; a stored
  generated column `live_hash` - the hash while live, NULL once removal starts
  - carries the unique key, and NULLs never collide in one. Its condition
  must match contract.Document.live. The hash is VARCHAR, not CHAR: a CHAR's
  value depends on the PAD_CHAR_TO_FULL_LENGTH SQL mode, so MariaDB refuses
  a generated column that reads one (error 1901).
- **Insertion order.** `conversions_of` is ordered by `created_at`, and two
  conversions made in the same microsecond keep the order they were added in
  (the reference's sort is stable) through an auto-increment `seq`.
- **Time.** DATETIME(6) in UTC, not TIMESTAMP, which ends in 2038 before
  MariaDB 11.5 (decision 17). Values go in as naive UTC and come out
  UTC-aware, as contract.now() makes them.
- **Copies.** Every read builds a new record, so a caller that changes one
  without saving it changes nothing stored - what the reference's deepcopies
  imitate.

One connection per thread: the API serves requests on a thread pool, and a
PyMySQL connection must not be shared between threads. Each connection is
autocommit, and no method needs a transaction: the insert race is settled by
the unique key, and save_document's look-then-write is on an ID that is never
reused or deleted (a removed document stays, as its tombstone).

| Variable           | Default     | Meaning                                       |
| ------------------ | ----------- | --------------------------------------------- |
| `MARIADB_HOST`     | (required)  | meksha                                        |
| `MARIADB_PORT`     | `3306`      |                                               |
| `MARIADB_DATABASE` | `protocol`  | created by kuat (item 22)                     |
| `MARIADB_USER`     | `protocol`  |                                               |
| `MARIADB_PASSWORD` | (required)  | never in a URL, so never in a logged setting  |
"""
import json
import os
import threading
from datetime import datetime, timezone
from typing import Optional

import pymysql

from . import contract as c

ER_DUP_ENTRY = 1062

# The removal states, as SQL. Must say what contract.Document.live says.
_NOT_LIVE = f"('{c.REMOVAL_PENDING}', '{c.REMOVED}')"

SCHEMA = (
    f"""
    CREATE TABLE IF NOT EXISTS documents (
        document_id           VARCHAR(64)     NOT NULL,
        content_hash          VARCHAR(64)     NOT NULL,
        original_name         VARCHAR(255)    NOT NULL,
        media_type            TEXT            NOT NULL,
        size                  BIGINT UNSIGNED NOT NULL,
        source_key            VARCHAR(1024)   NULL,
        submitted_at          DATETIME(6)     NOT NULL,
        submitted_by          VARCHAR(255)    NULL,
        state                 VARCHAR(32)     NOT NULL,
        current_conversion_id VARCHAR(64)     NULL,
        removed_at            DATETIME(6)     NULL,
        live_hash             VARCHAR(64) AS (IF(state IN {_NOT_LIVE}, NULL, content_hash)) STORED,
        PRIMARY KEY (document_id),
        UNIQUE KEY one_live_document_per_hash (live_hash),
        KEY content_hash (content_hash)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS conversions (
        seq            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        conversion_id  VARCHAR(64)     NOT NULL,
        document_id    VARCHAR(64)     NOT NULL,
        mode           VARCHAR(16)     NOT NULL,
        converter      VARCHAR(64)     NOT NULL,
        options        JSON            NOT NULL,
        state          VARCHAR(32)     NOT NULL,
        role           VARCHAR(16)     NULL,
        output_key     VARCHAR(1024)   NULL,
        tool_versions  JSON            NOT NULL,
        attempts       INT UNSIGNED    NOT NULL,
        created_at     DATETIME(6)     NOT NULL,
        started_at     DATETIME(6)     NULL,
        finished_at    DATETIME(6)     NULL,
        error          TEXT            NULL,
        deleted_at     DATETIME(6)     NULL,
        PRIMARY KEY (conversion_id),
        UNIQUE KEY seq (seq),
        KEY document_created (document_id, created_at, seq),
        CONSTRAINT conversion_of_a_document FOREIGN KEY (document_id)
            REFERENCES documents (document_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
)

_DOC_FIELDS = ("document_id", "content_hash", "original_name", "media_type", "size", "source_key",
               "submitted_at", "submitted_by", "state", "current_conversion_id", "removed_at")
_CONV_FIELDS = ("conversion_id", "document_id", "mode", "converter", "options", "state", "role",
                "output_key", "tool_versions", "attempts", "created_at", "started_at",
                "finished_at", "error", "deleted_at")
_JSON_FIELDS = ("options", "tool_versions")


def _to_db(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return value


def _from_db(name: str, value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc)
    if name in _JSON_FIELDS:
        return json.loads(value) if value else {}
    return value


def _row(record, fields) -> tuple:
    return tuple(_to_db(getattr(record, f)) for f in fields)


def _upsert(table: str, fields) -> str:
    cols = ", ".join(fields)
    marks = ", ".join(["%s"] * len(fields))
    updates = ", ".join(f"{f} = VALUES({f})" for f in fields[1:])
    return f"INSERT INTO {table} ({cols}) VALUES ({marks}) ON DUPLICATE KEY UPDATE {updates}"


_DOC_COLS = ", ".join(_DOC_FIELDS)
_CONV_COLS = ", ".join(_CONV_FIELDS)
_INSERT_DOC = f"INSERT INTO documents ({_DOC_COLS}) VALUES ({', '.join(['%s'] * len(_DOC_FIELDS))})"
_UPDATE_DOC = (f"UPDATE documents SET {', '.join(f'{f} = %s' for f in _DOC_FIELDS[1:])} "
               "WHERE document_id = %s")
# Safe as an upsert: a conversion's only unique keys are its ID and `seq`,
# which an insert never names. A document's is not - see save_document.
_SAVE_CONV = _upsert("conversions", _CONV_FIELDS)


def connect(host: Optional[str] = None, port: Optional[int] = None, user: Optional[str] = None,
            password: Optional[str] = None, database: Optional[str] = None) -> pymysql.Connection:
    """One connection, from the arguments or the environment."""
    def env(name, default=None):
        value = os.environ.get(name, "").strip() or default
        if value is None:
            raise SystemExit(f"{name} is not set")
        return value
    return pymysql.connect(
        host=host or env("MARIADB_HOST"),
        port=int(port or env("MARIADB_PORT", "3306")),
        user=user or env("MARIADB_USER", "protocol"),
        password=password if password is not None else env("MARIADB_PASSWORD"),
        database=database or env("MARIADB_DATABASE", "protocol"),
        charset="utf8mb4",
        autocommit=True,
        # Belt and braces: DATETIME is not converted by the session time zone,
        # but NOW() and anything an operator runs by hand is.
        init_command="SET time_zone = '+00:00'",
    )


def create_schema(conn: pymysql.Connection) -> None:
    with conn.cursor() as cur:
        for statement in SCHEMA:
            cur.execute(statement)


class MariaDBRegistry:
    """ports.Registry. memory.Registry is the reference."""

    def __init__(self, connect_fn=connect):
        """`connect_fn` makes one connection; it is called once per thread."""
        self._connect = connect_fn
        self._local = threading.local()

    def _cursor(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.ping()
            except pymysql.err.Error:
                # Closed by the server (idle past wait_timeout, a restart):
                # a new one, as PyMySQL asks, rather than ping(reconnect=True).
                conn = None
        if conn is None:
            conn = self._local.conn = self._connect()
        return conn.cursor()

    def _one(self, sql: str, args, fields, cls):
        with self._cursor() as cur:
            cur.execute(sql, args)
            row = cur.fetchone()
        return cls(**{f: _from_db(f, v) for f, v in zip(fields, row)}) if row else None

    # --- documents -----------------------------------------------------------

    def find_live_by_hash(self, content_hash: str) -> Optional[c.Document]:
        return self._live_by_hash(content_hash)

    def _live_by_hash(self, content_hash: str) -> Optional[c.Document]:
        # add_document_if_absent reads through this, not the public method,
        # so it settles the race on what the table says whatever wraps that.
        return self._one(f"SELECT {_DOC_COLS} FROM documents WHERE live_hash = %s",
                         (content_hash,), _DOC_FIELDS, c.Document)

    def add_document_if_absent(self, doc: c.Document) -> tuple[c.Document, bool]:
        """The idempotency point: the unique key on `live_hash` decides the race.

        A duplicate on it means a live document with this content exists, and
        that one is returned. If it has gone out of the live set by the time
        it is read back (removed in between), the insert is tried again.
        """
        for _ in range(3):
            try:
                with self._cursor() as cur:
                    cur.execute(_INSERT_DOC, _row(doc, _DOC_FIELDS))
                return self.get_document(doc.document_id), True
            except pymysql.err.IntegrityError as exc:
                if exc.args[0] != ER_DUP_ENTRY:
                    raise
                existing = self._live_by_hash(doc.content_hash)
                if existing is not None:
                    return existing, False
                if self.get_document(doc.document_id) is not None:
                    raise        # the duplicate was the ID itself, not the content
        raise RuntimeError(f"could not settle the insert of content {doc.content_hash[:12]}")

    def get_document(self, document_id: str) -> Optional[c.Document]:
        return self._one(f"SELECT {_DOC_COLS} FROM documents WHERE document_id = %s",
                         (document_id,), _DOC_FIELDS, c.Document)

    def save_document(self, doc: c.Document) -> None:
        """An update by ID - never INSERT ... ON DUPLICATE KEY UPDATE, which
        would also fire on the content hash's unique key and overwrite a
        DIFFERENT document's row. Saving one that was never added inserts it,
        as the reference does."""
        values = _row(doc, _DOC_FIELDS)
        with self._cursor() as cur:
            cur.execute("SELECT 1 FROM documents WHERE document_id = %s", (doc.document_id,))
            if cur.fetchone():
                cur.execute(_UPDATE_DOC, values[1:] + values[:1])
            else:
                cur.execute(_INSERT_DOC, values)

    # --- conversions ---------------------------------------------------------

    def add_conversion(self, conv: c.Conversion) -> None:
        self.save_conversion(conv)      # the reference overwrites too

    def get_conversion(self, conversion_id: str) -> Optional[c.Conversion]:
        return self._one(f"SELECT {_CONV_COLS} FROM conversions WHERE conversion_id = %s",
                         (conversion_id,), _CONV_FIELDS, c.Conversion)

    def save_conversion(self, conv: c.Conversion) -> None:
        with self._cursor() as cur:
            cur.execute(_SAVE_CONV, _row(conv, _CONV_FIELDS))

    def conversions_of(self, document_id: str) -> list[c.Conversion]:
        with self._cursor() as cur:
            cur.execute(f"SELECT {_CONV_COLS} FROM conversions WHERE document_id = %s "
                        "ORDER BY created_at, seq", (document_id,))
            rows = cur.fetchall()
        return [c.Conversion(**{f: _from_db(f, v) for f, v in zip(_CONV_FIELDS, row)}) for row in rows]
