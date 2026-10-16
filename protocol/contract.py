"""The non-blocking conversion contract: its vocabulary and its records.

Everything here comes from Kamino's `converter-services-2026-10-10.md`
("The non-blocking contract"), which is the specification; this module only
gives it names. States reuse the conversion leg of `pipeline_spec.md` §4
rather than inventing a second vocabulary.

The record fields ARE the registry schema (To decide 8, decided 2026-10-16:
the MariaDB DDL follows these fields). One deliberate deviation: `marker_version` and `llm_service`
became `tool_versions` and `options`, because the API serves more than one
converter and each pins different packages.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# --- document and conversion states ------------------------------------------

CONVERSION_PENDING = "conversion_pending"
CONVERTING = "converting"
CONVERSION_FAILED = "conversion_failed"
VALIDATION_PENDING = "validation_pending"

# Removal (To decide 9, decided 2026-10-16 as implemented here; decision 12
# names the two states). Entered from any other state; see service.remove.
REMOVAL_PENDING = "removal_pending"
REMOVED = "removed"

# --- reconversion modes (decision 13) ----------------------------------------

INITIAL = "initial"          # the first conversion of a document; not requestable
ADD = "add"
UPDATE = "update"
REPLACE = "replace"
MODES = (ADD, UPDATE, REPLACE)
DEFAULT_MODE = UPDATE

# --- conversion roles ---------------------------------------------------------
#
# A conversion has no role until it has succeeded: a pending or failed one is
# not history, it is nothing yet.

CURRENT = "current"
SUPERSEDED = "superseded"     # update: kept as history, not served
ALTERNATIVE = "alternative"   # add: kept beside the current one
REPLACED = "replaced"         # replace: the previous current, deleted
PRUNED = "pruned"             # deleted by a history prune; the row survives

# --- completion stream events --------------------------------------------------

EVENT_CONVERSION = "conversion"
EVENT_REMOVED = "removed"
EVENT_PRUNED = "pruned"


def now() -> datetime:
    """UTC, always. The registry stores DATETIME in UTC (decision 17)."""
    return datetime.now(timezone.utc)


@dataclass
class Document:
    document_id: str
    content_hash: str               # sha256 of the source bytes
    original_name: str
    media_type: str
    size: int
    source_key: Optional[str]       # sources/<document_id>/<original name>
    submitted_at: datetime
    submitted_by: Optional[str] = None
    state: str = CONVERSION_PENDING
    current_conversion_id: Optional[str] = None
    removed_at: Optional[datetime] = None

    @property
    def live(self) -> bool:
        return self.state not in (REMOVAL_PENDING, REMOVED)


@dataclass
class Conversion:
    conversion_id: str
    document_id: str
    mode: str
    converter: str
    options: dict = field(default_factory=dict)
    state: str = CONVERSION_PENDING
    role: Optional[str] = None
    output_key: Optional[str] = None    # outputs/<document_id>/<conversion_id>/
    tool_versions: dict = field(default_factory=dict)
    attempts: int = 0
    created_at: datetime = field(default_factory=now)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None         # for operators; never echoed to callers
    deleted_at: Optional[datetime] = None


def source_key(document_id: str, name: str) -> str:
    return f"sources/{document_id}/{name}"


def output_prefix(document_id: str, conversion_id: str = "") -> str:
    """outputs/<document_id>/ or outputs/<document_id>/<conversion_id>/"""
    if conversion_id:
        return f"outputs/{document_id}/{conversion_id}/"
    return f"outputs/{document_id}/"
