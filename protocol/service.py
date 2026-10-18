"""The contract itself: what the API promises, and what the workers do.

Two halves:

- **Requests** (`submit`, `status`, `reconvert`, `remove`, `prune`) - called by
  the API. Each records the request, queues the work and returns at once.
  Nothing here waits on a conversion (decision 10).
- **Jobs** (`run_conversion`, `run_removal`, `run_prune`) - called by workers,
  through `worker.handle`; `recover_conversion` for one whose worker died
  holding it, through `worker.recover`.

The rules from the specification that shape the code:

- Nothing is lost unless asked (11): the source is kept from intake until
  removal; outputs go only through `replace`, a prune, or removal.
- A reconversion's mode acts only once it has succeeded (13); a failure
  touches nothing.
- At-least-once: a job may run twice, so a conversion writes only under its
  own `outputs/<document_id>/<conversion_id>/`, clearing it first.
- Removal runs index -> outputs -> source, so a half-done removal never leaves
  vectors pointing at nothing (12).
"""
import hashlib
import json
import re
import uuid
from typing import Optional

from . import contract as c
from . import cost
from . import routing
from .config import Settings
from .ports import Converter, EventStream, JobQueue, ObjectStore, Registry, VectorIndex
from .routing import InvalidRequest

PROVENANCE_FILE = "provenance.json"
PROVENANCE_SCHEMA = "protocol/provenance/1"
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


class NotFound(LookupError):
    """No such document (404)."""


class Conflict(RuntimeError):
    """The document is removed or being removed (409)."""


class Busy(RuntimeError):
    """The queue is full; try later (429)."""


class TooLarge(ValueError):
    """The source is over the size cap (413)."""


def new_id() -> str:
    return uuid.uuid4().hex


def safe_name(name: str) -> str:
    """The original name, reduced to one harmless path segment."""
    base = _CONTROL.sub("", (name or "").replace("\\", "/").split("/")[-1]).strip()
    if base in ("", ".", ".."):
        base = "source"
    return base[:255]


def _safe_relative(name: str) -> str:
    """A converter's output name, refused if it could escape its folder."""
    parts = name.replace("\\", "/").split("/")
    if not name or name.startswith("/") or any(p in ("", ".", "..") for p in parts):
        raise ValueError(f"converter produced an unsafe output name: {name!r}")
    return "/".join(parts)


class ConversionService:
    def __init__(self, registry: Registry, store: ObjectStore, queue: JobQueue,
                 events: EventStream, index: VectorIndex,
                 converters: dict[str, Converter], settings: Optional[Settings] = None):
        self.registry = registry
        self.store = store
        self.queue = queue
        self.events = events
        self.index = index
        self.converters = converters
        self.settings = settings or Settings()

    # --- requests ------------------------------------------------------------

    def _plan(self, converter: str, name: str, data: Optional[bytes],
              options: dict) -> tuple[str, int]:
        """The queue a conversion waits in and how long it may run (cost.py).

        Worked out from the source and the options alone, so a retry, which
        has both, lands back in the lane its first attempt was given.
        """
        est = cost.estimate(converter, name, data, options)
        s = self.settings
        return (cost.queue(converter, est, s.marker_heavy_seconds),
                cost.timeout(est, s.job_timeout_base, s.job_timeout_per_page, s.job_timeout_unknown))

    def _admit(self, queue: str) -> None:
        """Backpressure: refuse new work for a queue that is already full."""
        if self.queue.depth(queue) >= self.settings.queue_max_depth:
            raise Busy(f"queue {queue} is full")

    def submit(self, data: bytes, original_name: str, media_type: str = "application/octet-stream",
               converter: str = routing.AUTO, options: Optional[dict] = None,
               submitted_by: Optional[str] = None) -> tuple[c.Document, Optional[c.Conversion], bool]:
        """Accept a source. Returns (document, its first conversion, created?).

        The same content again returns the existing live document and no new
        conversion: submission is idempotent, and a reconversion is an explicit
        request, not a re-upload.
        """
        if len(data) > self.settings.max_upload_bytes:
            raise TooLarge(f"source is over {self.settings.max_upload_bytes} bytes")
        name = safe_name(original_name)
        chosen = routing.choose(name, converter)
        opts = routing.validate_options(chosen, options, self.settings)
        content_hash = hashlib.sha256(data).hexdigest()

        existing = self.registry.find_live_by_hash(content_hash)
        if existing:
            return existing, None, False
        queue, timeout = self._plan(chosen, name, data, opts)
        self._admit(queue)

        # The source is stored before the registry row exists, so a row never
        # points at a missing source. Losing the insert race to an identical
        # concurrent upload costs one extra put, cleaned up below.
        document_id = new_id()
        key = c.source_key(document_id, name)
        self.store.put(key, data)
        doc = c.Document(document_id=document_id, content_hash=content_hash, original_name=name,
                         media_type=media_type or "application/octet-stream", size=len(data),
                         source_key=key, submitted_at=c.now(), submitted_by=submitted_by)
        stored, created = self.registry.add_document_if_absent(doc)
        if not created:
            self.store.delete(key)
            return stored, None, False

        conv = c.Conversion(conversion_id=new_id(), document_id=document_id, mode=c.INITIAL,
                            converter=chosen, options=opts)
        self.registry.add_conversion(conv)
        self.queue.enqueue(queue, {"kind": "convert", "conversion_id": conv.conversion_id},
                           timeout_seconds=timeout)
        return stored, conv, True

    def status(self, document_id: str) -> tuple[c.Document, list[c.Conversion]]:
        doc = self.registry.get_document(document_id)
        if doc is None:
            raise NotFound(document_id)
        return doc, self.registry.conversions_of(document_id)

    def _live(self, document_id: str) -> c.Document:
        doc = self.registry.get_document(document_id)
        if doc is None:
            raise NotFound(document_id)
        if not doc.live:
            raise Conflict(f"document {document_id} is {doc.state}")
        return doc

    def reconvert(self, document_id: str, mode: str = c.DEFAULT_MODE,
                  converter: str = routing.AUTO, options: Optional[dict] = None) -> c.Conversion:
        doc = self._live(document_id)
        if mode not in c.MODES:
            raise InvalidRequest("mode must be one of " + ", ".join(c.MODES))
        chosen = routing.choose(doc.original_name, converter)
        opts = routing.validate_options(chosen, options, self.settings)
        # Only marker's cost depends on the source, so only marker reads it here.
        data = self.store.get(doc.source_key) if chosen == "marker" else None
        queue, timeout = self._plan(chosen, doc.original_name, data, opts)
        self._admit(queue)
        conv = c.Conversion(conversion_id=new_id(), document_id=document_id, mode=mode,
                            converter=chosen, options=opts)
        self.registry.add_conversion(conv)
        self.queue.enqueue(queue, {"kind": "convert", "conversion_id": conv.conversion_id},
                           timeout_seconds=timeout)
        return conv

    def remove(self, document_id: str) -> c.Document:
        """Queue the removal cascade. Asking twice is the same request."""
        doc = self.registry.get_document(document_id)
        if doc is None:
            raise NotFound(document_id)
        if doc.state in (c.REMOVAL_PENDING, c.REMOVED):
            return doc
        doc.state = c.REMOVAL_PENDING
        self.registry.save_document(doc)
        self.queue.enqueue(routing.MAINTENANCE_QUEUE, {"kind": "remove", "document_id": document_id})
        return doc

    def prune(self, document_id: str, keep_alternatives: bool = False) -> c.Document:
        doc = self._live(document_id)
        self.queue.enqueue(routing.MAINTENANCE_QUEUE, {"kind": "prune", "document_id": document_id,
                                                       "keep_alternatives": bool(keep_alternatives)})
        return doc

    # --- jobs ----------------------------------------------------------------

    def run_conversion(self, conversion_id: str) -> None:
        conv = self.registry.get_conversion(conversion_id)
        if conv is None or conv.state in (c.VALIDATION_PENDING, c.CONVERSION_FAILED):
            return  # unknown, or already finished by an earlier delivery
        doc = self.registry.get_document(conv.document_id)
        if doc is None or not doc.live:
            self._fail(conv, doc, "document removed before conversion")
            return

        prefix = c.output_prefix(doc.document_id, conv.conversion_id)
        conv.state = c.CONVERTING
        conv.attempts += 1
        conv.started_at = c.now()
        conv.error = None
        self.registry.save_conversion(conv)
        if doc.current_conversion_id is None and doc.state != c.CONVERTING:
            doc.state = c.CONVERTING
            self.registry.save_document(doc)

        data = None
        try:
            converter = self.converters[conv.converter]
            data = self.store.get(doc.source_key)
            result = converter.convert(data, doc.original_name, conv.options)
            self.store.delete_prefix(prefix)   # at-least-once: clear an earlier attempt
            for name, content in result.files.items():
                self.store.put(prefix + _safe_relative(name), content)
        except Exception as exc:  # noqa: BLE001 - every failure is recorded, then retried or parked
            self.store.delete_prefix(prefix)
            self._attempt_failed(conv, doc, data, f"{type(exc).__name__}: {exc}")
            return

        conv.tool_versions = dict(result.tool_versions)
        conv.finished_at = c.now()
        self.store.put(prefix + PROVENANCE_FILE, self._provenance(doc, conv, sorted(result.files)))

        # Removed while converting: the output has nowhere to belong.
        doc = self.registry.get_document(doc.document_id)
        if doc is None or not doc.live:
            self.store.delete_prefix(prefix)
            self._fail(conv, doc, "document removed while converting")
            return

        conv.state = c.VALIDATION_PENDING
        conv.output_key = prefix
        previous = self._apply_mode(doc, conv)
        self.events.append({"event": c.EVENT_CONVERSION, "document_id": doc.document_id,
                            "conversion_id": conv.conversion_id, "state": conv.state,
                            "output_key": conv.output_key, "mode": conv.mode, "role": conv.role,
                            "previous_conversion_id": previous})

    def recover_conversion(self, conversion_id: str) -> None:
        """A conversion whose worker died while it held the job (Kamino item 87).

        Called when the queue finds the job abandoned - the worker killed by a
        restart, a reboot or the OOM killer - so run_conversion never reached
        its own error handling. A conversion caught mid-attempt counts that
        attempt as failed and is retried or parked exactly as an error inside
        the converter would be; so a source that kills its worker every time
        is dead-lettered after PROTOCOL_MAX_ATTEMPTS, not retried for ever.
        One the worker had taken but not yet started is queued again as it was.
        """
        conv = self.registry.get_conversion(conversion_id)
        if conv is None or conv.state in (c.VALIDATION_PENDING, c.CONVERSION_FAILED):
            return  # unknown, or finished before the worker died
        doc = self.registry.get_document(conv.document_id)
        if doc is None or not doc.live:
            self._fail(conv, doc, "document removed before conversion")
            return
        # Only marker's lane depends on the source (_plan), so only marker reads it.
        data = self.store.get(doc.source_key) if conv.converter == "marker" else None
        if conv.state != c.CONVERTING:
            queue, timeout = self._plan(conv.converter, doc.original_name, data, conv.options)
            self.queue.enqueue(queue, {"kind": "convert", "conversion_id": conv.conversion_id},
                               timeout_seconds=timeout)
            return
        self.store.delete_prefix(c.output_prefix(doc.document_id, conv.conversion_id))
        self._attempt_failed(conv, doc, data, f"worker died during attempt {conv.attempts}")

    def _attempt_failed(self, conv: c.Conversion, doc: c.Document, data: Optional[bytes],
                        reason: str) -> None:
        """Retry after a delay that doubles per attempt, or park it once out of attempts."""
        reason = reason[:1000]
        if conv.attempts < self.settings.max_attempts:
            conv.state = c.CONVERSION_PENDING
            conv.error = reason
            self.registry.save_conversion(conv)
            delay = self.settings.retry_base_seconds * 2 ** (conv.attempts - 1)
            queue, timeout = self._plan(conv.converter, doc.original_name, data, conv.options)
            self.queue.enqueue(queue, {"kind": "convert", "conversion_id": conv.conversion_id},
                               delay, timeout_seconds=timeout)
        else:
            self._fail(conv, doc, reason)

    def _apply_mode(self, doc: c.Document, conv: c.Conversion) -> Optional[str]:
        """Decision 13, applied once the new conversion has succeeded.

        Returns the conversion that stopped being current, if any, so the
        ingestion side can ingest the new points and then remove the old ones -
        "ingest new, then remove old", so a search never sees a gap.
        """
        previous = self.registry.get_conversion(doc.current_conversion_id) if doc.current_conversion_id else None
        if conv.mode == c.ADD and previous is not None:
            conv.role = c.ALTERNATIVE
            self.registry.save_conversion(conv)
            return None
        if previous is not None:
            if conv.mode == c.REPLACE:
                # Only the current output goes; earlier history is untouched.
                self.store.delete_prefix(c.output_prefix(doc.document_id, previous.conversion_id))
                previous.role = c.REPLACED
                previous.deleted_at = c.now()
            else:
                previous.role = c.SUPERSEDED
            self.registry.save_conversion(previous)
        conv.role = c.CURRENT
        self.registry.save_conversion(conv)
        doc.current_conversion_id = conv.conversion_id
        doc.state = conv.state
        self.registry.save_document(doc)
        return previous.conversion_id if previous else None

    def _fail(self, conv: c.Conversion, doc: Optional[c.Document], reason: str) -> None:
        conv.state = c.CONVERSION_FAILED
        conv.error = reason
        conv.finished_at = c.now()
        self.registry.save_conversion(conv)
        # A failed reconversion leaves the document as it was; only a document
        # with nothing current takes the failure as its own state.
        if doc is not None and doc.live and doc.current_conversion_id is None:
            doc.state = c.CONVERSION_FAILED
            self.registry.save_document(doc)
        if doc is not None and doc.live:
            self.queue.dead_letter(routing.queue_for(conv.converter),
                                   {"kind": "convert", "conversion_id": conv.conversion_id}, reason)
        self.events.append({"event": c.EVENT_CONVERSION, "document_id": conv.document_id,
                            "conversion_id": conv.conversion_id, "state": conv.state,
                            "output_key": None, "mode": conv.mode, "role": None,
                            "previous_conversion_id": None})

    def _provenance(self, doc: c.Document, conv: c.Conversion, files: list[str]) -> bytes:
        record = {
            "schema": PROVENANCE_SCHEMA,
            "document_id": doc.document_id,
            "conversion_id": conv.conversion_id,
            "source": {"key": doc.source_key, "name": doc.original_name,
                       "sha256": doc.content_hash, "size": doc.size, "media_type": doc.media_type},
            "converter": conv.converter,
            "mode": conv.mode,
            "options": conv.options,
            "tool_versions": conv.tool_versions,
            "attempts": conv.attempts,
            "started_at": conv.started_at.isoformat() if conv.started_at else None,
            "finished_at": conv.finished_at.isoformat() if conv.finished_at else None,
            "files": files,
        }
        return (json.dumps(record, indent=2, sort_keys=True) + "\n").encode()

    def run_removal(self, document_id: str) -> None:
        doc = self.registry.get_document(document_id)
        if doc is None or doc.state == c.REMOVED:
            return
        # Index first, then outputs, then source: a removal that stops halfway
        # leaves files without vectors, never vectors pointing at nothing.
        self.index.delete_document(document_id)
        self.store.delete_prefix(c.output_prefix(document_id))
        if doc.source_key:
            self.store.delete(doc.source_key)
        removed_at = c.now()
        for conv in self.registry.conversions_of(document_id):
            if conv.deleted_at is None:
                conv.deleted_at = removed_at
                self.registry.save_conversion(conv)
        # The tombstone keeps what proves the removal happened - the ID, the
        # content hash and when - and drops the rest, the name included.
        doc.state = c.REMOVED
        doc.removed_at = removed_at
        doc.source_key = None
        doc.original_name = ""
        doc.current_conversion_id = None
        doc.submitted_by = None
        self.registry.save_document(doc)
        self.events.append({"event": c.EVENT_REMOVED, "document_id": document_id})

    def run_prune(self, document_id: str, keep_alternatives: bool = False) -> None:
        doc = self.registry.get_document(document_id)
        if doc is None or not doc.live:
            return
        pruned = []
        for conv in self.registry.conversions_of(document_id):
            if conv.deleted_at is not None or conv.conversion_id == doc.current_conversion_id:
                continue
            if conv.role == c.SUPERSEDED or (conv.role == c.ALTERNATIVE and not keep_alternatives):
                if conv.role == c.ALTERNATIVE:
                    self.index.delete_conversion(conv.conversion_id)
                self.store.delete_prefix(c.output_prefix(document_id, conv.conversion_id))
                conv.role = c.PRUNED
                conv.deleted_at = c.now()
                self.registry.save_conversion(conv)
                pruned.append(conv.conversion_id)
        if pruned:
            self.events.append({"event": c.EVENT_PRUNED, "document_id": document_id,
                                "conversion_ids": pruned})
