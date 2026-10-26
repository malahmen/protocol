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
- A timeout is final: retried, it would only hold its lane as long again.
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
from .ports import (Converter, ConversionTimeout, EventStream, JobQueue, ObjectStore, Registry,
                    VectorIndex)
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


# What new_id makes. Anything else is refused before the registry sees it:
# MariaDB's utf8mb4_bin is a PAD SPACE collation, so "<id> " found the row,
# and the removal then deleted Qdrant points and S3 outputs under the padded
# ID - none - while the source and the record went (Kamino item 93).
_ID = re.compile(r"[0-9a-f]{32}")


def is_id(value) -> bool:
    return isinstance(value, str) and _ID.fullmatch(value) is not None


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
                cost.timeout(est, s.job_timeout_base, s.job_timeout_per_page, s.job_timeout_unknown,
                             s.job_timeout_per_audio_minute))

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

    def _known(self, document_id: str) -> c.Document:
        """The document, or NotFound - also for anything that is not an ID."""
        doc = self.registry.get_document(document_id) if is_id(document_id) else None
        if doc is None:
            raise NotFound(document_id)
        return doc

    def status(self, document_id: str) -> tuple[c.Document, list[c.Conversion]]:
        doc = self._known(document_id)
        return doc, self.registry.conversions_of(doc.document_id)

    def _live(self, document_id: str) -> c.Document:
        doc = self._known(document_id)
        if not doc.live:
            raise Conflict(f"document {doc.document_id} is {doc.state}")
        return doc

    def reconvert(self, document_id: str, mode: str = c.DEFAULT_MODE,
                  converter: str = routing.AUTO, options: Optional[dict] = None) -> c.Conversion:
        doc = self._live(document_id)
        if mode not in c.MODES:
            raise InvalidRequest("mode must be one of " + ", ".join(c.MODES))
        chosen = routing.choose(doc.original_name, converter)
        opts = routing.validate_options(chosen, options, self.settings)
        # Only some converters' cost depends on the source; only they read it here.
        data = self.store.get(doc.source_key) if chosen in cost.READS_SOURCE else None
        queue, timeout = self._plan(chosen, doc.original_name, data, opts)
        self._admit(queue)
        conv = c.Conversion(conversion_id=new_id(), document_id=doc.document_id, mode=mode,
                            converter=chosen, options=opts)
        self.registry.add_conversion(conv)
        self.queue.enqueue(queue, {"kind": "convert", "conversion_id": conv.conversion_id},
                           timeout_seconds=timeout)
        return conv

    def remove(self, document_id: str) -> c.Document:
        """Queue the removal cascade. Asking again while it is pending queues it
        again: a removal stranded by an error could otherwise never be asked
        for (Kamino item 94), and a second run finds it removed and does
        nothing."""
        doc = self._known(document_id)
        if doc.state == c.REMOVED:
            return doc
        if doc.state == c.REMOVAL_PENDING:
            self.queue.enqueue(routing.MAINTENANCE_QUEUE, {"kind": "remove", "document_id": doc.document_id})
            return doc
        for _ in range(5):
            pointer = doc.current_conversion_id
            doc.state = c.REMOVAL_PENDING
            if self.registry.save_live_document(doc, pointer):
                break
            # A conversion finished meanwhile, or another removal started.
            doc = self._known(document_id)
            if not doc.live:
                return self.remove(document_id)
        else:
            raise RuntimeError(f"document {doc.document_id}: its pointer kept moving")
        self.queue.enqueue(routing.MAINTENANCE_QUEUE, {"kind": "remove", "document_id": doc.document_id})
        return doc

    def prune(self, document_id: str, keep_alternatives: bool = False) -> c.Document:
        doc = self._live(document_id)
        self.queue.enqueue(routing.MAINTENANCE_QUEUE, {"kind": "prune", "document_id": doc.document_id,
                                                       "keep_alternatives": bool(keep_alternatives)})
        return doc

    # --- jobs ----------------------------------------------------------------

    def run_conversion(self, conversion_id: str) -> None:
        conv = self.registry.get_conversion(conversion_id)
        if conv is None:
            return
        # Finished by an earlier delivery, which may have stopped before its
        # last step - the document's pointer, the event: finish it again.
        if conv.state == c.VALIDATION_PENDING:
            self._finish(conv.conversion_id)
            return
        if conv.state == c.CONVERSION_FAILED:
            self._settle_failure(conv)
            return
        found = (conv.state, conv.attempts)
        doc = self.registry.get_document(conv.document_id)
        if doc is None or not doc.live:
            self._fail(conv, found, "document removed before conversion")
            return

        prefix = c.output_prefix(doc.document_id, conv.conversion_id)
        conv.state = c.CONVERTING
        conv.attempts += 1
        conv.started_at = c.now()
        conv.error = None
        # This delivery owns the attempt it claims here, and only while the
        # registry still shows it (Kamino item 188): a second delivery of the
        # same job - a retry beside a worker taken for dead - claims the next
        # attempt, and from then on the first one writes nothing.
        if not self.registry.update_conversion_if(conv, *found):
            return
        mine = (c.CONVERTING, conv.attempts)
        if doc.current_conversion_id is None and doc.state != c.CONVERTING:
            doc.state = c.CONVERTING
            self.registry.save_live_document(doc, None)

        data = None
        try:
            converter = self.converters[conv.converter]
            data = self.store.get(doc.source_key)
            result = converter.convert(data, doc.original_name, conv.options)
            if not self._owns(conv.conversion_id, mine):
                return                         # taken over while converting: its output, not ours
            self.store.delete_prefix(prefix)   # at-least-once: clear an earlier attempt
            for name, content in result.files.items():
                self.store.put(prefix + _safe_relative(name), content)
        except Exception as exc:  # noqa: BLE001 - every failure is recorded, then retried or parked
            if not self._owns(conv.conversion_id, mine):
                return
            self.store.delete_prefix(prefix)
            reason = f"{type(exc).__name__}: {exc}"
            if isinstance(exc, ConversionTimeout):
                # Final (Kamino item 79): the allowance is ~3x the measured
                # rate, so a run that used it all was misjudged or stuck, and
                # a retry would hold the lane as long again. A reconversion
                # with a page range, or a larger allowance, is the way back.
                self._fail(conv, mine, reason + " (a timeout is final; not retried)")
            else:
                self._attempt_failed(conv, mine, doc, data, reason)
            return

        conv.tool_versions = dict(result.tool_versions)
        conv.finished_at = c.now()
        self.store.put(prefix + PROVENANCE_FILE, self._provenance(doc, conv, sorted(result.files)))

        # Removed while converting: the output has nowhere to belong.
        doc = self.registry.get_document(doc.document_id)
        if doc is None or not doc.live:
            self.store.delete_prefix(prefix)
            self._fail(conv, mine, "document removed while converting")
            return

        # Decision 13: the mode acts now that the conversion has succeeded.
        # The role it will have and the conversion it takes over from are
        # recorded with its new state, in one write, so _finish can be run
        # again from the registry alone.
        conv.state = c.VALIDATION_PENDING
        conv.output_key = prefix
        current = doc.current_conversion_id
        if current == conv.conversion_id:
            current = None             # already applied: never its own previous (item 188)
        if conv.mode == c.ADD and current is not None:
            conv.role, conv.previous_conversion_id = c.ALTERNATIVE, None
        else:
            conv.role, conv.previous_conversion_id = c.CURRENT, current
        if not self.registry.update_conversion_if(conv, *mine):
            return                     # another delivery took the attempt over: it finishes
        self._finish(conv.conversion_id)

    def _owns(self, conversion_id: str, mine: tuple[str, int]) -> bool:
        """Is the attempt this delivery claimed still the conversion's?"""
        stored = self.registry.get_conversion(conversion_id)
        return stored is not None and (stored.state, stored.attempts) == mine

    def recover_conversion(self, conversion_id: str) -> None:
        """A conversion whose worker died while it held the job (Kamino item 87).

        Called when the queue finds the job abandoned - the worker killed by a
        restart, a reboot or the OOM killer - so run_conversion never reached
        its own error handling. A conversion caught mid-attempt counts that
        attempt as failed and is retried or parked exactly as an error inside
        the converter would be; so a source that kills its worker every time
        is dead-lettered after PROTOCOL_MAX_ATTEMPTS, not retried for ever.
        One the worker had taken but not yet started is queued again as it was.
        One that had finished converting is finished (_finish).
        """
        conv = self.registry.get_conversion(conversion_id)
        if conv is None:
            return
        if conv.state == c.VALIDATION_PENDING:
            self._finish(conv.conversion_id)   # the worker may have died finishing it
            return
        if conv.state == c.CONVERSION_FAILED:
            self._settle_failure(conv)
            return
        found = (conv.state, conv.attempts)
        doc = self.registry.get_document(conv.document_id)
        if doc is None or not doc.live:
            self._fail(conv, found, "document removed before conversion")
            return
        # Only some converters' plan depends on the source (_plan); only they read it.
        data = self.store.get(doc.source_key) if conv.converter in cost.READS_SOURCE else None
        if conv.state != c.CONVERTING:
            queue, timeout = self._plan(conv.converter, doc.original_name, data, conv.options)
            self.queue.enqueue(queue, {"kind": "convert", "conversion_id": conv.conversion_id},
                               timeout_seconds=timeout)
            return
        self.store.delete_prefix(c.output_prefix(doc.document_id, conv.conversion_id))
        self._attempt_failed(conv, found, doc, data, f"worker died during attempt {conv.attempts}")

    def _attempt_failed(self, conv: c.Conversion, mine: tuple[str, int], doc: c.Document,
                        data: Optional[bytes], reason: str) -> None:
        """Retry after a delay that doubles per attempt, or park it once out of attempts.

        `mine`: the state and attempt count the registry must still show."""
        reason = reason[:1000]
        if conv.attempts < self.settings.max_attempts:
            conv.state = c.CONVERSION_PENDING
            conv.error = reason
            if not self.registry.update_conversion_if(conv, *mine):
                return
            delay = self.settings.retry_base_seconds * 2 ** (conv.attempts - 1)
            queue, timeout = self._plan(conv.converter, doc.original_name, data, conv.options)
            self.queue.enqueue(queue, {"kind": "convert", "conversion_id": conv.conversion_id},
                               delay, timeout_seconds=timeout)
        else:
            self._fail(conv, mine, reason)

    def _finish(self, conversion_id: str) -> None:
        """Make a converted conversion's mode take effect, then announce it.

        Idempotent, and run again by every redelivery of a converted job
        (Kamino item 166): a step that failed - the registry or the store
        unreachable for a moment - is retried by the job's retry, and this
        picks up from what the registry shows. So the order matters:

        1. the previous current conversion is marked `superseded` or
           `replaced` (its output still there);
        2. the document points at the new one;
        3. only then is a replaced output deleted;
        4. the event goes out - again on a redelivery: the stream is
           at-least-once, and its consumers dedupe on `conversion_id`.

        A document never points at a deleted output, whichever step fails.
        The event carries the conversion that stopped being current, so the
        ingestion side can ingest the new points and then remove the old
        ones - "ingest new, then remove old", so a search never sees a gap.
        """
        for _ in range(5):
            conv = self.registry.get_conversion(conversion_id)
            doc = self.registry.get_document(conv.document_id)
            if doc is None or not doc.live:
                # Removed meanwhile - perhaps after the cascade deleted the
                # outputs: this one's go too, and the removal is the event.
                self.store.delete_prefix(c.output_prefix(conv.document_id, conv.conversion_id))
                if conv.deleted_at is None:
                    conv.deleted_at = c.now()
                    self.registry.save_conversion(conv)
                return
            if conv.role != c.CURRENT or doc.current_conversion_id == conv.conversion_id:
                break
            if doc.current_conversion_id != conv.previous_conversion_id:
                # Another conversion became current after this one converted:
                # this one, finishing later, takes over from that one.
                conv.previous_conversion_id = doc.current_conversion_id
                self.registry.save_conversion(conv)
            previous = (self.registry.get_conversion(conv.previous_conversion_id)
                        if conv.previous_conversion_id else None)
            if previous is not None and previous.role == c.CURRENT:
                previous.role = c.REPLACED if conv.mode == c.REPLACE else c.SUPERSEDED
                self.registry.save_conversion(previous)
            doc.current_conversion_id = conv.conversion_id
            doc.state = conv.state
            if self.registry.save_live_document(doc, conv.previous_conversion_id):
                break
            # Removed, or another conversion took the pointer first: look again.
        else:
            raise RuntimeError(f"conversion {conversion_id}: the document's pointer kept moving")
        if conv.role not in (c.CURRENT, c.ALTERNATIVE) or conv.deleted_at is not None:
            return            # superseded, replaced or pruned since: a later event said so
        previous_id = conv.previous_conversion_id if conv.role == c.CURRENT else None
        if conv.mode == c.REPLACE and previous_id:
            previous = self.registry.get_conversion(previous_id)
            if previous is not None and previous.deleted_at is None:
                # Only the current output goes; earlier history is untouched.
                self.store.delete_prefix(c.output_prefix(doc.document_id, previous_id))
                previous.role = c.REPLACED
                previous.deleted_at = c.now()
                self.registry.save_conversion(previous)
        self.events.append({"event": c.EVENT_CONVERSION, "document_id": doc.document_id,
                            "conversion_id": conv.conversion_id, "state": conv.state,
                            "output_key": conv.output_key, "mode": conv.mode, "role": conv.role,
                            "previous_conversion_id": previous_id})

    def _fail(self, conv: c.Conversion, found: Optional[tuple[str, int]], reason: str) -> None:
        """Park the conversion as failed - if the registry still shows it as
        `found` (state, attempts): a delivery that lost its attempt to
        another records nothing (Kamino item 188)."""
        conv.state = c.CONVERSION_FAILED
        conv.error = reason
        conv.finished_at = c.now()
        if found is not None and not self.registry.update_conversion_if(conv, *found):
            return
        self._settle_failure(conv)

    def _settle_failure(self, conv: c.Conversion) -> None:
        """What follows a failure: the document's state, the event, the dead
        letter. Run again by a redelivery of a failed job, so a stream or
        queue error here is retried instead of losing them (item 188)."""
        doc = self.registry.get_document(conv.document_id)
        # A failed reconversion leaves the document as it was; only a document
        # with nothing current takes the failure as its own state.
        if (doc is not None and doc.live and doc.current_conversion_id is None
                and doc.state != c.CONVERSION_FAILED):
            doc.state = c.CONVERSION_FAILED
            self.registry.save_live_document(doc, None)
        self.events.append({"event": c.EVENT_CONVERSION, "document_id": conv.document_id,
                            "conversion_id": conv.conversion_id, "state": conv.state,
                            "output_key": None, "mode": conv.mode, "role": None,
                            "previous_conversion_id": None})
        # Last: a redelivery that finds it failed sends the event again (the
        # stream is at-least-once) but a dead letter only if this one was lost.
        if doc is not None and doc.live:
            self.queue.dead_letter(routing.queue_for(conv.converter),
                                   {"kind": "convert", "conversion_id": conv.conversion_id},
                                   conv.error or "")

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
        doc = self.registry.get_document(document_id) if is_id(document_id) else None
        if doc is None or doc.state == c.REMOVED:
            return
        # The record's spelling from here on, never the job's: the cascade
        # deletes by this ID (Kamino item 93).
        document_id = doc.document_id
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
        # Announced before the tombstone is written (Kamino item 188): a
        # stream error leaves the document removal_pending, and the job's
        # retry runs the cascade - written to run twice - and announces again.
        self.events.append({"event": c.EVENT_REMOVED, "document_id": document_id})
        # The tombstone keeps what proves the removal happened - the ID, the
        # content hash and when - and drops the rest, the name included.
        doc.state = c.REMOVED
        doc.removed_at = removed_at
        doc.source_key = None
        doc.original_name = ""
        doc.current_conversion_id = None
        doc.submitted_by = None
        self.registry.save_document(doc)

    def run_prune(self, document_id: str, keep_alternatives: bool = False) -> None:
        doc = self.registry.get_document(document_id) if is_id(document_id) else None
        if doc is None or not doc.live:
            return
        document_id = doc.document_id
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
