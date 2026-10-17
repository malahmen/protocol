"""In-memory implementations of every port.

For the test suite and the development server - never for a deployment:
nothing here survives a restart, and nothing is shared between processes.
Each one mirrors the guarantee its real backend must give, so a test that
passes here describes behaviour the real adapter has to match.
"""
import copy
import threading
from typing import Optional

from .contract import Conversion, Document


class Registry:
    def __init__(self):
        self._lock = threading.Lock()
        self.documents: dict[str, Document] = {}
        self.conversions: dict[str, Conversion] = {}

    # Copies in and out, as a database would: a caller that changes a record
    # without saving it must not change what is stored.

    def find_live_by_hash(self, content_hash: str) -> Optional[Document]:
        with self._lock:
            for doc in self.documents.values():
                if doc.content_hash == content_hash and doc.live:
                    return copy.deepcopy(doc)
        return None

    def add_document_if_absent(self, doc: Document) -> tuple[Document, bool]:
        # Unique among LIVE documents: a tombstone keeps its hash, and the same
        # content submitted after a removal is a new document. MariaDB has no
        # partial unique index, so mariadb_registry carries this on a generated
        # column (the hash while live, NULL once removal starts), uniquely keyed.
        with self._lock:
            for existing in self.documents.values():
                if existing.content_hash == doc.content_hash and existing.live:
                    return copy.deepcopy(existing), False
            self.documents[doc.document_id] = copy.deepcopy(doc)
            return copy.deepcopy(doc), True

    def get_document(self, document_id: str) -> Optional[Document]:
        with self._lock:
            doc = self.documents.get(document_id)
            return copy.deepcopy(doc) if doc else None

    def save_document(self, doc: Document) -> None:
        with self._lock:
            self.documents[doc.document_id] = copy.deepcopy(doc)

    def add_conversion(self, conv: Conversion) -> None:
        with self._lock:
            self.conversions[conv.conversion_id] = copy.deepcopy(conv)

    def get_conversion(self, conversion_id: str) -> Optional[Conversion]:
        with self._lock:
            conv = self.conversions.get(conversion_id)
            return copy.deepcopy(conv) if conv else None

    def save_conversion(self, conv: Conversion) -> None:
        with self._lock:
            self.conversions[conv.conversion_id] = copy.deepcopy(conv)

    def conversions_of(self, document_id: str) -> list[Conversion]:
        with self._lock:
            found = [copy.deepcopy(cv) for cv in self.conversions.values()
                     if cv.document_id == document_id]
        return sorted(found, key=lambda cv: cv.created_at)


class Store:
    def __init__(self):
        self._lock = threading.Lock()
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, data: bytes) -> None:
        with self._lock:
            self.objects[key] = bytes(data)

    def get(self, key: str) -> bytes:
        with self._lock:
            return self.objects[key]

    def list(self, prefix: str) -> list[str]:
        with self._lock:
            return sorted(k for k in self.objects if k.startswith(prefix))

    def delete(self, key: str) -> None:
        with self._lock:
            self.objects.pop(key, None)

    def delete_prefix(self, prefix: str) -> int:
        with self._lock:
            doomed = [k for k in self.objects if k.startswith(prefix)]
            for key in doomed:
                del self.objects[key]
            return len(doomed)


class Queue:
    """FIFO per queue name. `delay_seconds` and `timeout_seconds` are recorded,
    not acted on."""

    def __init__(self):
        self._lock = threading.Lock()
        self.jobs: dict[str, list[tuple[dict, int, Optional[int]]]] = {}
        self.dead: list[tuple[str, dict, str]] = []

    def enqueue(self, queue: str, job: dict, delay_seconds: int = 0,
                timeout_seconds: Optional[int] = None) -> None:
        with self._lock:
            self.jobs.setdefault(queue, []).append((dict(job), delay_seconds, timeout_seconds))

    def depth(self, queue: str) -> int:
        with self._lock:
            return len(self.jobs.get(queue, []))

    def dead_letter(self, queue: str, job: dict, reason: str) -> None:
        with self._lock:
            self.dead.append((queue, dict(job), reason))

    def pop(self, queue: str) -> Optional[dict]:
        with self._lock:
            pending = self.jobs.get(queue)
            return pending.pop(0)[0] if pending else None

    def pop_any(self) -> Optional[dict]:
        with self._lock:
            for pending in self.jobs.values():
                if pending:
                    return pending.pop(0)[0]
        return None


class Events:
    def __init__(self):
        self._lock = threading.Lock()
        self.events: list[dict] = []

    def append(self, event: dict) -> None:
        with self._lock:
            self.events.append(dict(event))


class Index:
    """Records what the service asked to delete; holds no vectors."""

    def __init__(self):
        self.deleted_documents: list[str] = []
        self.deleted_conversions: list[str] = []

    def delete_document(self, document_id: str) -> int:
        self.deleted_documents.append(document_id)
        return 0

    def delete_conversion(self, conversion_id: str) -> int:
        self.deleted_conversions.append(conversion_id)
        return 0
