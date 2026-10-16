"""What the service needs from the outside world, and nothing more.

Each interface is the narrow slice of a real backend the contract uses:

| Port         | Real backend                                 | Adapter                         |
| ------------ | -------------------------------------------- | ------------------------------- |
| Registry     | MariaDB on meksha (decision 17)              | to come                         |
| ObjectStore  | Garage on taris: `sources`, `outputs`        | to come                         |
| JobQueue     | RQ on meksha's Redis, one queue per converter| redis_backends.RQJobQueue       |
| EventStream  | Redis Stream `conversions.events`            | redis_backends.RedisStream      |
| VectorIndex  | Qdrant, points filtered by document_id       | to come                         |
| Converter    | marker, markitdown, Speaches (transcription) | converters.EngineConverter (marker, markitdown) |

memory.py has every port, for the tests and the development server. The
remaining adapters are Kamino `todo.md` item 69.
Keeping the contract behind these ports is what lets it be written and tested
before any of them exists.
"""
from dataclasses import dataclass, field
from typing import Optional, Protocol

from .contract import Conversion, Document


class Registry(Protocol):
    def find_live_by_hash(self, content_hash: str) -> Optional[Document]: ...

    def add_document_if_absent(self, doc: Document) -> tuple[Document, bool]:
        """Insert unless a live document has the same content hash.

        Returns (the stored document, created?). This is the idempotency
        point - INSERT ... ON DUPLICATE KEY in MariaDB - so two concurrent
        submissions of one file end up as one document.
        """
        ...

    def get_document(self, document_id: str) -> Optional[Document]: ...
    def save_document(self, doc: Document) -> None: ...
    def add_conversion(self, conv: Conversion) -> None: ...
    def get_conversion(self, conversion_id: str) -> Optional[Conversion]: ...
    def save_conversion(self, conv: Conversion) -> None: ...
    def conversions_of(self, document_id: str) -> list[Conversion]: ...


class ObjectStore(Protocol):
    """Keys carry their store as the first segment: sources/... or outputs/..."""

    def put(self, key: str, data: bytes) -> None: ...
    def get(self, key: str) -> bytes: ...
    def list(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...
    def delete_prefix(self, prefix: str) -> int: ...


class JobQueue(Protocol):
    """Jobs are plain JSON-able dicts, never pickled callables.

    RQ pickles the callable it is given, so anything that can write to Redis
    can run code in a worker (Kamino: "The API is load-bearing"). Jobs here
    name a kind and an ID; the worker maps the kind to a handler it already has.
    """

    def enqueue(self, queue: str, job: dict, delay_seconds: int = 0,
                timeout_seconds: Optional[int] = None) -> None:
        """`timeout_seconds`: how long the job may run once started; None = the
        backend's default. Sized per job from its estimated cost (cost.py)."""
        ...
    def depth(self, queue: str) -> int: ...
    def dead_letter(self, queue: str, job: dict, reason: str) -> None: ...


class EventStream(Protocol):
    def append(self, event: dict) -> None: ...


class VectorIndex(Protocol):
    def delete_document(self, document_id: str) -> int: ...
    def delete_conversion(self, conversion_id: str) -> int: ...


@dataclass
class ConverterOutput:
    files: dict[str, bytes]                  # relative name -> content
    tool_versions: dict[str, str] = field(default_factory=dict)


class Converter(Protocol):
    name: str

    def convert(self, data: bytes, original_name: str, options: dict) -> ConverterOutput: ...
