"""The one function a worker runs per job.

Jobs are plain dicts naming a kind and an ID (see ports.JobQueue). A worker
maps the kind to a handler it already has, so the queue never carries code.
The RQ wiring - one worker per converter queue, `SimpleWorker` for marker so
its models load once, retries and heartbeats - lands with TODO items 21-23.
"""
from .service import ConversionService


class UnknownJob(ValueError):
    pass


def handle(service: ConversionService, job: dict) -> None:
    kind = job.get("kind")
    if kind == "convert":
        service.run_conversion(job["conversion_id"])
    elif kind == "remove":
        service.run_removal(job["document_id"])
    elif kind == "prune":
        service.run_prune(job["document_id"], bool(job.get("keep_alternatives")))
    else:
        raise UnknownJob(f"unknown job kind: {kind!r}")


def drain(service: ConversionService, queue) -> int:
    """Run every queued job until none is left (memory queue only). Returns the count."""
    count = 0
    while (job := queue.pop_any()) is not None:
        handle(service, job)
        count += 1
    return count
