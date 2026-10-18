"""The one function a worker runs per job.

Jobs are plain dicts naming a kind and an ID (see ports.JobQueue). A worker
maps the kind to a handler it already has, so the queue never carries code.

On RQ (protocol/redis_backends.py) every job names the same function,
`run_job` below, with the dict as its one argument, and the worker refuses
any job that names anything else. run_job reaches the service a worker
process `bind`s at startup: RQ calls a function by import path, so the
service cannot travel with the job - and must not.
"""
from typing import Optional

from . import routing
from .service import ConversionService

# The import path RQ is given for every job. A worker runs nothing else.
JOB_FUNC = "protocol.worker.run_job"

_service: Optional[ConversionService] = None


def bind(service: ConversionService) -> None:
    """Give this worker process the service its jobs run against."""
    global _service
    _service = service


def bound() -> ConversionService:
    if _service is None:
        raise RuntimeError("no service bound in this worker process (protocol.worker.bind)")
    return _service


def run_job(job: dict) -> None:
    """RQ's entry point: one job dict, run against the bound service."""
    service = bound()
    if not isinstance(job, dict):
        raise UnknownJob(f"a job is a dict, got {type(job).__name__}")
    handle(service, job)


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


def recover(service: ConversionService, job: dict) -> None:
    """A job whose worker died while running it (Kamino item 87).

    A conversion goes back through the service, which counts the lost
    attempt. A removal or a prune is queued again whole: both are written to
    run twice, so finishing a half-done one is running it again.
    """
    kind = job.get("kind")
    if kind == "convert":
        service.recover_conversion(job["conversion_id"])
    elif kind in ("remove", "prune"):
        service.queue.enqueue(routing.MAINTENANCE_QUEUE, job)
    else:
        raise UnknownJob(f"unknown job kind: {kind!r}")


def drain(service: ConversionService, queue) -> int:
    """Run every queued job until none is left (memory queue only). Returns the count."""
    count = 0
    while (job := queue.pop_any()) is not None:
        handle(service, job)
        count += 1
    return count
