"""The one function a worker runs per job.

Jobs are plain dicts naming a kind and an ID (see ports.JobQueue). A worker
maps the kind to a handler it already has, so the queue never carries code.

On RQ (protocol/redis_backends.py) every job names the same function,
`run_job` below, with the dict as its one argument, and the worker refuses
any job that names anything else. run_job reaches the service a worker
process `bind`s at startup: RQ calls a function by import path, so the
service cannot travel with the job - and must not.
"""
import logging
from typing import Optional

from . import routing
from .service import ConversionService

# The import path RQ is given for every job. A worker runs nothing else.
JOB_FUNC = "protocol.worker.run_job"

# A job that fails outside the converter - the registry, the store, the index
# or the queue unreachable - is queued again after INFRA_RETRY_BASE seconds,
# doubling, up to INFRA_RETRY_MAX times (30 s ... 16 min, ~32 min in all),
# then dead-lettered with the reason (Kamino item 94). Before, RQ just failed
# it: the worker was alive, so item 87's sweep never saw it, and the
# conversion stayed `converting` (a removal `removal_pending`) for good.
INFRA_RETRY_BASE = 30
INFRA_RETRY_MAX = 6

log = logging.getLogger(__name__)

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
    """RQ's entry point: one job dict, run against the bound service.

    The service records every failure inside a converter itself; what escapes
    here failed around it (see INFRA_RETRY_BASE), and is retried."""
    service = bound()
    if not isinstance(job, dict):
        raise UnknownJob(f"a job is a dict, got {type(job).__name__}")
    try:
        handle(service, job)
    except UnknownJob:
        raise
    except Exception as exc:
        retry_infra(service, job, exc)
        raise


def retry_infra(service: ConversionService, job: dict, exc: BaseException) -> None:
    """Queue `job` again on the queue it came from, later; or dead-letter it."""
    from rq import get_current_job

    current = get_current_job()
    if current is None:
        return                      # not under RQ (tests drive handle directly)
    queue, tries = current.origin, int(job.get("infra_attempts", 0)) + 1
    reason = f"{type(exc).__name__}: {exc}"[:1000]
    if tries > INFRA_RETRY_MAX:
        service.queue.dead_letter(queue, job, f"gave up after {INFRA_RETRY_MAX} retries: {reason}")
        log.error("job %s dead-lettered on %s: %s", job, queue, reason)
        return
    delay = INFRA_RETRY_BASE * 2 ** (tries - 1)
    service.queue.enqueue(queue, dict(job, infra_attempts=tries), delay,
                          timeout_seconds=current.timeout)
    log.warning("job %s failed outside the converter (%s); retry %d of %d in %ds",
                job, reason, tries, INFRA_RETRY_MAX, delay)


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
