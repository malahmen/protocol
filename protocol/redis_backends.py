"""The queue and the completion stream, on meksha's Redis (Kamino item 69).

    RQJobQueue    JobQueue on RQ: one RQ queue per protocol queue name
    RedisStream   EventStream on the Redis Stream `conversions.events`
    GuardedWorker an RQ SimpleWorker that runs protocol's one job function and
                  refuses everything else
    run_worker    bind a service and work a set of queues

**What makes RQ safe here.** An RQ job names a function by import path and
the worker calls it, so by default anything that can write to Redis can run
any importable callable in a worker - and with RQ's pickle serializer,
arbitrary code outright (Kamino, converter-services: "The API is
load-bearing"). Three things close that, and all three must hold:

  1. Jobs are serialized as JSON, never pickle: a pickled payload does not
     even deserialize.
  2. Every job protocol enqueues names the same function,
     `protocol.worker.run_job`, with one plain dict as its argument - the
     queue carries data, never code (ports.JobQueue).
  3. GuardedWorker checks every job before it runs anything: the function
     must be that one, the arguments one dict, and no success, failure or
     stopped callback may be named - RQ imports and calls a callback by name
     too. A job that fails the check is failed without being run, and
     failing it does not run its callbacks (RQ calls those only from inside
     perform_job, which a refused job never reaches).

Redis itself stays closed to everything but the API and the workers; this is
the second wall, not the first.

Timeouts are RQ's: a job that runs past its `timeout_seconds` gets
JobTimeoutException raised inside the conversion, which the service records
as a failed attempt like any other error. That needs the job on the worker's
main thread, which SimpleWorker gives.

**A worker that dies holding a job** (Kamino item 87) - killed by a restart,
a reboot, the OOM killer - never reaches the service's error handling, and
RQ notices only when the job's work-in-progress entry expires: its timeout
plus a minute, up to 8.8 h for a big marker job, and then it fails the job
without telling protocol. So each worker keeps a key of its own alive,
`protocol:alive:<worker>`, refreshed from a thread every ALIVE_EVERY seconds
for ALIVE_TTL - RQ's own worker key cannot serve, as a SimpleWorker sets it
to the job's timeout and does not touch it again until the job ends. Every
worker, between jobs, sweeps protocol's queues (`sweep`) for a job in progress
whose worker has no such key, and hands it to `worker.recover`: the service
counts the lost attempt and retries or parks it. A worker that comes back up
finds its predecessor's job within about ALIVE_TTL plus SWEEP_EVERY seconds;
any idle worker does, so one busy for hours in its own lane does not hold up
another lane's recovery.

The cost of the key: a worker that cannot reach Redis for ALIVE_TTL seconds
while converting is taken for dead, and its job runs a second time beside it.
The service is written for at-least-once; that is the case it covers.
"""
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from redis import Redis
from rq import Queue, SimpleWorker
from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.registry import ScheduledJobRegistry, StartedJobRegistry
from rq.serializers import JSONSerializer
from rq.utils import parse_composite_key

from . import worker as protocol_worker
from .worker import JOB_FUNC

EVENTS_STREAM = "conversions.events"
DEAD_LETTER_PREFIX = "protocol:dead:"
DEAD_LETTER_MAX = 10000                  # per queue; the oldest are dropped past it
FAILED_TTL = 7 * 24 * 3600               # how long RQ keeps a failed job to look at
ALIVE_PREFIX = "protocol:alive:"
ALIVE_TTL = 60                           # a worker silent this long is dead
ALIVE_EVERY = 15                         # how often a live one says otherwise
SWEEP_EVERY = 60                         # between sweeps, while a worker is idle
SWEEP_CLAIM_PREFIX = "protocol:sweep:"
SWEEP_CLAIM_TTL = 300                    # one sweeper per abandoned job

log = logging.getLogger("protocol.redis_backends")


def connect(url: Optional[str] = None) -> Redis:
    """meksha's Redis. The password comes from REDIS_PASSWORD, never the URL,
    so it needs no URL-encoding and never appears in a logged REDIS_URL -
    the same rule as protocol-droid's workers."""
    return Redis.from_url(url or os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
                          password=os.environ.get("REDIS_PASSWORD") or None)


def _json_job(job: dict) -> dict:
    """The job as JSON will carry it, or a TypeError now rather than in a worker."""
    if not isinstance(job, dict) or "kind" not in job:
        raise TypeError(f"a job is a dict with a kind, got {job!r}")
    return json.loads(json.dumps(job))


class RQJobQueue:
    """ports.JobQueue on RQ. memory.Queue is the reference for its behaviour."""

    def __init__(self, connection: Redis):
        self.connection = connection

    def _queue(self, name: str) -> Queue:
        return Queue(name, connection=self.connection, serializer=JSONSerializer)

    def enqueue(self, queue: str, job: dict, delay_seconds: int = 0,
                timeout_seconds: Optional[int] = None) -> None:
        q = self._queue(queue)
        payload = _json_job(job)
        # result_ttl=0: the service records outcomes itself; RQ need keep none.
        kw = dict(job_timeout=timeout_seconds, result_ttl=0, failure_ttl=FAILED_TTL)
        if delay_seconds > 0:
            q.enqueue_in(timedelta(seconds=delay_seconds), JOB_FUNC, payload, **kw)
        else:
            q.enqueue(JOB_FUNC, payload, **kw)

    def depth(self, queue: str) -> int:
        """Jobs waiting, delayed retries included - as memory.Queue counts them."""
        q = self._queue(queue)
        return q.count + ScheduledJobRegistry(queue=q).count

    def dead_letter(self, queue: str, job: dict, reason: str) -> None:
        key = DEAD_LETTER_PREFIX + queue
        entry = json.dumps({"job": _json_job(job), "reason": reason,
                            "at": datetime.now(timezone.utc).isoformat()})
        pipe = self.connection.pipeline()
        pipe.lpush(key, entry)
        pipe.ltrim(key, 0, DEAD_LETTER_MAX - 1)
        pipe.execute()

    def dead_letters(self, queue: str) -> list[dict]:
        """Newest first. For inspection; the service never reads them back."""
        return [json.loads(e) for e in self.connection.lrange(DEAD_LETTER_PREFIX + queue, 0, -1)]


class RedisStream:
    """ports.EventStream on a Redis Stream.

    Each entry has two fields: `event` (its type, for a consumer that filters)
    and `data` (the whole event as JSON, so None and nesting survive - stream
    fields are flat strings). Trimmed to about `maxlen` entries: it announces
    completions, it is not their record; the registry is.
    """

    def __init__(self, connection: Redis, stream: str = EVENTS_STREAM, maxlen: int = 100000):
        self.connection = connection
        self.stream = stream
        self.maxlen = maxlen

    def append(self, event: dict) -> None:
        self.connection.xadd(self.stream,
                             {"event": str(event.get("event", "")),
                              "data": json.dumps(event, sort_keys=True, default=str)},
                             maxlen=self.maxlen, approximate=True)


class JobRefused(Exception):
    """A job that is not protocol's: run nothing, fail it."""


def check(job) -> None:
    """Raise JobRefused unless this is a job protocol enqueued. Runs nothing."""
    try:
        func_name = job.func_name            # deserializes: a pickled job fails here
        args, kwargs = job.args, job.kwargs
    except Exception as exc:  # noqa: BLE001 - unreadable is refused, whatever the reason
        raise JobRefused(f"cannot read the job ({type(exc).__name__})") from exc
    if func_name != JOB_FUNC:
        raise JobRefused(f"names {func_name!r}, not {JOB_FUNC}")
    if kwargs or len(args) != 1 or not isinstance(args[0], dict):
        raise JobRefused("arguments are not exactly one job dict")
    for which in ("success", "failure", "stopped"):
        if getattr(job, f"_{which}_callback_name", None):
            raise JobRefused(f"names a {which} callback")


class Alive:
    """Keeps `protocol:alive:<name>` set while the worker process lives.

    A thread, so it goes on while the main thread is inside a conversion; a
    daemon, so it dies with the process - which is the point.
    """

    def __init__(self, connection: Redis, name: str):
        self.connection = connection
        self.key = ALIVE_PREFIX + name
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="protocol-alive", daemon=True)

    def _beat(self) -> None:
        self.connection.set(self.key, "1", ex=ALIVE_TTL)

    def _run(self) -> None:
        while not self._stop.wait(ALIVE_EVERY):
            try:
                self._beat()
            except Exception as exc:  # noqa: BLE001 - Redis away for a moment: try again
                log.warning("could not refresh %s: %s", self.key, exc)

    def __enter__(self) -> "Alive":
        self._beat()                 # before the first job can be taken
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        try:
            self.connection.delete(self.key)
        except Exception:  # noqa: BLE001 - it expires anyway
            pass


def sweep(connection: Redis, queues: Iterable[str], recover) -> int:
    """Hand every job in progress on `queues` whose worker is dead to `recover`.

    `recover` takes the job dict (worker.recover, with the service bound).
    The job's RQ record goes only after `recover` returns: if it fails - the
    registry unreachable, say - the job stays where it is for the next sweep.
    A job that is not protocol's is left to RQ. Returns how many were handed on.
    """
    handed = 0
    for name in queues:
        registry = StartedJobRegistry(name, connection=connection, serializer=JSONSerializer)
        for entry in connection.zrange(registry.key, 0, -1):
            entry = entry.decode()
            job_id, _execution_id = parse_composite_key(entry)
            worker = connection.hget(f"rq:execution:{entry}", "worker_name")
            if not worker or connection.exists(ALIVE_PREFIX + worker.decode()):
                continue     # alive - or no record of whose it is, which RQ's expiry handles
            claim = SWEEP_CLAIM_PREFIX + job_id
            if not connection.set(claim, "1", nx=True, ex=SWEEP_CLAIM_TTL):
                continue     # another worker's sweep has it
            try:
                job = Job.fetch(job_id, connection=connection, serializer=JSONSerializer)
                check(job)
                payload = job.args[0]
                recover(payload)
            except (NoSuchJobError, JobRefused) as exc:
                log.warning("left abandoned job %s on %s to RQ: %s", job_id, name, exc)
                continue     # the claim expires; RQ's own expiry fails it
            except Exception:  # noqa: BLE001 - try again next sweep
                log.exception("could not recover abandoned job %s on %s", job_id, name)
                connection.delete(claim)
                continue
            connection.zrem(registry.key, entry)
            job.delete(remove_from_queue=True)
            handed += 1
            log.warning("job %s on %s was abandoned by dead worker %s; recovered %s",
                        job_id, name, worker.decode(), json.dumps(payload, sort_keys=True))
    return handed


class GuardedWorker(SimpleWorker):
    """SimpleWorker - no fork, so marker's models load once - that runs only
    protocol's job function. See the module docstring for why each check.

    Its maintenance, between jobs, also sweeps `sweep_queues` for jobs whose
    worker died (`sweep`)."""

    serializer = JSONSerializer
    sweep_queues: tuple = ()

    def execute_job(self, job, queue):
        try:
            check(job)
        except JobRefused as exc:
            self.log.error("refused job %s: %s", job.id, exc)
            self.handle_job_failure(job, queue, exc_string=f"refused: {exc}")
            return
        super().execute_job(job, queue)

    def run_maintenance_tasks(self):
        super().run_maintenance_tasks()
        try:
            sweep(self.connection, self.sweep_queues,
                  lambda job: protocol_worker.recover(protocol_worker.bound(), job))
        except Exception:  # noqa: BLE001 - a failed sweep must not stop the worker
            self.log.exception("sweep for abandoned jobs failed")


def run_worker(service, queues: Iterable[str], connection: Redis, burst: bool = False,
               sweep_queues: Optional[Iterable[str]] = None) -> bool:
    """Bind `service` and work `queues`, most urgent first (routing.worker_queues).

    The scheduler runs inside the worker, so delayed retries are moved back
    onto their queue when they fall due. Between jobs the worker sweeps
    `sweep_queues` (default: its own) for jobs a dead worker left behind;
    the deployment passes every queue protocol has a worker for.

    worker_ttl: RQ blocks waiting for a job for worker_ttl - 15 seconds and
    runs its maintenance - the sweep - only between waits, so its default
    420 would put up to seven minutes between sweeps.
    """
    protocol_worker.bind(service)
    names = list(queues)
    rq_queues = [Queue(n, connection=connection, serializer=JSONSerializer) for n in names]
    w = GuardedWorker(rq_queues, connection=connection, serializer=JSONSerializer,
                      maintenance_interval=SWEEP_EVERY, worker_ttl=SWEEP_EVERY + 15)
    w.sweep_queues = tuple(sweep_queues if sweep_queues is not None else names)
    with Alive(connection, w.name):
        return w.work(burst=burst, with_scheduler=True)
