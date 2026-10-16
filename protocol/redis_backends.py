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
"""
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from redis import Redis
from rq import Queue, SimpleWorker
from rq.registry import ScheduledJobRegistry
from rq.serializers import JSONSerializer

from . import worker as protocol_worker
from .worker import JOB_FUNC

EVENTS_STREAM = "conversions.events"
DEAD_LETTER_PREFIX = "protocol:dead:"
DEAD_LETTER_MAX = 10000                  # per queue; the oldest are dropped past it
FAILED_TTL = 7 * 24 * 3600               # how long RQ keeps a failed job to look at


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


class GuardedWorker(SimpleWorker):
    """SimpleWorker - no fork, so marker's models load once - that runs only
    protocol's job function. See the module docstring for why each check."""

    serializer = JSONSerializer

    def execute_job(self, job, queue):
        try:
            check(job)
        except JobRefused as exc:
            self.log.error("refused job %s: %s", job.id, exc)
            self.handle_job_failure(job, queue, exc_string=f"refused: {exc}")
            return
        super().execute_job(job, queue)


def run_worker(service, queues: Iterable[str], connection: Redis, burst: bool = False) -> bool:
    """Bind `service` and work `queues`, most urgent first (routing.worker_queues).

    The scheduler runs inside the worker, so delayed retries are moved back
    onto their queue when they fall due.
    """
    protocol_worker.bind(service)
    names = list(queues)
    rq_queues = [Queue(n, connection=connection, serializer=JSONSerializer) for n in names]
    w = GuardedWorker(rq_queues, connection=connection, serializer=JSONSerializer)
    return w.work(burst=burst, with_scheduler=True)
