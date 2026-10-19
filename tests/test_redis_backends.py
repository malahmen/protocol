"""protocol/redis_backends.py against a real Redis: the queue, the stream, the
guarded worker, and protocol's service running through them.

Needs rq and redis (requirements.txt) and a Redis to point at:

    PROTOCOL_TEST_REDIS_URL=redis://localhost:16379/15 python3 -m unittest discover -s tests

**The suite flushes that database.** There is no default URL on purpose, so
it can never reach a Redis that holds anything; without the variable these
checks skip. CI starts a throwaway Redis 7 for them (meksha runs `redis:7`).
"""
import http.server
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
import zlib
from unittest import mock

# RQ imports a callback by name, as "test_redis_backends.canary": this folder
# must be importable however the suite was started, or a callback that runs
# is not seen.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import helpers
from protocol import routing
from protocol.config import Settings

try:
    from rq import Callback, Queue
    from rq.webhook import Webhook
    from rq.job import Job
    from rq.registry import FailedJobRegistry, ScheduledJobRegistry, StartedJobRegistry
    from rq.serializers import JSONSerializer

    from protocol import redis_backends as rb
    HAVE_RQ = True
except ImportError:  # pragma: no cover - the stdlib-only run
    HAVE_RQ = False

URL = os.environ.get("PROTOCOL_TEST_REDIS_URL", "")
# A file, not a list: the module RQ imports by name may be a second copy of
# this one, with a list of its own - a callback that ran would go unseen.
CANARY = os.path.join(tempfile.gettempdir(), f"protocol-canary-{os.getpid()}")


def canary(*args, **kwargs):
    """Stands in for an attacker's callable: if a refused job reaches it, the file says so."""
    with open(CANARY, "a") as fh:
        fh.write(repr(args) + "\n")


def canary_calls():
    try:
        with open(CANARY) as fh:
            return fh.read().splitlines()
    except FileNotFoundError:
        return []


def reachable():
    if not (HAVE_RQ and URL):
        return False
    try:
        return rb.connect(URL).ping()
    except Exception:  # noqa: BLE001
        return False


@unittest.skipUnless(reachable(), "set PROTOCOL_TEST_REDIS_URL to a throwaway Redis (rq and redis installed)")
class RedisCase(unittest.TestCase):
    def setUp(self):
        self.conn = rb.connect(URL)
        self.conn.flushdb()
        self.queue = rb.RQJobQueue(self.conn)
        if os.path.exists(CANARY):
            os.remove(CANARY)
        self.addCleanup(lambda: os.path.exists(CANARY) and os.remove(CANARY))

    def build(self, settings=None, **converters):
        service, parts, convs = helpers.build(settings, **converters)
        service.queue = self.queue
        service.events = rb.RedisStream(self.conn)
        return service, parts, convs

    def work(self, service, queues):
        rb.run_worker(service, queues, self.conn, burst=True)


class Queueing(RedisCase):
    def test_a_job_is_stored_as_json_naming_the_one_function(self):
        self.queue.enqueue("convert.markitdown", {"kind": "convert", "conversion_id": "c1"})
        (job_id,) = Queue("convert.markitdown", connection=self.conn).job_ids
        raw = self.conn.hget(f"rq:job:{job_id}", "data")
        func, _instance, args, kwargs = json.loads(zlib.decompress(raw))   # JSON, not pickle
        self.assertEqual(func, rb.JOB_FUNC)
        self.assertEqual((args, kwargs), ([{"kind": "convert", "conversion_id": "c1"}], {}))

    def test_the_timeout_travels_with_the_job(self):
        self.queue.enqueue("convert.marker.heavy", {"kind": "convert", "conversion_id": "c1"},
                           timeout_seconds=31680)
        (job,) = Queue("convert.marker.heavy", connection=self.conn,
                       serializer=JSONSerializer).jobs
        self.assertEqual(job.timeout, 31680)

    def test_depth_counts_delayed_retries_like_the_reference(self):
        self.queue.enqueue("convert.markitdown", {"kind": "convert", "conversion_id": "a"})
        self.queue.enqueue("convert.markitdown", {"kind": "convert", "conversion_id": "b"})
        self.queue.enqueue("convert.markitdown", {"kind": "convert", "conversion_id": "c"}, 30)
        self.assertEqual(self.queue.depth("convert.markitdown"), 3)
        self.assertEqual(self.queue.depth("convert.marker.light"), 0)

    def test_a_job_that_is_not_json_is_refused_at_the_door(self):
        with self.assertRaises(TypeError):
            self.queue.enqueue("convert.markitdown", {"kind": "convert", "x": object()})
        with self.assertRaises(TypeError):
            self.queue.enqueue("convert.markitdown", {"no": "kind"})

    def test_dead_letters_are_kept_newest_first_and_capped(self):
        with mock.patch.object(rb, "DEAD_LETTER_MAX", 2):
            for i in range(3):
                self.queue.dead_letter("convert.marker", {"kind": "convert", "conversion_id": f"c{i}"}, f"r{i}")
        letters = self.queue.dead_letters("convert.marker")
        self.assertEqual([d["job"]["conversion_id"] for d in letters], ["c2", "c1"])
        self.assertEqual(letters[0]["reason"], "r2")


class Stream(RedisCase):
    def test_an_event_survives_the_stream_whole(self):
        event = {"event": "conversion", "document_id": "d1", "conversion_id": "c1",
                 "state": "validation_pending", "output_key": None, "role": None}
        rb.RedisStream(self.conn).append(event)
        ((_id, fields),) = self.conn.xrange(rb.EVENTS_STREAM)
        self.assertEqual(fields[b"event"], b"conversion")
        self.assertEqual(json.loads(fields[b"data"]), event)


class ThroughTheService(RedisCase):
    def test_submit_queue_work_announce(self):
        service, parts, _ = self.build()
        doc, conv, _ = service.submit(b"PK\x03\x04", "notes.docx")
        self.assertEqual(self.queue.depth("convert.markitdown"), 1)
        self.work(service, routing.worker_queues("markitdown"))
        conv = parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual(conv.state, "validation_pending")
        self.assertEqual(self.queue.depth("convert.markitdown"), 0)
        ((_id, fields),) = self.conn.xrange(rb.EVENTS_STREAM)
        self.assertEqual(json.loads(fields[b"data"])["conversion_id"], conv.conversion_id)

    def _no_delay(self):
        # The infra retry's delay, and the service's own: 0, so a burst worker
        # reaches the requeued job.
        from protocol import worker as protocol_worker
        patcher = mock.patch.object(protocol_worker, "INFRA_RETRY_BASE", 0, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        return Settings(retry_base_seconds=0)

    def test_a_store_blip_outside_the_converter_is_retried_not_stranded(self):
        # Item 94: the converter fails, then Garage blips while its partial
        # output is cleared - outside the converter's try. RQ failed the job,
        # the worker was alive, and the conversion stayed `converting` for good.
        service, parts, _ = self.build(self._no_delay(),
                                       markitdown=helpers.FakeConverter("markitdown", fail_times=1))
        store, real, calls = parts["store"], parts["store"].delete_prefix, []

        def blip(prefix):
            calls.append(prefix)
            if len(calls) == 1:
                raise ConnectionError("garage blip")
            return real(prefix)
        store.delete_prefix = blip
        doc, conv, _ = service.submit(b"PK", "notes.docx")
        self.work(service, routing.worker_queues("markitdown"))
        conv = parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual(conv.state, "validation_pending")

    def test_a_removal_blip_is_retried(self):
        service, parts, _ = self.build(self._no_delay())
        doc, _, _ = service.submit(b"PK", "notes.docx")
        self.work(service, routing.worker_queues("markitdown"))
        index, real, calls = parts["index"], parts["index"].delete_document, []

        def blip(document_id):
            calls.append(document_id)
            if len(calls) == 1:
                raise ConnectionError("qdrant blip")
            return real(document_id)
        index.delete_document = blip
        service.remove(doc.document_id)
        self.work(service, [routing.MAINTENANCE_QUEUE])
        self.assertEqual(service.status(doc.document_id)[0].state, "removed")

    def test_a_job_that_fails_outside_the_converter_every_time_is_dead_lettered(self):
        service, parts, _ = self.build(self._no_delay())
        doc, conv, _ = service.submit(b"PK", "notes.docx")
        parts["registry"].save_conversion = mock.Mock(side_effect=ConnectionError("mariadb down"))
        self.work(service, routing.worker_queues("markitdown"))
        (dead,) = self.queue.dead_letters("convert.markitdown")
        self.assertIn("gave up after 6 retries: ConnectionError: mariadb down", dead["reason"])
        self.assertEqual(self.queue.depth("convert.markitdown"), 0)

    def test_a_marker_worker_takes_the_light_lane_first(self):
        service, _, convs = self.build()
        light, heavy = routing.MARKER_LIGHT_QUEUE, routing.MARKER_HEAVY_QUEUE
        # Heavy queued first, light second: the light job must still go first.
        for name, lane in (("book.pdf", heavy), ("note.pdf", light)):
            with mock.patch.object(service, "_plan", lambda *a, lane=lane: (lane, 600)):
                service.submit(name.encode(), name)
        self.work(service, routing.worker_queues("marker"))
        self.assertEqual([call[1] for call in convs["marker"].calls], ["note.pdf", "book.pdf"])

    def test_a_timed_out_conversion_fails_at_once_and_is_not_retried(self):
        slow = helpers.FakeConverter("markitdown", on_convert=lambda: time.sleep(5))
        service, parts, _ = self.build(Settings(job_timeout_unknown=1), markitdown=slow)
        doc, conv, _ = service.submit(b"PK", "slow.docx")
        started = time.monotonic()
        self.work(service, routing.worker_queues("markitdown"))
        self.assertLess(time.monotonic() - started, 4)          # stopped, not waited out
        conv = parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual((conv.state, conv.attempts), ("conversion_failed", 1))
        self.assertIn("JobTimedOut", conv.error)
        self.assertIn("not retried", conv.error)
        self.assertEqual(self.queue.depth("convert.markitdown"), 0)
        self.assertEqual(len(self.queue.dead_letters("convert.markitdown")), 1)


class Died(BaseException):
    """A killed worker, from inside: nothing after it runs - not RQ's failure
    handling, not the service's."""


def die():
    raise Died()


class Abandoned(RedisCase):
    """A worker killed holding a job (Kamino item 87): another worker's sweep
    finds the job and the service retries it."""

    def take_and_die(self, service, queue_name):
        """A GuardedWorker takes the next job, starts it, and is killed."""
        q = Queue(queue_name, connection=self.conn, serializer=JSONSerializer)
        w = rb.GuardedWorker([q], connection=self.conn, serializer=JSONSerializer)
        rb.protocol_worker.bind(service)
        job, queue = w.dequeue_job_and_maintain_ttl(timeout=1)
        w.prepare_execution(job)
        w.prepare_job_execution(job)
        with self.assertRaises(Died):
            job.func(*job.args)
        return w, job

    def started(self, queue_name):
        return StartedJobRegistry(queue_name, connection=self.conn, serializer=JSONSerializer)

    def sweep(self, service, queues=(routing.QUEUES["markitdown"],)):
        rb.protocol_worker.bind(service)
        return rb.sweep(self.conn, queues,
                        lambda job: rb.protocol_worker.recover(service, job))

    def test_a_dead_workers_conversion_is_found_and_retried(self):
        lane = routing.QUEUES["markitdown"]
        service, parts, _ = self.build(markitdown=helpers.FakeConverter("markitdown", on_convert=die))
        doc, conv, _ = service.submit(b"PK", "notes.docx")
        _w, job = self.take_and_die(service, lane)
        self.assertEqual(self.started(lane).count, 1)
        self.assertEqual(self.sweep(service), 1)
        conv = parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual((conv.state, conv.error), ("conversion_pending", "worker died during attempt 1"))
        self.assertEqual(self.started(lane).count, 0)
        self.assertFalse(Job.exists(job.id, connection=self.conn))
        self.assertEqual(self.queue.depth(lane), 1)                 # the retry, delayed
        self.assertEqual(self.sweep(service), 0)                    # and only once

    def test_a_live_workers_job_is_left_alone(self):
        lane = routing.QUEUES["markitdown"]
        service, parts, _ = self.build(markitdown=helpers.FakeConverter("markitdown", on_convert=die))
        doc, conv, _ = service.submit(b"PK", "notes.docx")
        w, _job = self.take_and_die(service, lane)
        with rb.Alive(self.conn, w.name):
            self.assertEqual(self.sweep(service), 0)
        self.assertEqual(self.started(lane).count, 1)
        self.assertEqual(parts["registry"].get_conversion(conv.conversion_id).state, "converting")
        self.assertFalse(self.conn.exists(rb.ALIVE_PREFIX + w.name))   # gone with its worker

    def test_a_failed_recovery_leaves_the_job_for_the_next_sweep(self):
        lane = routing.QUEUES["markitdown"]
        service, parts, _ = self.build(markitdown=helpers.FakeConverter("markitdown", on_convert=die))
        doc, conv, _ = service.submit(b"PK", "notes.docx")
        self.take_and_die(service, lane)
        with mock.patch.object(service, "recover_conversion", side_effect=ConnectionError("db away")):
            self.assertEqual(self.sweep(service), 0)
        self.assertEqual(self.started(lane).count, 1)
        self.assertEqual(self.sweep(service), 1)

    def test_a_foreign_job_in_progress_is_left_to_rq(self):
        lane = routing.QUEUES["markitdown"]
        Queue(lane, connection=self.conn, serializer=JSONSerializer).enqueue("os.system", "true")
        q = Queue(lane, connection=self.conn, serializer=JSONSerializer)
        w = rb.GuardedWorker([q], connection=self.conn, serializer=JSONSerializer)
        job, _ = w.dequeue_job_and_maintain_ttl(timeout=1)
        w.prepare_execution(job)
        service, _, _ = self.build()
        self.assertEqual(self.sweep(service), 0)
        self.assertEqual(self.started(lane).count, 1)

    def test_a_worker_sweeps_other_lanes_at_start_and_keeps_its_key_while_it_works(self):
        lane = routing.QUEUES["markitdown"]
        service, parts, convs = self.build(markitdown=helpers.FakeConverter("markitdown", on_convert=die))
        doc, conv, _ = service.submit(b"PK", "notes.docx")
        self.take_and_die(service, lane)
        # The maintenance worker - another lane - finds it when it starts.
        rb.run_worker(service, [routing.MAINTENANCE_QUEUE], self.conn, burst=True,
                      sweep_queues=[lane, routing.MAINTENANCE_QUEUE])
        self.assertEqual(parts["registry"].get_conversion(conv.conversion_id).state, "conversion_pending")
        # The retry is delayed; bring it forward and let a markitdown worker run it.
        q = Queue(lane, connection=self.conn, serializer=JSONSerializer)
        for job_id in ScheduledJobRegistry(queue=q).get_job_ids():
            q.enqueue_job(Job.fetch(job_id, connection=self.conn, serializer=JSONSerializer))
        alive = []
        convs["markitdown"].on_convert = lambda: alive.append(self.conn.keys(rb.ALIVE_PREFIX + "*"))
        self.work(service, [lane])
        self.assertEqual(parts["registry"].get_conversion(conv.conversion_id).state, "validation_pending")
        self.assertEqual([len(keys) for keys in alive], [1])          # its key, while converting
        self.assertEqual(self.conn.keys(rb.ALIVE_PREFIX + "*"), [])   # and gone after


class Refused(RedisCase):
    """Anything that can write to Redis must not be able to run code in a worker."""

    def setUp(self):
        super().setUp()
        self.service, _, _ = self.build()
        self.proof = os.path.join(tempfile.mkdtemp(), "ran")

    def assert_refused(self, queue_name):
        self.work(self.service, [queue_name])
        self.assertFalse(os.path.exists(self.proof), "a refused job ran")
        self.assertEqual(canary_calls(), [], "a refused job's callable or callback ran")
        failed = FailedJobRegistry(queue_name, connection=self.conn, serializer=JSONSerializer)
        self.assertEqual(failed.count, 1)

    def test_a_job_naming_another_function(self):
        Queue("convert.markitdown", connection=self.conn, serializer=JSONSerializer) \
            .enqueue("os.system", f"touch {self.proof}")
        self.assert_refused("convert.markitdown")

    def test_a_pickled_job(self):
        # RQ's default serializer: the payload does not even deserialize as JSON.
        Queue("convert.markitdown", connection=self.conn).enqueue("os.system", f"touch {self.proof}")
        self.work(self.service, ["convert.markitdown"])
        self.assertFalse(os.path.exists(self.proof), "a pickled job ran")

    def test_the_right_function_with_a_callback(self):
        Queue("convert.markitdown", connection=self.conn, serializer=JSONSerializer).enqueue(
            rb.JOB_FUNC, {"kind": "convert", "conversion_id": "x"},
            on_failure=Callback("test_redis_backends.canary"),
            on_success=Callback("test_redis_backends.canary"))
        self.assert_refused("convert.markitdown")

    def test_the_right_function_with_the_wrong_arguments(self):
        Queue("convert.markitdown", connection=self.conn, serializer=JSONSerializer) \
            .enqueue(rb.JOB_FUNC, "not a dict")
        self.assert_refused("convert.markitdown")

    def test_a_failure_callback_planted_in_the_started_registry_never_runs(self):
        # RQ's registry cleanup, every worker's maintenance, calls an expired
        # in-progress job's failure callback - it never passes through
        # execute_job (Kamino item 92).
        q = Queue("convert.markitdown", connection=self.conn, serializer=JSONSerializer)
        job = q.enqueue("os.getcwd", on_failure=Callback("test_redis_backends.canary"))
        self.conn.lrem(q.key, 0, job.id)
        self.conn.zadd(f"rq:wip:{q.name}", {f"{job.id}:planted": 1})
        self.work(self.service, ["convert.markitdown"])
        self.assertEqual(canary_calls(), [], "the registry cleanup ran a job's failure callback")

    def _listener(self):
        hits = []

        class Hook(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                hits.append(self.path)
                self.send_response(200)
                self.end_headers()
            do_POST = do_GET  # noqa: N815

            def log_message(self, *args):
                pass

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = http.server.HTTPServer(("127.0.0.1", port), Hook)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return port, hits

    def test_a_refused_jobs_webhooks_are_not_sent(self):
        port, hits = self._listener()
        Queue("convert.markitdown", connection=self.conn, serializer=JSONSerializer).enqueue(
            "os.system", f"touch {self.proof}",
            webhooks=[Webhook(f"http://127.0.0.1:{port}/forged", "failed")])
        self.assert_refused("convert.markitdown")
        time.sleep(0.3)
        self.assertEqual(hits, [], "a refused job's webhook was sent")

    def test_the_right_function_carrying_a_webhook_is_refused(self):
        port, hits = self._listener()
        Queue("convert.markitdown", connection=self.conn, serializer=JSONSerializer).enqueue(
            rb.JOB_FUNC, {"kind": "convert", "conversion_id": "x"},
            webhooks=[Webhook(f"http://127.0.0.1:{port}/forged", "finished")])
        self.assert_refused("convert.markitdown")
        time.sleep(0.3)
        self.assertEqual(hits, [])

    def test_a_worker_survives_refusing_and_goes_on_working(self):
        Queue("convert.markitdown", connection=self.conn, serializer=JSONSerializer) \
            .enqueue("os.system", f"touch {self.proof}")
        doc, conv, _ = self.service.submit(b"PK", "fine.docx")
        self.work(self.service, ["convert.markitdown"])
        self.assertFalse(os.path.exists(self.proof))
        self.assertEqual(self.service.registry.get_conversion(conv.conversion_id).state,
                         "validation_pending")


if __name__ == "__main__":
    unittest.main()
