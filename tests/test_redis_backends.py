"""protocol/redis_backends.py against a real Redis: the queue, the stream, the
guarded worker, and protocol's service running through them.

Needs rq and redis (requirements.txt) and a Redis to point at:

    PROTOCOL_TEST_REDIS_URL=redis://localhost:16379/15 python3 -m unittest discover -s tests

**The suite flushes that database.** There is no default URL on purpose, so
it can never reach a Redis that holds anything; without the variable these
checks skip. CI starts a throwaway Redis 7 for them (meksha runs `redis:7`).
"""
import json
import os
import tempfile
import time
import unittest
import zlib
from unittest import mock

import helpers
from protocol import routing
from protocol.config import Settings

try:
    from rq import Callback, Queue
    from rq.registry import FailedJobRegistry
    from rq.serializers import JSONSerializer

    from protocol import redis_backends as rb
    HAVE_RQ = True
except ImportError:  # pragma: no cover - the stdlib-only run
    HAVE_RQ = False

URL = os.environ.get("PROTOCOL_TEST_REDIS_URL", "")
CANARY = []


def canary(*args, **kwargs):
    """Stands in for an attacker's callable: if a refused job reaches it, the list says so."""
    CANARY.append(args)


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
        CANARY.clear()

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

    def test_a_marker_worker_takes_the_light_lane_first(self):
        service, _, convs = self.build()
        light, heavy = routing.MARKER_LIGHT_QUEUE, routing.MARKER_HEAVY_QUEUE
        # Heavy queued first, light second: the light job must still go first.
        for name, lane in (("book.pdf", heavy), ("note.pdf", light)):
            with mock.patch.object(service, "_plan", lambda *a, lane=lane: (lane, 600)):
                service.submit(name.encode(), name)
        self.work(service, routing.worker_queues("marker"))
        self.assertEqual([call[1] for call in convs["marker"].calls], ["note.pdf", "book.pdf"])

    def test_a_timed_out_conversion_is_a_failed_attempt_and_retried(self):
        slow = helpers.FakeConverter("markitdown", on_convert=lambda: time.sleep(5))
        service, parts, _ = self.build(Settings(job_timeout_unknown=1), markitdown=slow)
        doc, conv, _ = service.submit(b"PK", "slow.docx")
        started = time.monotonic()
        self.work(service, routing.worker_queues("markitdown"))
        self.assertLess(time.monotonic() - started, 4)          # stopped, not waited out
        conv = parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual((conv.state, conv.attempts), ("conversion_pending", 1))
        self.assertIn("JobTimeoutException", conv.error)
        self.assertEqual(self.queue.depth("convert.markitdown"), 1)   # the retry, delayed


class Refused(RedisCase):
    """Anything that can write to Redis must not be able to run code in a worker."""

    def setUp(self):
        super().setUp()
        self.service, _, _ = self.build()
        self.proof = os.path.join(tempfile.mkdtemp(), "ran")

    def assert_refused(self, queue_name):
        self.work(self.service, [queue_name])
        self.assertFalse(os.path.exists(self.proof), "a refused job ran")
        self.assertEqual(CANARY, [], "a refused job's callable or callback ran")
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
