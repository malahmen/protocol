"""protocol.deploy - the composition root - without any backend.

What it protects: every lane a job can be queued in has a worker, marker's two
lanes each have their own, a missing Qdrant collection list removes nothing
rather than refusing to start, and `check` reports a missing setting by name
without printing a credential.
"""
import contextlib
import io
import os
import unittest
from unittest import mock

import helpers  # noqa: F401 - puts the repository on sys.path

from protocol import cost, routing

try:
    from protocol import deploy
except ImportError as exc:      # the backends' clients are not installed
    deploy = None
    SKIP = f"needs requirements.txt ({exc.name})"
else:
    SKIP = ""


@unittest.skipIf(deploy is None, SKIP)
class WorkersTest(unittest.TestCase):
    def queues_worked(self):
        return {q for _, qs in deploy.WORKERS.values() for q in qs}

    def test_every_lane_a_conversion_can_be_queued_in_has_a_worker(self):
        lanes = {cost.queue("marker", est, 300) for est in (cost.UNKNOWN, cost.Estimate(1, False, 1.0))}
        lanes |= {routing.QUEUES["markitdown"], routing.MAINTENANCE_QUEUE}
        self.assertEqual(lanes - self.queues_worked(), set())

    def test_marker_lanes_have_a_worker_each(self):
        self.assertEqual(deploy.WORKERS["marker-light"][1], (routing.MARKER_LIGHT_QUEUE,))
        self.assertEqual(deploy.WORKERS["marker-heavy"][1], (routing.MARKER_HEAVY_QUEUE,))

    def test_transcription_has_a_worker(self):
        # It had none, and audio waited in its queue for good (Kamino item 89).
        self.assertEqual(deploy.WORKERS["transcription"],
                         ("transcription", (routing.QUEUES["transcription"],)))

    def test_every_converter_s_queue_has_a_worker(self):
        for name in routing.CONVERTERS:
            if name != "marker":          # marker's work waits in its two lanes, not its family name
                self.assertIn(routing.queue_for(name), self.queues_worked(), name)

    def test_maintenance_runs_no_converter(self):
        self.assertIsNone(deploy.WORKERS["maintenance"][0])


@unittest.skipIf(deploy is None, SKIP)
class IndexTest(unittest.TestCase):
    def test_no_collections_named_removes_nothing_and_says_so(self):
        with mock.patch.dict(os.environ, {"QDRANT_URL": "http://q:6333"}, clear=True), \
                self.assertLogs("protocol.deploy", "WARNING") as logs:
            index = deploy.index_from_env()
            self.assertEqual(index.delete_document("d1"), 0)
            self.assertEqual(index.delete_conversion("c1"), 0)
        self.assertTrue(any("d1" in line for line in logs.output))
        self.assertTrue(any("c1" in line for line in logs.output))

    def test_named_collections_get_the_real_index(self):
        env = {"QDRANT_URL": "http://q:6333", "QDRANT_COLLECTIONS": "a, b"}
        with mock.patch.dict(os.environ, env, clear=True):
            index = deploy.index_from_env()
        self.assertIsInstance(index, deploy.QdrantIndex)
        self.assertEqual(index.collections, ["a", "b"])


@unittest.skipIf(deploy is None, SKIP)
class CheckTest(unittest.TestCase):
    def test_reports_each_backend_and_fails_without_settings(self):
        env = {"MARIADB_PASSWORD": "hunter2-secret", "REDIS_URL": "redis://127.0.0.1:1/0"}
        out = io.StringIO()
        with mock.patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(out):
            rc = deploy.check()
        text = out.getvalue()
        self.assertEqual(rc, 1)
        for name in ("MariaDB", "Redis", "Garage", "Qdrant"):
            self.assertIn(f"FAIL  {name}", text)
        self.assertIn("MARIADB_HOST is not set", text)
        self.assertIn("QDRANT_URL is not set", text)
        self.assertNotIn("hunter2", text)

    def test_unknown_worker_is_refused(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            deploy.main(["worker", "transcription"])


if __name__ == "__main__":
    unittest.main()
