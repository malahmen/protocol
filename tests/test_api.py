"""HTTP tests. They need fastapi and httpx (requirements-dev.txt); skipped without them."""
import json
import socket
import threading
import time
import unittest
import urllib.request

import helpers
from protocol.config import Settings
from protocol.worker import drain

try:
    from fastapi.testclient import TestClient

    import uvicorn

    from protocol.api import create_app
    HAVE_FASTAPI = True
except ImportError:  # pragma: no cover - the stdlib-only run
    HAVE_FASTAPI = False


@unittest.skipUnless(HAVE_FASTAPI, "fastapi/httpx not installed")
class ApiTest(unittest.TestCase):
    def setUp(self, settings=None):
        self.service, self.parts, self.converters = helpers.build(settings)
        self.client = TestClient(create_app(self.service))

    def upload(self, data=b"%PDF pretend", name="report.pdf", **form):
        return self.client.post("/documents", files={"file": (name, data, "application/pdf")}, data=form)

    def test_submit_answers_202_then_200_for_the_same_content(self):
        first = self.upload()
        self.assertEqual(first.status_code, 202)
        body = first.json()
        self.assertTrue(body["created"])
        self.assertEqual(body["status_url"], f"/documents/{body['document_id']}")
        again = self.upload(name="other.pdf")
        self.assertEqual(again.status_code, 200)
        self.assertEqual(again.json()["document_id"], body["document_id"])

    def test_status_after_conversion(self):
        doc_id = self.upload().json()["document_id"]
        drain(self.service, self.parts["queue"])
        body = self.client.get(f"/documents/{doc_id}").json()
        self.assertEqual(body["document"]["state"], "validation_pending")
        self.assertEqual(body["conversions"][0]["role"], "current")

    def test_a_failure_is_not_explained_to_callers(self):
        self.converters["marker"].fail_times = 99
        doc_id = self.upload().json()["document_id"]
        drain(self.service, self.parts["queue"])
        conv = self.client.get(f"/documents/{doc_id}").json()["conversions"][0]
        self.assertEqual(conv["error"], "conversion failed")

    def test_reconvert_remove_prune(self):
        doc_id = self.upload().json()["document_id"]
        drain(self.service, self.parts["queue"])
        r = self.client.post(f"/documents/{doc_id}/conversions", json={"mode": "add"})
        self.assertEqual(r.status_code, 202)
        self.assertEqual(r.json()["mode"], "add")
        self.assertEqual(self.client.delete(f"/documents/{doc_id}/history?keep_alternatives=true").status_code, 202)
        r = self.client.delete(f"/documents/{doc_id}")
        self.assertEqual((r.status_code, r.json()["state"]), (202, "removal_pending"))
        self.assertEqual(self.client.post(f"/documents/{doc_id}/conversions", json={}).status_code, 409)

    def test_a_padded_id_is_not_found(self):
        # Kamino item 93: "<id> " matched the row on MariaDB (PAD SPACE).
        doc_id = self.upload().json()["document_id"]
        drain(self.service, self.parts["queue"])
        for path in (f"/documents/{doc_id}%20", f"/documents/{doc_id.upper()}"):
            with self.subTest(path=path):
                self.assertEqual(self.client.delete(path).status_code, 404)
                self.assertEqual(self.client.get(path).status_code, 404)
        state = self.client.get(f"/documents/{doc_id}").json()["document"]["state"]
        self.assertNotIn(state, ("removal_pending", "removed"))

    def test_errors(self):
        self.assertEqual(self.client.get("/documents/nope").status_code, 404)
        self.assertEqual(self.upload(options="{not json").status_code, 400)
        self.assertEqual(self.upload(options=json.dumps({"use_llm": True})).status_code, 400)
        self.assertEqual(self.upload(converter="pandoc").status_code, 400)
        doc_id = self.upload().json()["document_id"]
        r = self.client.post(f"/documents/{doc_id}/conversions", json={"mode": "overwrite"})
        self.assertEqual(r.status_code, 400)

    def test_size_cap_is_413(self):
        self.setUp(Settings(max_upload_bytes=4))
        self.assertEqual(self.upload(data=b"12345").status_code, 413)

    def test_full_queue_is_429(self):
        self.setUp(Settings(queue_max_depth=1))
        self.upload(data=b"one")
        r = self.upload(data=b"two")
        self.assertEqual(r.status_code, 429)
        self.assertIn("Retry-After", r.headers)

    def test_healthz(self):
        self.assertEqual(self.client.get("/healthz").json(), {"ok": True})


@unittest.skipUnless(HAVE_FASTAPI, "fastapi/httpx/uvicorn not installed")
class NonBlockingTest(unittest.TestCase):
    """Kamino item 189: POST /documents ran the blocking submit (hash, PDF
    facts, the S3 put, MariaDB) on the event loop, so a slow store stalled
    every request - /healthz answered in 3.05 s behind a 3 s put."""

    def test_healthz_answers_while_an_upload_is_being_stored(self):
        service, parts, _ = helpers.build()
        store, real = parts["store"], parts["store"].put
        storing, release = threading.Event(), threading.Event()

        def slow_put(key, data):
            storing.set()
            release.wait(10)           # a large upload on its way to taris
            real(key, data)
        store.put = slow_put
        self.addCleanup(release.set)

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(create_app(service), log_level="error"))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 10)
        self.addCleanup(setattr, server, "should_exit", True)
        deadline = time.monotonic() + 10
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.02)
        base = f"http://127.0.0.1:{port}"

        body = (b'--x\r\nContent-Disposition: form-data; name="file"; filename="a.docx"\r\n\r\n'
                b"hello\r\n--x--\r\n")
        upload = urllib.request.Request(base + "/documents", data=body,
                                        headers={"Content-Type": "multipart/form-data; boundary=x"})
        answers = []
        uploader = threading.Thread(target=lambda: answers.append(urllib.request.urlopen(upload, timeout=15).status))
        uploader.start()
        self.assertTrue(storing.wait(10), "the upload never reached the store")
        started = time.monotonic()
        with urllib.request.urlopen(base + "/healthz", timeout=5) as reply:
            self.assertEqual(json.load(reply), {"ok": True})
        self.assertLess(time.monotonic() - started, 2, "/healthz waited for the upload")
        self.assertTrue(uploader.is_alive(), "the upload finished first: nothing was proven")
        release.set()
        uploader.join(15)
        self.assertEqual(answers, [202])


if __name__ == "__main__":
    unittest.main()
