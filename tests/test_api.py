"""HTTP tests. They need fastapi and httpx (requirements-dev.txt); skipped without them."""
import json
import unittest

import helpers
from protocol.config import Settings
from protocol.worker import drain

try:
    from fastapi.testclient import TestClient

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


if __name__ == "__main__":
    unittest.main()
