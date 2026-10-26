"""Transcription: Speaches over HTTP, and what it costs (Kamino item 89).

Audio used to be routed to convert.transcription and wait there for good: no
worker took that queue. These checks cover the converter that does, against a
stand-in Speaches on a local port (the real one was checked by hand on
corelia: a 3.3 s clip came back as its words); the cost and timeout made from
a recording's length; the refusal where nothing transcribes; and the worker.
"""
import email.parser
import email.policy
import io
import json
import sys
import threading
import types
import unittest
import wave
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import helpers
from protocol import cost
from protocol.config import Settings
from protocol.converters import (DEFAULT_SPEACHES_MODEL, EngineMissing, SpeachesConverter,
                                 converter_for)
from protocol.worker import drain


def wav(seconds: float, rate: int = 16000) -> bytes:
    """A silent mono WAV of that length."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(b"\0\0" * int(seconds * rate))
    return buf.getvalue()


class FakeSpeaches:
    """Speaches' two endpoints this uses, on 127.0.0.1, recording each request."""

    def __init__(self, models=(DEFAULT_SPEACHES_MODEL,), status=200, text="hello there"):
        self.requests = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def _send(self, code, body):
                raw = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                outer.requests.append(("GET", self.path, dict(self.headers), None))
                self._send(200, {"data": [{"id": m} for m in models]})

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                msg = email.parser.BytesParser(policy=email.policy.default).parsebytes(
                    b"Content-Type: " + self.headers["Content-Type"].encode() + b"\r\n\r\n" + body)
                parts = {p.get_param("name", header="content-disposition"):
                         (p.get_filename(), p.get_payload(decode=True)) for p in msg.iter_parts()}
                outer.requests.append(("POST", self.path, dict(self.headers), parts))
                if status != 200:
                    self._send(status, {"detail": "model not found"})
                    return
                self._send(200, {"task": "transcribe", "language": "en", "duration": 3.3, "text": f" {text} ",
                                 "segments": [{"start": 0.0, "end": 3.2, "text": f" {text}"}]})

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class Cost(unittest.TestCase):
    def test_a_wav_s_length_is_read_without_any_library(self):
        self.assertAlmostEqual(cost.audio_seconds(wav(2.5)), 2.5, places=3)

    def test_an_hour_of_audio_costs_whisper_s_measured_rate(self):
        est = cost.estimate("transcription", "talk.wav", wav(3600, rate=100), {})
        self.assertAlmostEqual(est.audio_seconds, 3600, places=1)
        self.assertAlmostEqual(est.seconds, 3600 * 0.094, places=1)

    def test_the_timeout_is_the_base_plus_its_minutes(self):
        est = cost.estimate("transcription", "talk.wav", wav(600, rate=100), {})
        # 10 minutes at 18 s per audio minute (~3x the measured rate).
        self.assertEqual(cost.timeout(est, 1800, 180, 21600, 18), 1800 + 180)

    def test_a_recording_it_cannot_read_gets_the_unknown_timeout(self):
        est = cost.estimate("transcription", "x.mp3", b"not audio", {})
        self.assertIsNone(est.audio_seconds)
        self.assertEqual(cost.timeout(est, 1800, 180, 21600, 18), 21600)

    def test_other_formats_are_read_with_mutagen_when_it_is_there(self):
        fake = types.ModuleType("mutagen")
        fake.File = lambda _f: types.SimpleNamespace(info=types.SimpleNamespace(length=90.0))
        with mock.patch.dict(sys.modules, {"mutagen": fake}):
            self.assertEqual(cost.audio_seconds(b"ID3 an mp3"), 90.0)

    def test_without_mutagen_only_a_wav_is_read(self):
        with mock.patch.dict(sys.modules, {"mutagen": None}):
            self.assertIsNone(cost.audio_seconds(b"ID3 an mp3"))
            self.assertAlmostEqual(cost.audio_seconds(wav(1.0)), 1.0, places=3)

    def test_the_lane_is_transcription_s(self):
        est = cost.estimate("transcription", "a.wav", wav(1), {})
        self.assertEqual(cost.queue("transcription", est, 300), "convert.transcription")


class Converter(unittest.TestCase):
    def setUp(self):
        self.speaches = FakeSpeaches()
        self.addCleanup(self.speaches.close)

    def test_the_recording_goes_to_speaches_and_the_text_comes_back(self):
        out = SpeachesConverter(self.speaches.url, api_key="").convert(wav(1), "meeting.wav", {})
        self.assertEqual(out.files["meeting.md"], b"hello there\n")
        detail = json.loads(out.files["meeting.transcript.json"])
        self.assertEqual(detail["segments"][0]["end"], 3.2)
        self.assertEqual(out.tool_versions, {"speaches-model": DEFAULT_SPEACHES_MODEL})
        method, path, headers, parts = self.speaches.requests[0]
        self.assertEqual((method, path), ("POST", "/v1/audio/transcriptions"))
        self.assertEqual(parts["file"], ("meeting.wav", wav(1)))
        self.assertEqual(parts["response_format"][1], b"verbose_json")
        self.assertNotIn("Authorization", headers)

    def test_the_request_s_model_and_language_are_passed_through(self):
        SpeachesConverter(self.speaches.url).convert(
            wav(1), "a.wav", {"model": "Systran/faster-whisper-small", "language": "pt"})
        parts = self.speaches.requests[0][3]
        self.assertEqual(parts["model"][1], b"Systran/faster-whisper-small")
        self.assertEqual(parts["language"][1], b"pt")

    def test_no_language_lets_whisper_detect_it(self):
        SpeachesConverter(self.speaches.url).convert(wav(1), "a.wav", {})
        self.assertNotIn("language", self.speaches.requests[0][3])

    def test_an_api_key_is_a_bearer_token(self):
        SpeachesConverter(self.speaches.url, api_key="s3cret").convert(wav(1), "a.wav", {})
        self.assertEqual(self.speaches.requests[0][2]["Authorization"], "Bearer s3cret")

    def test_a_quote_in_the_file_name_cannot_break_the_form(self):
        SpeachesConverter(self.speaches.url).convert(wav(1), 'we"ird\r\nname.wav', {})
        self.assertEqual(self.speaches.requests[0][3]["file"][0], "weirdname.wav")

    def test_an_error_from_speaches_fails_the_job_with_its_status(self):
        bad = FakeSpeaches(status=404)
        self.addCleanup(bad.close)
        with self.assertRaises(RuntimeError) as cm:
            SpeachesConverter(bad.url).convert(wav(1), "a.wav", {})
        self.assertIn("HTTP 404", str(cm.exception))

    def test_an_unreachable_speaches_fails_the_job_saying_where(self):
        with self.assertRaises(RuntimeError) as cm:
            SpeachesConverter("http://127.0.0.1:9").convert(wav(1), "a.wav", {})
        self.assertIn("did not answer", str(cm.exception))

    def test_no_url_is_a_worker_that_was_not_configured(self):
        with self.assertRaises(EngineMissing):
            SpeachesConverter("").convert(wav(1), "a.wav", {})

    def test_warm_up_says_when_the_model_is_missing_and_never_raises(self):
        with self.assertLogs("protocol.converters", "WARNING") as logs:
            self.assertIsNotNone(SpeachesConverter(self.speaches.url, model="other/model").warm_up())
        self.assertIn("does not list other/model", logs.output[0])
        with self.assertLogs("protocol.converters", "WARNING"):
            self.assertIsNone(SpeachesConverter("http://127.0.0.1:9").warm_up())


class ThroughTheService(unittest.TestCase):
    def setUp(self):
        self.speaches = FakeSpeaches(text="the archive is converted")
        self.addCleanup(self.speaches.close)
        self.service, self.parts, _ = helpers.build(
            Settings(transcription_configured=True),
            transcription=SpeachesConverter(self.speaches.url))

    def test_audio_is_transcribed_and_stored(self):
        doc, conv, _ = self.service.submit(wav(2), "standup.wav")
        self.assertEqual(conv.converter, "transcription")
        drain(self.service, self.parts["queue"])
        conv = self.parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual(conv.state, "validation_pending")
        self.assertEqual(conv.tool_versions, {"speaches-model": DEFAULT_SPEACHES_MODEL})
        keys = self.parts["store"].list(conv.output_key)
        md = [k for k in keys if k.endswith("standup.md")]
        self.assertEqual(self.parts["store"].get(md[0]), b"the archive is converted\n")


class Replanned(unittest.TestCase):
    """Kamino item 203: a reconversion or a recovery plans the job again,
    and read the source for marker only - a recording got the 6 h unknown
    allowance (60 s of audio: 21600 s instead of 1818)."""

    def setUp(self):
        self.settings = Settings(transcription_configured=True)
        self.service, self.parts, _ = helpers.build(self.settings)
        self.queue = self.parts["queue"]
        self.expected = cost.timeout(cost.estimate("transcription", "talk.wav", wav(60), {}),
                                     self.settings.job_timeout_base, self.settings.job_timeout_per_page,
                                     self.settings.job_timeout_unknown,
                                     self.settings.job_timeout_per_audio_minute)
        self.assertLess(self.expected, self.settings.job_timeout_unknown)

    def last_timeout(self):
        return self.queue.jobs["convert.transcription"][-1][2]

    def test_a_reconverted_recording_keeps_its_length_based_timeout(self):
        doc, _, _ = self.service.submit(wav(60), "talk.wav")
        drain(self.service, self.queue)
        self.service.reconvert(doc.document_id)
        self.assertEqual(self.last_timeout(), self.expected)

    def test_a_recovered_recording_keeps_its_length_based_timeout(self):
        doc, conv, _ = self.service.submit(wav(60), "talk.wav")
        self.queue.pop_any()
        self.service.recover_conversion(conv.conversion_id)      # taken, not started
        self.assertEqual(self.last_timeout(), self.expected)


class Wiring(unittest.TestCase):
    # The worker itself is in test_deploy.py, which needs the backends' clients.
    def test_the_transcription_worker_runs_speaches_converter(self):
        self.assertIsInstance(converter_for("transcription"), SpeachesConverter)


if __name__ == "__main__":
    unittest.main()
