import unittest

import helpers  # noqa: F401 - puts the package on sys.path
from protocol import routing
from protocol.config import Settings
from protocol.routing import InvalidRequest


class ChooseTest(unittest.TestCase):
    def test_auto_routes_by_extension(self):
        self.assertEqual(routing.choose("scan.PDF"), "marker")
        self.assertEqual(routing.choose("photo.jpeg"), "marker")
        self.assertEqual(routing.choose("talk.mp3"), "transcription")
        self.assertEqual(routing.choose("notes.docx"), "markitdown")
        self.assertEqual(routing.choose("no-extension"), "markitdown")

    def test_audio_goes_to_transcription_not_markitdown(self):
        # Decision 18 changed protocol-droid's auto rule for audio.
        for ext in ("wav", "m4a", "flac", "ogg"):
            self.assertEqual(routing.choose(f"a.{ext}"), "transcription")

    def test_explicit_converter_wins(self):
        self.assertEqual(routing.choose("scan.pdf", "markitdown"), "markitdown")

    def test_unknown_converter_is_refused(self):
        with self.assertRaises(InvalidRequest):
            routing.choose("a.pdf", "pandoc")

    def test_one_queue_per_converter(self):
        queues = [routing.queue_for(name) for name in routing.CONVERTERS]
        self.assertEqual(len(set(queues)), len(queues))
        self.assertNotIn(routing.MAINTENANCE_QUEUE, queues)


class OptionsTest(unittest.TestCase):
    settings = Settings()

    def check(self, converter, options):
        return routing.validate_options(converter, options, self.settings)

    def test_marker_defaults_output_format(self):
        self.assertEqual(self.check("marker", {}), {"output_format": "markdown"})

    def test_unknown_options_are_refused(self):
        with self.assertRaises(InvalidRequest):
            self.check("marker", {"llm_service": "marker.services.openai.OpenAIService"})
        with self.assertRaises(InvalidRequest):
            self.check("markitdown", {"output_format": "markdown"})

    def test_marker_output_format_is_checked(self):
        with self.assertRaises(InvalidRequest):
            self.check("marker", {"output_format": "docx"})

    def test_booleans_must_be_booleans(self):
        with self.assertRaises(InvalidRequest):
            self.check("marker", {"force_ocr": "yes"})

    def test_use_llm_needs_a_configured_llm(self):
        with self.assertRaises(InvalidRequest):
            self.check("marker", {"use_llm": True})
        ok = routing.validate_options("marker", {"use_llm": True}, Settings(llm_configured=True))
        self.assertTrue(ok["use_llm"])

    def test_page_range_is_normalised_and_capped(self):
        self.assertEqual(self.check("marker", {"page_range": " 0, 5-10 "})["page_range"], "0,5-10")
        for bad in ("0-999999999", "10-5", "a-b", "²", "", "1,,2"):
            with self.assertRaises(InvalidRequest, msg=bad):
                self.check("marker", {"page_range": bad})

    def test_audio_is_refused_where_nothing_transcribes(self):
        # Queued anyway, it waited in convert.transcription for good (item 89).
        with self.assertRaises(InvalidRequest) as cm:
            routing.validate_options("transcription", {}, Settings())
        self.assertIn("SPEACHES_URL", str(cm.exception))

    def test_transcription_options(self):
        self.settings = Settings(transcription_configured=True)
        self.assertEqual(self.check("transcription", {"model": "Systran/faster-whisper-large-v3",
                                                      "language": "pt-PT"})["language"], "pt-PT")
        for bad in ({"model": "a b"}, {"model": "x" * 300}, {"language": "portuguese"},
                    {"language": 5}):
            with self.assertRaises(InvalidRequest, msg=bad):
                self.check("transcription", bad)

    def test_options_must_be_an_object(self):
        with self.assertRaises(InvalidRequest):
            self.check("marker", ["force_ocr"])


if __name__ == "__main__":
    unittest.main()
