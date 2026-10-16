"""protocol/converters.py - the real converters, onto protocol-droid's images.

The engines (`tasks` in marker's image, `markitdown_tasks` in markitdown's)
are not installed here; stand-in modules take their place, which is what these
checks are about: which module each converter reaches for, what crosses the
port, and what happens in a worker started in the wrong image. That the real
engines convert was checked by running protocol's pipeline inside both images.
"""
import sys
import types
import unittest
from unittest import mock

import helpers
from protocol import converters
from protocol.converters import EngineConverter, EngineMissing
from protocol.worker import drain


def engine(name, files=None, versions=None, fail=None, warm=None):
    """A stand-in engine module with protocol-droid's convert_bytes signature."""
    mod = types.ModuleType(name)
    mod.calls = []

    def convert_bytes(data, original_name, options):
        mod.calls.append((data, original_name, options))
        options["engine_default"] = True          # an engine may fill in defaults
        if fail:
            raise fail
        return (dict(files or {"doc.md": b"# Doc\n"}),
                dict(versions or {"engine": "9.9"}))

    mod.convert_bytes = convert_bytes
    if warm is not None:
        mod.warmed = []
        mod.get_models = lambda: mod.warmed.append("models")
        mod.prewarm = lambda: (mod.warmed.append("prewarm"), warm)[1]
    return mod


class Modules(unittest.TestCase):
    def test_each_converter_reaches_for_its_image_s_module(self):
        self.assertEqual(EngineConverter("marker").module_name, "tasks")
        self.assertEqual(EngineConverter("markitdown").module_name, "markitdown_tasks")

    def test_an_unknown_converter_is_refused(self):
        with self.assertRaises(ValueError):
            EngineConverter("transcription")    # Speaches, over HTTP: its own adapter

    def test_importing_the_adapters_imports_no_engine(self):
        # The core stays importable anywhere; only a conversion needs the image.
        for name in ("tasks", "markitdown_tasks"):
            self.assertNotIn(name, sys.modules)
        self.assertEqual(set(converters.engine_converters()), {"marker", "markitdown"})


class Convert(unittest.TestCase):
    def test_bytes_in_converter_output_out(self):
        mod = engine("tasks", files={"r.md": b"# R\n", "images/a.png": b"\x89PNG"},
                     versions={"marker-pdf": "2.0.0"})
        with mock.patch.dict(sys.modules, {"tasks": mod}):
            out = EngineConverter("marker").convert(b"%PDF", "r.pdf", {"output_format": "markdown"})
        self.assertEqual(out.files, {"r.md": b"# R\n", "images/a.png": b"\x89PNG"})
        self.assertEqual(out.tool_versions, {"marker-pdf": "2.0.0"})
        self.assertEqual(mod.calls[0][:2], (b"%PDF", "r.pdf"))

    def test_the_request_s_options_are_not_changed_by_the_engine(self):
        options = {"output_format": "markdown"}
        with mock.patch.dict(sys.modules, {"tasks": engine("tasks")}):
            EngineConverter("marker").convert(b"x", "r.pdf", options)
        self.assertEqual(options, {"output_format": "markdown"})

    def test_no_options_is_an_empty_dict_for_the_engine(self):
        mod = engine("markitdown_tasks")
        with mock.patch.dict(sys.modules, {"markitdown_tasks": mod}):
            EngineConverter("markitdown").convert(b"x", "a.docx", None)
        self.assertEqual(mod.calls[0][2], {"engine_default": True})

    def test_the_wrong_image_fails_the_job_and_says_so(self):
        conv = EngineConverter("marker", module="no_such_engine_module")   # constructing is fine
        with self.assertRaises(EngineMissing) as cm:
            conv.convert(b"x", "r.pdf", {})
        self.assertIn("not running in the marker image", str(cm.exception))

    def test_an_image_from_before_the_port_says_so(self):
        old = types.ModuleType("tasks")                  # protocol-droid before #26
        with mock.patch.dict(sys.modules, {"tasks": old}), self.assertRaises(EngineMissing) as cm:
            EngineConverter("marker").convert(b"x", "r.pdf", {})
        self.assertIn("has no convert_bytes", str(cm.exception))


class WarmUp(unittest.TestCase):
    def test_marker_loads_its_models_then_prewarms(self):
        mod = engine("tasks", warm=26.0)
        with mock.patch.dict(sys.modules, {"tasks": mod}):
            self.assertEqual(EngineConverter("marker").warm_up(), 26.0)
        self.assertEqual(mod.warmed, ["models", "prewarm"])

    def test_markitdown_has_nothing_to_warm(self):
        with mock.patch.dict(sys.modules, {"markitdown_tasks": engine("markitdown_tasks")}):
            self.assertIsNone(EngineConverter("markitdown").warm_up())


class ThroughTheService(unittest.TestCase):
    """The adapters inside protocol's real pipeline, over the in-memory backends."""

    def setUp(self):
        self.marker = engine("tasks", files={"report.md": b"# Report\n"}, versions={"marker-pdf": "2.0.0"})
        self.markitdown = engine("markitdown_tasks", files={"notes.md": b"# Notes\n"},
                                 versions={"markitdown": "0.1.8"})
        patcher = mock.patch.dict(sys.modules, {"tasks": self.marker, "markitdown_tasks": self.markitdown})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.service, self.parts, _ = helpers.build(
            marker=EngineConverter("marker"), markitdown=EngineConverter("markitdown"))

    def convert(self, data, name, **kw):
        doc, conv, _ = self.service.submit(data, name, **kw)
        drain(self.service, self.parts["queue"])
        return doc, self.parts["registry"].get_conversion(conv.conversion_id)

    def test_auto_routes_each_file_to_its_engine(self):
        _, pdf = self.convert(b"%PDF-1.7", "report.pdf")
        _, docx = self.convert(b"PK\x03\x04", "notes.docx")
        self.assertEqual((pdf.converter, docx.converter), ("marker", "markitdown"))
        self.assertEqual(len(self.marker.calls), 1)
        self.assertEqual(len(self.markitdown.calls), 1)

    def test_outputs_are_stored_with_protocol_s_provenance_and_the_engine_s_versions(self):
        doc, conv = self.convert(b"%PDF-1.7", "report.pdf")
        self.assertEqual(conv.state, "validation_pending")
        self.assertEqual(conv.tool_versions, {"marker-pdf": "2.0.0"})
        names = [k.rsplit("/", 1)[-1] for k in self.parts["store"].list(conv.output_key)]
        self.assertEqual(sorted(names), ["provenance.json", "report.md"])

    def test_marker_options_reach_the_engine(self):
        self.convert(b"%PDF-1.7", "report.pdf", options={"page_range": "0-2", "force_ocr": True})
        _, _, options = self.marker.calls[0]
        self.assertEqual(options["page_range"], "0-2")
        self.assertIs(options["force_ocr"], True)

    def test_an_engine_failure_is_retried_like_any_other(self):
        self.markitdown.convert_bytes = lambda *a: (_ for _ in ()).throw(RuntimeError("bad docx"))
        doc, conv, _ = self.service.submit(b"PK", "broken.docx")
        self.service.run_conversion(conv.conversion_id)
        conv = self.parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual(conv.state, "conversion_pending")
        self.assertIn("bad docx", conv.error)


if __name__ == "__main__":
    unittest.main()
