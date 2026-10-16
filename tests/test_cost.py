"""protocol/cost.py - splitting marker's work by expected cost (Kamino item 73).

The rule: a marker job estimated over PROTOCOL_MARKER_HEAVY_SECONDS waits in
the heavy lane, anything else in the light one, and anything whose cost cannot
be known counts as heavy. The same estimate sizes the job's timeout.

The estimate's logic runs everywhere, with the PDF facts patched. Reading real
PDFs needs pypdfium2 (requirements.txt); those checks skip without it.
"""
import unittest
from unittest import mock

import helpers
from protocol import cost, routing
from protocol.config import Settings
from protocol.worker import handle

try:
    import pypdfium2  # noqa: F401
    HAVE_PDFIUM = True
except ImportError:  # pragma: no cover - the stdlib-only run
    HAVE_PDFIUM = False

S = Settings()


def facts(pages, has_text):
    return mock.patch.object(cost, "pdf_facts", lambda data, page_range=None: (pages, has_text))


def lane(converter, name, data=b"%PDF", options=None, **kw):
    est = cost.estimate(converter, name, data, options or {})
    return cost.queue(converter, est, kw.get("heavy", S.marker_heavy_seconds))


class Estimate(unittest.TestCase):
    def test_a_short_text_pdf_is_light(self):
        with facts(3, True):
            self.assertEqual(lane("marker", "cheat.pdf"), routing.MARKER_LIGHT_QUEUE)

    def test_the_30_page_manual_is_light(self):
        # ~3.2 s/page: 96s, well under five minutes.
        with facts(30, True):
            self.assertEqual(lane("marker", "manual.pdf"), routing.MARKER_LIGHT_QUEUE)

    def test_a_scan_is_heavy(self):
        # 22 scanned pages at 54.7 s/page is ~20 minutes.
        with facts(22, False):
            est = cost.estimate("marker", "scan.pdf", b"%PDF", {})
        self.assertTrue(est.needs_ocr)
        self.assertAlmostEqual(est.seconds, 22 * cost.OCR_SECONDS_PER_PAGE)
        self.assertEqual(cost.queue("marker", est, S.marker_heavy_seconds), routing.MARKER_HEAVY_QUEUE)

    def test_a_short_scan_can_still_be_light(self):
        with facts(5, False):     # ~4.5 minutes
            self.assertEqual(lane("marker", "letter.pdf"), routing.MARKER_LIGHT_QUEUE)

    def test_a_long_text_pdf_is_heavy(self):
        with facts(387, True):    # ~20 minutes even without OCR
            self.assertEqual(lane("marker", "book.pdf"), routing.MARKER_HEAVY_QUEUE)

    def test_forcing_ocr_costs_ocr_rates(self):
        with facts(30, True):
            est = cost.estimate("marker", "manual.pdf", b"%PDF", {"force_ocr": True})
        self.assertTrue(est.needs_ocr)
        self.assertEqual(cost.queue("marker", est, S.marker_heavy_seconds), routing.MARKER_HEAVY_QUEUE)

    def test_an_image_is_one_ocr_d_page(self):
        est = cost.estimate("marker", "scan.png", b"\x89PNG", {})
        self.assertEqual((est.pages, est.needs_ocr), (1, True))
        self.assertEqual(cost.queue("marker", est, S.marker_heavy_seconds), routing.MARKER_LIGHT_QUEUE)

    def test_unknown_is_heavy(self):
        # pypdfium2 missing or the file unreadable; a DOCX sent to marker by
        # name; no bytes at all. A wrong "light" would break the light lane's
        # promise for everyone, so unknown never goes there.
        with facts(None, None):
            self.assertEqual(lane("marker", "x.pdf"), routing.MARKER_HEAVY_QUEUE)
        self.assertEqual(lane("marker", "report.docx"), routing.MARKER_HEAVY_QUEUE)
        self.assertEqual(lane("marker", "x.pdf", data=None), routing.MARKER_HEAVY_QUEUE)

    def test_the_threshold_is_configurable(self):
        with facts(30, True):
            self.assertEqual(lane("marker", "manual.pdf", heavy=60), routing.MARKER_HEAVY_QUEUE)

    def test_other_converters_keep_their_one_queue(self):
        self.assertEqual(lane("markitdown", "a.docx"), "convert.markitdown")
        self.assertEqual(lane("transcription", "a.wav"), "convert.transcription")


class Timeout(unittest.TestCase):
    def t(self, est):
        return cost.timeout(est, S.job_timeout_base, S.job_timeout_per_page, S.job_timeout_unknown)

    def test_base_plus_pages(self):
        self.assertEqual(self.t(cost.Estimate(10, True, 547.0)),
                         S.job_timeout_base + 10 * S.job_timeout_per_page)

    def test_every_scan_gets_room_for_its_pages(self):
        for pages in (1, 22, 166, 2000):
            self.assertGreater(self.t(cost.Estimate(pages, True, None)),
                               pages * cost.OCR_SECONDS_PER_PAGE, msg=pages)

    def test_unknown_gets_the_unknown_timeout(self):
        self.assertEqual(self.t(cost.UNKNOWN), S.job_timeout_unknown)


class Lanes(unittest.TestCase):
    def test_a_marker_worker_takes_the_light_lane_first(self):
        self.assertEqual(routing.worker_queues("marker"),
                         (routing.MARKER_LIGHT_QUEUE, routing.MARKER_HEAVY_QUEUE))
        self.assertEqual(routing.worker_queues("markitdown"), ("convert.markitdown",))

    def test_no_lane_is_another_converter_s_or_maintenance(self):
        lanes = [q for name in routing.CONVERTERS for q in routing.worker_queues(name)]
        self.assertEqual(len(set(lanes)), len(lanes))
        self.assertNotIn(routing.MAINTENANCE_QUEUE, lanes)


class ThroughTheService(unittest.TestCase):
    def setUp(self):
        self.service, self.parts, _ = helpers.build()
        self.jobs = self.parts["queue"].jobs

    def test_submit_queues_each_pdf_in_its_lane_with_its_timeout(self):
        with facts(3, True):
            self.service.submit(b"%PDF small", "small.pdf")
        with facts(22, False):
            self.service.submit(b"%PDF scan", "scan.pdf")
        (_, _, light_timeout), = self.jobs[routing.MARKER_LIGHT_QUEUE]
        (_, _, heavy_timeout), = self.jobs[routing.MARKER_HEAVY_QUEUE]
        self.assertEqual(light_timeout, S.job_timeout_base + 3 * S.job_timeout_per_page)
        self.assertEqual(heavy_timeout, S.job_timeout_base + 22 * S.job_timeout_per_page)

    def test_a_retry_returns_to_its_lane(self):
        service, parts, _ = helpers.build(marker=helpers.FakeConverter("marker", fail_times=1))
        with facts(3, True):
            service.submit(b"%PDF small", "small.pdf")
            job, _, _ = parts["queue"].jobs[routing.MARKER_LIGHT_QUEUE].pop(0)
            handle(service, job)                               # fails once
        (_, delay, timeout), = parts["queue"].jobs[routing.MARKER_LIGHT_QUEUE]
        self.assertEqual(delay, S.retry_base_seconds)
        self.assertEqual(timeout, S.job_timeout_base + 3 * S.job_timeout_per_page)
        self.assertNotIn(routing.MARKER_HEAVY_QUEUE, parts["queue"].jobs)

    def test_a_reconversion_is_planned_from_the_stored_source(self):
        with facts(22, False):
            doc, _, _ = self.service.submit(b"%PDF scan", "scan.pdf")
            self.service.reconvert(doc.document_id)
        self.assertEqual(len(self.jobs[routing.MARKER_HEAVY_QUEUE]), 2)

    def test_backpressure_is_per_lane(self):
        service, _, _ = helpers.build(Settings(queue_max_depth=1))
        with facts(22, False):
            service.submit(b"%PDF scan one", "one.pdf")        # fills the heavy lane
        with facts(3, True):
            service.submit(b"%PDF small", "small.pdf")         # the light lane is still open


def _pdf(pages_text):
    """A minimal PDF: one page per entry, with that text (or none) on it."""
    objs = ["<< /Type /Catalog /Pages 2 0 R >>", None,
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    kids = []
    for text in pages_text:
        body = (f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET" if text else "").encode()
        objs.append(f"<< /Length {len(body)} >>\nstream\n{body.decode()}\nendstream")
        content = len(objs)
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                    f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content} 0 R >>")
        kids.append(f"{len(objs)} 0 R")
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(kids)} >>"
    out, offsets = b"%PDF-1.4\n", []
    for i, obj in enumerate(objs, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n{obj}\nendobj\n".encode()
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    out += "".join(f"{o:010d} 00000 n \n" for o in offsets).encode()
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return out


LINE = "A text layer marker can read without OCR, long enough to count as one."


@unittest.skipUnless(HAVE_PDFIUM, "pypdfium2 not installed")
class RealPdfs(unittest.TestCase):
    def test_a_pdf_with_a_text_layer(self):
        self.assertEqual(cost.pdf_facts(_pdf([LINE] * 4)), (4, True))

    def test_a_pdf_without_one_reads_as_a_scan(self):
        self.assertEqual(cost.pdf_facts(_pdf([None] * 3)), (3, False))

    def test_page_range_counts_only_the_pages_that_exist(self):
        self.assertEqual(cost.pdf_facts(_pdf([LINE] * 5), "0-1,3,9-12")[0], 3)

    def test_the_text_layer_is_sampled_from_the_pages_converted(self):
        mixed = _pdf([None, None, LINE, LINE])
        self.assertEqual(cost.pdf_facts(mixed, "0-1"), (2, False))
        self.assertEqual(cost.pdf_facts(mixed, "2-3"), (2, True))

    def test_garbage_is_unknown_not_an_error(self):
        self.assertEqual(cost.pdf_facts(b"%PDF-1.7 pretend"), (None, None))

    def test_through_the_service_with_real_pdfs(self):
        service, parts, _ = helpers.build()
        service.submit(_pdf([LINE] * 3), "text.pdf")
        service.submit(_pdf([None] * 22), "scan.pdf")
        self.assertEqual(parts["queue"].depth(routing.MARKER_LIGHT_QUEUE), 1)
        self.assertEqual(parts["queue"].depth(routing.MARKER_HEAVY_QUEUE), 1)


if __name__ == "__main__":
    unittest.main()
