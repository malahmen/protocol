import json
import unittest

import helpers
from protocol import contract as c
from protocol import routing
from protocol.config import Settings
from protocol.ports import ConversionTimeout
from protocol.service import Busy, Conflict, NotFound, TooLarge, safe_name
from protocol.worker import UnknownJob, drain, handle, recover

PDF = b"%PDF-1.7 pretend"


class Base(unittest.TestCase):
    def setUp(self):
        self.service, self.parts, self.converters = helpers.build()
        self.store = self.parts["store"]
        self.queue = self.parts["queue"]
        self.events = self.parts["events"].events

    def submit_and_convert(self, data=PDF, name="report.pdf"):
        doc, conv, created = self.service.submit(data, name, "application/pdf")
        drain(self.service, self.queue)
        return self.service.status(doc.document_id)

    def reconvert(self, document_id, mode):
        conv = self.service.reconvert(document_id, mode)
        drain(self.service, self.queue)
        return conv


class SubmitTest(Base):
    def test_submit_stores_the_source_and_queues_the_right_converter(self):
        doc, conv, created = self.service.submit(PDF, "report.pdf", "application/pdf")
        self.assertTrue(created)
        self.assertEqual(doc.state, c.CONVERSION_PENDING)
        self.assertEqual(self.store.get(doc.source_key), PDF)
        self.assertEqual(doc.source_key, f"sources/{doc.document_id}/report.pdf")
        self.assertEqual(conv.mode, c.INITIAL)
        # PDF is placeholder bytes nothing can read: an unknown cost, so the
        # heavy lane (cost.py; test_cost.py covers the choice itself).
        self.assertEqual(self.queue.depth(routing.MARKER_HEAVY_QUEUE), 1)

    def test_audio_is_queued_for_transcription(self):
        service, parts, _ = helpers.build(Settings(transcription_configured=True))
        service.submit(b"RIFF", "meeting.wav")
        self.assertEqual(parts["queue"].depth("convert.transcription"), 1)

    def test_audio_is_refused_where_nothing_transcribes(self):
        with self.assertRaises(routing.InvalidRequest):
            self.service.submit(b"RIFF", "meeting.wav")
        self.assertEqual(self.queue.depth("convert.transcription"), 0)

    def test_the_same_content_is_the_same_document(self):
        first, _, _ = self.service.submit(PDF, "a.pdf")
        again, conv, created = self.service.submit(PDF, "renamed.pdf")
        self.assertFalse(created)
        self.assertIsNone(conv)
        self.assertEqual(again.document_id, first.document_id)
        self.assertEqual(self.queue.depth(routing.MARKER_HEAVY_QUEUE), 1)

    def test_losing_the_insert_race_leaves_no_orphan_source(self):
        # Another submission of the same bytes registered between the hash
        # check and the insert.
        winner, _, _ = self.service.submit(PDF, "a.pdf")
        self.parts["registry"].find_live_by_hash = lambda h: None
        stored, conv, created = self.service.submit(PDF, "b.pdf")
        self.assertFalse(created)
        self.assertEqual(stored.document_id, winner.document_id)
        self.assertEqual(self.store.list("sources/"), [winner.source_key])

    def test_size_cap(self):
        service, _, _ = helpers.build(Settings(max_upload_bytes=4))
        with self.assertRaises(TooLarge):
            service.submit(b"12345", "a.txt")

    def test_backpressure(self):
        service, _, _ = helpers.build(Settings(queue_max_depth=1))
        service.submit(b"one", "a.pdf")
        with self.assertRaises(Busy):
            service.submit(b"two", "b.pdf")
        # Another converter's queue is not full.
        service.submit(b"three", "c.docx")

    def test_the_name_is_reduced_to_one_segment(self):
        doc, _, _ = self.service.submit(b"x", "../../etc/passwd")
        self.assertEqual(doc.source_key, f"sources/{doc.document_id}/passwd")

    def test_safe_name(self):
        self.assertEqual(safe_name("a\\b\\c.pdf"), "c.pdf")
        self.assertEqual(safe_name("..") , "source")
        self.assertEqual(safe_name("x\x00y.pdf"), "xy.pdf")
        self.assertEqual(safe_name(""), "source")


class ConversionTest(Base):
    def test_a_conversion_writes_outputs_and_provenance_then_announces(self):
        doc, conversions = self.submit_and_convert()
        conv = conversions[0]
        self.assertEqual(doc.state, c.VALIDATION_PENDING)
        self.assertEqual(doc.current_conversion_id, conv.conversion_id)
        self.assertEqual(conv.role, c.CURRENT)
        prefix = f"outputs/{doc.document_id}/{conv.conversion_id}/"
        self.assertEqual(conv.output_key, prefix)
        self.assertEqual(self.store.list(prefix), [prefix + "out.md", prefix + "provenance.json"])
        record = json.loads(self.store.get(prefix + "provenance.json"))
        self.assertEqual(record["source"]["sha256"], doc.content_hash)
        self.assertEqual(record["tool_versions"], {"marker": "1.0"})
        self.assertEqual(self.events[-1]["event"], c.EVENT_CONVERSION)
        self.assertEqual(self.events[-1]["output_key"], prefix)
        self.assertIsNone(self.events[-1]["previous_conversion_id"])

    def test_a_second_delivery_of_a_finished_job_does_nothing(self):
        doc, conversions = self.submit_and_convert()
        calls = len(self.converters["marker"].calls)
        handle(self.service, {"kind": "convert", "conversion_id": conversions[0].conversion_id})
        self.assertEqual(len(self.converters["marker"].calls), calls)

    def test_a_redelivered_interrupted_job_starts_clean(self):
        doc, conv, _ = self.service.submit(PDF, "a.pdf")
        stale = f"outputs/{doc.document_id}/{conv.conversion_id}/stale.md"
        self.store.put(stale, b"half-written")
        drain(self.service, self.queue)
        self.assertNotIn(stale, self.store.list("outputs/"))

    def test_retries_then_dead_letter(self):
        service, parts, _ = helpers.build(marker=helpers.FakeConverter("marker", fail_times=99))
        doc, conv, _ = service.submit(PDF, "a.pdf")
        drain(service, parts["queue"])
        doc, (conv,) = service.status(doc.document_id)
        self.assertEqual(conv.state, c.CONVERSION_FAILED)
        self.assertEqual(conv.attempts, 3)
        self.assertEqual(doc.state, c.CONVERSION_FAILED)
        self.assertEqual(len(parts["queue"].dead), 1)
        self.assertEqual(parts["store"].list("outputs/"), [])

    def test_retry_delays_double(self):
        service, parts, _ = helpers.build(marker=helpers.FakeConverter("marker", fail_times=2))
        doc, conv, _ = service.submit(PDF, "a.pdf")
        delays = []
        while (pending := parts["queue"].jobs.get(routing.MARKER_HEAVY_QUEUE)):
            job, delay, _timeout = pending.pop(0)
            delays.append(delay)
            handle(service, job)
        self.assertEqual(delays, [0, 30, 60])
        self.assertEqual(service.status(doc.document_id)[0].state, c.VALIDATION_PENDING)

    def test_a_timeout_is_final(self):
        class TimesOut(helpers.FakeConverter):
            def convert(self, *a):
                raise ConversionTimeout("ran past 1800 s")
        service, parts, _ = helpers.build(marker=TimesOut("marker"))
        doc, conv, _ = service.submit(PDF, "a.pdf")
        drain(service, parts["queue"])
        doc, (conv,) = service.status(doc.document_id)
        self.assertEqual((conv.state, conv.attempts), (c.CONVERSION_FAILED, 1))
        self.assertIn("not retried", conv.error)
        self.assertEqual(len(parts["queue"].dead), 1)
        self.assertEqual(parts["store"].list("outputs/"), [])

    def test_an_unsafe_output_name_fails_the_conversion(self):
        service, parts, _ = helpers.build(
            marker=helpers.FakeConverter("marker", files={"../escape.md": b"x"}))
        doc, _, _ = service.submit(PDF, "a.pdf")
        drain(service, parts["queue"])
        self.assertEqual(service.status(doc.document_id)[0].state, c.CONVERSION_FAILED)
        self.assertFalse(any("escape" in k for k in parts["store"].list("")))

    def test_unknown_job_kind(self):
        with self.assertRaises(UnknownJob):
            handle(self.service, {"kind": "format-disk"})


class Died(BaseException):
    """What a killed worker looks like from inside: nothing after it runs."""


def die():
    raise Died()


class RecoveryTest(Base):
    """A worker that dies holding a job (Kamino item 87): the queue finds the
    job abandoned and hands it to worker.recover."""

    def start_and_die(self, service, queue):
        """Deliver the next job to a worker that is killed mid-conversion."""
        (job, _delay, _timeout) = queue.jobs[routing.MARKER_HEAVY_QUEUE].pop(0)
        with self.assertRaises(Died):
            handle(service, job)
        return job

    def dying(self):
        marker = helpers.FakeConverter("marker", on_convert=die)
        service, parts, _ = helpers.build(marker=marker)
        return service, parts, marker

    def test_a_conversion_killed_mid_attempt_is_retried_as_a_failed_attempt(self):
        service, parts, marker = self.dying()
        doc, conv, _ = service.submit(PDF, "a.pdf")
        job = self.start_and_die(service, parts["queue"])
        parts["store"].put(f"outputs/{doc.document_id}/{conv.conversion_id}/half.md", b"x")
        self.assertEqual(parts["registry"].get_conversion(conv.conversion_id).state, c.CONVERTING)
        recover(service, job)
        conv = parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual((conv.state, conv.attempts), (c.CONVERSION_PENDING, 1))
        self.assertEqual(conv.error, "worker died during attempt 1")
        self.assertEqual(parts["store"].list("outputs/"), [])
        (_job, delay, timeout), = parts["queue"].jobs[routing.MARKER_HEAVY_QUEUE]
        self.assertEqual(delay, 30)                       # the same backoff as an error
        self.assertIsNotNone(timeout)
        marker.on_convert = None
        drain(service, parts["queue"])
        self.assertEqual(service.status(doc.document_id)[0].state, c.VALIDATION_PENDING)

    def test_a_source_that_kills_every_worker_is_parked_after_max_attempts(self):
        service, parts, _ = self.dying()
        doc, conv, _ = service.submit(PDF, "a.pdf")
        for _ in range(3):
            recover(service, self.start_and_die(service, parts["queue"]))
        conv = parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual((conv.state, conv.attempts), (c.CONVERSION_FAILED, 3))
        self.assertEqual(conv.error, "worker died during attempt 3")
        self.assertEqual(service.status(doc.document_id)[0].state, c.CONVERSION_FAILED)
        self.assertEqual(len(parts["queue"].dead), 1)
        self.assertEqual(parts["queue"].jobs.get(routing.MARKER_HEAVY_QUEUE, []), [])

    def test_one_taken_but_not_started_is_queued_again_as_it_was(self):
        doc, conv, _ = self.service.submit(PDF, "a.pdf")
        (job, _d, timeout), = self.queue.jobs.pop(routing.MARKER_HEAVY_QUEUE)
        recover(self.service, job)
        conv = self.parts["registry"].get_conversion(conv.conversion_id)
        self.assertEqual((conv.state, conv.attempts, conv.error), (c.CONVERSION_PENDING, 0, None))
        self.assertEqual(self.queue.jobs[routing.MARKER_HEAVY_QUEUE], [(job, 0, timeout)])

    def test_one_that_finished_before_the_worker_died_is_left_alone(self):
        doc, (conv,) = self.submit_and_convert()
        recover(self.service, {"kind": "convert", "conversion_id": conv.conversion_id})
        self.assertEqual(self.queue.jobs.get(routing.MARKER_HEAVY_QUEUE, []), [])
        self.assertEqual(self.service.status(doc.document_id)[1][0].state, c.VALIDATION_PENDING)

    def test_one_whose_document_was_removed_meanwhile_fails(self):
        service, parts, _ = self.dying()
        doc, conv, _ = service.submit(PDF, "a.pdf")
        job = self.start_and_die(service, parts["queue"])
        service.remove(doc.document_id)
        recover(service, job)
        self.assertEqual(parts["registry"].get_conversion(conv.conversion_id).state,
                         c.CONVERSION_FAILED)

    def test_removals_and_prunes_run_again_whole(self):
        for job in ({"kind": "remove", "document_id": "d"},
                    {"kind": "prune", "document_id": "d", "keep_alternatives": False}):
            recover(self.service, job)
        self.assertEqual([j for j, _, _ in self.queue.jobs[routing.MAINTENANCE_QUEUE]],
                         [{"kind": "remove", "document_id": "d", "recoveries": 1},
                          {"kind": "prune", "document_id": "d", "keep_alternatives": False,
                           "recoveries": 1}])
        with self.assertRaises(UnknownJob):
            recover(self.service, {"kind": "format-disk"})

    def test_a_removal_that_kills_its_worker_every_time_is_parked(self):
        # Item 99: conversions stop after max_attempts; removals and prunes
        # were queued again for ever.
        job = {"kind": "remove", "document_id": "d"}
        for _ in range(self.service.settings.max_attempts):
            recover(self.service, job)
            pending = self.queue.jobs.get(routing.MAINTENANCE_QUEUE, [])
            job = pending.pop(0)[0] if pending else None
            if job is None:
                break
        self.assertIsNone(job, "still queued after max_attempts lost runs")
        ((queue, dead, reason),) = self.queue.dead
        self.assertEqual((queue, dead["document_id"]), (routing.MAINTENANCE_QUEUE, "d"))
        self.assertIn(f"died {self.service.settings.max_attempts} times", reason)


class ModesTest(Base):
    def setUp(self):
        super().setUp()
        self.doc, (self.first,) = self.submit_and_convert()
        self.doc_id = self.doc.document_id

    def roles(self):
        return {cv.conversion_id: cv.role for cv in self.service.status(self.doc_id)[1]}

    def test_update_makes_the_new_one_current_and_keeps_history(self):
        new = self.reconvert(self.doc_id, c.UPDATE)
        roles = self.roles()
        self.assertEqual(roles[new.conversion_id], c.CURRENT)
        self.assertEqual(roles[self.first.conversion_id], c.SUPERSEDED)
        self.assertTrue(self.store.list(self.first.output_key))
        self.assertEqual(self.events[-1]["previous_conversion_id"], self.first.conversion_id)

    def test_replace_deletes_only_the_current_output(self):
        second = self.reconvert(self.doc_id, c.UPDATE)
        second_key = c.output_prefix(self.doc_id, second.conversion_id)
        third = self.reconvert(self.doc_id, c.REPLACE)
        roles = self.roles()
        self.assertEqual(roles[third.conversion_id], c.CURRENT)
        self.assertEqual(roles[second.conversion_id], c.REPLACED)
        self.assertEqual(self.store.list(second_key), [])
        # Earlier history is untouched.
        self.assertEqual(roles[self.first.conversion_id], c.SUPERSEDED)
        self.assertTrue(self.store.list(self.first.output_key))

    def test_add_keeps_the_current_one_current(self):
        alt = self.reconvert(self.doc_id, c.ADD)
        roles = self.roles()
        self.assertEqual(roles[alt.conversion_id], c.ALTERNATIVE)
        self.assertEqual(roles[self.first.conversion_id], c.CURRENT)
        self.assertEqual(self.service.status(self.doc_id)[0].current_conversion_id,
                         self.first.conversion_id)

    def test_a_failed_reconversion_touches_nothing(self):
        self.converters["marker"].fail_times = 99
        failed = self.reconvert(self.doc_id, c.REPLACE)
        doc, conversions = self.service.status(self.doc_id)
        self.assertEqual(doc.state, c.VALIDATION_PENDING)
        self.assertEqual(doc.current_conversion_id, self.first.conversion_id)
        self.assertTrue(self.store.list(self.first.output_key))
        self.assertEqual({cv.conversion_id: cv.state for cv in conversions}[failed.conversion_id],
                         c.CONVERSION_FAILED)

    def test_mode_is_checked(self):
        with self.assertRaises(routing.InvalidRequest):
            self.service.reconvert(self.doc_id, "overwrite")
        with self.assertRaises(routing.InvalidRequest):
            self.service.reconvert(self.doc_id, c.INITIAL)

    def test_unknown_document(self):
        with self.assertRaises(NotFound):
            self.service.reconvert("nope")


class RemovalTest(Base):
    def test_removal_cascades_in_order_and_leaves_a_tombstone(self):
        doc, _ = self.submit_and_convert()
        order = []
        index, store = self.parts["index"], self.store
        real_doc, real_prefix, real_delete = index.delete_document, store.delete_prefix, store.delete
        index.delete_document = lambda d: (order.append("index"), real_doc(d))[1]
        store.delete_prefix = lambda p: (order.append("outputs") if p.startswith("outputs/") else None,
                                         real_prefix(p))[1]
        store.delete = lambda k: (order.append("source"), real_delete(k))[1]

        self.assertEqual(self.service.remove(doc.document_id).state, c.REMOVAL_PENDING)
        drain(self.service, self.queue)

        self.assertEqual(order, ["index", "outputs", "source"])
        self.assertEqual(self.store.list(""), [])
        tomb, conversions = self.service.status(doc.document_id)
        self.assertEqual(tomb.state, c.REMOVED)
        self.assertEqual(tomb.content_hash, doc.content_hash)
        self.assertIsNotNone(tomb.removed_at)
        self.assertEqual(tomb.original_name, "")
        self.assertIsNone(tomb.source_key)
        self.assertTrue(all(cv.deleted_at for cv in conversions))
        self.assertEqual(self.events[-1], {"event": c.EVENT_REMOVED, "document_id": doc.document_id})

    def test_asking_again_queues_again_and_the_second_run_changes_nothing(self):
        # Item 94: a removal stranded by an error (Qdrant or Garage down) stayed
        # removal_pending, and asking again returned early - for good. Now it
        # queues again; the cascade is written to run twice.
        doc, _ = self.submit_and_convert()
        self.service.remove(doc.document_id)
        self.service.remove(doc.document_id)
        self.assertEqual(self.queue.depth(routing.MAINTENANCE_QUEUE), 2)
        drain(self.service, self.queue)
        tomb, _ = self.service.status(doc.document_id)
        self.assertEqual(tomb.state, c.REMOVED)
        self.assertEqual([e for e in self.events if e["event"] == c.EVENT_REMOVED],
                         [{"event": c.EVENT_REMOVED, "document_id": doc.document_id}])

    def test_a_removed_document_asked_again_queues_nothing(self):
        doc, _ = self.submit_and_convert()
        self.service.remove(doc.document_id)
        drain(self.service, self.queue)
        self.service.remove(doc.document_id)
        self.assertEqual(self.queue.depth(routing.MAINTENANCE_QUEUE), 0)

    def test_a_removed_document_takes_no_more_requests(self):
        doc, _ = self.submit_and_convert()
        self.service.remove(doc.document_id)
        with self.assertRaises(Conflict):
            self.service.reconvert(doc.document_id)
        with self.assertRaises(Conflict):
            self.service.prune(doc.document_id)

    def test_the_same_content_after_removal_is_a_new_document(self):
        doc, _ = self.submit_and_convert()
        self.service.remove(doc.document_id)
        drain(self.service, self.queue)
        again, _, created = self.service.submit(PDF, "report.pdf")
        self.assertTrue(created)
        self.assertNotEqual(again.document_id, doc.document_id)

    def test_removed_while_converting(self):
        holder = {}

        def remove_mid_conversion():
            self.service.remove(holder["id"])
            self.service.run_removal(holder["id"])

        self.converters["marker"].on_convert = remove_mid_conversion
        doc, conv, _ = self.service.submit(PDF, "a.pdf")
        holder["id"] = doc.document_id
        drain(self.service, self.queue)
        _, (conv,) = self.service.status(doc.document_id)
        self.assertEqual(conv.state, c.CONVERSION_FAILED)
        self.assertEqual(self.store.list("outputs/"), [])


class IdTest(Base):
    """Kamino item 93: an ID is what new_id makes, or nothing at all. On
    MariaDB (PAD SPACE) "<id> " found the row and the removal deleted by the
    padded ID: source gone, outputs and vectors left, unreachable for good.
    These run on the real backends too (test_storage_backends, layer 3)."""

    def bad_ids(self, doc_id):
        return [doc_id + " ", doc_id + "  ", doc_id.upper(), " " + doc_id, doc_id + "\n", "", "../" + doc_id]

    def test_an_id_that_is_not_one_is_not_found(self):
        doc, _ = self.submit_and_convert()
        for bad in self.bad_ids(doc.document_id):
            with self.subTest(bad=bad):
                for call in (self.service.status, self.service.remove, self.service.prune,
                             self.service.reconvert):
                    with self.assertRaises(NotFound):
                        call(bad)
        self.assertEqual(self.queue.depth(routing.MAINTENANCE_QUEUE), 0)

    def test_a_removal_job_naming_a_padded_id_deletes_nothing(self):
        # A job that reached the queue another way: the cascade never runs on
        # an ID that is not one.
        doc, _ = self.submit_and_convert()
        outputs = self.store.list(c.output_prefix(doc.document_id))
        self.service.run_removal(doc.document_id + " ")
        again, _ = self.service.status(doc.document_id)
        self.assertEqual(again.state, doc.state)
        self.assertIsNotNone(again.source_key)
        self.assertEqual(self.store.list(c.output_prefix(doc.document_id)), outputs)

    def test_a_prune_job_naming_a_padded_id_prunes_nothing(self):
        doc, first = self.submit_and_convert()
        self.service.reconvert(doc.document_id)
        drain(self.service, self.queue)
        self.service.run_prune(doc.document_id + " ")
        _, conversions = self.service.status(doc.document_id)
        self.assertFalse(any(cv.role == c.PRUNED for cv in conversions))


class PruneTest(Base):
    def setUp(self):
        super().setUp()
        doc, (self.first,) = self.submit_and_convert()
        self.doc_id = doc.document_id
        self.second = self.reconvert(self.doc_id, c.UPDATE)
        self.alt = self.reconvert(self.doc_id, c.ADD)

    def roles(self):
        return {cv.conversion_id: cv.role for cv in self.service.status(self.doc_id)[1]}

    def test_prune_deletes_history_and_alternatives_but_not_the_current_one(self):
        self.service.prune(self.doc_id)
        drain(self.service, self.queue)
        roles = self.roles()
        self.assertEqual(roles[self.first.conversion_id], c.PRUNED)
        self.assertEqual(roles[self.alt.conversion_id], c.PRUNED)
        self.assertEqual(roles[self.second.conversion_id], c.CURRENT)
        self.assertTrue(self.store.list(c.output_prefix(self.doc_id, self.second.conversion_id)))
        self.assertEqual(self.store.list(c.output_prefix(self.doc_id, self.first.conversion_id)), [])
        # Only an alternative can have had points of its own.
        self.assertEqual(self.parts["index"].deleted_conversions, [self.alt.conversion_id])
        self.assertEqual(self.events[-1]["event"], c.EVENT_PRUNED)

    def test_keep_alternatives(self):
        self.service.prune(self.doc_id, keep_alternatives=True)
        drain(self.service, self.queue)
        roles = self.roles()
        self.assertEqual(roles[self.alt.conversion_id], c.ALTERNATIVE)
        self.assertEqual(roles[self.first.conversion_id], c.PRUNED)

    def test_pruned_rows_survive(self):
        self.service.prune(self.doc_id)
        drain(self.service, self.queue)
        pruned = [cv for cv in self.service.status(self.doc_id)[1] if cv.role == c.PRUNED]
        self.assertTrue(pruned and all(cv.deleted_at and cv.tool_versions for cv in pruned))


if __name__ == "__main__":
    unittest.main()
