"""The storage adapters against the real backends (Kamino item 69):
s3_store on Garage, qdrant_index on Qdrant, mariadb_registry on MariaDB.

Three layers:

1. **Behaviour shared with the reference.** Each port's behaviour is written
   once and run against memory.py AND the real adapter, so "passes in memory"
   keeps meaning "the real one does the same" (memory.py's promise).
2. **What only the real one has**: Garage's batching and its refusals,
   Qdrant's filters, MariaDB's unique key under a race.
3. **The whole service suite again** - every test in test_service.py, with the
   registry on MariaDB and the stores on Garage underneath.

Needs boto3 and PyMySQL (requirements.txt) and the backends:

    eval "$(tests/backends/up.sh)"       # throwaway Garage, Qdrant, MariaDB
    python3 -m unittest discover -s tests

**The suite empties what it is pointed at** - both buckets, its Qdrant
collections, both tables. There are no default addresses, so it can never
reach a real backend by accident; without the variables these checks skip.
"""
import json
import os
import threading
import unittest
import unittest.mock
import urllib.parse
from datetime import datetime, timedelta, timezone

import helpers
import test_service
from protocol import contract as c
from protocol import memory

try:
    from protocol import s3_store
    HAVE_S3 = True
except ImportError:  # pragma: no cover - the stdlib-only run
    HAVE_S3 = False
try:
    from protocol import mariadb_registry
    HAVE_PYMYSQL = True
except ImportError:  # pragma: no cover
    HAVE_PYMYSQL = False
from protocol import qdrant_index  # stdlib only

S3_ENDPOINT = os.environ.get("PROTOCOL_TEST_S3_ENDPOINT", "")
QDRANT_URL = os.environ.get("PROTOCOL_TEST_QDRANT_URL", "")
MARIADB_URL = os.environ.get("PROTOCOL_TEST_MARIADB_URL", "")
COLLECTIONS = ["protocol_test_a", "protocol_test_b"]
FOREIGN = "protocol_test_not_ours"


# --- connections ---------------------------------------------------------------

def s3_client():
    return s3_store.connect(S3_ENDPOINT, os.environ["PROTOCOL_TEST_S3_ACCESS_KEY_ID"],
                            os.environ["PROTOCOL_TEST_S3_SECRET_ACCESS_KEY"])


def db_connect():
    u = urllib.parse.urlsplit(MARIADB_URL)
    return mariadb_registry.connect(u.hostname, u.port or 3306, urllib.parse.unquote(u.username),
                                    urllib.parse.unquote(u.password or ""), u.path.lstrip("/"))


def _up(check) -> bool:
    try:
        check()
        return True
    except Exception:  # noqa: BLE001
        return False


S3_UP = HAVE_S3 and bool(S3_ENDPOINT) and _up(lambda: s3_client().head_bucket(Bucket="sources"))
DB_UP = HAVE_PYMYSQL and bool(MARIADB_URL) and _up(lambda: db_connect().ping())
QDRANT_UP = bool(QDRANT_URL) and _up(lambda: qdrant_index.urllib.request.urlopen(QDRANT_URL + "/readyz", timeout=5))
NEEDS_S3 = unittest.skipUnless(S3_UP, "set PROTOCOL_TEST_S3_* to a throwaway Garage (tests/backends/up.sh)")
NEEDS_DB = unittest.skipUnless(DB_UP, "set PROTOCOL_TEST_MARIADB_URL to a throwaway MariaDB (tests/backends/up.sh)")
NEEDS_QDRANT = unittest.skipUnless(QDRANT_UP, "set PROTOCOL_TEST_QDRANT_URL to a throwaway Qdrant (tests/backends/up.sh)")


def empty_buckets():
    client = s3_client()
    for bucket in s3_store.STORES:
        store = s3_store.S3Store(client)
        for name in store._names(bucket, ""):
            client.delete_object(Bucket=bucket, Key=name)


def fresh_registry():
    """Both tables dropped and made again from SCHEMA - so the DDL itself is
    under test, not a schema left behind by an earlier run."""
    conn = db_connect()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS conversions")
        cur.execute("DROP TABLE IF EXISTS documents")
    mariadb_registry.create_schema(conn)
    conn.close()
    return mariadb_registry.MariaDBRegistry(db_connect)


# --- 1. behaviour shared with the reference ------------------------------------

T0 = datetime(2026, 10, 17, 9, 30, 0, 123456, tzinfo=timezone.utc)


def doc(document_id="d1", content_hash="a" * 64, **kw):
    fields = dict(original_name="report.pdf", media_type="application/pdf", size=10,
                  source_key=f"sources/{document_id}/report.pdf", submitted_at=T0)
    fields.update(kw)
    return c.Document(document_id=document_id, content_hash=content_hash, **fields)


def conv(conversion_id="c1", document_id="d1", **kw):
    fields = dict(mode=c.INITIAL, converter="marker", created_at=T0)
    fields.update(kw)
    return c.Conversion(conversion_id=conversion_id, document_id=document_id, **fields)


class RegistryBehaviour:
    """Every Registry must do this. setUp provides self.registry."""

    def test_the_first_document_for_a_hash_is_created_the_second_is_not(self):
        first, created = self.registry.add_document_if_absent(doc("d1"))
        self.assertTrue(created)
        again, created = self.registry.add_document_if_absent(doc("d2"))
        self.assertFalse(created)
        self.assertEqual(again.document_id, "d1")
        self.assertIsNone(self.registry.get_document("d2"))

    def test_a_removed_documents_hash_is_free_again(self):
        for h, state in (("1" * 64, c.REMOVAL_PENDING), ("2" * 64, c.REMOVED)):
            with self.subTest(state=state):
                old, _ = self.registry.add_document_if_absent(doc("old-" + state, h))
                old.state = state
                self.registry.save_document(old)
                self.assertIsNone(self.registry.find_live_by_hash(h))
                new, created = self.registry.add_document_if_absent(doc("new-" + state, h))
                self.assertTrue(created)
                self.assertEqual(self.registry.find_live_by_hash(h).document_id, "new-" + state)
                self.assertEqual(self.registry.get_document("old-" + state).state, state)

    def test_a_document_round_trips_every_field(self):
        original = doc("d1", submitted_by="tester", state=c.VALIDATION_PENDING,
                       current_conversion_id="c9", removed_at=T0 + timedelta(hours=1))
        self.registry.add_document_if_absent(original)
        self.assertEqual(self.registry.get_document("d1"), original)

    def test_a_time_comes_back_as_the_same_instant(self):
        local = datetime(2026, 10, 17, 10, 30, tzinfo=timezone(timedelta(hours=1)))
        self.registry.add_document_if_absent(doc("d1", submitted_at=local))
        self.assertEqual(self.registry.get_document("d1").submitted_at, local)

    def test_reads_are_copies(self):
        self.registry.add_document_if_absent(doc("d1"))
        mine = self.registry.get_document("d1")
        mine.state = c.REMOVED
        self.assertEqual(self.registry.get_document("d1").state, c.CONVERSION_PENDING)

    def test_save_changes_only_that_document(self):
        self.registry.add_document_if_absent(doc("d1", "1" * 64))
        self.registry.add_document_if_absent(doc("d2", "2" * 64))
        d1 = self.registry.get_document("d1")
        d1.state, d1.current_conversion_id = c.VALIDATION_PENDING, "c1"
        self.registry.save_document(d1)
        self.assertEqual(self.registry.get_document("d1").current_conversion_id, "c1")
        self.assertEqual(self.registry.get_document("d2").state, c.CONVERSION_PENDING)

    def test_missing_records_are_none(self):
        self.assertIsNone(self.registry.get_document("nope"))
        self.assertIsNone(self.registry.get_conversion("nope"))
        self.assertEqual(self.registry.conversions_of("nope"), [])
        self.assertIsNone(self.registry.find_live_by_hash("f" * 64))

    def test_a_conversion_round_trips_every_field(self):
        self.registry.add_document_if_absent(doc("d1"))
        original = conv("c1", options={"page_range": "0-3", "nested": {"a": [1, 2]}},
                        state=c.VALIDATION_PENDING, role=c.CURRENT, output_key="outputs/d1/c1/",
                        tool_versions={"marker-pdf": "2.0.0"}, attempts=2,
                        started_at=T0, finished_at=T0 + timedelta(seconds=5),
                        error="RuntimeError: once", deleted_at=None)
        self.registry.add_conversion(original)
        self.assertEqual(self.registry.get_conversion("c1"), original)
        original.role, original.deleted_at = c.PRUNED, T0 + timedelta(days=1)
        self.registry.save_conversion(original)
        self.assertEqual(self.registry.get_conversion("c1"), original)

    def test_conversions_come_in_creation_order_ties_in_insertion_order(self):
        self.registry.add_document_if_absent(doc("d1"))
        for cid, at in (("late", T0 + timedelta(seconds=1)), ("tie-1", T0), ("tie-2", T0),
                        ("early", T0 - timedelta(seconds=1))):
            self.registry.add_conversion(conv(cid, created_at=at))
        self.assertEqual([cv.conversion_id for cv in self.registry.conversions_of("d1")],
                         ["early", "tie-1", "tie-2", "late"])

    def test_concurrent_submissions_of_one_content_make_one_document(self):
        results, barrier = [], threading.Barrier(8)

        def submit(i):
            barrier.wait()
            results.append(self.registry.add_document_if_absent(doc(f"d{i}", "c" * 64)))

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sum(created for _, created in results), 1)
        self.assertEqual(len({d.document_id for d, _ in results}), 1)


class StoreBehaviour:
    """Every ObjectStore must do this. setUp provides self.store."""

    def test_put_get_and_overwrite(self):
        self.store.put("sources/d1/a.pdf", b"\x00\xffbinary")
        self.assertEqual(self.store.get("sources/d1/a.pdf"), b"\x00\xffbinary")
        self.store.put("sources/d1/a.pdf", b"")
        self.assertEqual(self.store.get("sources/d1/a.pdf"), b"")

    def test_a_missing_object_is_a_key_error(self):
        with self.assertRaises(KeyError):
            self.store.get("sources/nope/a.pdf")

    def test_list_is_sorted_and_respects_the_prefix_exactly(self):
        for key in ("outputs/d10/c/x.md", "outputs/d1/c2/b.md", "outputs/d1/c1/a.md", "sources/d1/s.pdf"):
            self.store.put(key, b"x")
        self.assertEqual(self.store.list("outputs/d1/"), ["outputs/d1/c1/a.md", "outputs/d1/c2/b.md"])
        self.assertEqual(self.store.list("outputs/d1"),
                         ["outputs/d1/c1/a.md", "outputs/d1/c2/b.md", "outputs/d10/c/x.md"])
        self.assertEqual(self.store.list(""), ["outputs/d1/c1/a.md", "outputs/d1/c2/b.md",
                                               "outputs/d10/c/x.md", "sources/d1/s.pdf"])

    def test_names_with_spaces_and_unicode(self):
        key = "sources/d1/Relatório final (v2) ✓.pdf"
        self.store.put(key, b"x")
        self.assertEqual(self.store.list("sources/d1/"), [key])
        self.assertEqual(self.store.get(key), b"x")

    def test_delete_is_quiet_about_what_is_not_there(self):
        self.store.put("sources/d1/a.pdf", b"x")
        self.store.delete("sources/d1/a.pdf")
        self.store.delete("sources/d1/a.pdf")
        self.assertEqual(self.store.list("sources/"), [])

    def test_delete_prefix_counts_and_spares_the_neighbours(self):
        for key in ("outputs/d1/c1/a.md", "outputs/d1/c1/b/c.md", "outputs/d1/c10/a.md", "outputs/d2/c1/a.md"):
            self.store.put(key, b"x")
        self.assertEqual(self.store.delete_prefix("outputs/d1/c1/"), 2)
        self.assertEqual(self.store.list(""), ["outputs/d1/c10/a.md", "outputs/d2/c1/a.md"])
        self.assertEqual(self.store.delete_prefix("outputs/d9/"), 0)


class MemoryRegistry(RegistryBehaviour, unittest.TestCase):
    def setUp(self):
        self.registry = memory.Registry()


@NEEDS_DB
class MariaDBRegistry(RegistryBehaviour, unittest.TestCase):
    def setUp(self):
        self.registry = fresh_registry()


class MemoryStore(StoreBehaviour, unittest.TestCase):
    def setUp(self):
        self.store = memory.Store()


@NEEDS_S3
class GarageStore(StoreBehaviour, unittest.TestCase):
    def setUp(self):
        empty_buckets()
        self.store = s3_store.S3Store(s3_client())


# --- 2. what only the real ones have -------------------------------------------

@NEEDS_S3
class GarageOnly(unittest.TestCase):
    def setUp(self):
        empty_buckets()
        self.store = s3_store.S3Store(s3_client())

    def test_each_store_is_its_own_bucket(self):
        self.store.put("sources/d1/a.pdf", b"s")
        self.store.put("outputs/d1/c1/a.md", b"o")
        client = self.store.client
        self.assertEqual(client.get_object(Bucket="sources", Key="d1/a.pdf")["Body"].read(), b"s")
        self.assertEqual(client.get_object(Bucket="outputs", Key="d1/c1/a.md")["Body"].read(), b"o")

    def test_delete_prefix_beyond_one_batch(self):
        for i in range(1005):                     # S3 deletes at most 1000 per request
            self.store.put(f"outputs/d1/c1/page-{i:04}.md", b"x")
        self.assertEqual(self.store.delete_prefix("outputs/d1/"), 1005)
        self.assertEqual(self.store.list("outputs/"), [])

    def test_a_whole_bucket_is_never_emptied(self):
        self.store.put("outputs/d1/c1/a.md", b"x")
        for prefix in ("outputs/", "outputs", "", "o"):
            with self.subTest(prefix=prefix), self.assertRaises(ValueError):
                self.store.delete_prefix(prefix)
        self.assertEqual(self.store.list(""), ["outputs/d1/c1/a.md"])

    def test_a_key_outside_the_stores_is_refused(self):
        for key in ("elsewhere/d1/a.pdf", "sources", "sources/", "/sources/d1"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.store.put(key, b"x")


@NEEDS_DB
class MariaDBOnly(unittest.TestCase):
    def setUp(self):
        self.registry = fresh_registry()

    def test_create_schema_is_repeatable(self):
        conn = db_connect()
        mariadb_registry.create_schema(conn)
        mariadb_registry.create_schema(conn)

    def test_a_conversion_needs_its_document(self):
        import pymysql
        with self.assertRaises(pymysql.err.IntegrityError):
            self.registry.add_conversion(conv("c1", document_id="nobody"))

    def test_a_long_media_type_and_a_255_character_name_fit(self):
        long = doc("d1", original_name="é" * 255, media_type="application/x-" + "a" * 2000)
        self.registry.add_document_if_absent(long)
        self.assertEqual(self.registry.get_document("d1"), long)

    def test_the_live_hash_matches_document_live(self):
        """The generated column restates contract.Document.live in SQL; this
        is where the two would drift apart."""
        states = [c.CONVERSION_PENDING, c.CONVERTING, c.CONVERSION_FAILED, c.VALIDATION_PENDING,
                  c.REMOVAL_PENDING, c.REMOVED]
        for i, state in enumerate(states):
            d = doc(f"d{i}", f"{i}" * 64, state=state)
            self.registry.add_document_if_absent(d)
            found = self.registry.find_live_by_hash(d.content_hash)
            self.assertEqual(found is not None, d.live, state)

    def test_times_are_stored_as_utc_and_come_back_utc(self):
        """The reference keeps whatever it is given (always UTC, from
        contract.now()); DATETIME has no zone, so MariaDB must normalise."""
        local = datetime(2026, 10, 17, 10, 30, tzinfo=timezone(timedelta(hours=1)))
        self.registry.add_document_if_absent(doc("d1", submitted_at=local))
        self.assertEqual(self.registry.get_document("d1").submitted_at.utcoffset(), timedelta(0))
        with db_connect().cursor() as cur:
            cur.execute("SELECT submitted_at FROM documents WHERE document_id = 'd1'")
            self.assertEqual(cur.fetchone()[0], datetime(2026, 10, 17, 9, 30))

    def test_a_dropped_connection_comes_back(self):
        self.registry.add_document_if_absent(doc("d1"))
        self.registry._local.conn.close()
        self.assertEqual(self.registry.get_document("d1").document_id, "d1")


@NEEDS_QDRANT
class QdrantIndex(unittest.TestCase):
    def setUp(self):
        self.index = qdrant_index.QdrantIndex(QDRANT_URL, COLLECTIONS)
        for name in COLLECTIONS + [FOREIGN]:
            self._call("DELETE", f"/collections/{name}")
            self._call("PUT", f"/collections/{name}", {"vectors": {"size": 2, "distance": "Dot"}})
        points = [(1, "d1", "c1"), (2, "d1", "c1"), (3, "d1", "c2"), (4, "d2", "c3")]
        for name in COLLECTIONS + [FOREIGN]:
            self._call("PUT", f"/collections/{name}/points?wait=true", {"points": [
                {"id": pid, "vector": [1.0, 0.0], "payload": {"document_id": d, "conversion_id": cv}}
                for pid, d, cv in points]})

    def tearDown(self):
        for name in COLLECTIONS + [FOREIGN]:
            self._call("DELETE", f"/collections/{name}")

    def _call(self, method, path, body=None):
        req = qdrant_index.urllib.request.Request(
            QDRANT_URL + path, method=method, data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"})
        try:
            with qdrant_index.urllib.request.urlopen(req, timeout=30) as reply:
                return json.load(reply)["result"]
        except qdrant_index.urllib.error.HTTPError as exc:
            if method == "DELETE" and exc.code == 404:
                return None
            raise

    def ids(self, name):
        res = self._call("POST", f"/collections/{name}/points/scroll", {"limit": 100})
        return sorted(p["id"] for p in res["points"])

    def test_a_documents_points_go_from_every_named_collection(self):
        self.assertEqual(self.index.delete_document("d1"), 6)      # 3 in each of the two
        for name in COLLECTIONS:
            self.assertEqual(self.ids(name), [4])

    def test_a_collection_it_was_not_given_is_untouched(self):
        self.index.delete_document("d1")
        self.assertEqual(self.ids(FOREIGN), [1, 2, 3, 4])

    def test_a_conversions_points_go_and_its_documents_others_stay(self):
        self.assertEqual(self.index.delete_conversion("c1"), 4)
        for name in COLLECTIONS:
            self.assertEqual(self.ids(name), [3, 4])

    def test_nothing_to_delete_is_zero(self):
        self.assertEqual(self.index.delete_document("nobody"), 0)

    def test_a_collection_that_does_not_exist_yet_is_zero(self):
        self.assertEqual(qdrant_index.QdrantIndex(QDRANT_URL, ["protocol_test_never_made"])
                         .delete_document("d1"), 0)

    def test_qdrant_refusing_is_an_error_not_a_zero(self):
        index = qdrant_index.QdrantIndex(QDRANT_URL, COLLECTIONS)
        # A malformed filter, on purpose: Qdrant's 400 must surface.
        with self.assertRaises(qdrant_index.QdrantError):
            index._post(f"/collections/{COLLECTIONS[0]}/points/count", {"filter": {"must": "not a list"}})


class QdrantSettings(unittest.TestCase):
    def test_collections_must_be_named(self):
        with self.assertRaises(ValueError):
            qdrant_index.QdrantIndex("http://x", [])
        env = {"QDRANT_URL": "http://x", "QDRANT_COLLECTIONS": " , "}
        with unittest.mock.patch.dict(os.environ, env), self.assertRaises(SystemExit):
            qdrant_index.QdrantIndex.from_env()


# --- 3. the whole service suite on the real backends ---------------------------

def _real_parts():
    parts = {}
    if S3_UP:
        empty_buckets()
        parts["store"] = s3_store.S3Store(s3_client())
    if DB_UP:
        parts["registry"] = fresh_registry()
    return parts


def _on_real_backends(case):
    class OnRealBackends(case):
        def setUp(self):
            helpers.BACKENDS = _real_parts
            self.addCleanup(setattr, helpers, "BACKENDS", None)
            super().setUp()

    OnRealBackends.__name__ = OnRealBackends.__qualname__ = case.__name__ + "OnRealBackends"
    return unittest.skipUnless(S3_UP and DB_UP, "needs both Garage and MariaDB")(OnRealBackends)


for _case in (test_service.SubmitTest, test_service.ConversionTest, test_service.ModesTest,
              test_service.RemovalTest, test_service.PruneTest):
    globals()[_case.__name__ + "OnRealBackends"] = _on_real_backends(_case)
del _case


if __name__ == "__main__":
    unittest.main()
