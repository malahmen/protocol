"""The deployment: the real service, built from the environment (Kamino item 70).

    python -m protocol.deploy api [--host H] [--port P]   the HTTP API
    python -m protocol.deploy worker <name>               one worker (WORKERS)
    python -m protocol.deploy check                       every backend, reachable?
    python -m protocol.deploy queues                      each queue's depth and dead letters

Everything else in this package is wiring-free: the service takes its
backends as arguments (ports.py), so the tests run it on memory.py. This is
the one place that picks the real ones - MariaDB, Garage, Redis, Qdrant and
protocol-droid's engines - from the variables in the README's Configuration
section. scomp-link's protocol.sh runs these commands in containers.

**The API** creates the registry's tables before it serves (CREATE TABLE IF
NOT EXISTS, so every start is the same; the database itself is kuat's). It
converts nothing, so it carries no converter.

**A worker** is one process per lane, named in WORKERS: marker's light and
heavy lanes each get their own (cost.py - a small document must never wait
behind a book), markitdown one, and the removals and prunes one. A converter's
worker runs inside that converter's image, where its engine module imports;
it warms the engine up before it takes a job, so the first document does not
pay for the model load.

**Qdrant before ingestion exists.** Which collections hold the points is the
ingestion side's choice (qdrant_index.py), and nothing ingests yet. Without
QDRANT_COLLECTIONS the service runs with an index that removes nothing and
says so in the log on every removal, and `check` reports it - rather than
refusing to start, or guessing a collection name that removal would then
trust.
"""
import argparse
import json
import logging
import os
import sys
import urllib.request
from typing import Optional

from . import mariadb_registry, redis_backends, routing, s3_store
from .config import Settings
from .converters import EngineConverter
from .qdrant_index import QdrantIndex
from .service import ConversionService

log = logging.getLogger("protocol.deploy")

# name -> (the converter it runs, or None; the queues it works, most urgent first)
WORKERS = {
    "marker-light": ("marker", (routing.MARKER_LIGHT_QUEUE,)),
    "marker-heavy": ("marker", (routing.MARKER_HEAVY_QUEUE,)),
    "markitdown": ("markitdown", (routing.QUEUES["markitdown"],)),
    "maintenance": (None, (routing.MAINTENANCE_QUEUE,)),
}


class UnconfiguredIndex:
    """ports.VectorIndex while no collection is named: removes nothing, loudly."""

    def _none(self, what: str, ident: str) -> int:
        log.warning("QDRANT_COLLECTIONS is not set: no vectors removed for %s %s", what, ident)
        return 0

    def delete_document(self, document_id: str) -> int:
        return self._none("document", document_id)

    def delete_conversion(self, conversion_id: str) -> int:
        return self._none("conversion", conversion_id)


def index_from_env():
    if os.environ.get("QDRANT_COLLECTIONS", "").strip():
        return QdrantIndex.from_env()
    log.warning("QDRANT_COLLECTIONS is not set: removal will delete no vectors")
    return UnconfiguredIndex()


def build(converter: Optional[str] = None) -> ConversionService:
    """The service on the real backends, with `converter`'s engine if named."""
    redis = redis_backends.connect()
    converters = {converter: EngineConverter(converter)} if converter else {}
    return ConversionService(
        registry=mariadb_registry.MariaDBRegistry(),
        store=s3_store.S3Store(s3_store.connect(), s3_store.buckets_from_env()),
        queue=redis_backends.RQJobQueue(redis),
        events=redis_backends.RedisStream(redis),
        index=index_from_env(),
        converters=converters,
        settings=Settings.from_env(),
    )


# --- commands ----------------------------------------------------------------

def serve(host: str, port: int) -> None:
    import uvicorn

    from .api import create_app

    conn = mariadb_registry.connect()
    try:
        mariadb_registry.create_schema(conn)
    finally:
        conn.close()
    log.info("registry schema in place")
    uvicorn.run(create_app(build()), host=host, port=port)


def work(name: str) -> None:
    converter, queues = WORKERS[name]
    service = build(converter)
    if converter:
        seconds = service.converters[converter].warm_up()
        log.info("%s warmed up%s", converter, f" in {seconds:.1f}s" if seconds else "")
    log.info("worker %s on %s", name, ", ".join(queues))
    redis_backends.run_worker(service, queues, redis_backends.connect())


def _qdrant_check() -> str:
    url = os.environ.get("QDRANT_URL", "").strip().rstrip("/")
    if not url:
        raise RuntimeError("QDRANT_URL is not set")
    req = urllib.request.Request(url + "/collections")
    if os.environ.get("QDRANT_API_KEY"):
        req.add_header("api-key", os.environ["QDRANT_API_KEY"])
    with urllib.request.urlopen(req, timeout=10) as reply:
        have = {c["name"] for c in json.load(reply)["result"]["collections"]}
    named = [n.strip() for n in os.environ.get("QDRANT_COLLECTIONS", "").split(",") if n.strip()]
    if not named:
        return "reachable; WARNING: QDRANT_COLLECTIONS is not set, so removal deletes no vectors"
    missing = [n for n in named if n not in have]
    return "reachable" + (f"; not created yet (ingestion makes them): {', '.join(missing)}" if missing else "")


def _mariadb_check() -> str:
    conn = mariadb_registry.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() "
                        "AND TABLE_NAME IN ('documents', 'conversions')")
            tables = cur.fetchone()[0]
    finally:
        conn.close()
    return "reachable; " + ("schema in place" if tables == 2 else "schema not created yet (the API makes it)")


def _s3_check() -> str:
    client = s3_store.connect()
    buckets = s3_store.buckets_from_env()
    for bucket in buckets.values():
        client.head_bucket(Bucket=bucket)
    return "reachable; buckets " + ", ".join(buckets.values())


def _redis_check() -> str:
    redis_backends.connect().ping()
    return "reachable"


def queues() -> int:
    """Each queue: jobs waiting, jobs dead-lettered, and which worker takes it."""
    q = redis_backends.RQJobQueue(redis_backends.connect())
    takers = {queue: name for name, (_, qs) in WORKERS.items() for queue in qs}
    names = sorted(set(takers) | {routing.QUEUES["transcription"]})
    for name in names:
        dead = len(q.dead_letters(routing.QUEUES["marker"] if name.startswith("convert.marker.") else name))
        print(f"{name:24} waiting {q.depth(name):5}   dead {dead:5}   "
              f"{'worker ' + takers[name] if name in takers else 'NO WORKER'}")
    return 0


def check() -> int:
    """One line per backend, without a credential in any of them. 0 = all reachable."""
    failed = 0
    for name, probe in (("MariaDB", _mariadb_check), ("Redis", _redis_check),
                        ("Garage", _s3_check), ("Qdrant", _qdrant_check)):
        try:
            print(f"ok    {name}: {probe()}")
        except (Exception, SystemExit) as exc:  # noqa: BLE001 - report each, then the verdict
            failed += 1
            print(f"FAIL  {name}: {type(exc).__name__}: {exc}")
    return 1 if failed else 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m protocol.deploy", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    api = sub.add_parser("api", help="the HTTP API")
    api.add_argument("--host", default="0.0.0.0")
    api.add_argument("--port", type=int, default=8080)
    w = sub.add_parser("worker", help="one worker")
    w.add_argument("name", choices=sorted(WORKERS))
    sub.add_parser("check", help="is every backend reachable?")
    sub.add_parser("queues", help="each queue's depth and dead letters")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.command == "api":
        serve(args.host, args.port)
    elif args.command == "worker":
        work(args.name)
    elif args.command == "queues":
        return queues()
    else:
        return check()
    return 0


if __name__ == "__main__":
    sys.exit(main())
