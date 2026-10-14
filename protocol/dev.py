"""A development server: the real API over in-memory backends.

    python -m protocol.dev            # http://127.0.0.1:8080, docs at /docs

For trying the contract's shape with curl before any backend exists. Nothing
persists, and **nothing is really converted**: every converter is replaced by
a stand-in that decodes the source as UTF-8 and writes it back as `out.md`.
Jobs run in a background thread a moment after they are queued, so the
202-then-poll flow behaves as it will in a deployment.
"""
import argparse
import logging
import threading
import time

from . import memory
from .api import create_app
from .config import Settings
from .ports import ConverterOutput
from .routing import CONVERTERS
from .service import ConversionService
from .worker import handle

log = logging.getLogger("protocol.dev")


class EchoConverter:
    """Stand-in only. Writes the source back as Markdown, if it is text at all."""

    def __init__(self, name: str):
        self.name = name

    def convert(self, data: bytes, original_name: str, options: dict) -> ConverterOutput:
        text = data.decode("utf-8")  # binary input fails, and exercises the retry path
        return ConverterOutput({"out.md": text.encode()}, {"dev-echo": "0"})


def build() -> tuple[ConversionService, memory.Queue]:
    queue = memory.Queue()
    service = ConversionService(memory.Registry(), memory.Store(), queue, memory.Events(),
                                memory.Index(), {name: EchoConverter(name) for name in CONVERTERS},
                                Settings.from_env())
    return service, queue


def _work(service: ConversionService, queue: memory.Queue) -> None:
    while True:
        job = queue.pop_any()
        if job is None:
            time.sleep(0.5)
            continue
        try:
            handle(service, job)
        except Exception:  # noqa: BLE001 - a dev worker logs and carries on
            log.exception("job failed: %s", job)


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    log.warning("in-memory backends and a stand-in converter: nothing persists, nothing is really converted")
    service, queue = build()
    threading.Thread(target=_work, args=(service, queue), daemon=True).start()
    uvicorn.run(create_app(service), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
