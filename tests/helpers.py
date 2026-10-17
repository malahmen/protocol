"""Shared fixtures: an in-memory service and converters that do as they are told."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from protocol import memory  # noqa: E402
from protocol.config import Settings  # noqa: E402
from protocol.ports import ConverterOutput  # noqa: E402
from protocol.routing import CONVERTERS  # noqa: E402
from protocol.service import ConversionService  # noqa: E402


class FakeConverter:
    """Succeeds with `files`, or raises for the first `fail_times` calls."""

    def __init__(self, name, files=None, fail_times=0, on_convert=None):
        self.name = name
        self.files = files if files is not None else {"out.md": b"# converted\n"}
        self.fail_times = fail_times
        self.on_convert = on_convert
        self.calls = []

    def convert(self, data, original_name, options):
        self.calls.append((data, original_name, dict(options)))
        if self.on_convert:
            self.on_convert()
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("converter exploded")
        return ConverterOutput(dict(self.files), {self.name: "1.0"})


# Set by test_storage_backends while it reruns test_service on real backends:
# a function returning the parts to use instead of the in-memory ones (any
# subset of "registry", "store", "index"). None: everything in memory.
BACKENDS = None


def build(settings=None, **converter_overrides):
    converters = {name: FakeConverter(name) for name in CONVERTERS}
    converters.update(converter_overrides)
    parts = {
        "registry": memory.Registry(),
        "store": memory.Store(),
        "queue": memory.Queue(),
        "events": memory.Events(),
        "index": memory.Index(),
    }
    if BACKENDS is not None:
        parts.update(BACKENDS())
    service = ConversionService(converters=converters, settings=settings or Settings(), **parts)
    return service, parts, converters
