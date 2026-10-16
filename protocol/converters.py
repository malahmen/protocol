"""The real converters: protocol's port onto protocol-droid's images.

marker and markitdown run in images of their own, built by protocol-droid
(`marker-service`, `markitdown-service`; Kamino items 75 and 76). Each image
carries the engine's side of the port, a module with

    convert_bytes(data, original_name, options) -> (files, tool_versions)

- `tasks` in marker's image, `markitdown_tasks` in markitdown's. This module is
protocol's side: it imports the engine's module only when a conversion (or a
warm-up) runs, so it imports anywhere and is tested without either engine, and
only a worker running inside the matching image can actually convert. A worker
started in the wrong image fails its first job with a message that says so,
rather than at import.

Transcription is not here: it calls Speaches over HTTP and arrives with its
own adapter (Kamino item 68).
"""
import importlib
from typing import Optional

from .ports import ConverterOutput

# Which module in which image holds each engine's convert_bytes.
ENGINE_MODULES = {
    "marker": "tasks",
    "markitdown": "markitdown_tasks",
}


class EngineMissing(RuntimeError):
    """The engine's module is not importable: the worker is not in its image."""


class EngineConverter:
    """One converter, backed by its engine's convert_bytes."""

    def __init__(self, name: str, module: Optional[str] = None):
        if module is None and name not in ENGINE_MODULES:
            raise ValueError(f"no engine module known for {name!r}")
        self.name = name
        self.module_name = module or ENGINE_MODULES[name]
        self._module = None

    def _engine(self):
        if self._module is None:
            try:
                self._module = importlib.import_module(self.module_name)
            except ImportError as exc:
                raise EngineMissing(
                    f"{self.name}: cannot import {self.module_name!r} - this worker is not running "
                    f"in the {self.name} image (protocol-droid's {self.name}-service)") from exc
            if not callable(getattr(self._module, "convert_bytes", None)):
                raise EngineMissing(
                    f"{self.name}: {self.module_name!r} has no convert_bytes - the image predates "
                    "the converter port (protocol-droid #26)")
        return self._module

    def convert(self, data: bytes, original_name: str, options: dict) -> ConverterOutput:
        # A copy: the engine fills in defaults, and the conversion's recorded
        # options must stay what the request asked for.
        files, versions = self._engine().convert_bytes(data, original_name, dict(options or {}))
        return ConverterOutput(dict(files), {str(k): str(v) for k, v in versions.items()})

    def warm_up(self) -> Optional[float]:
        """Pay the engine's first-conversion costs before taking jobs.

        marker's image loads its models and converts one OCR'd page with a
        table (protocol-droid's tasks.prewarm: a fresh worker's first scanned
        page took 25.5s cold, 15.2s warmed). markitdown has nothing to warm,
        so this returns None for it. Seconds taken otherwise, or None if the
        engine's own warm-up was skipped or failed - it reports, never raises.
        """
        engine = self._engine()
        if callable(getattr(engine, "get_models", None)):
            engine.get_models()
        prewarm = getattr(engine, "prewarm", None)
        return prewarm() if callable(prewarm) else None


def engine_converters() -> dict:
    """The converters this worker could run, by name. Which one it does run is
    decided by the queue it consumes (and so by the image it was started in)."""
    return {name: EngineConverter(name) for name in ENGINE_MODULES}
