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

Transcription is the exception: SpeachesConverter calls Speaches' OpenAI-style
API over HTTP (Kamino item 89), so its worker runs in protocol's own image with
no Whisper in it - the model stays loaded in Speaches, on corelia.
"""
import importlib
import json
import logging
import os
import time
import urllib.error
import urllib.request
import uuid
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


log = logging.getLogger("protocol.converters")

DEFAULT_SPEACHES_MODEL = "deepdml/faster-whisper-large-v3-turbo-ct2"


def _multipart(fields: dict, file_field: str, filename: str, data: bytes) -> tuple[bytes, str]:
    """A multipart/form-data body, by hand: urllib has no encoder for one."""
    boundary = "protocol-" + uuid.uuid4().hex
    safe = "".join(ch for ch in os.path.basename(filename) if ch not in '"\\\r\n') or "recording"
    out = []
    for key, value in fields.items():
        out.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode())
    out.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{file_field}"; filename="{safe}"\r\n'
               "Content-Type: application/octet-stream\r\n\r\n".encode())
    out.append(data)
    out.append(f"\r\n--{boundary}--\r\n".encode())
    return b"".join(out), f"multipart/form-data; boundary={boundary}"


class SpeachesConverter:
    """Transcription, by Speaches' /v1/audio/transcriptions (Kamino item 89).

    SPEACHES_URL names the server; SPEACHES_MODEL the model a request names
    none of; SPEACHES_API_KEY, if set, is sent as a bearer token. A request's
    `model` and `language` options (validated in routing) are passed through.

    Output: `<stem>.md`, the text - what ingestion reads, as it reads
    markitdown's - and `<stem>.transcript.json`, Speaches' whole verbose answer
    (language, duration, timed segments). Speaches has no version endpoint,
    so the model is what tool_versions records.
    """

    name = "transcription"

    def __init__(self, url: Optional[str] = None, model: Optional[str] = None,
                 api_key: Optional[str] = None, http_timeout: float = 21600):
        self.url = (url if url is not None else os.environ.get("SPEACHES_URL", "")).strip().rstrip("/")
        self.model = (model or os.environ.get("SPEACHES_MODEL", "") or DEFAULT_SPEACHES_MODEL).strip()
        self.api_key = api_key if api_key is not None else os.environ.get("SPEACHES_API_KEY", "")
        # The job's own timeout (cost.py) is what stops a long transcription;
        # this only keeps a dead connection from outliving it.
        self.http_timeout = http_timeout

    def _call(self, path: str, body: Optional[bytes] = None, content_type: Optional[str] = None,
              timeout: float = 30):
        if not self.url:
            raise EngineMissing("transcription: SPEACHES_URL is not set for this worker")
        req = urllib.request.Request(self.url + path, data=body, method="POST" if body is not None else "GET")
        if content_type:
            req.add_header("Content-Type", content_type)
        if self.api_key:
            req.add_header("Authorization", f"Bearer {self.api_key}")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as reply:
                return json.load(reply)
        except urllib.error.HTTPError as exc:
            with exc:
                detail = exc.read(300).decode("utf-8", "replace").strip()
            raise RuntimeError(f"Speaches answered HTTP {exc.code} to {path}: {detail}") from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Speaches at {self.url} did not answer {path}: {exc}") from None

    def convert(self, data: bytes, original_name: str, options: dict) -> ConverterOutput:
        options = dict(options or {})
        model = options.get("model") or self.model
        fields = {"model": model, "response_format": "verbose_json"}
        if options.get("language"):
            fields["language"] = options["language"]
        body, ctype = _multipart(fields, "file", original_name, data)
        reply = self._call("/v1/audio/transcriptions", body, ctype, self.http_timeout)
        if not isinstance(reply, dict) or not isinstance(reply.get("text"), str):
            raise RuntimeError("Speaches' answer has no text")
        stem = os.path.splitext(os.path.basename(original_name))[0] or "recording"
        files = {
            f"{stem}.md": (reply["text"].strip() + "\n").encode("utf-8"),
            f"{stem}.transcript.json": json.dumps(reply, indent=2, ensure_ascii=False).encode("utf-8"),
        }
        return ConverterOutput(files, {"speaches-model": model})

    def warm_up(self) -> Optional[float]:
        """Ask Speaches which models it has, so a worker pointed at the wrong
        server, or one without the model, says so at start. Speaches loads a
        model on its first request, on its own side, so nothing is paid here.
        Reports, never raises: the worker still starts, and its jobs retry."""
        started = time.monotonic()
        try:
            listed = {m.get("id") for m in self._call("/v1/models").get("data", [])}
        except (EngineMissing, RuntimeError) as exc:
            log.warning("transcription: %s", exc)
            return None
        if self.model not in listed:
            log.warning("transcription: Speaches at %s does not list %s (it has: %s)",
                        self.url, self.model, ", ".join(sorted(m for m in listed if m)) or "none")
        return time.monotonic() - started


def converter_for(name: str):
    """The converter a worker for `name` runs."""
    return SpeachesConverter() if name == "transcription" else EngineConverter(name)


def engine_converters() -> dict:
    """The converters this worker could run, by name. Which one it does run is
    decided by the queue it consumes (and so by the image it was started in)."""
    return {name: EngineConverter(name) for name in ENGINE_MODULES}
