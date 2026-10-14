"""Deployment settings, read from the environment.

| Variable                       | Default      | Meaning                                              |
| ------------------------------ | ------------ | ---------------------------------------------------- |
| `PROTOCOL_MAX_UPLOAD_BYTES`    | 536870912    | largest accepted source (512 MiB); larger is a 413   |
| `PROTOCOL_QUEUE_MAX_DEPTH`     | 1000         | jobs waiting in one queue before submissions get 429 |
| `PROTOCOL_MAX_ATTEMPTS`        | 3            | tries per conversion before it is dead-lettered      |
| `PROTOCOL_RETRY_BASE_SECONDS`  | 30           | first retry delay; doubles per attempt               |
| `PAGE_RANGE_MAX_PAGES`         | 2000         | most pages one marker page_range may cover           |
| `PAGE_RANGE_MAX_INDEX`         | 100000       | highest page a marker page_range may name            |
| `LLM_SERVICE`                  | unset        | set = marker's `use_llm` is allowed (the endpoint is the worker's configuration) |
"""
import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer, got {raw!r}")
    if value < 1:
        raise SystemExit(f"{name} must be at least 1, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    max_upload_bytes: int = 512 * 1024 * 1024
    queue_max_depth: int = 1000
    max_attempts: int = 3
    retry_base_seconds: int = 30
    page_range_max_pages: int = 2000
    page_range_max_index: int = 100000
    llm_configured: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            max_upload_bytes=_int("PROTOCOL_MAX_UPLOAD_BYTES", cls.max_upload_bytes),
            queue_max_depth=_int("PROTOCOL_QUEUE_MAX_DEPTH", cls.queue_max_depth),
            max_attempts=_int("PROTOCOL_MAX_ATTEMPTS", cls.max_attempts),
            retry_base_seconds=_int("PROTOCOL_RETRY_BASE_SECONDS", cls.retry_base_seconds),
            page_range_max_pages=_int("PAGE_RANGE_MAX_PAGES", cls.page_range_max_pages),
            page_range_max_index=_int("PAGE_RANGE_MAX_INDEX", cls.page_range_max_index),
            llm_configured=bool(os.environ.get("LLM_SERVICE", "").strip()),
        )
