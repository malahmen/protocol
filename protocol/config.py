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
| `PROTOCOL_MARKER_HEAVY_SECONDS`| 300          | a marker job estimated longer than this waits in the heavy lane (cost.py) |
| `PROTOCOL_JOB_TIMEOUT_BASE`    | 1800         | every job's allowance before its pages                |
| `PROTOCOL_JOB_TIMEOUT_PER_PAGE`| 180          | per page: ~3x OCR's measured 54.7 s/page              |
| `PROTOCOL_JOB_TIMEOUT_UNKNOWN` | 21600        | a job whose page count cannot be known (6 h)          |
| `PROTOCOL_JOB_TIMEOUT_PER_AUDIO_MINUTE` | 18  | per minute of audio: ~3x Whisper's measured 0.094 s/s |
| `SPEACHES_URL`                 | unset        | set = audio is accepted and transcribed by Speaches there; unset = audio is refused at submit |
| `SPEACHES_MODEL`               | deepdml/faster-whisper-large-v3-turbo-ct2 | the model a request names none of |
| `SPEACHES_API_KEY`             | unset        | sent as a bearer token, if Speaches was started with one |
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
    marker_heavy_seconds: int = 300
    job_timeout_base: int = 1800
    job_timeout_per_page: int = 180
    job_timeout_unknown: int = 21600
    job_timeout_per_audio_minute: int = 18
    transcription_configured: bool = False

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
            marker_heavy_seconds=_int("PROTOCOL_MARKER_HEAVY_SECONDS", cls.marker_heavy_seconds),
            job_timeout_base=_int("PROTOCOL_JOB_TIMEOUT_BASE", cls.job_timeout_base),
            job_timeout_per_page=_int("PROTOCOL_JOB_TIMEOUT_PER_PAGE", cls.job_timeout_per_page),
            job_timeout_unknown=_int("PROTOCOL_JOB_TIMEOUT_UNKNOWN", cls.job_timeout_unknown),
            job_timeout_per_audio_minute=_int("PROTOCOL_JOB_TIMEOUT_PER_AUDIO_MINUTE",
                                              cls.job_timeout_per_audio_minute),
            transcription_configured=bool(os.environ.get("SPEACHES_URL", "").strip()),
        )
