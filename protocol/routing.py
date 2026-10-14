"""Which converter takes a file, which queue it waits in, which options it accepts.

One API, several converters, **a work queue per converter** (decision 16), so
a light markitdown job never waits behind marker and a long recording never
blocks either. `auto` routes by extension, as protocol-droid's `--backend auto`
does, with one change: audio goes to transcription (decision 18), not to
markitdown.

Options are allowlisted per converter. Anything not listed is refused at the
API with a 400, so a request can never reach a converter's flags directly.
"""
import os
import re

AUTO = "auto"

PDF_AND_IMAGES = frozenset("pdf png jpg jpeg tiff tif webp gif bmp".split())
AUDIO = frozenset("mp3 wav m4a ogg oga opus flac aac webm".split())

QUEUES = {
    "marker": "convert.marker",
    "markitdown": "convert.markitdown",
    "transcription": "convert.transcription",
}
CONVERTERS = tuple(QUEUES)

# Removal and prune touch only the registry, the stores and the index: they
# share one light queue rather than waiting behind a conversion.
MAINTENANCE_QUEUE = "maintenance"

MARKER_FORMATS = ("markdown", "json", "html", "chunks")

# [0-9]+ and not str.isdigit(): isdigit() accepts '²', which int() refuses.
_NUM_RE = re.compile(r"[0-9]+\Z")
_LANG_RE = re.compile(r"[a-z]{2,3}(-[A-Za-z0-9]{2,8})?\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9._/-]{1,200}\Z")


class InvalidRequest(ValueError):
    """A request this service refuses with a 400."""


def extension(name: str) -> str:
    return os.path.splitext(name)[1].lower().lstrip(".")


def choose(original_name: str, requested: str = AUTO) -> str:
    """The converter for a file: the one requested, or one picked by extension."""
    if requested and requested != AUTO:
        if requested not in QUEUES:
            raise InvalidRequest(f"converter must be one of: {AUTO}, " + ", ".join(CONVERTERS))
        return requested
    ext = extension(original_name)
    if ext in PDF_AND_IMAGES:
        return "marker"
    if ext in AUDIO:
        return "transcription"
    return "markitdown"


def queue_for(converter: str) -> str:
    return QUEUES[converter]


def validate_page_range(spec, max_pages: int, max_index: int) -> str:
    """Normalise and cap a marker page_range such as "0,5-10".

    Capped, not just parsed: marker expands a range eagerly, and "0-999999999"
    once OOM-killed protocol-droid's in-process worker (exit 137) - the worker
    died with the job and took the queue's processing with it.
    """
    if not isinstance(spec, str) or not spec.strip():
        raise InvalidRequest("page_range must be a non-empty string")
    total = 0
    parts = []
    for raw in spec.strip().split(","):
        item = raw.strip()
        lo_s, sep, hi_s = item.partition("-")
        if not _NUM_RE.match(lo_s) or (sep and not _NUM_RE.match(hi_s)):
            raise InvalidRequest(f"page_range element {item!r} is not a page or a page range")
        lo = int(lo_s)
        hi = int(hi_s) if sep else lo
        if hi < lo:
            raise InvalidRequest(f"page_range element {item!r} counts backwards")
        if hi > max_index:
            raise InvalidRequest(f"page_range element {item!r} is past page {max_index}")
        total += hi - lo + 1
        if total > max_pages:
            raise InvalidRequest(f"page_range covers more than {max_pages} pages")
        parts.append(f"{lo}-{hi}" if sep else str(lo))
    return ",".join(parts)


def _bool(options: dict, key: str) -> None:
    if key in options and not isinstance(options[key], bool):
        raise InvalidRequest(f"{key} must be true or false")


def validate_options(converter: str, options, settings) -> dict:
    """Return the options normalised, or raise InvalidRequest."""
    if options is None:
        options = {}
    if not isinstance(options, dict):
        raise InvalidRequest("options must be an object")
    allowed = {
        "marker": {"output_format", "force_ocr", "use_llm", "page_range"},
        "markitdown": set(),
        "transcription": {"model", "language"},
    }[converter]
    unknown = sorted(set(options) - allowed)
    if unknown:
        raise InvalidRequest(f"{converter} does not accept: " + ", ".join(unknown))
    out = dict(options)

    if converter == "marker":
        fmt = out.get("output_format", "markdown")
        if fmt not in MARKER_FORMATS:
            raise InvalidRequest("output_format must be one of " + ", ".join(MARKER_FORMATS))
        out["output_format"] = fmt
        _bool(out, "force_ocr")
        _bool(out, "use_llm")
        # The LLM endpoint is deployment configuration, never request data: a
        # caller who can name a base URL has a request-forgery primitive. A
        # request may only say use_llm: true, and only where one is configured.
        if out.get("use_llm") and not settings.llm_configured:
            raise InvalidRequest("use_llm: no LLM is configured for this deployment")
        if "page_range" in out:
            out["page_range"] = validate_page_range(
                out["page_range"], settings.page_range_max_pages, settings.page_range_max_index)

    if converter == "transcription":
        # The model is a name the speech server looks up, not an import path,
        # so a free string is safe - within a strict alphabet.
        if "model" in out and not (isinstance(out["model"], str) and _MODEL_RE.match(out["model"])):
            raise InvalidRequest("model must be a model name")
        if "language" in out and not (isinstance(out["language"], str) and _LANG_RE.match(out["language"])):
            raise InvalidRequest("language must be a language code such as pt or pt-PT")
    return out
