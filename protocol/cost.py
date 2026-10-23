"""How long a conversion will take, estimated before it is queued (Kamino item 73).

One marker queue held jobs ~16,000x apart - a 3-page text PDF converts in
0.55s and a 166-page scan in 2.5 h - so behind one book every small document
waited hours. More workers do not help on corelia: they share the same 16
threads, so each job just gets slower. So marker's work is split by expected
cost into a light queue and a heavy one, each with its own worker, and the
same estimate sizes the job's timeout (as protocol-droid's tasks.job_timeout
does for its own queue, item 71).

Rates measured on corelia, idle, 2026-10-16 (marker 2.0.0, 32 CPUs, no GPU):
54.7 s/page when the page is OCR'd, ~3.2 s/page on a text layer (that one
includes the table OCR a text PDF still pays). A page needs OCR when the PDF
has no text layer, when it is an image, or when the request forces it.

Transcription has no pages: its cost is the audio's length. Whisper
large-v3-turbo on corelia's CPU (16 threads, int8) took 0.094 s per second of
audio, 2026-10-18 (Kamino item 68), and the job's allowance is ~3x that per
minute, plus the base. The length is read with the standard library for a WAV
and with mutagen for anything else, when it is installed (the API's image has
it); unread, the job gets the unknown allowance.

The estimate reads the PDF with pypdfium2 when it is installed (the API
process has it). Without it, or for a file it cannot read, nothing is known,
and an unknown job is treated as heavy: the light queue's promise is that
nothing slow is in it, and a wrong guess there would break it for everyone.
"""
from dataclasses import dataclass
from typing import Optional

from . import routing

OCR_SECONDS_PER_PAGE = 54.7
TEXT_SECONDS_PER_PAGE = 3.2

# A text layer this thin is not one: scans often carry a stray OCR'd header.
_TEXT_LAYER_MIN_CHARS = 50
_SAMPLE_PAGES = 3


TRANSCRIPTION_SECONDS_PER_AUDIO_SECOND = 0.094


@dataclass(frozen=True)
class Estimate:
    pages: Optional[int]          # pages that will be converted; None = unknown
    needs_ocr: bool
    seconds: Optional[float]      # None = unknown
    audio_seconds: Optional[float] = None   # a recording's length; None = not audio, or unknown


UNKNOWN = Estimate(None, True, None)


def audio_seconds(data: bytes) -> Optional[float]:
    """A recording's length in seconds, or None if it cannot be read."""
    import io
    import wave
    try:
        with wave.open(io.BytesIO(data)) as w:
            if w.getframerate() > 0:
                return w.getnframes() / w.getframerate()
    except (wave.Error, EOFError, ValueError):
        pass
    try:
        import mutagen
    except ImportError:
        return None
    try:
        info = mutagen.File(io.BytesIO(data))
        length = getattr(getattr(info, "info", None), "length", None)
    except Exception:  # noqa: BLE001 - an unreadable file is only an unknown cost here
        return None
    return float(length) if length and length > 0 else None


def _range_indices(spec: str) -> list[int]:
    """Page indices a validated page_range names (routing caps their number)."""
    out = []
    for item in spec.split(","):
        lo, _, hi = item.partition("-")
        out.extend(range(int(lo), (int(hi) if hi else int(lo)) + 1))
    return out


def pdf_facts(data: bytes, page_range: Optional[str] = None) -> tuple[Optional[int], Optional[bool]]:
    """(pages that will be converted, has a text layer) - or (None, None) if unknown."""
    try:
        import pypdfium2
    except ImportError:
        return None, None
    try:
        pdf = pypdfium2.PdfDocument(data)
    except Exception:  # noqa: BLE001 - an unreadable file is only an unknown cost here
        return None, None
    try:
        total = len(pdf)
        pages = sorted(i for i in set(_range_indices(page_range)) if i < total) if page_range \
            else list(range(total))
        chars = 0
        for i in pages[:_SAMPLE_PAGES]:
            page = pdf[i]
            try:
                text = page.get_textpage()
                try:
                    chars += len(text.get_text_range().strip())
                finally:
                    text.close()
            finally:
                page.close()
        return len(pages), chars >= _TEXT_LAYER_MIN_CHARS
    except Exception:  # noqa: BLE001
        return None, None
    finally:
        pdf.close()


def estimate(converter: str, original_name: str, data: Optional[bytes], options: dict) -> Estimate:
    """What converting this will cost: marker's by its pages, a transcription's
    by its length. markitdown's is too small to plan for."""
    if converter == "transcription":
        length = audio_seconds(data) if data is not None else None
        if length is None:
            return Estimate(None, False, None)
        return Estimate(None, False, length * TRANSCRIPTION_SECONDS_PER_AUDIO_SECOND, length)
    if converter != "marker":
        return Estimate(None, False, None)
    forced = bool(options.get("force_ocr"))
    ext = routing.extension(original_name)
    if ext in routing.PDF_AND_IMAGES and ext != "pdf":
        return Estimate(1, True, OCR_SECONDS_PER_PAGE)     # an image: one page, OCR'd
    if ext != "pdf" or data is None:
        # Anything else sent to marker by name (a DOCX, say) has a length this
        # cannot read: unknown, so heavy.
        return UNKNOWN
    pages, has_text = pdf_facts(data, options.get("page_range"))
    if pages is None:
        return UNKNOWN
    needs_ocr = forced or not has_text
    rate = OCR_SECONDS_PER_PAGE if needs_ocr else TEXT_SECONDS_PER_PAGE
    return Estimate(pages, needs_ocr, pages * rate)


def queue(converter: str, est: Estimate, heavy_seconds: int) -> str:
    """The queue a conversion waits in."""
    if converter != "marker":
        return routing.queue_for(converter)
    if est.seconds is None or est.seconds > heavy_seconds:
        return routing.MARKER_HEAVY_QUEUE
    return routing.MARKER_LIGHT_QUEUE


def timeout(est: Estimate, base: int, per_page: int, unknown: int,
            per_audio_minute: int = 18) -> int:
    """Seconds the job may run before it is stopped.

    A budget per page of ~3x the OCR rate leaves room for a slower node and a
    busy host; the base covers what a job can owe once (a cold model cache
    downloads the OCR weights inside the first job). A recording gets the
    same base and ~3x Whisper's rate per minute of audio.
    """
    if est.audio_seconds is not None:
        return base + int(est.audio_seconds * per_audio_minute / 60 + 0.999)
    if est.pages is None:
        return unknown
    return base + est.pages * per_page
