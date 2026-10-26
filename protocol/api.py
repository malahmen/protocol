"""The HTTP front door: the contract's endpoints, and nothing that waits.

| Method and path                          | Answers | Does                                          |
| ---------------------------------------- | ------- | --------------------------------------------- |
| `POST /documents`                        | 202/200 | store the source, register it, queue it       |
| `GET /documents/{id}`                    | 200     | the document and its conversions              |
| `POST /documents/{id}/conversions`       | 202     | queue a reconversion (`add`/`update`/`replace`) |
| `DELETE /documents/{id}`                 | 202     | queue the removal cascade                     |
| `DELETE /documents/{id}/history`         | 202     | queue a history prune                         |
| `GET /healthz`                           | 200     | liveness                                      |

No authentication: the LAN is trusted (decision 15). The API is still the
only way in - Redis and the buckets are never exposed to callers.
"""
import json
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import contract as c
from . import routing
from .service import Busy, Conflict, ConversionService, NotFound, TooLarge
from .routing import InvalidRequest

_CHUNK = 1024 * 1024


def _ts(value) -> Optional[str]:
    return value.isoformat() if value else None


def document_view(doc: c.Document) -> dict:
    return {
        "document_id": doc.document_id,
        "state": doc.state,
        "content_hash": doc.content_hash,
        "original_name": doc.original_name or None,
        "media_type": doc.media_type,
        "size": doc.size,
        "source_key": doc.source_key,
        "current_conversion_id": doc.current_conversion_id,
        "submitted_at": _ts(doc.submitted_at),
        "removed_at": _ts(doc.removed_at),
    }


def conversion_view(conv: c.Conversion) -> dict:
    return {
        "conversion_id": conv.conversion_id,
        "mode": conv.mode,
        "role": conv.role,
        "state": conv.state,
        "converter": conv.converter,
        "options": conv.options,
        "output_key": conv.output_key,
        "tool_versions": conv.tool_versions,
        "attempts": conv.attempts,
        "created_at": _ts(conv.created_at),
        "started_at": _ts(conv.started_at),
        "finished_at": _ts(conv.finished_at),
        "deleted_at": _ts(conv.deleted_at),
        # The recorded reason stays in the registry for operators: it can hold
        # paths and internals. Callers learn only that it failed.
        "error": "conversion failed" if conv.state == c.CONVERSION_FAILED else None,
    }


class ReconversionIn(BaseModel):
    mode: str = c.DEFAULT_MODE
    converter: str = routing.AUTO
    options: dict = {}


def create_app(service: ConversionService) -> FastAPI:
    app = FastAPI(title="protocol - conversion API")

    @app.exception_handler(InvalidRequest)
    async def _invalid(_: Request, exc: InvalidRequest):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @app.exception_handler(NotFound)
    async def _not_found(_: Request, exc: NotFound):
        return JSONResponse({"detail": "document not found"}, status_code=404)

    @app.exception_handler(Conflict)
    async def _conflict(_: Request, exc: Conflict):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(TooLarge)
    async def _too_large(_: Request, exc: TooLarge):
        return JSONResponse({"detail": str(exc)}, status_code=413)

    @app.exception_handler(Busy)
    async def _busy(_: Request, exc: Busy):
        return JSONResponse({"detail": str(exc)}, status_code=429, headers={"Retry-After": "60"})

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    # A plain def, not async (Kamino item 189): service.submit hashes, reads
    # the PDF, stores the source on Garage and writes MariaDB, all blocking.
    # On the event loop that stalled every other request for as long as a
    # large upload took to store - /healthz included, which scomp-link reads
    # with a 3 s timeout. FastAPI runs a plain def on its thread pool.
    @app.post("/documents")
    def submit(file: UploadFile = File(...), converter: str = Form(routing.AUTO),
               options: str = Form("{}"), submitted_by: Optional[str] = Form(None)):
        try:
            parsed = json.loads(options or "{}")
        except json.JSONDecodeError:
            raise InvalidRequest("options must be a JSON object")
        # Read in chunks and stop one past the cap, so an oversized upload is
        # refused without holding all of it in memory.
        cap = service.settings.max_upload_bytes
        buf = bytearray()
        while chunk := file.file.read(_CHUNK):
            buf.extend(chunk)
            if len(buf) > cap:
                raise TooLarge(f"source is over {cap} bytes")
        doc, conv, created = service.submit(bytes(buf), file.filename or "", file.content_type or "",
                                            converter=converter, options=parsed,
                                            submitted_by=submitted_by)
        body = {"document_id": doc.document_id, "state": doc.state, "created": created,
                "conversion_id": conv.conversion_id if conv else None,
                "status_url": f"/documents/{doc.document_id}"}
        return JSONResponse(body, status_code=202 if created else 200)

    @app.get("/documents/{document_id}")
    def status(document_id: str):
        doc, conversions = service.status(document_id)
        return {"document": document_view(doc),
                "conversions": [conversion_view(cv) for cv in conversions]}

    @app.post("/documents/{document_id}/conversions", status_code=202)
    def reconvert(document_id: str, body: ReconversionIn):
        conv = service.reconvert(document_id, body.mode, body.converter, body.options)
        return {"document_id": document_id, "conversion_id": conv.conversion_id,
                "mode": conv.mode, "status_url": f"/documents/{document_id}"}

    @app.delete("/documents/{document_id}", status_code=202)
    def remove(document_id: str):
        doc = service.remove(document_id)
        return {"document_id": doc.document_id, "state": doc.state}

    @app.delete("/documents/{document_id}/history", status_code=202)
    def prune(document_id: str, keep_alternatives: bool = False):
        service.prune(document_id, keep_alternatives)
        return {"document_id": document_id, "keep_alternatives": keep_alternatives}

    return app
