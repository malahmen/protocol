# protocol

[![ci](https://github.com/malahmen/protocol/actions/workflows/ci.yml/badge.svg)](https://github.com/malahmen/protocol/actions/workflows/ci.yml)

**One conversion API for every converter**, running on corelia next to the models. \
You hand it a document; it answers at once with an ID, converts in the background, and tells you when it's done. \
Nothing waits on a conversion, and nothing is lost unless you ask for it.

- **Non-blocking.** Every request answers `202 Accepted` and an ID. The work is queued; the result is announced on a completion stream.
- **Nothing is lost.** The source is kept from intake until you remove it. Every conversion keeps its own output folder, its provenance and its registry row.
- **One door, many converters.** marker, markitdown and transcription sit behind the same endpoints, each with **its own queue**, so a quick job never waits behind a slow one.

The specification is Kamino's `converter-services-2026-10-10.md` - "The non-blocking contract" and decisions 10-18. This repository implements it; where the two disagree, the report wins and this is a bug.

---

## Status

The contract is built and tested. The backends it runs on are not, yet.

| Part | State |
| --- | --- |
| The contract: submit, status, reconvert, remove, prune, and the worker jobs behind them | **built**, 50 tests |
| The HTTP API (FastAPI) | **built** |
| In-memory backends and a development server | **built**: try the API with curl today |
| Registry on MariaDB (meksha) | waits on TODO item 22 |
| Source and output stores on Garage (taris) | waits on TODO item 22 |
| Queues (RQ) and the completion stream (Redis Stream), on meksha's Redis | waits on TODO items 22-23 |
| Qdrant removal by document | waits on TODO item 23 |
| The converters: marker, markitdown, transcription through Speaches | wait on TODO items 20-21 |
| Install and management on corelia | a scomp-link script, `protocol/protocol.sh` - TODO item 23 |

TODO items are in Kamino's `review-2026-10-10.md`, "Planned work".

---

## Quick start

```sh
git clone https://github.com/malahmen/protocol.git
cd protocol
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt

.venv/bin/python -m unittest discover -s tests     # the suite
.venv/bin/python -m protocol.dev                    # the API on http://127.0.0.1:8080 (docs at /docs)
```

The development server runs the **real API over in-memory backends**. \
Nothing persists, and **nothing is really converted**: every converter is replaced by a stand-in that writes a text file back as `out.md`. \
It is for trying the contract's shape:

```sh
curl -s -F file=@notes.md localhost:8080/documents             # 202 {"document_id": ..., "status_url": ...}
curl -s localhost:8080/documents/<id>                          # state, conversions, output keys
curl -s -X POST localhost:8080/documents/<id>/conversions \
     -H 'content-type: application/json' -d '{"mode": "add"}'   # 202, a second conversion
curl -s -X DELETE localhost:8080/documents/<id>/history         # 202, prune non-current outputs
curl -s -X DELETE localhost:8080/documents/<id>                 # 202, the removal cascade
```

---

## The API

| Method and path | Answers | Does |
| --- | --- | --- |
| `POST /documents` | `202`, or `200` for content it already has | store the source, register it, queue its conversion |
| `GET /documents/{id}` | `200` | the document and every conversion of it |
| `POST /documents/{id}/conversions` | `202` | queue a reconversion, with a mode |
| `DELETE /documents/{id}` | `202` | queue the removal cascade |
| `DELETE /documents/{id}/history` | `202` | queue a history prune (`?keep_alternatives=true` keeps alternatives) |
| `GET /healthz` | `200` | liveness |

`POST /documents` is multipart: `file`, plus optional `converter` (default `auto`), `options` (a JSON object) and `submitted_by`. \
The same content again returns the existing document: submission is idempotent, and a reconversion is an explicit request, not a re-upload.

Refusals: `400` for a bad request or option, `404` for an unknown document, `409` for a removed one, `413` over the size cap, `429` with `Retry-After` when that converter's queue is full.

No authentication: the LAN is trusted (decision 15). The API is still the only way in - Redis and the stores are never exposed to callers.

### Converters

| Converter | Takes, with `auto` | Options |
| --- | --- | --- |
| `marker` | PDF and images | `output_format` (`markdown`, `json`, `html`, `chunks`), `force_ocr`, `use_llm`, `page_range` |
| `transcription` | audio (`mp3`, `wav`, `m4a`, `ogg`, `opus`, `flac`, `aac`, `webm`) | `model`, `language` |
| `markitdown` | everything else | none |

Options are allowlisted: anything else is a `400`, so a request never reaches a converter's flags directly. \
`use_llm` is accepted only where an LLM is configured, and the endpoint is the deployment's, never the request's - a caller who can name a URL can aim the service at anything. \
`page_range` is capped, not just parsed: an unbounded range once took protocol-droid's worker down with the job.

Rendering - Markdown to PDF or DOCX with pandoc and xelatex - is not here: it stays holo-convert's, locally or in its container (Kamino decisions 3-4).

### A document's life

| State | Means |
| --- | --- |
| `conversion_pending` | queued |
| `converting` | a worker has it |
| `validation_pending` | converted; waiting for review (Kamino `pipeline_spec.md` §4) |
| `conversion_failed` | every attempt failed; the job is dead-lettered for inspection |
| `removal_pending` | removal queued |
| `removed` | a tombstone: only the ID, the content hash and the removal time remain |

### Reconversion modes

A reconversion runs exactly like the first conversion. **Only when it has succeeded** does its mode act; a failure touches nothing.

| Mode | The new conversion | The previous current one |
| --- | --- | --- |
| `update` _(default)_ | becomes current | kept as `superseded` history |
| `replace` | becomes current | its output is **deleted** (`replaced`); earlier history is untouched |
| `add` | kept as an `alternative` | stays current |

A history prune deletes `superseded` outputs (and `alternative` ones, unless kept), never the current one. Every pruned conversion keeps its registry row - what was produced, by which versions, with which options - after its files are gone.

### The completion stream

Each finished job appends one event (Redis Stream `conversions.events`, once the adapter exists):

```json
{"event": "conversion", "document_id": "...", "conversion_id": "...", "state": "validation_pending",
 "output_key": "outputs/<document_id>/<conversion_id>/", "mode": "update", "role": "current",
 "previous_conversion_id": "..."}
{"event": "removed", "document_id": "..."}
{"event": "pruned", "document_id": "...", "conversion_ids": ["..."]}
```

`previous_conversion_id` is there for the ingestion side: ingest the new points first, then remove the old ones, so a search never sees a gap.

### Where things are kept

| What | Key |
| --- | --- |
| The source | `sources/<document_id>/<original name>` |
| A conversion's output | `outputs/<document_id>/<conversion_id>/` |
| Its provenance | `outputs/<document_id>/<conversion_id>/provenance.json` |

Both stores are Garage buckets on taris; the registry is a MariaDB database on meksha (decision 17). \
Removal runs **index, then outputs, then source**, so a removal that stops halfway leaves files without vectors - never vectors pointing at nothing.

---

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `PROTOCOL_MAX_UPLOAD_BYTES` | `536870912` | largest accepted source (512 MiB) |
| `PROTOCOL_QUEUE_MAX_DEPTH` | `1000` | jobs waiting in one queue before submissions get `429` |
| `PROTOCOL_MAX_ATTEMPTS` | `3` | tries per conversion before it is dead-lettered |
| `PROTOCOL_RETRY_BASE_SECONDS` | `30` | first retry delay; doubles per attempt |
| `PAGE_RANGE_MAX_PAGES` | `2000` | most pages one marker `page_range` may cover |
| `PAGE_RANGE_MAX_INDEX` | `100000` | highest page a marker `page_range` may name |
| `LLM_SERVICE` | unset | set: marker's `use_llm` is allowed |

The backends' settings (MariaDB, Garage, Redis, Qdrant, Speaches) arrive with their adapters.

---

## How it is built

```
protocol/
  contract.py   the vocabulary: states, modes, roles, events, the two records
  ports.py      what the service needs from each backend - and nothing more
  routing.py    which converter takes a file, its queue, its allowed options
  service.py    the contract: the requests the API makes, the jobs workers run
  worker.py     one job -> one handler
  api.py        the HTTP endpoints
  memory.py     in-memory backends, for the tests and the dev server
  dev.py        the development server
  config.py     settings from the environment
tests/          stdlib unittest; the HTTP tests skip without fastapi
```

The contract talks to its backends only through `ports.py`. That is what let it be written and tested before any backend exists, and what each real adapter has to honour: `memory.py` is the reference for the guarantees.

Rules the code keeps, each from the specification:

- **Jobs are data, never code.** RQ pickles whatever callable it is given, so anything that can write to the queue could run code in a worker. Jobs here are plain dicts naming a kind and an ID; the worker already knows the handler.
- **At-least-once.** A job may run twice. A conversion writes only under its own folder and clears it first, so a second run overwrites its own first attempt and nothing else.
- **Failures are recorded, not echoed.** The registry keeps the reason for operators; callers see only `conversion failed`.
- **Idempotent intake.** The registry insert is the uniqueness point (`INSERT ... ON DUPLICATE KEY` on MariaDB). The source is stored before the row exists, so a row never points at a missing source.

### Still open

Each of these is a decision in the specification, not a gap in the code:

- **"Succeeded" means converted, for now.** The specification says a *failed or rejected* reconversion touches nothing. Modes act when the conversion succeeds; if review must approve it first, the same step (`_apply_mode`) moves to the acceptance.
- **Hash uniqueness among live documents.** A removed document's content can be submitted again as a new document. MariaDB has no partial unique index, so the schema needs a generated column - the hash while live, `NULL` once removed - with a unique index on it.
- **The registry schema** (To decide 8), **the removal states** (9) and **the ingestion format** (2) - the code follows the sketch and names it where it deviates (`contract.py`).
- **Not built:** pre-signed uploads straight into the source store, webhooks (allowlisted targets only, when they come), and the retention rule for history.

---

## Related

- **[protocol-droid](https://github.com/malahmen/protocol-droid)** - the marker and markitdown engine. Its worker image becomes this service's marker worker; its provenance and option rules are the models for the ones here.
- **[holo-convert](https://github.com/malahmen/holo-convert)** - the other direction: Markdown to PDF and DOCX.
- **[Speaches](https://speaches.ai/)** - the speech server on corelia that the transcription converter calls.
- **[scomp-link](https://github.com/malahmen/scomp-link)** - where the scripts that install and manage this on corelia will live.

## License

[MIT](LICENSE)
