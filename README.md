# protocol

[![ci](https://github.com/malahmen/protocol/actions/workflows/ci.yml/badge.svg)](https://github.com/malahmen/protocol/actions/workflows/ci.yml)

**One conversion API for every converter**, running on corelia next to the models. \
You hand it a document; it answers at once with an ID, converts in the background, and tells you when it's done. \
Nothing waits on a conversion, and nothing is lost unless you ask for it.

- **Non-blocking.** Every request answers `202 Accepted` and an ID. The work is queued; the result is announced on a completion stream.
- **Nothing is lost.** The source is kept from intake until you remove it. Every conversion keeps its own output folder, its provenance and its registry row.
- **One door, many converters.** marker, markitdown and transcription sit behind the same endpoints, each with **its own queue**, so a quick job never waits behind a slow one. marker's queue is itself split in two by expected cost: a 3-page text PDF converts in half a second and a 166-page scan in 2.5 hours, so each estimated job goes to a light or a heavy lane with its own worker (`protocol/cost.py`).

The specification is Kamino's `converter-services-2026-10-10.md` - "The non-blocking contract" and decisions 10-18. This repository implements it; where the two disagree, the report wins and this is a bug.

---

## Status

The contract is built and tested, and so is every adapter: the converters, the queue, the stores, the registry and the index. The backends exist on meksha and the NAS (item 22), and `protocol/deploy.py` runs the service on them; scomp-link's `protocol.sh` installs it on corelia (item 70).

| Part | State |
| --- | --- |
| The contract: submit, status, reconvert, remove, prune, and the worker jobs behind them | **built** |
| The HTTP API (FastAPI) | **built** |
| In-memory backends and a development server | **built**: try the API with curl today |
| Registry on MariaDB (meksha) | **built** (`protocol/mariadb_registry.py`, with its DDL); the database and its user exist on meksha (kuat's `protocol_stores`) |
| Source and output stores on Garage (taris) | **built** (`protocol/s3_store.py`); the buckets `protocol-sources` and `protocol-outputs` exist on the NAS |
| Queues (RQ) and the completion stream (Redis Stream), on a Redis of protocol's own on meksha (port 6380, with a password) | **built** (`protocol/redis_backends.py`): JSON jobs, one job function, a worker that refuses any other |
| Qdrant removal by document and by conversion | **built** (`protocol/qdrant_index.py`) |
| The converters: marker and markitdown, in protocol-droid's two images (`protocol/converters.py`) | **built**; transcription through Speaches waits on item 68 (running Speaches on corelia, and measuring it) |
| Running it: the API and a worker per lane, from the environment (`protocol/deploy.py`), in images (`Containerfile`) | **built** |
| Install and management on corelia | **built**: scomp-link's `protocol/protocol.sh` |

Item numbers are Kamino's `todo.md` (private), the to-do list for every project.

---

## Marker's two lanes

Before a marker job is queued, `protocol/cost.py` estimates how long it will run, from the PDF's page count (cut to `page_range`) and whether it needs OCR - no text layer, an image, or `force_ocr` - at the rates measured on corelia (54.7 s/page with OCR, ~3.2 without; marker 2.0.0, 32 CPUs, no GPU):

| Document | Pages | OCR | Estimate | Lane |
| --- | ---: | --- | ---: | --- |
| a CLI cheat sheet | 3 | no | 10 s | light |
| a monitor manual, with tables | 30 | no | 96 s | light |
| a scanned pamphlet | 22 | yes | 20 min | heavy |
| a scanned book | 166 | yes | 2.5 h | heavy |
| a 387-page text book | 387 | no | 21 min | heavy |

Over `PROTOCOL_MARKER_HEAVY_SECONDS` (five minutes) it waits in `convert.marker.heavy`, otherwise in `convert.marker.light`, each with its own worker - more workers on one CPU host would only share its threads. **Anything whose cost cannot be read counts as heavy**: the light lane's promise is that nothing slow is in it. The same estimate sizes the job's timeout, and a retry is planned again from the same source, so it returns to its lane. **A timeout is final**: the allowance is about three times the measured rate, so a job that uses it all was misjudged or is stuck, and a retry would hold the lane as long again. It fails at once and is dead-lettered; reconvert it with a `page_range`, or with a larger `PROTOCOL_JOB_TIMEOUT_*`.

Reading a PDF's facts takes a few milliseconds and needs `pypdfium2` (in `requirements.txt`); the core runs without it, and then every PDF is an unknown cost.

---

## The queue carries data, never code

An RQ job names a function by import path, and a worker calls it - so by default anything that can write to Redis can run any importable function in a worker. That is not hypothetical: through a plain RQ `SimpleWorker`, a job naming `os.system` runs, **pickled or JSON**. JSON alone does not close it.

`protocol/redis_backends.py` closes it three ways, and all three must hold: jobs are JSON (a pickled payload does not deserialize); every job protocol enqueues names the one function `protocol.worker.run_job` with a single dict; and `GuardedWorker` checks every job before running anything - that function, one dict, and no success, failure or stopped callback, webhook or dependency. A job that fails the check is failed without being run. RQ also reaches a job's callbacks and webhooks where no check runs - its registry cleanup calls an expired job's failure callback, and failing a job sends its webhooks - so the queues, the worker, the registries and the sweep load jobs as `GuardedJob`, whose callbacks and webhooks do nothing. `tests/test_redis_backends.py` enqueues each of those against a real Redis and proves nothing ran.

Redis itself stays reachable only by the API and the workers. This is the second wall, not the first.

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

The adapters' tests need the real backends and skip without them. `tests/backends/up.sh` starts throwaway ones in podman - Garage, Qdrant and MariaDB, on 127.0.0.1 only - and prints the variables that point the suite at them (`ENGINE=docker` for Docker). **The suite empties whatever those variables name**, so they have no defaults; never point them at taris or meksha.

```sh
eval "$(tests/backends/up.sh)"
.venv/bin/python -m unittest discover -s tests     # now also on Garage, Qdrant, MariaDB
tests/backends/up.sh down
```

With the backends up, every test in `tests/test_service.py` runs a second time with the registry on MariaDB and the stores on Garage underneath.

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

## Running it

`python -m protocol.deploy` builds the service on the real backends from the variables below and runs one part of it:

```sh
python -m protocol.deploy check                 # every backend reachable? one line each, no credentials
python -m protocol.deploy api --port 8080       # the HTTP API; creates the registry's tables first
python -m protocol.deploy worker marker-light   # one worker: marker-light, marker-heavy, markitdown, maintenance
python -m protocol.deploy queues                # each queue's depth and dead letters, and its worker
```

A converter's worker must run where its engine imports, so the `Containerfile` builds three images from one recipe: the API's on plain Python, and protocol on top of protocol-droid's `marker-service` and `markitdown-service` for the two converters' workers (`--build-arg BASE=...`). On corelia, scomp-link's `protocol.sh` builds them from source, holds the credentials as podman secrets, and runs each part as a systemd user service.

---

## The API

| Method and path | Answers | Does |
| --- | --- | --- |
| `POST /documents` | `202`, or `200` for content it already has | store the source, register it, queue its conversion |
| `GET /documents/{id}` | `200` | the document and every conversion of it |
| `POST /documents/{id}/conversions` | `202` | queue a reconversion, with a mode |
| `DELETE /documents/{id}` | `202` | queue the removal cascade (asking again while it is pending queues it again; a second run finds it removed) |
| `DELETE /documents/{id}/history` | `202` | queue a history prune (`?keep_alternatives=true` keeps alternatives) |
| `GET /healthz` | `200` | liveness |

`POST /documents` is multipart: `file`, plus optional `converter` (default `auto`), `options` (a JSON object) and `submitted_by`. \
The same content again returns the existing document: submission is idempotent, and a reconversion is an explicit request, not a re-upload.

Refusals: `400` for a bad request or option, `404` for an unknown document (or anything that is not a document ID - 32 lowercase hex characters), `409` for a removed one, `413` over the size cap, `429` with `Retry-After` when that converter's queue is full.

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
| `PROTOCOL_RETRY_BASE_SECONDS` | `30` | first retry delay; doubles per attempt (a timeout is not retried) |
| `PAGE_RANGE_MAX_PAGES` | `2000` | most pages one marker `page_range` may cover |
| `PAGE_RANGE_MAX_INDEX` | `100000` | highest page a marker `page_range` may name |
| `LLM_SERVICE` | unset | set: marker's `use_llm` is allowed |
| `PROTOCOL_MARKER_HEAVY_SECONDS` | `300` | a marker job estimated longer than this waits in the heavy lane |
| `PROTOCOL_JOB_TIMEOUT_BASE` | `1800` | every job's allowance before its pages |
| `PROTOCOL_JOB_TIMEOUT_PER_PAGE` | `180` | per page: ~3x OCR's measured 54.7 s/page |
| `PROTOCOL_JOB_TIMEOUT_UNKNOWN` | `21600` | a job whose page count cannot be known (6 h) |

The backends:

| Variable | Default | Meaning |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | protocol's Redis on meksha (`redis://192.168.3.52:6380/0`): the queues and the completion stream |
| `REDIS_PASSWORD` | unset | never in the URL, so never in a logged setting |
| `MARIADB_HOST` | (required) | the registry's MariaDB, on meksha |
| `MARIADB_PORT` | `3306` | |
| `MARIADB_DATABASE` / `MARIADB_USER` | `protocol` / `protocol` | |
| `MARIADB_PASSWORD` | (required) | |
| `S3_ENDPOINT` | (required) | Garage on taris, e.g. `http://192.168.3.5:3900`; addressed path-style |
| `S3_REGION` | `garage` | Garage's `s3_region` |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` | (required) | the service's Garage key |
| `S3_SOURCES_BUCKET` / `S3_OUTPUTS_BUCKET` | `sources` / `outputs` | `protocol-sources` / `protocol-outputs` on the NAS |
| `QDRANT_URL` | (required) | |
| `QDRANT_API_KEY` | unset | |
| `QDRANT_COLLECTIONS` | unset | the collections ingestion writes, comma-separated: removal deletes points only there. Unset, removal deletes no vectors and logs that it did not |

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
  cost.py       a marker job's estimated cost: its lane and its timeout
  converters.py marker and markitdown, through protocol-droid's images
  memory.py     in-memory backends, for the tests and the dev server
  redis_backends.py  the queue and the completion stream on Redis; the guarded worker
  s3_store.py   the source and output stores on Garage (S3)
  mariadb_registry.py  the registry on MariaDB, and its DDL
  qdrant_index.py      removing points from Qdrant
  dev.py        the development server
  deploy.py     the deployment: the API, a worker per lane, `check`, `queues`
  config.py     settings from the environment
Containerfile   the images: the API's, and a worker's on top of its engine's
tests/          stdlib unittest; each backend's tests skip without it
  backends/up.sh  throwaway Garage, Qdrant and MariaDB for those tests
```

The contract talks to its backends only through `ports.py`. That is what let it be written and tested before any backend exists, and what each real adapter has to honour: `memory.py` is the reference for the guarantees.

Rules the code keeps, each from the specification:

- **Jobs are data, never code.** RQ pickles whatever callable it is given, so anything that can write to the queue could run code in a worker. Jobs here are plain dicts naming a kind and an ID; the worker already knows the handler.
- **At-least-once.** A job may run twice. A conversion writes only under its own folder and clears it first, so a second run overwrites its own first attempt and nothing else.
- **A dead worker's job is not lost.** A worker killed holding a job (a restart, a reboot, the OOM killer) never reaches the service's error handling, and RQ would only fail the job once its timeout ran out, hours later for a big PDF, without telling protocol. Each worker keeps a short-lived key alive in Redis from a thread; between jobs, every worker sweeps every lane for a job in progress whose worker's key has gone, and hands it back to the service. The lost run counts as a failed attempt, so a source that kills its worker every time is dead-lettered after `PROTOCOL_MAX_ATTEMPTS` instead of looping. Recovery takes about two minutes (`redis_backends.py`).
- **Trouble around a conversion is retried, then parked.** A job that fails outside the converter - the registry, the store, the index or the queue unreachable for a moment - is queued again after 30 s, doubling, up to six times, then dead-lettered with the reason, so nothing is left `converting` or `removal_pending` with no job. A recovered removal or prune counts its lost runs like a conversion's attempts, and a recovered job is never recovered twice.
- **The record's ID, not the request's.** A job acts on the document ID the registry holds: MariaDB's collation ignores trailing spaces, and a removal by "<id> " once deleted the source while leaving outputs and vectors behind.
- **Failures are recorded, not echoed.** The registry keeps the reason for operators; callers see only `conversion failed`.
- **Idempotent intake.** The registry insert is the uniqueness point: on MariaDB, a unique key on the hash of live documents, so of two concurrent submissions of the same file one insert wins and the other gets the winner back. The source is stored before the row exists, so a row never points at a missing source.

### Still open

Each of these is a decision in the specification, not a gap in the code:

- **"Succeeded" means converted, for now.** The specification says a *failed or rejected* reconversion touches nothing. Modes act when the conversion succeeds; if review must approve it first, the same step (`_apply_mode`) moves to the acceptance.
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
