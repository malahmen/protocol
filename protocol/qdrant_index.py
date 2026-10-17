"""Removing a document's or a conversion's points from Qdrant (Kamino item 69).

    QdrantIndex   ports.VectorIndex over Qdrant's REST API

The service only ever deletes - removal (index, then outputs, then source)
and a prune of alternative outputs. Ingestion writes the points, and every
point carries `document_id` and `conversion_id` in its payload
(converter-services, "The registry"), so both deletions are a payload
filter. Two REST calls do it, so this needs no client library: the standard
library's HTTP, with the system's CA store for an https Qdrant.

Which collections hold the points is the ingestion side's choice, so they are
named here explicitly and never discovered: a filter on `document_id` across
every collection would also reach collections other tools own, whose points
may use the same payload key for something else.

| Variable              | Default    | Meaning                                        |
| --------------------- | ---------- | ---------------------------------------------- |
| `QDRANT_URL`          | (required) | e.g. `http://192.168.3.52:6333`                |
| `QDRANT_API_KEY`      | unset      | sent as `api-key` when set                     |
| `QDRANT_COLLECTIONS`  | (required) | comma-separated; the collections ingestion writes |
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional


class QdrantError(RuntimeError):
    pass


class QdrantIndex:
    """ports.VectorIndex. memory.Index is the reference: each call returns how
    many points went (memory.Index, holding none, always says 0)."""

    def __init__(self, url: str, collections: list[str], api_key: Optional[str] = None,
                 timeout: float = 30.0):
        if not collections:
            raise ValueError("name at least one collection")
        self.url = url.rstrip("/")
        self.collections = list(collections)
        self.api_key = api_key
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> "QdrantIndex":
        url = os.environ.get("QDRANT_URL", "").strip()
        names = [n.strip() for n in os.environ.get("QDRANT_COLLECTIONS", "").split(",") if n.strip()]
        if not url:
            raise SystemExit("QDRANT_URL is not set")
        if not names:
            raise SystemExit("QDRANT_COLLECTIONS is not set: name the collections ingestion writes")
        return cls(url, names, os.environ.get("QDRANT_API_KEY") or None)

    def _post(self, path: str, body: dict) -> Optional[dict]:
        """The reply's `result`, or None when the collection does not exist."""
        req = urllib.request.Request(self.url + path, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        if self.api_key:
            req.add_header("api-key", self.api_key)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as reply:
                return json.load(reply)["result"]
        except urllib.error.HTTPError as exc:
            with exc:             # an error reply holds its connection open until closed
                if exc.code == 404:
                    return None   # nothing was ever ingested into it: nothing to delete
                detail = exc.read()[:300].decode("utf-8", "replace")
            raise QdrantError(f"Qdrant answered {exc.code} to {path}: {detail}") from exc

    def _delete_where(self, key: str, value: str) -> int:
        """Count, then delete, every point whose payload `key` is `value`.

        The count is for the caller's record; the delete waits until Qdrant
        has applied it, so a removal's next step never runs ahead of it.
        """
        flt = {"must": [{"key": key, "match": {"value": value}}]}
        total = 0
        for name in self.collections:
            base = "/collections/" + urllib.parse.quote(name, safe="")
            counted = self._post(base + "/points/count", {"filter": flt, "exact": True})
            if counted is None:
                continue
            # Deleted even at a count of 0: a point ingested after the count
            # must not survive the removal it raced.
            self._post(base + "/points/delete?wait=true", {"filter": flt})
            total += counted["count"]
        return total

    def delete_document(self, document_id: str) -> int:
        return self._delete_where("document_id", document_id)

    def delete_conversion(self, conversion_id: str) -> int:
        return self._delete_where("conversion_id", conversion_id)
