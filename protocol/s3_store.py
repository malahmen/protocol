"""The source and output stores on Garage, through its S3 API (Kamino item 69).

    S3Store   ports.ObjectStore over S3 buckets, one per store
    connect   an S3 client for Garage, from the environment

A key's first segment names its store - `sources/<document_id>/<name>`,
`outputs/<document_id>/<conversion_id>/...` (contract.py) - and each store
is its own bucket, so the rest of the key is the object's name inside it.
memory.Store is the reference for the behaviour, with one deliberate
difference: `delete_prefix` refuses a prefix that would empty a whole bucket.
The service never asks for one, and on a real store that mistake is not
undoable.

Garage on taris answers on a name with no DNS behind it, so buckets are
addressed path-style - the same quirk kuat's rclone profile records.

| Variable                  | Default    | Meaning                              |
| ------------------------- | ---------- | ------------------------------------ |
| `S3_ENDPOINT`             | (required) | e.g. `http://192.168.3.5:3900`       |
| `S3_REGION`               | `garage`   | Garage's `s3_region`                 |
| `S3_ACCESS_KEY_ID`        | (required) | the service's Garage key             |
| `S3_SECRET_ACCESS_KEY`    | (required) | its secret                           |
| `S3_SOURCES_BUCKET`       | `sources`  | the bucket behind `sources/...` keys |
| `S3_OUTPUTS_BUCKET`       | `outputs`  | the bucket behind `outputs/...` keys |
"""
import os
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

STORES = ("sources", "outputs")
_DELETE_BATCH = 1000                     # S3's DeleteObjects limit per request


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is not set")
    return value


def connect(endpoint: Optional[str] = None, access_key_id: Optional[str] = None,
            secret_access_key: Optional[str] = None, region: Optional[str] = None):
    """An S3 client for Garage. Arguments override the environment (for tests)."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint or _required("S3_ENDPOINT"),
        region_name=region or os.environ.get("S3_REGION", "garage"),
        aws_access_key_id=access_key_id or _required("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=secret_access_key or _required("S3_SECRET_ACCESS_KEY"),
        config=Config(
            s3={"addressing_style": "path"},
            # Since botocore 1.36 every upload carries a CRC checksum unless
            # told otherwise; S3-compatible stores have not all followed.
            # Send and check one only where the operation demands it.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def buckets_from_env() -> dict[str, str]:
    return {"sources": os.environ.get("S3_SOURCES_BUCKET", "sources"),
            "outputs": os.environ.get("S3_OUTPUTS_BUCKET", "outputs")}


def _missing(exc: ClientError) -> bool:
    return exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound")


class S3Store:
    """ports.ObjectStore on S3 buckets. memory.Store is the reference."""

    def __init__(self, client, buckets: Optional[dict[str, str]] = None):
        self.client = client
        self.buckets = dict(buckets or {s: s for s in STORES})

    def _split(self, key: str) -> tuple[str, str]:
        store, sep, name = key.partition("/")
        if not sep or not name or store not in self.buckets:
            raise ValueError(f"a key starts with one of {sorted(self.buckets)}/, got {key!r}")
        return self.buckets[store], name

    def put(self, key: str, data: bytes) -> None:
        bucket, name = self._split(key)
        self.client.put_object(Bucket=bucket, Key=name, Body=bytes(data))

    def get(self, key: str) -> bytes:
        """KeyError for a missing object, as memory.Store raises."""
        bucket, name = self._split(key)
        try:
            return self.client.get_object(Bucket=bucket, Key=name)["Body"].read()
        except ClientError as exc:
            if _missing(exc):
                raise KeyError(key) from exc
            raise

    def _names(self, bucket: str, prefix: str) -> list[str]:
        names = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            names.extend(obj["Key"] for obj in page.get("Contents", ()))
        return names

    def list(self, prefix: str) -> list[str]:
        """Every key starting with `prefix`, sorted - across stores if the
        prefix is shorter than a store's name, as memory.Store would."""
        found = []
        for store, bucket in self.buckets.items():
            head = store + "/"
            if prefix.startswith(head):
                found.extend(head + n for n in self._names(bucket, prefix[len(head):]))
            elif head.startswith(prefix):
                found.extend(head + n for n in self._names(bucket, ""))
        return sorted(found)

    def delete(self, key: str) -> None:
        """Deleting what is not there is not an error, here or in S3."""
        bucket, name = self._split(key)
        self.client.delete_object(Bucket=bucket, Key=name)

    def delete_prefix(self, prefix: str) -> int:
        bucket, rest = self._split(prefix)        # refuses "outputs/" and shorter
        names = self._names(bucket, rest)
        for i in range(0, len(names), _DELETE_BATCH):
            batch = names[i:i + _DELETE_BATCH]
            reply = self.client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": n} for n in batch], "Quiet": True})
            if reply.get("Errors"):
                first = reply["Errors"][0]
                raise RuntimeError(f"could not delete {len(reply['Errors'])} of {len(batch)} objects "
                                   f"under {prefix!r}: {first.get('Code')} {first.get('Message')}")
        return len(names)
