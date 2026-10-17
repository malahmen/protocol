#!/usr/bin/env bash
# Throwaway Garage, Qdrant and MariaDB for the storage adapters' tests.
#
#   tests/backends/up.sh            # start them, then print the variables to export
#   tests/backends/up.sh down       # stop and remove them
#
# The suite EMPTIES what these variables point at (buckets, collections,
# tables), which is why none of them has a default and why this script binds
# every port to 127.0.0.1. Never point them at taris or meksha.
#
# Versions follow what the homelab runs: Qdrant and MariaDB as kuat pins them
# (qdrant_version, mariadb_version); Garage as taris runs it, in the NAS's
# Container Station (dxflrs/garage:v2.4.1).
#
# ENGINE=docker for CI; podman otherwise.
set -euo pipefail

ENGINE="${ENGINE:-podman}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREFIX="protocol-test"
GARAGE_IMAGE="docker.io/dxflrs/garage:v2.4.1"
QDRANT_IMAGE="docker.io/qdrant/qdrant:v1.19.1"
MARIADB_IMAGE="docker.io/library/mariadb:11.4"
S3_PORT="${S3_PORT:-13900}"
QDRANT_PORT="${QDRANT_PORT:-16333}"
MARIADB_PORT="${MARIADB_PORT:-13306}"

# Fixed, published test credentials - Garage's key format is GK + 24 hex
# characters and a 64-hex secret.
S3_KEY_ID="GK000000000000000000000001"
S3_SECRET="0000000000000000000000000000000000000000000000000000000000000001"
DB_PASSWORD="protocol-test"

down() {
    "$ENGINE" rm -f "$PREFIX-garage" "$PREFIX-qdrant" "$PREFIX-mariadb" >/dev/null 2>&1 || true
}

wait_for() {  # wait_for <what> <command...>
    local what="$1"; shift
    for _ in $(seq 1 60); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    echo "$what did not come up within 60 s" >&2
    return 1
}

if [[ "${1:-}" == "down" ]]; then down; exit 0; fi
down

# The :Z relabel is podman's for SELinux hosts; docker does not need it.
relabel=""; [[ "$ENGINE" == podman ]] && relabel=",Z"

"$ENGINE" run -d --name "$PREFIX-garage" -p "127.0.0.1:$S3_PORT:3900" \
    -v "$HERE/garage.toml:/etc/garage.toml:ro$relabel" "$GARAGE_IMAGE" >/dev/null
"$ENGINE" run -d --name "$PREFIX-qdrant" -p "127.0.0.1:$QDRANT_PORT:6333" "$QDRANT_IMAGE" >/dev/null
"$ENGINE" run -d --name "$PREFIX-mariadb" -p "127.0.0.1:$MARIADB_PORT:3306" \
    -e MARIADB_ROOT_PASSWORD="$DB_PASSWORD" -e MARIADB_DATABASE=protocol \
    -e MARIADB_USER=protocol -e MARIADB_PASSWORD="$DB_PASSWORD" "$MARIADB_IMAGE" >/dev/null

# RUST_LOG=warn: the CLI logs every RPC handshake otherwise.
garage() { "$ENGINE" exec -e RUST_LOG=warn "$PREFIX-garage" /garage "$@"; }

# A one-node layout, then the two buckets the service uses and one key that
# may read and write them - the shape item 22 creates on taris.
wait_for "Garage" garage status
node="$(garage status 2>/dev/null | awk '/NO ROLE ASSIGNED/ {print $1; exit}')"
garage layout assign -z test -c 1G "$node" >/dev/null
garage layout apply --version 1 >/dev/null
garage key import --yes -n protocol-test "$S3_KEY_ID" "$S3_SECRET" >/dev/null
for bucket in sources outputs; do
    garage bucket create "$bucket" >/dev/null
    garage bucket allow --read --write "$bucket" --key protocol-test >/dev/null
done

wait_for "Qdrant" curl -sf "http://127.0.0.1:$QDRANT_PORT/readyz"
wait_for "MariaDB" "$ENGINE" exec "$PREFIX-mariadb" healthcheck.sh --connect --innodb_initialized

cat <<EOF
export PROTOCOL_TEST_S3_ENDPOINT=http://127.0.0.1:$S3_PORT
export PROTOCOL_TEST_S3_ACCESS_KEY_ID=$S3_KEY_ID
export PROTOCOL_TEST_S3_SECRET_ACCESS_KEY=$S3_SECRET
export PROTOCOL_TEST_QDRANT_URL=http://127.0.0.1:$QDRANT_PORT
export PROTOCOL_TEST_MARIADB_URL=mysql://protocol:$DB_PASSWORD@127.0.0.1:$MARIADB_PORT/protocol
EOF
