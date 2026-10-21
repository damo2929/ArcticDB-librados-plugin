#!/usr/bin/env bash
# Build the dev container image and/or run a command inside it with the repo mounted at /work.
#   scripts/container.sh image            # build the image
#   scripts/container.sh run <cmd...>     # run a command in the container
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${ARCTICDB_RADOS_IMAGE:-localhost/arcticdb-rados-dev:latest}"
SRC="${ARCTICDB_SRC:-$ROOT/build/ArcticDB}"

case "${1:-}" in
image)
    [[ -f "$SRC/environment-dev.yml" ]] || "$ROOT/scripts/apply.sh"
    ctx="$(mktemp -d)"
    trap 'rm -rf "$ctx"' EXIT
    cp "$ROOT/docker/Containerfile" "$SRC/environment-dev.yml" "$ctx/"
    podman build -t "$IMAGE" "$ctx"
    ;;
run)
    shift
    exec podman run --rm $([[ -t 0 ]] && echo -it) --network host --userns=keep-id \
        -v "$ROOT:/work:Z" -w /work \
        -e ARCTICDB_IN_CONTAINER=1 -e HOME=/work/build/home -e SCCACHE_DIR=/work/build/sccache \
        -e CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-$(( $(nproc) / 2 ))}" \
        "$IMAGE" "$@"
    ;;
*)
    echo "usage: $0 image | run <cmd...>" >&2
    exit 2
    ;;
esac
