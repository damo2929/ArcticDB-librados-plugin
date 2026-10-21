#!/usr/bin/env bash
# Run the upstream ArcticDB test suites with the RADOS backend registered.
#   scripts/test.sh cpp [gtest filter]   C++ storage suites; RADOS runs against the mock client
#   scripts/test.sh py  [pytest args]    Python suites against the minicluster (scripts/minicluster.sh up)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ ! -f /.containerenv && -z "${ARCTICDB_IN_CONTAINER:-}" ]]; then
    exec "$ROOT/scripts/container.sh" run scripts/test.sh "$@"
fi

PRESET="${ARCTIC_CMAKE_PRESET:-linux-conda-release}"
BUILD="$ROOT/build/ArcticDB/cpp/out/$PRESET-build"

case "${1:-}" in
cpp)
    filter="${2:-*GenericStorageTest*:*TestStorageOperations*:*Rados*}"
    exec "$BUILD/arcticdb/test_unit_arcticdb" --gtest_filter="$filter"
    ;;
py)
    shift
    cd "$ROOT/build/ArcticDB"
    # Python protobuf stubs, generated the same way upstream's `make protoc` does.
    if [[ ! -f python/arcticc/pb2/rados_storage_pb2.py ]]; then
        python setup.py protoc --build-lib python
    fi
    eval "$("$ROOT/scripts/minicluster.sh" env)"
    export PYTHONPATH="$ROOT/build/ArcticDB/python"
    cd python
    [[ $# -gt 0 ]] || set -- tests/integration/arcticdb/test_arctic.py -k rados
    exec python -m pytest -p no:cacheprovider "$@"
    ;;
*)
    echo "usage: $0 cpp [gtest filter] | py [pytest args]" >&2
    exit 2
    ;;
esac
