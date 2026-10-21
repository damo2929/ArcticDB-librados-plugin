#!/usr/bin/env bash
# Build the patched ArcticDB (C++ unit tests and/or the Python extension) inside the dev container.
#   scripts/build.sh [cmake targets...]      default targets: test_unit_arcticdb arcticdb_ext
# Run on the host; it re-executes itself in the container via scripts/container.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f /.containerenv && -z "${ARCTICDB_IN_CONTAINER:-}" ]]; then
    [[ -d "$ROOT/build/ArcticDB/cpp/arcticdb/storage/rados" ]] || "$ROOT/scripts/apply.sh"
    exec "$ROOT/scripts/container.sh" run scripts/build.sh "$@"
fi

PRESET="${ARCTIC_CMAKE_PRESET:-linux-conda-release}"
TARGETS=("$@")
[[ ${#TARGETS[@]} -gt 0 ]] || TARGETS=(test_unit_arcticdb arcticdb_ext)

# Ubuntu's librados headers live in /usr/include next to unrelated system headers (boost, fmt, ...). Expose only the
# rados headers and library, so nothing else from /usr/include leaks into a build that uses conda-forge dependencies.
SDK="$ROOT/build/rados-sdk"
mkdir -p "$SDK/include" "$SDK/lib"
rm -rf "$SDK/include/rados"
cp -r /usr/include/rados "$SDK/include/rados"
ln -sf "$(readlink -f /usr/lib/x86_64-linux-gnu/librados.so)" "$SDK/lib/librados.so"
export RADOS_ROOT="$SDK"

cd "$ROOT/build/ArcticDB/cpp"
PYVER="$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
cmake --preset "$PRESET" -DTEST=ON -DARCTICDB_WITH_RADOS=ON -DBUILD_PYTHON_VERSION="$PYVER" \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,-rpath-link,/usr/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu/ceph" \
    -DCMAKE_SHARED_LINKER_FLAGS="-Wl,-rpath-link,/usr/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu/ceph"
cmake --build "out/$PRESET-build" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-$(( $(nproc) / 2 ))}" --target "${TARGETS[@]}"

# Make the extension importable from the source tree (python/arcticdb_ext*.so), like upstream's `make symlink`.
ext="$(find "out/$PRESET-build" -maxdepth 3 -name 'arcticdb_ext*.so' | head -n1 || true)"
if [[ -n "$ext" ]]; then
    ln -sf "$(realpath "$ext")" "$ROOT/build/ArcticDB/python/$(basename "$ext")"
    echo "Linked $(basename "$ext") into build/ArcticDB/python"
fi
