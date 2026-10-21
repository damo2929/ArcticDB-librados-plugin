#!/usr/bin/env bash
# Produce a patched ArcticDB source tree with the RADOS backend:
#   1. clone the pinned ArcticDB tag (ARCTICDB_VERSION) into build/ArcticDB (or $ARCTICDB_SRC)
#   2. reset it to pristine upstream (keeping cpp/out build dirs)
#   3. copy overlay/ on top
#   4. git-apply patches/*.patch in order
set -euo pipefail
shopt -s nullglob

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(tr -d '[:space:]' < "$ROOT/ARCTICDB_VERSION")"
SRC="${ARCTICDB_SRC:-$ROOT/build/ArcticDB}"

if [[ ! -d "$SRC/.git" ]]; then
    mkdir -p "$(dirname "$SRC")"
    git clone --branch "$VERSION" --depth 1 https://github.com/man-group/ArcticDB.git "$SRC"
fi

cd "$SRC"
current="$(git describe --tags --exact-match 2>/dev/null || true)"
if [[ "$current" != "$VERSION" ]]; then
    git fetch --depth 1 origin tag "$VERSION"
    git checkout -q "$VERSION"
fi
git submodule update --init --recursive --depth 1

# Back to pristine upstream, but keep build outputs so rebuilds stay incremental.
git checkout -q -- .
git clean -fdq -e cpp/out -e '*.egg-info' -e build -e 'python/arcticdb_ext*.so'

cp -a "$ROOT/overlay/." "$SRC/"

for p in "$ROOT"/patches/*.patch; do
    echo "applying $(basename "$p")"
    git apply --whitespace=nowarn "$p"
done

echo "Patched ArcticDB $VERSION ready at $SRC"
