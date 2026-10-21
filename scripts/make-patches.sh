#!/usr/bin/env bash
# Regenerate patches/*.patch from the edits made to upstream files in the patched tree (build/ArcticDB).
# Our own engine lives in overlay/; patches only contain the hooks that register it with ArcticDB.
# Workflow: scripts/apply.sh -> edit upstream files in build/ArcticDB -> scripts/make-patches.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${ARCTICDB_SRC:-$ROOT/build/ArcticDB}"
cd "$SRC"

declare -A PATCH_GROUPS=(
    [0001-build-glue]="cpp/arcticdb/CMakeLists.txt cpp/proto/arcticc/pb2/proto/CMakeLists.txt"
    [0002-engine-registration]="cpp/arcticdb/toolbox/query_stats.hpp cpp/arcticdb/toolbox/query_stats.cpp cpp/arcticdb/entity/protobufs.hpp cpp/arcticdb/storage/storage_factory.cpp cpp/arcticdb/util/error_code.hpp"
    [0003-overrides-and-bindings]="cpp/arcticdb/storage/storage_override.hpp cpp/arcticdb/storage/library_manager.cpp cpp/arcticdb/storage/python_bindings.cpp"
    [0004-python-adapter]="python/arcticdb/arctic.py"
    [0005-upstream-test-suites]="pyproject.toml cpp/arcticdb/util/test/test_utils.hpp cpp/arcticdb/storage/test/test_storage_operations.cpp cpp/arcticdb/storage/test/test_storage_exceptions.cpp python/tests/conftest.py python/tests/util/mark.py python/tests/unit/arcticdb/test_column_stats_optimisation.py python/tests/unit/arcticdb/test_column_stats_isin.py python/tests/unit/arcticdb/test_column_stats_creation.py"
)

grouped=()
rm -f "$ROOT"/patches/*.patch
for name in $(printf '%s\n' "${!PATCH_GROUPS[@]}" | sort); do
    # shellcheck disable=SC2086
    files=(${PATCH_GROUPS[$name]})
    grouped+=("${files[@]}")
    git diff --no-color -- "${files[@]}" > "$ROOT/patches/$name.patch"
    [[ -s "$ROOT/patches/$name.patch" ]] || rm "$ROOT/patches/$name.patch"
done

# Every modified upstream file must belong to a group, so nothing is silently dropped.
mapfile -t modified < <(git diff --name-only)
for f in "${modified[@]}"; do
    if [[ ! " ${grouped[*]} " =~ " $f " ]]; then
        echo "error: $f is modified but not in any patch group in $0" >&2
        exit 1
    fi
done

ls -1 "$ROOT"/patches/
