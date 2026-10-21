# ArcticDB native RADOS backend

> **Proof of concept: no guarantee, no warranty.**
>
> This project grew out of a conversation with a customer at [croit GmbH](https://croit.io). I built it as a prototype
> to find out whether ArcticDB could use a Ceph RADOS pool directly through librados, without going through RGW/S3.
>
> It is **not production software**. It comes with no guarantee, warranty or support of any kind, express or implied.
> It may lose or corrupt data, and its on-disk layout may change without notice. It is published as a **starting
> point** for anyone who wants to build on it, test it further, or take the idea upstream to ArcticDB.
>
> What has and hasn't been tested, the known limitations and the open design questions are all listed in
> [docs/design.md](docs/design.md). Please read it before relying on any of this.

A storage engine that lets ArcticDB use a Ceph RADOS pool directly through librados, with no RGW or S3 layer in between:

```python
from arcticdb import Arctic
ac = Arctic("rados://arcticdb?conf=/etc/ceph/ceph.conf&client_id=arcticdb&meta_pool=arcticdb-meta")
```

ArcticDB has no runtime plugin loading; backends are compiled into its C++ core. So this repo is our own engine
(`overlay/`) plus a small set of hook patches (`patches/`) that register it with a pinned upstream release
(`ARCTICDB_VERSION`).

## Layout

| Path | What |
|---|---|
| `overlay/cpp/arcticdb/storage/rados/` | The engine: `RadosStorage`, the sharded omap key index, chunking, the librados client (`rados_client_impl`), the shared cluster handle registry, the URI override, the Python bindings and `rados.cmake` |
| `overlay/cpp/arcticdb/storage/mock/rados_mock_client.*` | In-memory RADOS used by the upstream C++ storage test suites |
| `overlay/cpp/proto/arcticc/pb2/rados_storage.proto` | Library config (persisted layout, plus runtime-only connection settings) |
| `overlay/cpp/CMake/FindRados.cmake` | librados ships no pkg-config or CMake package |
| `overlay/python/arcticdb/adapters/rados_library_adapter.py` | `rados://` URI adapter for `Arctic(...)` |
| `overlay/python/arcticdb/storage_fixtures/rados.py` | `StorageFixture` so the upstream pytest suites run against RADOS |
| `patches/` | Hooks only: build glue, factory / proto / error-code registration, override + bindings, adapter list, upstream test-suite parameters |
| `scripts/` | `apply.sh`, `make-patches.sh`, `container.sh`, `build.sh`, `minicluster.sh`, `test.sh` |
| `docs/` | Design and tuning notes |

## Workflow

```bash
scripts/apply.sh                 # clone pinned ArcticDB into build/ArcticDB, copy overlay, apply patches
scripts/container.sh image       # Ubuntu 24.04 + conda-forge deps + librados/ceph packages
scripts/build.sh                 # build test_unit_arcticdb + arcticdb_ext with ARCTICDB_WITH_RADOS=ON
scripts/minicluster.sh up        # throwaway 1 mon / 1 mgr / 3 memstore OSD cluster, pools arcticdb + arcticdb-meta
scripts/test.sh cpp              # upstream C++ storage suites (RADOS via the mock client)
scripts/test.sh py [pytest args] # upstream Python suites against the minicluster
scripts/minicluster.sh down
```

To change an upstream hook, edit the file in `build/ArcticDB`, then run `scripts/make-patches.sh` to regenerate `patches/`.
Engine code is edited in `overlay/` and copied in again by `scripts/apply.sh`.

To bump ArcticDB, change `ARCTICDB_VERSION` and run `scripts/apply.sh`. If a patch no longer applies, fix it in the
tree and run `make-patches.sh`.

## Design

See [docs/design.md](docs/design.md). In short:

- **Namespaces.** Each library gets its own RADOS namespace, and each key is one object named
  `<key type>/<tokenized key>`. CRUSH spreads the objects, so no bucketizer is needed.
- **Key index.** Listing uses a sharded omap index (`_idx/<key type>/<shard>`), filtered by stream-id prefix on the
  OSD. `TABLE_DATA` is left unindexed so bulk writes stay a single round trip.
- **Atomic and conditional writes.** Atom keys are written with `create(exclusive)+write_full`, which gives
  `DuplicateKeyException` semantics. `write_if_none` is atomic, so `ReliableStorageLock` works. `update(upsert=false)`
  uses `assert_exists+write_full`.
- **Async I/O.** Every op is librados aio. Reads are true async through `AsyncStorage`, and the segment
  references the librados buffer without a copy. Writes send straight from the segment buffer.
- **Large segments.** Segments larger than `chunk_size` (default 64 MiB) are split across objects, staying below
  `osd_max_write_size`.
- **Optional `meta_pool`.** Refs, versions, locks and the index can go in a replicated NVMe pool, with bulk data in
  the main pool.
