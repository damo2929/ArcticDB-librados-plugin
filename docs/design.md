# RFC: a native Ceph RADOS storage backend for ArcticDB

| | |
|---|---|
| Status | Proof of concept / prototype, with no guarantee or warranty. Built after a conversation with a croit GmbH customer to test feasibility. Intended as a starting point for others and for discussion with the ArcticDB maintainers before any upstream PR |
| Target | ArcticDB v6.28.1 (pinned in `ARCTICDB_VERSION`) |
| Repository | https://github.com/damo2929/ArcticDB-librados-plugin |
| Author | Damien Dye (croit GmbH) |

This document explains what the backend does, why each decision was made, which alternatives were rejected, what has
and has not been tested, and what is still weak. It aims to give a reviewer everything needed to judge whether
this belongs upstream, and in what shape.

---

## 1. Motivation

Many ArcticDB users who run on-premise storage run Ceph, and today they reach it through RGW, Ceph's S3 gateway. That
path is:

```
ArcticDB → AWS SDK → HTTP(S) → RGW → librados → OSDs
```

For ArcticDB's access pattern (many small-to-medium immutable objects, plus frequent small reference-key rewrites),
the gateway adds cost at every step:

- **Gateway hop and HTTP framing.** Every key read or write is an HTTP request through an extra hop. RGW's CPU and
  its connection limits become a throughput ceiling, independent of how many OSDs the cluster has.
- **Bucket index.** RGW keeps a bucket index in omap. Every PUT and DELETE updates it in a two-phase operation, and
  every list is a sorted walk of it. ArcticDB writes many keys, so this is write amplification it doesn't need.
- **Inherited S3 limitations.** The S3 backend has to probe for conditional-write support (`NEEDS_TEST`) because
  some S3 implementations ignore `If-None-Match`. RADOS has native exclusive create and compound atomic operations.

A librados client talks straight to the primary OSD for each object:

```
ArcticDB → librados → OSDs
```

CRUSH spreads objects across all OSDs, so client parallelism scales with the cluster rather than with the number of
gateways. Section 12 gives the expected size of the gain, which is **unmeasured so far**.

**Non-goals:**

- replacing the S3 backend
- supporting CephFS
- requiring any server-side changes to Ceph (no custom object classes)

---

## 2. How ArcticDB storage backends work (what we plug into)

These findings come from reading the v6.28.1 source:

- **Storage hooks.** Every backend subclasses `Storage` (`cpp/arcticdb/storage/storage.hpp`) and implements private
  `do_*` hooks:
  - `do_write`, `do_write_if_none`, `do_update`
  - `do_read` (×2), `do_remove` (single and batch)
  - `do_key_exists`, `do_iterate_type_until_match`
  - `do_fast_delete`, `do_key_path`
  - optionally `do_visit_object_sizes`
- **Async reads.** A backend may also implement `AsyncStorage::do_async_read`. ArcticDB uses it when
  `has_async_api()` returns true.
- **Selection.**
  - In C++, `storage_factory.cpp` chooses the backend by the protobuf type name of the library's
    `VariantStorage.config`.
  - In Python, `Arctic(uri)` picks the first adapter in `arctic.py::_LIBRARY_ADAPTERS` whose `supports_uri()`
    matches.
- **Threading.** All storage calls run on ArcticDB's folly IO thread pool, so every hook must be thread-safe.
- **Reference implementations.**
  - S3 is the structural reference: a mockable client interface, a real client and a mock client, with the logic on
    top.
  - Memory and LMDB define the strict semantics that `GenericStorageTest` checks:
    - writing an existing atom key throws `DuplicateKeyException`
    - reading, removing, or `update(upsert=false)` on a missing key throws `KeyNotFoundException`
- **Connection settings.** Settings that must not be persisted (credentials, client-specific paths) are applied at
  open time through a `StorageOverride`. They are removed before a library config is saved by a "masking override"
  returned by the adapter.
- **No plugin mechanism.** Backends are compiled into `arcticdb_ext`; there is no way to load one at runtime.

The last point is why this work is delivered as source plus patches (section 9), not as a separately installed
plugin.

---

## 3. Architecture

```
python/arcticdb/adapters/rados_library_adapter.py   rados:// URI → protobuf Config + RadosOverride
        │
cpp/arcticdb/storage/rados/
  rados_storage.{hpp,cpp}          RadosStorage : Storage, AsyncStorage   ← all ArcticDB semantics live here
  rados_client_interface.hpp       bytes-level client API (no librados types)
  rados_client_impl.{hpp,cpp}      librados implementation (aio)          ← only file that needs librados.hpp...
  rados_cluster_registry.{hpp,cpp} shared, fork-aware Rados handles       ← ...and this one
  rados_override.hpp               runtime connection settings
  rados_python_bindings.hpp        pybind registration of RadosOverride
cpp/arcticdb/storage/mock/rados_mock_client.{hpp,cpp}   in-memory RADOS for tests
cpp/proto/arcticc/pb2/rados_storage.proto               library config
```

**Why this split:**

- **The interface is defined in bytes and errno, not in `Segment`s.** This differs from the S3 interface on
  purpose: serialization, chunking, index maintenance and error mapping then all live in `RadosStorage`, so the mock
  exercises exactly the same code as production. With a segment-level mock, as S3 has, most of the
  backend-specific logic would go untested.
- **librados is confined to two `.cpp` files.** The storage, the mock, the proto and the Python adapter build
  without librados. As a result:
  - `-DARCTICDB_WITH_RADOS=OFF` (the default) still builds the RADOS storage and runs it in the upstream test suites
    against the mock;
  - only connecting to a real cluster needs librados, and without it that fails with a clear
    `E_NOT_IMPLEMENTED_BY_STORAGE` error.

  This matters for upstream (section 9): official wheels don't need to link librados for the code to be compiled
  and tested in CI.

---

## 4. Data layout

### 4.1 One RADOS namespace per library

The namespace name is `<namespace_prefix>/<library path>`. Inside it, each ArcticDB key is one object:

```
<key type long name>/<tokenized key>          e.g.  tdata/<token>, vref/*sym, sl/<token>
```

**Why namespaces rather than a name prefix:**

- **Isolation.** A library cannot see another library's objects.
- **Scoped listing.** Listing returns only this library's objects. The OSD still visits the PG, but it filters by
  namespace before returning anything.
- **Simple deletion.** `fast_delete` is "delete everything in the namespace".
- **Per-library access control.** cephx capabilities can be scoped per namespace
  (`allow rw pool=x namespace=lib/a`), which RGW bucket prefixes don't offer.

**Why reuse the S3 token format.** `to_tokenized_key` and `variant_key_from_bytes` are already used and tested for
S3/Azure, and object names stay human-readable in `rados ls`.

**Why there is no bucketizer** (the NFS-backed S3 backend hashes keys into directory buckets): RADOS hashes the full
object name into a PG, so there are no hot prefixes to spread.

### 4.2 Optional `meta_pool`

`TABLE_DATA`, `TABLE_INDEX`, `APPEND_DATA`, `MULTI_KEY` and `COLUMN_STATS` go to `pool`. Everything else goes to
`meta_pool`, which defaults to `pool`. "Everything else" means:

- refs, versions, the symbol list, snapshots, locks and logs;
- the key index (section 5).

**Why:** the version chain and symbol-list traffic is small and latency-bound, while segment traffic is large and
throughput-bound. Splitting them lets operators put metadata on replicated NVMe and bulk data on cheaper media, a
common Ceph pattern (RGW does the same with index and data pools).

**Constraint:** `meta_pool` must be replicated because it uses omap. Erasure-coded data pools are not supported or
tested yet (section 11).

### 4.3 Large segments

The OSD rejects a single write larger than `osd_max_write_size` (default 90 MiB) and objects larger than
`osd_max_object_size` (default 128 MiB). ArcticDB segments are usually far smaller, but wide frames or large strings
can exceed these limits.

So a segment larger than `chunk_size` (default 64 MiB) is split:

- **Write order.** Tail chunks are written in parallel first, to `_chunk/<object>/<n>`. The head (the first chunk,
  plus an `adb.chunks=<count>` xattr) is written **last**, so the object only becomes visible once it is complete.
- **Atomic create still works.** For exclusive writes the head uses `create(exclusive)`, so the create-race
  semantics are unchanged.
- **Reads cost no extra round trip.** A read fetches the head and its xattrs in one op, then the tails in parallel.

**Why not libradosstriper:**

- its operations are not atomic: they take shared or exclusive `cls_lock`s, and a reader can see zero-filled ranges
  during a write;
- it doesn't support compound ops, so we couldn't keep exclusive create;
- it is a separate library and package;
- ArcticDB objects are immutable and written whole, so a minimal head-last scheme is enough.

---

## 5. The key index (the main design decision)

### Problem

ArcticDB lists keys by type and stream-id prefix (`iterate_type`). The hot cases are:

- `list_symbols`, which reads `SYMBOL_LIST` keys on every call;
- snapshot and lock scans;
- version-key scans;
- incomplete (`APPEND_DATA`) discovery.

RADOS has no server-side name-prefix listing. Object listing (pgls) walks every object in every PG of the pool and
can only filter on namespace or on an xattr equality. One key type of one library therefore costs work
proportional to the whole pool. On a shared pool with hundreds of millions of objects, that is unusable for
`list_symbols`.

### Options considered

| Option | Verdict |
|---|---|
| Namespace scan and filter in memory, as the NFS-backed S3 backend does | Correct but O(pool). Kept only as the fallback for rarely listed types |
| Namespace per (library, key type) | Still a full-PG walk on the OSD per list; doesn't fix the cost |
| Custom object class (cls) for prefix listing or index maintenance | Fastest and atomic, but needs a module deployed on every OSD. Many users don't control their OSDs; rejected for v1 (could be an optional add-on later) |
| pgls xattr filter (`"plain"` filter) | Only matches xattr equality, not name prefixes |
| **Client-maintained omap index** | **Chosen.** Works on any stock cluster, uses one OSD round trip per shard page, and filters by prefix on the OSD via `omap_get_vals2(filter_prefix=…)` |

### Design

- **Shard objects.** For each indexed key type there are N shard objects, `_idx/<key type>/<shard>`
  (N = `index_shards`, default 8), in `meta_pool`.
- **Entries.**
  - The omap key is `<stream id>\x1f<token>`, so the OSD's `filter_prefix` performs ArcticDB's stream-id prefix
    match.
  - The value is `<size>\n<token>`. The token is repeated because a string id could itself contain `\x1f`.
- **Shard choice.** `fnv64(entry) % N`. fnv is used because the mapping must be identical in every process and on
  every platform, which `std::hash` doesn't guarantee.
- **Listing.** The first page of every shard is requested concurrently, and further pages only where `more` is
  set. Each candidate is re-checked on the client with `variant_key_id_starts_with`, which is also correct for
  numeric ids.
- **Sizes for free.** The size stored in the entry serves `visit_object_sizes` with no per-object stat.

### Consistency: "leak, never lie"

The data object and its index entry are different RADOS objects. RADOS compound ops are atomic per object only, so
the two cannot be updated in one transaction. The ordering is therefore:

- **write:** data first, then the index entry;
- **remove:** the index entry first, then the data.

**What a crash leaves behind:** at worst an object that exists but isn't indexed (a leak). It never leaves an index
entry pointing at nothing (a lie). This matters because ArcticDB treats `KeyNotFound` while reading a listed key as
corruption in several places, whereas it already tolerates listings that miss keys (`library.hpp` documents
iteration as best effort).

**Repair:** `RadosStorage::rebuild_index()` rebuilds the index from a namespace scan and removes orphaned chunk
tails. It is not yet exposed to Python, and it is not safe to run while writers are active.

### What is not indexed, and why

By default only `TABLE_DATA` is unindexed (configurable with `unindexed=`):

- **It is the bulk of all writes,** often more than 95% of objects. Indexing it would double the op count of the
  hottest path.
- **It is only listed by maintenance paths.** These are full deletes without `fast_delete`, size reports and tools.
  Normal reads reach data keys through index keys, never by listing.

The cost: listing `TABLE_DATA` falls back to a namespace scan. Indexed types pay one extra omap write (one RTT, after
the data write) per write.

---

## 6. Operation semantics

| ArcticDB hook | RADOS operation(s) | Behaviour |
|---|---|---|
| `write` atom key | `create(exclusive) + write_full` (+ index) | `-EEXIST` → `DuplicateKeyException` (matches LMDB/Memory; S3 silently overwrites) |
| `write` ref key | `write_full` + `adb.chunks=1` xattr (+ index) | Overwrite. The xattr reset stops a previous chunked version's marker from surviving |
| `write_if_none` | `create(exclusive) + write_full` | One atomic OSD transaction. `-EEXIST` → `AtomicOperationFailedException`. `supports_atomic_writes()` = `YES` (no runtime probe needed) |
| `update(upsert=false)` | `assert_exists + write_full` | One atomic op. `-ENOENT` → `KeyNotFoundException` |
| `update(upsert=true)` | `write_full` | |
| `read` | `read(0, 0)` + `getxattrs` in one op (+ parallel tail reads if chunked) | `-ENOENT` → `KeyNotFoundException(key)` |
| `remove` (batch ≤ 1000) | `omap_rm_keys` per shard, then concurrent removes (≤ 256 in flight) | Missing keys → one `KeyNotFoundException` listing all of them, unless `ignores_missing_key_` |
| `key_exists` | `stat` | |
| `iterate_type` | index (section 5) or namespace scan | Missing index or namespace → empty result, not an error (matches S3) |
| `fast_delete` | list namespace(s), remove everything, index objects first | Returns `true` |
| `key_path` | `<pool>/<namespace>/<object>` | |

**Why atom keys use exclusive create:**

- It is free: the guard is in the same op, so there is no extra round trip.
- It catches accidental double-writes.
- It lets the backend pass the strict `GenericStorageTest` suite.

ArcticDB already handles `DuplicateKeyException` where it matters (for example
`CopyCompressedInterStoreTask`), because LMDB behaves the same way.

**Error mapping:**

| librados error | ArcticDB error |
|---|---|
| `-ENOENT` | `KeyNotFoundException` |
| `-EACCES` / `-EPERM` | `E_PERMISSION` |
| `-EDQUOT` / `-ENOSPC` | `E_QUOTA_EXCEEDED` |
| `-ETIMEDOUT` / `-EAGAIN` / `-EBUSY` / `-ESHUTDOWN` / `-ENOTCONN` | new `E_RADOS_RETRYABLE` (5061) |
| `-EFBIG` / `-EMSGSIZE` | `E_UNEXPECTED_RADOS_ERROR`, with a hint to lower `chunk_size` |
| anything else | new `E_UNEXPECTED_RADOS_ERROR` (5060) |

Adding new error codes is a change to a shared upstream file (`error_code.hpp`). This follows the precedent of the
backend-specific Mongo, Azure and S3 codes. Reusing `E_UNEXPECTED_S3_ERROR` would have been misleading.

---

## 7. Performance design

| Technique | Why |
|---|---|
| **One `librados::Rados` per (cluster, user, conf, keyring, overrides) per process**, shared by all libraries and `Arctic` instances | A Rados handle owns messenger threads, OSD sessions and the objecter throttle. One per library would multiply threads and connections. Recreated after `fork()` (detected by pid), because handles don't survive fork. The old handles are leaked deliberately, since calling `shutdown()` in the child would touch threads that no longer exist |
| **One `IoCtx` per pool per library, namespace set once at construction** | librados documents that changing namespace/locator on a shared IoCtx is not thread-safe. Ops themselves are. Setting the namespace once makes the IoCtx safely shareable across ArcticDB's IO threads |
| **Every op is `aio_operate`; the completion callback only fulfils a `folly::Promise`** | No thread blocks per op inside librados, and batch operations (remove, index fan-out, chunk I/O, stats) put many ops in flight at once |
| **True `AsyncStorage` reads, continued on ArcticDB's IO executor** | The librados finisher thread must never run decode work, and must never block, since it also has to deliver other completions. The chunked-read path is written as future composition for this reason |
| **Zero-copy writes**: `bufferlist::static_from_mem` over `Segment::serialize_header()` output, with the segment kept alive until completion | Avoids copying every segment |
| **Zero-copy reads**: `Segment::from_bytes(copy=false)` over the returned bufferlist, with `set_keepalive` | Same as the LMDB backend's approach. If librados returns a fragmented bufferlist, `c_str()` makes one contiguous copy |
| **Whole-object read (`len=0`) with the xattrs in the same op** | One round trip per key, and no stat needed for chunk detection |
| **`FADVISE_SEQUENTIAL` on reads** | ArcticDB reads objects whole |
| **Delete batches of 1000, ≤ 256 ops in flight** | Bounded memory and objecter pressure |

Section 12 lists the relevant client tuning, and `docs/tuning.md` has the details.

**ArcticDB's write API is synchronous per key.** `do_write` is called on an IO thread and waits for its op. Write
concurrency therefore equals `VersionStore.NumIOThreads`. That is how every backend works today. An asynchronous
write API would be an upstream change outside this scope.

---

## 8. Configuration and Python API

```python
Arctic("rados://<pool>[/<namespace_prefix>]?conf=/etc/ceph/ceph.conf&client_id=arcticdb"
       "&meta_pool=arcticdb-meta&op_timeout=30&conf.objecter_inflight_op_bytes=1073741824")
```

Options are split by whether they are persisted:

- **Persisted with each library (layout):** `pool`, `meta_pool`, `namespace_prefix`, `index_shards`,
  `unindexed_key_types`, `chunk_size`. Every client of a library must agree on these. A client with a different
  `index_shards` would look in the wrong shards.
- **Never persisted (connection):** `conf`, `client_id`, `keyring`, `cluster`, `op_timeout`, `conf.*`. These are
  applied at open time by `RadosOverride` and blanked by the masking override before a config is saved. The
  `is_storage_config_ok` hook additionally refuses to persist a config containing a keyring path.

The keyring is only ever a *path*. The secret stays in the keyring file and is never in any ArcticDB config. This
fits Ceph practice better than the S3 adapter's access/secret-key query parameters.

---

## 9. Integration with upstream

### What changes in upstream files

All the logic is in new files. Upstream files only get registration hooks, kept in `patches/` (about 610 lines,
most of them test parameters):

| Patch | Files | What it changes |
|---|---|---|
| 0001 build glue | `cpp/arcticdb/CMakeLists.txt`, proto `CMakeLists.txt` | `include(storage/rados/rados.cmake)`; append `${ARCTICDB_RADOS_LIBRARIES}`; add `rados_storage.proto` |
| 0002 engine registration | `protobufs.hpp`, `storage_factory.cpp`, `error_code.hpp`, `query_stats.{hpp,cpp}` | proto include/alias; factory branch; 2 error codes; 5 `Rados_*` query-stats task types |
| 0003 overrides and bindings | `storage_override.hpp`, `library_manager.cpp`, `python_bindings.cpp` | `RadosOverride` in the override variant; visitor; config check; bindings |
| 0004 Python adapter | `arctic.py` | Register `RadosLibraryAdapter` |
| 0005 upstream test suites | `test_utils.hpp`, `test_storage_operations.cpp`, `test_storage_exceptions.cpp`, `conftest.py`, `mark.py`, `pyproject.toml`, the three `test_column_stats_*.py` | Add `rados` to the existing parametrised suites; parametrise the column-stats suites over mem/rados; register the `rados` pytest mark (no new test classes) |

### Dependency strategy (the main open question for maintainers)

- **librados is not in vcpkg or on conda-forge.** It is a distro package (`librados-dev` + `libradospp-dev` on
  Debian/Ubuntu, `librados-devel` + `libradospp-devel` on Fedora/RHEL), and it is ABI-tied to the Ceph release.
- **Current approach:** `ARCTICDB_WITH_RADOS` defaults to OFF. Default builds compile the backend and test it
  against the mock, with no librados dependency. Builds with the option ON link `librados.so.2`.
- **Alternatives for official wheels,** for discussion:
  1. **Ship two flavours:** a separate `arcticdb[rados]` build, or a Ceph-specific wheel.
  2. **`dlopen("librados.so.2")` at runtime:** the wheel would carry no hard dependency and would use the host's
     librados if present. The C++ API (`librados.hpp`) is not convenient to `dlopen`. This would mean moving
     `rados_client_impl` to the stable C API (`librados.h`), which is feasible because the client only uses about
     15 functions.
  3. **Leave it source-only** for users who build ArcticDB themselves.

  The preference here is (2), because it removes the packaging problem entirely. It has not been done yet, so as
  to keep the first version simple and reviewable.
- **Observed conflict.** When linking the conda-forge build against Ubuntu's librados, the linker warns that
  `libceph-common` needs `libboost_thread.so.1.83.0`, while ArcticDB uses 1.90. Both load because their SONAMEs
  differ, but this is a real risk of symbol clashes at runtime. Option (2) with the C API reduces but does not
  remove it. **This has not been investigated beyond the warning.**

### Contribution logistics

- **License and CLA.** ArcticDB is under the BSL 1.1 and requires the Individual Contributor License Agreement in
  the repository root. Nothing in this repo has been offered upstream yet. It is a proof of concept to start the
  conversation.

---

## 10. Testing

**Principle:** use upstream's test suites and fixtures rather than writing parallel ones. RADOS is added as a
parameter wherever upstream already parametrises over backends.

### C++ (mock client, no cluster needed)

`GenericStorageTest` (with a `RadosMockStorageFactory`) and `TestStorageOperations` (with a `"rados"`
`StorageGenerator`) now run against RADOS. They cover:

- duplicate atom-key writes, and read/update/remove of missing keys;
- listing and prefix matching (`test_list`, `test_exists_matching`, `test_exists_not_matching`,
  `test_exists_checks_everything`). The last one exercises the unindexed namespace-scan path, because it lists
  `TABLE_DATA`.

**Result (2026-10-21):** `--gtest_filter='*Storage*:*Lock*:*GenericStorageTest*:*StorageTestSuite*:*rados*'` →
**111/111 passed**, including every RADOS case. The other backends' tests in the same run also passed, which shows
the hook patches didn't break them.

### Python (real cluster)

`storage_fixtures/rados.py` implements upstream's `StorageFixture`. Each fixture uses an isolated
`namespace_prefix` and cleans up through `delete_library` (`fast_delete`). The conftest patch adds `rados` to:

- `arctic_client`, `arctic_client_v1`
- `basic_store_factory`, `object_store_factory`
- `object_and_mem_and_lmdb_version_store`

These are gated by `ARCTICDB_STORAGE_RADOS=1` and `ARCTICDB_RADOS_TEST_URI`. `scripts/minicluster.sh` starts a
disposable cluster:

- Ceph 19.2.3, 1 mon, 1 mgr, 3 memstore OSDs, with cephx;
- replicated pools `arcticdb` and `arcticdb-meta`;
- a `client.arcticdb` user with pool-scoped caps.

**Results (2026-10-21, minicluster, upstream's local backends disabled so only the `rados` parameter runs):**

| Suite | Result |
|---|---|
| `tests/integration/arcticdb/test_arctic.py` | **238 passed, 0 failed** (the 1,587 skips are the other backends' parameters) |
| Column stats: `test_column_stats_{creation,optimisation,isin}.py` + `test_admin_tools.py::test_get_sizes_includes_column_stats` | **469 passed on RADOS, 0 failed** (467 on the in-memory baseline in the same run; 1 xfail each, upstream-marked) |

Findings from these runs, recorded here for transparency:

- **cephx caps.** With `mon 'profile r'`, the first `Arctic(...)` call **hung indefinitely**. The cause was the client
  capabilities, not the backend: `rados ls` with the same user hung too. With `mon 'allow r'` it works.
  `minicluster.sh` now grants `mon 'allow r'`. Two lessons:
  1. the required caps must be documented: `mon 'allow r'`, `osd 'allow rwx pool=<data>, allow rwx pool=<meta>'`;
  2. `op_timeout` should probably default to non-zero, so that misconfiguration fails instead of hanging. librados
     waits forever by default.
- **A false alarm.** An early smoke test compared frames with `DataFrame.equals` and failed. The same comparison
  fails identically on upstream's `mem://` and `lmdb://` backends (the values, index and dtypes are equal; the
  difference is frame metadata). `assert_frame_equal` passes on all three. This is not a RADOS issue. A
  150-iteration random write/read/verify stress run showed 0 mismatches on RADOS and on LMDB.

### 10.1 Column stats

Column stats (per-segment min/max, used to skip segments on filtered reads) need no backend-specific code. They are
stored as `COLUMN_STATS` keys, which this backend puts in the data pool and tracks in the key index like any other
indexed type.

To verify that, without writing new tests:

- **Unit suites.** The upstream column-stats unit suites (`test_column_stats_creation.py`,
  `test_column_stats_optimisation.py`, `test_column_stats_isin.py`) were hard-wired to the in-memory backend. Patch
  0005 parametrises their store fixture over `mem` and `rados`.
- **Query-stats assertions.** Their query-stats assertions (e.g. "exactly one `COLUMN_STATS` read") now accept
  either backend's operation names.
- **New task types.** This required `Rados_*` task types in `query_stats` (patch 0002). The backend now reports
  `Rados_GetObject`, `Rados_PutObject`, `Rados_DeleteObject`, `Rados_HeadObject` and `Rados_ListObjects`, the same
  way the S3 and memory backends report theirs.
- **Already parametrised.** `test_admin_tools.py::test_get_sizes_includes_column_stats` was already parametrised over
  backends (`arctic_client`), so it covers RADOS without changes.

### Not yet tested

- **Mock limitations.** The C++ mock cannot test real librados behaviour, such as:
  - aio completion threading, cursors, zero-copy buffer lifetimes;
  - fork handling, timeouts, error codes from a real OSD.

  Only the Python suite against a real cluster covers these.
- **Untested scenarios:**
  - chunking (> 64 MiB segments) against a real OSD;
  - concurrent writers from multiple processes or hosts (including `ReliableStorageLock` contention across
    clients);
  - `rebuild_index()`;
  - `meta_pool` on distinct media;
  - erasure-coded data pools;
  - multi-OSD failure or recovery during I/O.
- **Performance:** not measured (section 12).

---

## 11. Known limitations and open issues

These are stated plainly so reviewers don't have to find them:

1. **The index and the data are not updated atomically** (section 5). The ordering avoids dangling entries but can
   leak unindexed objects after a crash. `rebuild_index()` repairs this offline only, and isn't exposed to Python.
2. **Chunked atom keys:** tails are written before the exclusive head. If two writers race on the *same* chunked
   atom key, the loser has rewritten the tails before getting `-EEXIST`. Atom keys are content-addressed, so the
   bytes are identical in practice, but this is reasoning, not a guarantee enforced by the backend.
3. **Overwriting a chunked ref key with a smaller value leaves the old tails.** These are not reclaimed by
   `rebuild_index()` while the head exists. Ref keys over 64 MiB are not expected.
4. **Every delete of a data-pool key costs two ops**: the remove plus a probe of `_chunk/<obj>/1`. Without a read
   before the delete, there is no way to know whether a head was chunked. This doubles delete ops for data keys.
   Alternatives: record chunking in the key index, or always `getxattrs` before removing.
5. **Namespace scans and `fast_delete` hold whole listings in memory and walk slices sequentially.** Parallel slice
   listing and streaming deletes are straightforward follow-ups.
6. **`query_stats` coverage is basic.** RADOS ops are now recorded (`Rados_*` task types, patch 0002), but internal
   ops are not broken out separately: index omap updates and lists, chunk tails, and probe removes all count under
   the parent operation. The delete timing is per batch, not per key.
7. **Pool placement per key type is hard-coded** (`RadosStorage::pool_for`).
8. **The Python-level mock is not usable.** The mock is per-`Storage`-instance in memory, and `Arctic` opens a
   library several times, so Python tests need a real cluster. That is why they are opt-in.
9. **Boost ABI mixing** when linking a conda-built ArcticDB against distro librados (section 9).
10. **Erasure-coded data pools are not supported or tested.** EC pools don't support omap, and the meta pool must be
    replicated.
11. **A single namespace prefix per Arctic URI.** cephx namespace-scoped caps are possible but not yet documented or
    tested.

---

## 12. Expected performance and how it will be measured

There are **no numbers yet.** Based on the architecture, the expected gains over RGW/S3 on the same cluster are:

- higher small-object throughput, with no HTTP and no bucket-index two-phase writes;
- much lower `list_symbols` latency, with no bucket-index walk and no prefix scan;
- bulk throughput that is closer to `rados bench`.

These are expectations, not results. `bench/bench.py` measures the following workloads on any set of Arctic URIs:

- `write_wide` / `read_wide` (MB/s)
- `batch_read` (MB/s)
- `append_ticks` (ms per op)
- `list_symbols` (ms)
- `delete_library` (s)

The plan is to run it against this backend and against RGW on the same cluster, with `rados bench` as the ceiling.
All results will be published, including any where RADOS is not faster.

---

## 13. How this was produced

The design, code, scripts and this document were developed with an AI coding assistant (Claude Code) working under
the author's direction. The author made the key decisions:

- client-only (no cls modules);
- replicated pools;
- delivery as a separate engine plus patches;
- reuse of upstream test suites.

The assistant researched the ArcticDB and Ceph sources and wrote the implementation. Every claim about test
results in this document comes from runs recorded in this repository's history. Untested areas are marked as
such. Reviewers should apply the same scrutiny as for any first contribution, and probably more for the
concurrency and failure-handling paths listed in section 11.
