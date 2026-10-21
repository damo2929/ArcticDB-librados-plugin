# Design: native RADOS storage engine for ArcticDB

## Why not RGW/S3

The S3 path goes ArcticDB → AWS SDK → HTTP → RGW → librados → OSDs. Every key costs an HTTP request, RGW CPU, and a
bucket-index update in RGW's own omap, and RGW listing is a sorted, paginated walk of that index. Talking to RADOS
directly removes the gateway hop, the HTTP framing and the bucket index. Clients go straight to the primary OSD of
each object, so parallelism grows with the number of OSDs rather than the number of gateways.

## How ArcticDB storage backends plug in

- **`Storage` interface.** Backends subclass `Storage` (`cpp/arcticdb/storage/storage.hpp`) and implement the
  `do_*` hooks: write, write_if_none, update, read, remove (single and batch), key_exists, iterate_type, fast_delete,
  key_path and object sizes.
- **Async reads.** A backend can also implement `AsyncStorage` (`do_async_read`), which ArcticDB uses when
  `has_async_api()` returns true.
- **Selection.** The backend is chosen by protobuf type name in `storage_factory.cpp`. On the Python side it is
  chosen by URI scheme through the adapters in `arctic.py::_LIBRARY_ADAPTERS`.
- **Threading.** All storage calls run on ArcticDB's folly IO thread pool, so `do_*` must be thread-safe.
- **Existing backends.**
  - S3 is structured as mockable client interface → real client → logic.
  - Memory and LMDB define the strict semantics: atom-key writes throw `DuplicateKeyException`, missing keys throw
    `KeyNotFoundException`, and `update(upsert=false)` on a missing key throws.

This engine follows the S3 structure (`RadosClientInterface` → `RadosClientImpl` / `MockRadosClient` →
`RadosStorage`) and gives Memory/LMDB semantics, so it can join `GenericStorageTest`.

## Object layout

```
pool "arcticdb"        namespace "<namespace_prefix>/<library path>"
  tdata/<token>                 TABLE_DATA          (data pool)
  tindex/<token>                TABLE_INDEX         (data pool)
  _chunk/tdata/<token>/<n>      tail chunks of segments > chunk_size
pool "arcticdb-meta"   same namespace           (or the data pool when no meta_pool is configured)
  vref/<token> ver/<token> sl/<token> snap/<token> lref/<token> alref/<token> ...
  _idx/<key type>/<shard>       omap key index
```

- **One namespace per library.** Libraries can't see each other's objects. Listing and `fast_delete` only touch the
  library's own namespace, and cephx caps can be scoped per namespace.
- **No bucketizer.** RADOS hashes the full object name into a PG. There are no hot prefixes, so unlike S3/NFS no
  sharding scheme is needed.

## Key index

RADOS has no server-side name-prefix listing. pgls walks every object in every PG and only filters on namespace,
which is O(objects in pool). That is unacceptable for `list_symbols`, which iterates `SYMBOL_LIST` keys on every
call, or for snapshot and version scans.

So each indexed key type has N omap shard objects (`_idx/<type>/<shard>`, N = `index_shards`, default 8).

- **Entry format.** Each entry is `<stream id>\x1f<token>` → `<size>\n<token>`. The shard is chosen by
  `fnv64(entry) % N`.
- **Prefix filtering on the OSD.** `omap_get_vals2(filter_prefix=<prefix>)` runs on the OSD, so a prefix listing
  only transfers matching keys.
- **Parallel reads.** All N shards are read in parallel.
- **Sizes for free.** The size in the entry value serves `visit_object_sizes` with no stat calls.
- **Ordering ("leak, never lie").** Writes put the data first and the index entry second. Deletes remove the index
  entry first and the data second. A crash leaves an invisible orphan, never a dangling entry.
  `RadosStorage::rebuild_index()` rebuilds the index and drops orphaned chunk tails.
- **`TABLE_DATA` is unindexed by default.** It is ~all of the write volume and is only listed by maintenance paths,
  so data writes stay a single round trip. Listing it falls back to a namespace scan (`object_list_slice`, 16
  slices). The `unindexed` URI option controls this.

## Operation mapping

| ArcticDB | RADOS |
|---|---|
| `write` atom key | `create(exclusive)+write_full` (one OSD transaction). `-EEXIST` → `DuplicateKeyException` |
| `write` ref key | `write_full` |
| `write_if_none` | `create(exclusive)+write_full`. `-EEXIST` → `AtomicOperationFailedException`. `supports_atomic_writes = YES` |
| `update(upsert=false)` | `assert_exists+write_full`. `-ENOENT` → `KeyNotFoundException` |
| `read` | `read(0, 0)` (whole object) + `getxattrs` in one op, via aio |
| `remove` (batch) | per-shard `omap_rm_keys`, then concurrent aio removes (up to 256 in flight, 1000 per batch) |
| `key_exists` | `stat` |
| `iterate_type` | index shards (parallel `omap_get_vals2` with prefix filter) or namespace scan |
| `fast_delete` | list namespace(s), remove all concurrently, index objects first |

Errors map as follows:

| librados error | ArcticDB error |
|---|---|
| `-ENOENT` | `KeyNotFoundException` |
| `-EACCES` / `-EPERM` | `E_PERMISSION` |
| `-EDQUOT` / `-ENOSPC` | `E_QUOTA_EXCEEDED` |
| `-ETIMEDOUT` / `-EAGAIN` / `-EBUSY` / `-ESHUTDOWN` / `-ENOTCONN` | `E_RADOS_RETRYABLE` |
| anything else | `E_UNEXPECTED_RADOS_ERROR` |

## Performance path

- **Shared cluster handle.** There is one `librados::Rados` per (cluster, user, conf, keyring, overrides) per
  process (`rados_cluster_registry`). All libraries share its messenger threads, OSD sessions and objecter throttle.
  The handle is recreated after `fork()`.
- **Shared IoCtx.** One `IoCtx` per pool per library, with the namespace set once and never changed. librados ops
  on it are thread-safe.
- **All aio.** Every op uses `aio_operate`. Completion callbacks only fulfil a `folly::Promise`. Async reads are
  continued on ArcticDB's IO executor, never on the librados finisher thread.
- **Zero-copy write.** `write_full` sends from a `bufferlist::static_from_mem` over the bytes returned by
  `Segment::serialize_header()`. The segment is kept alive until the op completes.
- **Zero-copy read.** The returned `bufferlist` (contiguous for the normal single-message case) backs the
  `Segment` through `Segment::from_bytes(copy=false)` plus `set_keepalive`.
- **Sequential read hint.** Reads carry `FADVISE_SEQUENTIAL`.
- **Large segments.** Segments larger than `chunk_size` (64 MiB default) are split: tails are written in parallel
  first, the head is committed last with an `adb.chunks` xattr, and reads fetch the tails in parallel. This keeps
  writes under `osd_max_write_size` (90 MiB) and `osd_max_object_size` (128 MiB).

## Known limitations

- **No cross-object transactions.** Index updates are separate ops from the data writes (see the ordering rule).
  They are invisible to correctness: listing tolerates leaks, and ArcticDB already treats iteration as best effort.
- **Exclusive create on chunked atom keys.** For a chunked atom key, the tails are written before the exclusive
  head. Atom keys are content-addressed, so a losing writer only rewrites identical bytes.
- **Old tails on overwrite.** Replacing a chunked ref key with a smaller one leaves the old tails behind, because
  ref keys are never that large in practice. `rebuild_index()` does not reclaim them while the head exists.
- **Synchronous writes.** ArcticDB's write API is synchronous per key, so write concurrency equals
  `VersionStore.NumIOThreads`. Raise it (see tuning.md).
