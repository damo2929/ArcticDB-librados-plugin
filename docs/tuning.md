# Tuning ArcticDB on RADOS

## Pools

- **Pool types.** Use a replicated pool for the data pool. Optionally add a `meta_pool` on NVMe for refs, versions,
  locks and the key index. The meta pool must be replicated because it uses omap.
- **Compression.** Turn BlueStore compression off for both pools. ArcticDB segments are already LZ4/ZSTD-compressed,
  so a second pass only costs CPU:
  `ceph osd pool set arcticdb compression_mode none`
- **Placement groups.** Size PGs for parallelism. Set the autoscaler `bulk` flag on the data pool so it starts wide:
  `ceph osd pool set arcticdb bulk true`
- **Object sizes.** Segment size is set by `rows_per_segment` × `columns_per_segment` in `LibraryOptions`. Objects
  of 1–16 MiB use BlueStore and the network efficiently. Keep segments under `chunk_size` (64 MiB) to avoid chunking.

## Client (librados) options

Pass options with `conf.<option>=<value>` in the URI, or set them in ceph.conf under `[client.arcticdb]`.

| Option | Default | Suggested | Why |
|---|---|---|---|
| `objecter_inflight_ops` | 1024 | 8192 | Caps concurrent ops per client. ArcticDB fans out one op per segment. |
| `objecter_inflight_op_bytes` | 100 MiB | 1–4 GiB | Caps bytes in flight. With 100 MiB, large reads serialise. |
| `ms_async_op_threads` | 3 | 4–8 | Messenger worker threads on very fast networks. |
| `rados_osd_op_timeout` | 0 (wait forever) | URI `op_timeout=30` | Fail with `E_RADOS_RETRYABLE` instead of hanging when OSDs flap. |

## ArcticDB options

```python
from arcticdb_ext import set_config_int
set_config_int("VersionStore.NumIOThreads", 64)   # writes are synchronous per key: this is write concurrency
set_config_int("VersionStore.NumCPUThreads", 16)
set_config_int("Rados.Async", 1)                  # default; 0 forces synchronous reads on IO threads
```

## Measuring

`bench/bench.py` compares:

- this backend
- the same cluster through RGW (S3 URI)
- the raw `rados bench` ceiling
