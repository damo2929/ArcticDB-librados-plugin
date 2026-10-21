"""
Compare ArcticDB storage backends on the same cluster, e.g. native RADOS vs RGW/S3:

    python bench/bench.py \
        --uri "rados://arcticdb?conf=/etc/ceph/ceph.conf&client_id=arcticdb" \
        --uri "s3://rgw.example:arcticdb?access=...&secret=..." \
        --rows 2000000 --cols 50 --symbols 2000

Workloads:
  write_wide / read_wide   one large frame, written and read back in full (bulk throughput)
  batch_read               read many medium symbols in one batch (parallel fan-out)
  append_ticks             many small appends to one symbol (small-write latency, version chain)
  list_symbols             list_symbols over many symbols (key index)
  delete_library           delete everything (fast_delete / batch remove)

Use `rados bench -p <pool> 30 write --no-cleanup && rados bench -p <pool> 30 seq` for the raw cluster ceiling.
"""

import argparse
import time
import uuid

import numpy as np
import pandas as pd

from arcticdb import Arctic, WritePayload
from arcticdb_ext import set_config_int


# Runs fn and returns (elapsed seconds, result).
def timed(fn):
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


# Random float frame with a 1-second DatetimeIndex starting `start` seconds after 2000-01-01.
def frame(rows, cols, start=0):
    idx = pd.date_range("2000-01-01", periods=rows, freq="s") + pd.Timedelta(seconds=start)
    return pd.DataFrame(np.random.rand(rows, cols), index=idx, columns=[f"c{i}" for i in range(cols)])


# Runs every workload against one Arctic URI in a fresh library and returns {metric: value}.
def run(uri, args):
    ac = Arctic(uri)
    name = f"bench_{uuid.uuid4().hex[:8]}"
    lib = ac.create_library(name)
    results = {}

    wide = frame(args.rows, args.cols)
    mb = wide.memory_usage(deep=False).sum() / 1e6
    t, _ = timed(lambda: lib.write("wide", wide))
    results["write_wide MB/s"] = mb / t
    t, _ = timed(lambda: lib.read("wide").data)
    results["read_wide MB/s"] = mb / t

    medium = frame(args.rows // 100, args.cols)
    syms = [f"m{i}" for i in range(args.batch)]
    lib.write_batch([lib_write_payload(s, medium) for s in syms])
    t, _ = timed(lambda: lib.read_batch(syms))
    results["batch_read MB/s"] = len(syms) * medium.memory_usage().sum() / 1e6 / t

    lib.write("ticks", frame(1, args.cols))
    t, _ = timed(lambda: [lib.append("ticks", frame(10, args.cols, start=1 + 10 * i)) for i in range(args.appends)])
    results["append_ticks ms/op"] = 1000 * t / args.appends

    tiny = frame(1, 2)
    lib.write_batch([lib_write_payload(f"s{i}", tiny) for i in range(args.symbols)])
    t, listed = timed(lambda: lib.list_symbols())
    results["list_symbols ms"] = 1000 * t
    assert len(listed) >= args.symbols

    t, _ = timed(lambda: ac.delete_library(name))
    results["delete_library s"] = t
    return results


# Batch write payload helper.
def lib_write_payload(symbol, data):
    return WritePayload(symbol, data)


# Parses arguments, runs the workloads for each URI and prints a comparison table.
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--uri", action="append", required=True)
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--cols", type=int, default=50)
    p.add_argument("--batch", type=int, default=100)
    p.add_argument("--appends", type=int, default=200)
    p.add_argument("--symbols", type=int, default=1000)
    p.add_argument("--io-threads", type=int, default=64)
    args = p.parse_args()

    set_config_int("VersionStore.NumIOThreads", args.io_threads)
    table = {uri.split("?")[0]: run(uri, args) for uri in args.uri}
    print(pd.DataFrame(table).round(2).to_string())


if __name__ == "__main__":
    main()
