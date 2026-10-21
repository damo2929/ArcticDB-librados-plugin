/* Native Ceph RADOS storage backend for ArcticDB.
 *
 * Client abstraction between RadosStorage and librados. It deals only in object names and bytes, so the storage
 * logic (key naming, index, chunking, error mapping) is exercised identically by the real librados client and by the
 * in-memory mock used in the upstream storage test suites. Nothing here includes librados headers, so the storage
 * and mock compile on builds without librados.
 */

#pragma once

#include <folly/futures/Future.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace arcticdb::storage::rados {

// Results carry negated errno values exactly as librados returns them (0 == success).
using RadosErrc = int;

// Which pool an object lives in. With no meta_pool configured both map to the same IoCtx.
enum class PoolKind : uint8_t { DATA, META };

// Read-only bytes plus whatever keeps them alive (a librados bufferlist, or a mock std::string).
struct RadosBytes {
    const uint8_t* data = nullptr;
    size_t size = 0;
    std::shared_ptr<void> keepalive;
};

struct RadosReadResult {
    RadosErrc rc = 0;
    RadosBytes bytes;
    std::map<std::string, std::string> xattrs;
};

enum class WriteMode : uint8_t {
    OVERWRITE,    // write_full
    EXCLUSIVE,    // create(exclusive) + write_full; -EEXIST if present
    MUST_EXIST,   // assert_exists + write_full; -ENOENT if absent
};

// A buffer we are asked to write. `keepalive` must outlive the I/O.
struct RadosWriteBuffer {
    const uint8_t* data = nullptr;
    size_t size = 0;
    std::shared_ptr<void> keepalive;
    std::map<std::string, std::string> xattrs;
};

struct OmapPage {
    RadosErrc rc = 0;
    std::vector<std::pair<std::string, std::string>> entries;
    bool more = false;
};

struct ListPage {
    RadosErrc rc = 0;
    std::vector<std::string> names;
    std::optional<std::string> cursor; // set if there are more
};

class RadosClientInterface {
  public:
    virtual ~RadosClientInterface() = default;

    // Whole-object read, plus xattrs in the same round trip.
    virtual folly::Future<RadosReadResult> read(PoolKind pool, const std::string& oid) = 0;

    virtual folly::Future<RadosErrc> write(
            PoolKind pool, const std::string& oid, RadosWriteBuffer buffer, WriteMode mode
    ) = 0;

    // stat; on success size is filled in.
    virtual folly::Future<RadosErrc> stat(PoolKind pool, const std::string& oid, uint64_t* size) = 0;

    virtual folly::Future<RadosErrc> remove(PoolKind pool, const std::string& oid) = 0;

    // omap index operations. omap_set creates the object if needed.
    virtual folly::Future<RadosErrc> omap_set(
            PoolKind pool, const std::string& oid, std::map<std::string, std::string> entries
    ) = 0;
    virtual folly::Future<RadosErrc> omap_rm(PoolKind pool, const std::string& oid, std::vector<std::string> keys) = 0;
    virtual folly::Future<OmapPage> omap_list(
            PoolKind pool, const std::string& oid, const std::string& start_after, const std::string& filter_prefix,
            uint64_t max_entries
    ) = 0;

    // Lists every object in this client's namespace in `pool`. Listing is split into `num_slices` independent slices
    // that can be paged concurrently. `cursor` is an opaque token returned by the previous page of the same slice.
    virtual ListPage list_objects(
            PoolKind pool, size_t slice, size_t num_slices, const std::optional<std::string>& cursor, size_t max
    ) = 0;

    [[nodiscard]] virtual bool pools_are_distinct() const = 0;
    [[nodiscard]] virtual std::string describe(PoolKind pool) const = 0;
};

} // namespace arcticdb::storage::rados
