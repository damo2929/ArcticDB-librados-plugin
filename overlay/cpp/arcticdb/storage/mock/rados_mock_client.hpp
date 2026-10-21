/* Native Ceph RADOS storage backend for ArcticDB.
 *
 * In-memory stand-in for a RADOS namespace, used when rados_storage Config.use_mock_storage_for_testing is set.
 * Mirrors librados error semantics (-ENOENT, -EEXIST, omap on missing object, ...). Like the other mock clients,
 * failures can be injected by putting "#Failure_<Operation>_<errno>_" in an object name.
 */

#pragma once

#include <arcticdb/storage/rados/rados_client_interface.hpp>
#include <arcticdb/storage/mock/storage_mock_client.hpp>

#include <mutex>

namespace arcticdb::storage::rados {

// Returns the negated errno encoded in the object name for this operation, if any.
std::optional<RadosErrc> has_object_failure_trigger(const std::string& oid, StorageOperation operation);

class MockRadosClient final : public RadosClientInterface {
  public:
    folly::Future<RadosReadResult> read(PoolKind pool, const std::string& oid) override;

    folly::Future<RadosErrc> write(PoolKind pool, const std::string& oid, RadosWriteBuffer buffer, WriteMode mode)
            override;

    folly::Future<RadosErrc> stat(PoolKind pool, const std::string& oid, uint64_t* size) override;

    folly::Future<RadosErrc> remove(PoolKind pool, const std::string& oid) override;

    folly::Future<RadosErrc> omap_set(PoolKind pool, const std::string& oid, std::map<std::string, std::string> entries)
            override;

    folly::Future<RadosErrc> omap_rm(PoolKind pool, const std::string& oid, std::vector<std::string> keys) override;

    folly::Future<OmapPage> omap_list(
            PoolKind pool, const std::string& oid, const std::string& start_after, const std::string& filter_prefix,
            uint64_t max_entries
    ) override;

    ListPage list_objects(
            PoolKind pool, size_t slice, size_t num_slices, const std::optional<std::string>& cursor, size_t max
    ) override;

    [[nodiscard]] bool pools_are_distinct() const override { return false; }
    [[nodiscard]] std::string describe(PoolKind) const override { return "mock"; }

  private:
    struct Object {
        std::shared_ptr<std::string> data = std::make_shared<std::string>();
        std::map<std::string, std::string> xattrs;
        std::map<std::string, std::string> omap;
    };

    std::map<std::string, Object> objects_;
    mutable std::mutex mutex_;
};

} // namespace arcticdb::storage::rados
