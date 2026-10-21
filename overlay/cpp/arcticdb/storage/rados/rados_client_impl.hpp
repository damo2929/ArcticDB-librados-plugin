/* Native Ceph RADOS storage backend for ArcticDB.
 *
 * librados implementation of RadosClientInterface. Every operation is submitted with aio_operate and completes a
 * folly::Promise from the librados finisher thread, so callers can fan out as many concurrent ops as they like and
 * the objecter pipelines them to the OSDs.
 */

#pragma once

#include <arcticdb/storage/rados/rados_client_interface.hpp>
#include <arcticdb/storage/rados/rados_cluster_registry.hpp>

#include <rados/librados.hpp>

namespace arcticdb::storage::rados {

class RadosClientImpl final : public RadosClientInterface {
  public:
    RadosClientImpl(
            const ConnectionParams& params, const std::string& pool, const std::string& meta_pool,
            const std::string& rados_namespace
    );

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

    [[nodiscard]] bool pools_are_distinct() const override { return distinct_pools_; }
    [[nodiscard]] std::string describe(PoolKind pool) const override;

  private:
    librados::IoCtx& ioctx(PoolKind pool) { return pool == PoolKind::META && distinct_pools_ ? meta_ : data_; }

    std::shared_ptr<librados::Rados> cluster_;
    // The namespace is set once here and never changed, so the IoCtxs can be shared by all IO threads.
    librados::IoCtx data_;
    librados::IoCtx meta_;
    std::string pool_;
    std::string meta_pool_;
    std::string namespace_;
    bool distinct_pools_;
};

} // namespace arcticdb::storage::rados
