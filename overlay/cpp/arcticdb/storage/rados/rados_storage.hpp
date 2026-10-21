/* Native Ceph RADOS storage backend for ArcticDB.
 *
 * Layout inside a pool:
 *   namespace  "<namespace_prefix>/<library path>"      one RADOS namespace per library
 *   object     "<key type long name>/<tokenized key>"   one object per key (data pool or meta pool, by key type)
 *   object     "_chunk/<object>/<n>"                    tail chunks of segments larger than chunk_size
 *   object     "_idx/<key type long name>/<shard>"      omap key index (meta pool)
 *
 * The omap index lets listing (list_symbols, snapshots, locks, version scans, ...) read only the keys it needs,
 * filtered by stream id prefix on the OSD, instead of walking every object of every PG. TABLE_DATA, which makes up
 * the bulk of the writes and is only listed by maintenance tools, is not indexed by default so data writes remain a
 * single round trip.
 *
 * Ordering is "leak, never lie": data is written before its index entry, and index entries are removed before their
 * data. A crash can leave an unindexed object behind, but the index never names an object that does not exist.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/rados/rados_client_interface.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/pb_util.hpp>

#include <bitset>

namespace arcticdb::storage::rados {

class RadosStorage final : public Storage, public AsyncStorage {
  public:
    using Config = arcticdb::proto::rados_storage::Config;

    // Defaults used when the config leaves the field at 0.
    static constexpr uint32_t DEFAULT_INDEX_SHARDS = 8;
    static constexpr uint64_t DEFAULT_CHUNK_SIZE = 64ULL << 20;
    // Largest batch ArcticDB will pass to do_remove(span); AsyncStore splits bigger deletes.
    static constexpr size_t DELETE_BATCH_SIZE = 1000;

    RadosStorage(const LibraryPath& lib, OpenMode mode, const Config& conf);

    // Storage hooks: see the definitions in rados_storage.cpp for what each one does on RADOS.
    std::string name() const final;

    [[nodiscard]] bool has_async_api() const final;

    AsyncStorage* async_api() final { return this; }

    [[nodiscard]] std::optional<size_t> max_delete_batch_size() const final { return DELETE_BATCH_SIZE; }

    [[nodiscard]] bool supports_object_size_calculation() const final { return true; }

    // Rebuild the omap index of every indexed key type from a full namespace scan, and remove chunk tails whose head
    // is gone. Returns the number of index entries written.
    size_t rebuild_index();

  private:
    void do_write(KeySegmentPair& key_seg) final;

    void do_write_if_none(KeySegmentPair& kv) final;

    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) final;

    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts opts) final;

    folly::Future<folly::Unit> do_async_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts)
            final;

    folly::Future<KeySegmentPair> do_async_read(VariantKey&& variant_key, ReadKeyOpts opts) final;

    void do_remove(VariantKey&& variant_key, RemoveOpts opts) final;

    void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final { return true; }

    SupportsAtomicWrites do_supports_atomic_writes() const final { return SupportsAtomicWrites::YES; }

    bool do_fast_delete() final;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix)
            final;

    void do_visit_object_sizes(KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor) final;

    std::string do_key_path(const VariantKey& key) const final;

    // Helpers
    void write_segment(KeySegmentPair& key_seg, WriteMode mode);
    folly::Future<RadosErrc> write_bytes(PoolKind pool, const std::string& oid, RadosWriteBuffer buffer, WriteMode mode);
    folly::Future<KeySegmentPair> read_segment(VariantKey&& variant_key, folly::Executor* executor);
    void remove_chunk_tails(PoolKind pool, const std::string& oid);

    [[nodiscard]] bool is_indexed(KeyType key_type) const { return indexed_.test(static_cast<size_t>(key_type)); }
    [[nodiscard]] static PoolKind pool_for(KeyType key_type);
    [[nodiscard]] std::string index_oid(KeyType key_type, const std::string& entry_key) const;
    [[nodiscard]] std::string index_shard_oid(KeyType key_type, uint32_t shard) const;
    void index_add(const VariantKey& key, uint64_t size);
    void index_remove(std::span<VariantKey> keys);

    // Visits (key, size) for every key of key_type matching prefix. size is only populated when want_sizes is set (or
    // when it is free, from the index). Stops when the visitor returns true.
    bool scan(
            KeyType key_type, const std::string& prefix, bool want_sizes,
            const std::function<bool(VariantKey&&, uint64_t)>& visitor
    );
    bool scan_index(
            KeyType key_type, const std::string& prefix, const std::function<bool(VariantKey&&, uint64_t)>& visitor
    );
    bool scan_namespace(
            KeyType key_type, const std::string& prefix, bool want_sizes,
            const std::function<bool(VariantKey&&, uint64_t)>& visitor
    );
    void for_each_object(PoolKind pool, const std::function<void(std::vector<std::string>&&)>& visitor);

    // librados (or mock) client bound to this library's namespace.
    std::unique_ptr<RadosClientInterface> client_;
    // RADOS namespace holding all of this library's objects.
    std::string namespace_;
    // Number of omap index shards per key type. Must be the same for every client of a library (persisted config).
    uint32_t index_shards_;
    // Segments larger than this are split across objects.
    uint64_t chunk_size_;
    // Which key types are tracked in the omap index.
    std::bitset<static_cast<size_t>(KeyType::UNDEFINED) + 1> indexed_;
};

// Convenience for tests and tooling: a VariantStorage holding a RADOS config for the given pool.
inline arcticdb::proto::storage::VariantStorage pack_config(const std::string& pool, bool use_mock = false) {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::rados_storage::Config cfg;
    cfg.set_pool(pool);
    cfg.set_use_mock_storage_for_testing(use_mock);
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

} // namespace arcticdb::storage::rados
