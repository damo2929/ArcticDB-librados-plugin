/* Native Ceph RADOS storage backend for ArcticDB. */

#include <arcticdb/storage/rados/rados_storage.hpp>

#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/mock/rados_mock_client.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/configs_map.hpp>

#ifdef ARCTICDB_WITH_RADOS
#include <arcticdb/storage/rados/rados_client_impl.hpp>
#endif

#include <folly/futures/Future.h>
#include <folly/hash/Hash.h>

#include <cerrno>
#include <cstring>

namespace arcticdb::storage::rados {

namespace {

constexpr std::string_view CHUNK_XATTR = "adb.chunks";
constexpr char ENTRY_SEP = '\x1f';
constexpr size_t OMAP_PAGE = 1000;
constexpr size_t LIST_PAGE = 1000;
constexpr size_t LIST_SLICES = 16;
// Upper bound on in-flight ops issued by one batch call (deletes, stats, chunk reads).
constexpr size_t MAX_IN_FLIGHT = 256;

[[noreturn]] void raise_rados_error(RadosErrc rc, std::string_view what, std::string_view object) {
    const auto message = fmt::format("RADOS {} failed for '{}': {} ({})", what, object, std::strerror(-rc), rc);
    switch (-rc) {
    case EACCES:
    case EPERM:
        raise<ErrorCode::E_PERMISSION>(message);
    case EDQUOT:
    case ENOSPC:
        raise<ErrorCode::E_QUOTA_EXCEEDED>(message);
    case ETIMEDOUT:
    case EAGAIN:
    case EBUSY:
    case ESHUTDOWN:
    case ENOTCONN:
        raise<ErrorCode::E_RADOS_RETRYABLE>(message);
    case EFBIG:
    case EMSGSIZE:
        raise<ErrorCode::E_UNEXPECTED_RADOS_ERROR>(fmt::format(
                "{}. The object exceeds osd_max_object_size / osd_max_write_size; lower chunk_size in the RADOS "
                "storage config",
                message
        ));
    default:
        raise<ErrorCode::E_UNEXPECTED_RADOS_ERROR>(message);
    }
}

std::string object_name(const VariantKey& key) {
    return fmt::format("{}/{}", key_type_long_name(variant_key_type(key)), to_tokenized_key(key));
}

std::string chunk_name(const std::string& oid, size_t n) { return fmt::format("_chunk/{}/{}", oid, n); }

std::string stream_id_string(const StreamId& id) {
    return util::variant_match(
            id, [](const StringId& s) { return s; }, [](NumericId n) { return std::to_string(n); }
    );
}

// omap key: "<stream id>\x1f<token>" so that OSD-side filter_prefix does stream id prefix matching.
std::string index_entry_key(const VariantKey& key) {
    return fmt::format("{}{}{}", stream_id_string(variant_key_id(key)), ENTRY_SEP, to_tokenized_key(key));
}

// omap value: "<size>\n<token>". The token is repeated in the value because a string id may itself contain the
// separator, which would make splitting the omap key ambiguous.
std::string index_entry_value(const VariantKey& key, uint64_t size) {
    return fmt::format("{}\n{}", size, to_tokenized_key(key));
}

std::pair<VariantKey, uint64_t> decode_index_entry(const std::string& value, KeyType key_type) {
    const auto nl = value.find('\n');
    util::check(nl != std::string::npos, "Corrupt RADOS index entry '{}'", value);
    const uint64_t size = std::stoull(value.substr(0, nl));
    const auto* token = reinterpret_cast<const uint8_t*>(value.data() + nl + 1);
    return {variant_key_from_bytes(token, value.size() - nl - 1, key_type), size};
}

bool matches_prefix(const VariantKey& key, const std::string& prefix) {
    return prefix.empty() || variant_key_id_starts_with(key, prefix);
}

template<typename T>
std::vector<folly::Try<T>> collect_windowed(size_t count, const std::function<folly::Future<T>(size_t)>& make) {
    std::vector<folly::Try<T>> results;
    results.reserve(count);
    for (size_t start = 0; start < count; start += MAX_IN_FLIGHT) {
        const auto end = std::min(count, start + MAX_IN_FLIGHT);
        std::vector<folly::Future<T>> futures;
        futures.reserve(end - start);
        for (auto i = start; i < end; ++i)
            futures.emplace_back(make(i));
        for (auto& r : folly::collectAll(std::move(futures)).get())
            results.emplace_back(std::move(r));
    }
    return results;
}

std::string build_namespace(const std::string& prefix, const LibraryPath& lib) {
    auto path = lib.to_delim_path('/');
    return prefix.empty() ? path : fmt::format("{}/{}", prefix, path);
}

std::unique_ptr<RadosClientInterface> create_client(const RadosStorage::Config& conf, const std::string& ns) {
    if (conf.use_mock_storage_for_testing()) {
        log::storage().warn("Using mock RADOS client");
        return std::make_unique<MockRadosClient>();
    }
#ifdef ARCTICDB_WITH_RADOS
    ConnectionParams params{
            .cluster_name = conf.cluster_name(),
            .client_id = conf.client_id(),
            .conf_path = conf.conf_path(),
            .keyring = conf.keyring(),
            .conf_overrides = {conf.conf_overrides().begin(), conf.conf_overrides().end()},
            .op_timeout_s = conf.op_timeout_s(),
    };
    return std::make_unique<RadosClientImpl>(params, conf.pool(), conf.meta_pool(), ns);
#else
    (void)ns;
    raise<ErrorCode::E_NOT_IMPLEMENTED_BY_STORAGE>(
            "This build of ArcticDB was compiled without librados (ARCTICDB_WITH_RADOS=OFF)"
    );
#endif
}

} // namespace

RadosStorage::RadosStorage(const LibraryPath& lib, OpenMode mode, const Config& conf) :
    Storage(lib, mode),
    namespace_(build_namespace(conf.namespace_prefix(), lib)),
    index_shards_(conf.index_shards() ? conf.index_shards() : DEFAULT_INDEX_SHARDS),
    chunk_size_(conf.chunk_size() ? conf.chunk_size() : DEFAULT_CHUNK_SIZE) {
    client_ = create_client(conf, namespace_);

    indexed_.set();
    if (conf.unindexed_key_types().empty()) {
        indexed_.reset(static_cast<size_t>(KeyType::TABLE_DATA));
    } else {
        for (const auto& name : conf.unindexed_key_types()) {
            bool found = false;
            foreach_key_type([&](KeyType kt) {
                if (name == key_type_long_name(kt)) {
                    indexed_.reset(static_cast<size_t>(kt));
                    found = true;
                }
            });
            util::check(found, "Unknown key type '{}' in RADOS unindexed_key_types", name);
        }
    }
}

std::string RadosStorage::name() const { return fmt::format("rados_storage-{}", client_->describe(PoolKind::DATA)); }

bool RadosStorage::has_async_api() const { return ConfigsMap::instance()->get_int("Rados.Async", 1) == 1; }

PoolKind RadosStorage::pool_for(KeyType key_type) {
    switch (key_type) {
    case KeyType::TABLE_DATA:
    case KeyType::TABLE_INDEX:
    case KeyType::APPEND_DATA:
    case KeyType::MULTI_KEY:
    case KeyType::COLUMN_STATS:
        return PoolKind::DATA;
    default:
        return PoolKind::META;
    }
}

std::string RadosStorage::index_shard_oid(KeyType key_type, uint32_t shard) const {
    return fmt::format("_idx/{}/{}", key_type_long_name(key_type), shard);
}

std::string RadosStorage::index_oid(KeyType key_type, const std::string& entry_key) const {
    // fnv is stable across processes and platforms, which the shard mapping has to be.
    return index_shard_oid(key_type, static_cast<uint32_t>(folly::hash::fnv64(entry_key) % index_shards_));
}

void RadosStorage::index_add(const VariantKey& key, uint64_t size) {
    const auto key_type = variant_key_type(key);
    if (!is_indexed(key_type))
        return;
    auto entry = index_entry_key(key);
    auto oid = index_oid(key_type, entry);
    std::map<std::string, std::string> entries{{std::move(entry), index_entry_value(key, size)}};
    if (auto rc = client_->omap_set(PoolKind::META, oid, std::move(entries)).get(); rc < 0)
        raise_rados_error(rc, "index update", oid);
}

void RadosStorage::index_remove(std::span<VariantKey> keys) {
    // One omap_rm per shard object, all in flight together.
    std::map<std::string, std::vector<std::string>> by_shard;
    for (const auto& key : keys) {
        const auto key_type = variant_key_type(key);
        if (!is_indexed(key_type))
            continue;
        auto entry = index_entry_key(key);
        by_shard[index_oid(key_type, entry)].push_back(std::move(entry));
    }
    if (by_shard.empty())
        return;
    std::vector<folly::Future<RadosErrc>> futures;
    std::vector<std::string> oids;
    for (auto& [oid, entries] : by_shard) {
        futures.emplace_back(client_->omap_rm(PoolKind::META, oid, std::move(entries)));
        oids.push_back(oid);
    }
    auto results = folly::collectAll(std::move(futures)).get();
    for (size_t i = 0; i < results.size(); ++i) {
        const auto rc = results[i].value();
        if (rc < 0 && rc != -ENOENT)
            raise_rados_error(rc, "index remove", oids[i]);
    }
}

folly::Future<RadosErrc> RadosStorage::write_bytes(
        PoolKind pool, const std::string& oid, RadosWriteBuffer buffer, WriteMode mode
) {
    if (buffer.size <= chunk_size_) {
        // An overwrite may replace a chunked object; reset the marker so readers don't look for tails.
        if (mode != WriteMode::EXCLUSIVE)
            buffer.xattrs.emplace(std::string{CHUNK_XATTR}, "1");
        return client_->write(pool, oid, std::move(buffer), mode);
    }

    // Tails first, in parallel; the head (first chunk + chunk count) last, so the object only becomes visible once
    // complete and an exclusive create still decides the race.
    const size_t chunks = (buffer.size + chunk_size_ - 1) / chunk_size_;
    auto tails = collect_windowed<RadosErrc>(chunks - 1, [&](size_t i) {
        const auto offset = (i + 1) * chunk_size_;
        RadosWriteBuffer tail{
                buffer.data + offset, std::min<size_t>(chunk_size_, buffer.size - offset), buffer.keepalive, {}
        };
        return client_->write(pool, chunk_name(oid, i + 1), std::move(tail), WriteMode::OVERWRITE);
    });
    for (auto& t : tails)
        if (t.value() < 0)
            return t.value();

    RadosWriteBuffer head{buffer.data, chunk_size_, std::move(buffer.keepalive), {}};
    head.xattrs.emplace(std::string{CHUNK_XATTR}, std::to_string(chunks));
    return client_->write(pool, oid, std::move(head), mode);
}

void RadosStorage::write_segment(KeySegmentPair& key_seg, WriteMode mode) {
    const auto& key = key_seg.variant_key();
    const auto oid = object_name(key);
    const auto pool = pool_for(key_seg.key_type());

    auto segment = key_seg.segment_ptr();
    auto [data, size, owned] = segment->serialize_header();
    // Zero copy: librados sends straight from the segment's buffer, which stays alive until the op completes.
    std::shared_ptr<void> keepalive = std::make_shared<std::pair<std::shared_ptr<Segment>, std::unique_ptr<Buffer>>>(
            segment, std::move(owned)
    );

    const auto rc = write_bytes(pool, oid, RadosWriteBuffer{data, size, std::move(keepalive), {}}, mode).get();
    if (rc == -EEXIST && mode == WriteMode::EXCLUSIVE)
        throw DuplicateKeyException(key);
    if (rc == -ENOENT && mode == WriteMode::MUST_EXIST)
        throw KeyNotFoundException(
                key, fmt::format("do_update called with upsert=false on non-existent key(s): {}", key)
        );
    if (rc < 0)
        raise_rados_error(rc, "write", oid);

    index_add(key, size);
}

void RadosStorage::do_write(KeySegmentPair& key_seg) {
    // Atom keys are immutable and content addressed, so an exclusive create costs nothing extra and gives the same
    // DuplicateKeyException semantics as LMDB and the memory storage. Ref keys are overwritten in place.
    const auto mode = std::holds_alternative<AtomKey>(key_seg.variant_key()) ? WriteMode::EXCLUSIVE
                                                                             : WriteMode::OVERWRITE;
    write_segment(key_seg, mode);
}

void RadosStorage::do_write_if_none(KeySegmentPair& kv) {
    // create(exclusive) + write_full is a single atomic OSD transaction.
    const auto oid = object_name(kv.variant_key());
    auto segment = kv.segment_ptr();
    auto [data, size, owned] = segment->serialize_header();
    std::shared_ptr<void> keepalive = std::make_shared<std::pair<std::shared_ptr<Segment>, std::unique_ptr<Buffer>>>(
            segment, std::move(owned)
    );
    const auto rc =
            write_bytes(pool_for(kv.key_type()), oid, {data, size, std::move(keepalive), {}}, WriteMode::EXCLUSIVE)
                    .get();
    if (rc == -EEXIST)
        raise<ErrorCode::E_ATOMIC_OPERATION_FAILED>(fmt::format("Atomic write failed, key exists: {}", oid));
    if (rc < 0)
        raise_rados_error(rc, "write_if_none", oid);
    index_add(kv.variant_key(), size);
}

void RadosStorage::do_update(KeySegmentPair& key_seg, UpdateOpts opts) {
    write_segment(key_seg, opts.upsert_ ? WriteMode::OVERWRITE : WriteMode::MUST_EXIST);
}

folly::Future<KeySegmentPair> RadosStorage::read_segment(VariantKey&& variant_key, folly::Executor* executor) {
    auto oid = object_name(variant_key);
    const auto pool = pool_for(variant_key_type(variant_key));
    auto head_future = client_->read(pool, oid);
    if (executor)
        head_future = std::move(head_future).via(executor);
    // Never block in here: without an executor this runs on the librados finisher thread, which also has to deliver
    // the completions of any chunk reads.
    return std::move(head_future)
            .thenValue([this, pool, oid, key = std::move(variant_key)](RadosReadResult&& head
                       ) mutable -> folly::Future<KeySegmentPair> {
                if (head.rc == -ENOENT)
                    throw KeyNotFoundException(key);
                if (head.rc < 0)
                    raise_rados_error(head.rc, "read", oid);

                auto chunk_attr = head.xattrs.find(std::string{CHUNK_XATTR});
                const size_t chunks = chunk_attr == head.xattrs.end() ? 1 : std::stoull(chunk_attr->second);
                if (chunks <= 1) {
                    // Zero copy: the segment references the librados buffer and keeps it alive.
                    auto segment = Segment::from_bytes(head.bytes.data, head.bytes.size, false);
                    segment.set_keepalive(std::any(std::move(head.bytes.keepalive)));
                    return KeySegmentPair{std::move(key), std::move(segment)};
                }

                std::vector<folly::Future<RadosReadResult>> tails;
                tails.reserve(chunks - 1);
                for (size_t n = 1; n < chunks; ++n)
                    tails.emplace_back(client_->read(pool, chunk_name(oid, n)));
                return folly::collectAll(std::move(tails))
                        .toUnsafeFuture()
                        .thenValue([key = std::move(key), head = std::move(head), oid](
                                            std::vector<folly::Try<RadosReadResult>>&& parts
                                    ) mutable {
                            size_t total = head.bytes.size;
                            for (auto& part : parts) {
                                if (part.value().rc < 0)
                                    raise_rados_error(part.value().rc, "read chunk", oid);
                                total += part.value().bytes.size;
                            }
                            auto buffer = std::make_shared<Buffer>(total);
                            auto* dst = buffer->data();
                            std::memcpy(dst, head.bytes.data, head.bytes.size);
                            dst += head.bytes.size;
                            for (auto& part : parts) {
                                std::memcpy(dst, part.value().bytes.data, part.value().bytes.size);
                                dst += part.value().bytes.size;
                            }
                            return KeySegmentPair{std::move(key), Segment::from_buffer(buffer)};
                        });
            });
}

KeySegmentPair RadosStorage::do_read(VariantKey&& variant_key, ReadKeyOpts) {
    return read_segment(std::move(variant_key), nullptr).get();
}

void RadosStorage::do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) {
    auto key_seg = do_read(std::move(variant_key), opts);
    visitor(key_seg.variant_key(), std::move(*key_seg.segment_ptr()));
}

folly::Future<KeySegmentPair> RadosStorage::do_async_read(VariantKey&& variant_key, ReadKeyOpts) {
    // Run continuations (decode etc.) on the IO pool rather than the librados finisher thread.
    return read_segment(std::move(variant_key), &async::io_executor()).via(&async::io_executor());
}

folly::Future<folly::Unit> RadosStorage::do_async_read(
        VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts
) {
    return do_async_read(std::move(variant_key), opts).thenValue([visitor](KeySegmentPair&& key_seg) {
        visitor(key_seg.variant_key(), std::move(*key_seg.segment_ptr()));
        return folly::Unit{};
    });
}

void RadosStorage::remove_chunk_tails(PoolKind pool, const std::string& oid) {
    for (size_t n = 1;; ++n) {
        const auto name = chunk_name(oid, n);
        const auto rc = client_->remove(pool, name).get();
        if (rc == -ENOENT)
            return;
        if (rc < 0)
            raise_rados_error(rc, "remove chunk", name);
    }
}

void RadosStorage::do_remove(VariantKey&& variant_key, RemoveOpts opts) {
    std::array<VariantKey, 1> keys{std::move(variant_key)};
    do_remove(std::span(keys), opts);
}

void RadosStorage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) {
    index_remove(variant_keys);

    // Remove every head concurrently. Data pool objects can be chunked, so the first tail is probed in the same wave;
    // it only exists for chunked segments, in which case the rest of the tails are removed afterwards.
    struct Pending {
        size_t key_index;
        bool probe;
    };
    std::vector<Pending> pending;
    for (size_t i = 0; i < variant_keys.size(); ++i) {
        pending.push_back({i, false});
        if (pool_for(variant_key_type(variant_keys[i])) == PoolKind::DATA)
            pending.push_back({i, true});
    }
    auto results = collect_windowed<RadosErrc>(pending.size(), [&](size_t i) {
        const auto& key = variant_keys[pending[i].key_index];
        const auto oid = object_name(key);
        return client_->remove(pool_for(variant_key_type(key)), pending[i].probe ? chunk_name(oid, 1) : oid);
    });

    std::vector<VariantKey> missing;
    for (size_t i = 0; i < pending.size(); ++i) {
        const auto& key = variant_keys[pending[i].key_index];
        const auto rc = results[i].value();
        if (pending[i].probe) {
            if (rc == 0)
                remove_chunk_tails(PoolKind::DATA, object_name(key));
            continue;
        }
        if (rc == -ENOENT) {
            if (!opts.ignores_missing_key_)
                missing.push_back(key);
        } else if (rc < 0) {
            raise_rados_error(rc, "remove", object_name(key));
        }
    }
    if (!missing.empty())
        throw KeyNotFoundException(std::move(missing));
}

bool RadosStorage::do_key_exists(const VariantKey& key) {
    const auto oid = object_name(key);
    const auto rc = client_->stat(pool_for(variant_key_type(key)), oid, nullptr).get();
    if (rc == -ENOENT)
        return false;
    if (rc < 0)
        raise_rados_error(rc, "stat", oid);
    return true;
}

bool RadosStorage::scan_index(
        KeyType key_type, const std::string& prefix, const std::function<bool(VariantKey&&, uint64_t)>& visitor
) {
    // First page of every shard in flight at once; later pages only for shards that have more.
    std::vector<folly::Future<OmapPage>> first_pages;
    for (uint32_t shard = 0; shard < index_shards_; ++shard)
        first_pages.emplace_back(client_->omap_list(PoolKind::META, index_shard_oid(key_type, shard), "", prefix, OMAP_PAGE)
        );
    auto pages = folly::collectAll(std::move(first_pages)).get();

    for (uint32_t shard = 0; shard < index_shards_; ++shard) {
        auto page = std::move(pages[shard].value());
        const auto oid = index_shard_oid(key_type, shard);
        while (true) {
            if (page.rc == -ENOENT)
                break; // shard never written
            if (page.rc < 0)
                raise_rados_error(page.rc, "index list", oid);
            for (auto& [entry, value] : page.entries) {
                auto [key, size] = decode_index_entry(value, key_type);
                if (matches_prefix(key, prefix) && visitor(std::move(key), size))
                    return true;
            }
            if (!page.more || page.entries.empty())
                break;
            page = client_->omap_list(PoolKind::META, oid, page.entries.back().first, prefix, OMAP_PAGE).get();
        }
    }
    return false;
}

void RadosStorage::for_each_object(PoolKind pool, const std::function<void(std::vector<std::string>&&)>& visitor) {
    for (size_t slice = 0; slice < LIST_SLICES; ++slice) {
        std::optional<std::string> cursor;
        do {
            auto page = client_->list_objects(pool, slice, LIST_SLICES, cursor, LIST_PAGE);
            if (page.rc == -ENOENT)
                break;
            if (page.rc < 0)
                raise_rados_error(page.rc, "list", client_->describe(pool));
            visitor(std::move(page.names));
            cursor = std::move(page.cursor);
        } while (cursor);
    }
}

bool RadosStorage::scan_namespace(
        KeyType key_type, const std::string& prefix, bool want_sizes,
        const std::function<bool(VariantKey&&, uint64_t)>& visitor
) {
    const auto dir = fmt::format("{}/", key_type_long_name(key_type));
    const auto pool = pool_for(key_type);
    std::vector<std::pair<VariantKey, std::string>> matched;
    for_each_object(pool, [&](std::vector<std::string>&& names) {
        for (auto& name : names) {
            if (!name.starts_with(dir))
                continue;
            auto key = variant_key_from_bytes(
                    reinterpret_cast<const uint8_t*>(name.data() + dir.size()), name.size() - dir.size(), key_type
            );
            if (matches_prefix(key, prefix))
                matched.emplace_back(std::move(key), std::move(name));
        }
    });

    std::vector<uint64_t> sizes(matched.size(), 0);
    if (want_sizes) {
        auto results = collect_windowed<RadosErrc>(matched.size(), [&](size_t i) {
            return client_->stat(pool, matched[i].second, &sizes[i]);
        });
        for (size_t i = 0; i < results.size(); ++i) {
            const auto rc = results[i].value();
            if (rc < 0 && rc != -ENOENT) // deleted since listed: report as zero
                raise_rados_error(rc, "stat", matched[i].second);
        }
    }
    for (size_t i = 0; i < matched.size(); ++i)
        if (visitor(std::move(matched[i].first), sizes[i]))
            return true;
    return false;
}

bool RadosStorage::scan(
        KeyType key_type, const std::string& prefix, bool want_sizes,
        const std::function<bool(VariantKey&&, uint64_t)>& visitor
) {
    return is_indexed(key_type) ? scan_index(key_type, prefix, visitor)
                                : scan_namespace(key_type, prefix, want_sizes, visitor);
}

bool RadosStorage::do_iterate_type_until_match(
        KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix
) {
    return scan(key_type, prefix, false, [&visitor](VariantKey&& key, uint64_t) { return visitor(std::move(key)); });
}

void RadosStorage::do_visit_object_sizes(KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor) {
    scan(key_type, prefix, true, [&visitor](VariantKey&& key, uint64_t size) {
        visitor(key, size);
        return false;
    });
}

bool RadosStorage::do_fast_delete() {
    // The library owns its namespace outright, so deleting everything in it is a complete library delete.
    std::vector<PoolKind> pools{PoolKind::DATA};
    if (client_->pools_are_distinct())
        pools.push_back(PoolKind::META);
    for (auto pool : pools) {
        std::vector<std::string> names;
        for_each_object(pool, [&](std::vector<std::string>&& page) {
            std::move(page.begin(), page.end(), std::back_inserter(names));
        });
        // Index first, so a crash part way leaves unindexed objects rather than dangling index entries.
        std::stable_partition(names.begin(), names.end(), [](const auto& n) { return n.starts_with("_idx/"); });
        auto results = collect_windowed<RadosErrc>(names.size(), [&](size_t i) {
            return client_->remove(pool, names[i]);
        });
        for (size_t i = 0; i < results.size(); ++i) {
            const auto rc = results[i].value();
            if (rc < 0 && rc != -ENOENT)
                raise_rados_error(rc, "remove", names[i]);
        }
    }
    return true;
}

size_t RadosStorage::rebuild_index() {
    // Not safe against concurrent writers: run it with the library quiesced.
    size_t entries = 0;
    {
        std::vector<std::string> index_objects;
        for_each_object(PoolKind::META, [&](std::vector<std::string>&& page) {
            for (auto& name : page)
                if (name.starts_with("_idx/"))
                    index_objects.push_back(std::move(name));
        });
        collect_windowed<RadosErrc>(index_objects.size(), [&](size_t i) {
            return client_->remove(PoolKind::META, index_objects[i]);
        });
    }
    std::vector<PoolKind> pools{PoolKind::DATA};
    if (client_->pools_are_distinct())
        pools.push_back(PoolKind::META);
    for (auto pool : pools) {
        std::set<std::string> heads;
        std::vector<std::string> chunk_tails;
        std::vector<std::string> names;
        for_each_object(pool, [&](std::vector<std::string>&& page) {
            std::move(page.begin(), page.end(), std::back_inserter(names));
        });
        for (auto& name : names) {
            if (name.starts_with("_idx/"))
                continue;
            if (name.starts_with("_chunk/")) {
                chunk_tails.push_back(name);
                continue;
            }
            heads.insert(name);
        }
        // Orphaned chunk tails: "_chunk/<head>/<n>" with no head.
        for (const auto& tail : chunk_tails) {
            const auto head = tail.substr(7, tail.rfind('/') - 7);
            if (!heads.contains(head))
                client_->remove(pool, tail).get();
        }
        std::vector<std::pair<VariantKey, std::string>> to_index;
        for (const auto& name : heads) {
            const auto slash = name.find('/');
            if (slash == std::string::npos)
                continue;
            std::optional<KeyType> key_type;
            foreach_key_type([&](KeyType kt) {
                if (name.compare(0, slash, key_type_long_name(kt)) == 0)
                    key_type = kt;
            });
            if (!key_type || !is_indexed(*key_type) || pool_for(*key_type) != pool)
                continue;
            to_index.emplace_back(
                    variant_key_from_bytes(
                            reinterpret_cast<const uint8_t*>(name.data() + slash + 1), name.size() - slash - 1, *key_type
                    ),
                    name
            );
        }
        std::vector<uint64_t> sizes(to_index.size());
        collect_windowed<RadosErrc>(to_index.size(), [&](size_t i) {
            return client_->stat(pool, to_index[i].second, &sizes[i]);
        });
        for (size_t i = 0; i < to_index.size(); ++i) {
            index_add(to_index[i].first, sizes[i]);
            ++entries;
        }
    }
    return entries;
}

std::string RadosStorage::do_key_path(const VariantKey& key) const {
    return fmt::format("{}/{}", client_->describe(pool_for(variant_key_type(key))), object_name(key));
}

} // namespace arcticdb::storage::rados
