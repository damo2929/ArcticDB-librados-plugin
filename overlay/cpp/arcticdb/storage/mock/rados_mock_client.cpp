/* Native Ceph RADOS storage backend for ArcticDB. */

#include <arcticdb/storage/mock/rados_mock_client.hpp>

#include <folly/hash/Hash.h>

#include <cerrno>

namespace arcticdb::storage::rados {

// Parses "#Failure_<Operation>_<errno>_" out of an object name (same convention as the S3 mock) and returns the
// negated errno to fail that operation with.
std::optional<RadosErrc> has_object_failure_trigger(const std::string& oid, StorageOperation operation) {
    auto failure_string_for_operation = "#Failure_" + operation_to_string(operation) + "_";
    auto position = oid.rfind(failure_string_for_operation);
    if (position == std::string::npos)
        return std::nullopt;

    try {
        auto start = position + failure_string_for_operation.size();
        auto end = oid.find('_', start);
        return -std::abs(std::stoi(oid.substr(start, end - start)));
    } catch (std::exception&) {
        return std::nullopt;
    }
}

// Returns the object's bytes and xattrs, or -ENOENT.
folly::Future<RadosReadResult> MockRadosClient::read(PoolKind, const std::string& oid) {
    if (auto failure = has_object_failure_trigger(oid, StorageOperation::READ))
        return RadosReadResult{*failure, {}, {}};

    std::scoped_lock lock(mutex_);
    auto it = objects_.find(oid);
    if (it == objects_.end())
        return RadosReadResult{-ENOENT, {}, {}};

    // Share the stored string: segments read with copy=false reference it, and a later overwrite swaps in a new
    // string rather than mutating this one.
    auto data = it->second.data;
    RadosBytes bytes{reinterpret_cast<const uint8_t*>(data->data()), data->size(), data};
    return RadosReadResult{0, std::move(bytes), it->second.xattrs};
}

// Stores a copy of the bytes, honouring the exclusive / must-exist modes like the OSD does. Xattrs are merged.
folly::Future<RadosErrc> MockRadosClient::write(
        PoolKind, const std::string& oid, RadosWriteBuffer buffer, WriteMode mode
) {
    if (auto failure = has_object_failure_trigger(oid, StorageOperation::WRITE))
        return *failure;

    std::scoped_lock lock(mutex_);
    auto it = objects_.find(oid);
    if (mode == WriteMode::EXCLUSIVE && it != objects_.end())
        return -EEXIST;
    if (mode == WriteMode::MUST_EXIST && it == objects_.end())
        return -ENOENT;

    auto& object = objects_[oid];
    object.data = std::make_shared<std::string>(reinterpret_cast<const char*>(buffer.data), buffer.size);
    for (auto& [name, value] : buffer.xattrs)
        object.xattrs[name] = value;
    return 0;
}

// Reports existence and size.
folly::Future<RadosErrc> MockRadosClient::stat(PoolKind, const std::string& oid, uint64_t* size) {
    if (auto failure = has_object_failure_trigger(oid, StorageOperation::EXISTS))
        return *failure;

    std::scoped_lock lock(mutex_);
    auto it = objects_.find(oid);
    if (it == objects_.end())
        return -ENOENT;
    if (size)
        *size = it->second.data->size();
    return 0;
}

// Deletes an object, or -ENOENT.
folly::Future<RadosErrc> MockRadosClient::remove(PoolKind, const std::string& oid) {
    if (auto failure = has_object_failure_trigger(oid, StorageOperation::DELETE))
        return *failure;
    if (auto failure = has_object_failure_trigger(oid, StorageOperation::DELETE_LOCAL))
        return *failure;

    std::scoped_lock lock(mutex_);
    return objects_.erase(oid) ? 0 : -ENOENT;
}

// Sets omap entries, creating the object if needed (as the real client's create(false)+omap_set does).
folly::Future<RadosErrc> MockRadosClient::omap_set(
        PoolKind, const std::string& oid, std::map<std::string, std::string> entries
) {
    std::scoped_lock lock(mutex_);
    auto& object = objects_[oid];
    for (auto& [k, v] : entries)
        object.omap.insert_or_assign(k, std::move(v));
    return 0;
}

// Removes omap entries; -ENOENT if the object doesn't exist (as with assert_exists in the real client).
folly::Future<RadosErrc> MockRadosClient::omap_rm(PoolKind, const std::string& oid, std::vector<std::string> keys) {
    std::scoped_lock lock(mutex_);
    auto it = objects_.find(oid);
    if (it == objects_.end())
        return -ENOENT;
    for (const auto& k : keys)
        it->second.omap.erase(k);
    return 0;
}

// Returns sorted omap entries after start_after that start with filter_prefix, like omap_get_vals2.
folly::Future<OmapPage> MockRadosClient::omap_list(
        PoolKind, const std::string& oid, const std::string& start_after, const std::string& filter_prefix,
        uint64_t max_entries
) {
    if (auto failure = has_object_failure_trigger(oid, StorageOperation::LIST))
        return OmapPage{*failure, {}, false};

    std::scoped_lock lock(mutex_);
    OmapPage page;
    auto it = objects_.find(oid);
    if (it == objects_.end()) {
        page.rc = -ENOENT;
        return page;
    }
    const auto& omap = it->second.omap;
    auto pos = start_after.empty() ? omap.lower_bound(filter_prefix) : omap.upper_bound(start_after);
    for (; pos != omap.end(); ++pos) {
        if (!pos->first.starts_with(filter_prefix))
            break;
        if (page.entries.size() == max_entries) {
            page.more = true;
            break;
        }
        page.entries.emplace_back(pos->first, pos->second);
    }
    return page;
}

// Lists objects in the given slice (objects are assigned to slices by name hash), paging by object name.
ListPage MockRadosClient::list_objects(
        PoolKind, size_t slice, size_t num_slices, const std::optional<std::string>& cursor, size_t max
) {
    std::scoped_lock lock(mutex_);
    ListPage page;
    auto it = cursor ? objects_.upper_bound(*cursor) : objects_.begin();
    for (; it != objects_.end(); ++it) {
        if (folly::hash::fnv64(it->first) % num_slices != slice)
            continue;
        if (page.names.size() == max) {
            page.cursor = page.names.back();
            break;
        }
        page.names.push_back(it->first);
    }
    return page;
}

} // namespace arcticdb::storage::rados
