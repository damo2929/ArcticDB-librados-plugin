/* Native Ceph RADOS storage backend for ArcticDB. */

#include <arcticdb/storage/rados/rados_client_impl.hpp>

#include <arcticdb/util/preconditions.hpp>

#include <fmt/format.h>

#include <cstring>

namespace arcticdb::storage::rados {

namespace {

// Base for the per-op state that has to outlive aio_operate: output buffers, prvals and write keepalives.
// Owned by the completion callback, which fulfils the promise and frees everything.
template<typename T>
struct AioState {
    using Base = AioState<T>;
    virtual ~AioState() = default;
    virtual T result(int rc) = 0;

    folly::Promise<T> promise;
    librados::AioCompletion* completion = nullptr;

    static void on_complete(librados::completion_t, void* arg) {
        std::unique_ptr<AioState> self(static_cast<AioState*>(arg));
        const int rc = self->completion->get_return_value();
        self->completion->release();
        try {
            self->promise.setValue(self->result(rc));
        } catch (...) {
            self->promise.setException(folly::exception_wrapper(std::current_exception()));
        }
    }
};

struct RcState final : AioState<RadosErrc> {
    RadosErrc result(int rc) override { return rc; }
    std::shared_ptr<void> keepalive;
    librados::bufferlist bl;
};

template<typename State, typename Op>
auto submit(librados::IoCtx& ioctx, const std::string& oid, std::unique_ptr<State> state, Op& op) {
    auto future = state->promise.getFuture();
    typename State::Base* base = state.get();
    state->completion = librados::Rados::aio_create_completion(base, &State::Base::on_complete);
    int rc;
    if constexpr (std::is_same_v<Op, librados::ObjectReadOperation>)
        rc = ioctx.aio_operate(oid, state->completion, &op, nullptr);
    else
        rc = ioctx.aio_operate(oid, state->completion, &op);
    if (rc < 0) {
        // Never queued, so the callback will not run: complete inline.
        state->completion->release();
        state->promise.setValue(state->result(rc));
        return future;
    }
    state.release(); // owned by on_complete from here
    return future;
}

// Wrap caller memory in a bufferlist without copying. The caller keeps it alive via RcState::keepalive.
librados::bufferlist static_bufferlist(const uint8_t* data, size_t size) {
    if (size == 0)
        return {};
    return librados::bufferlist::static_from_mem(const_cast<char*>(reinterpret_cast<const char*>(data)), size);
}

} // namespace

RadosClientImpl::RadosClientImpl(
        const ConnectionParams& params, const std::string& pool, const std::string& meta_pool,
        const std::string& rados_namespace
) :
    cluster_(get_cluster(params)),
    pool_(pool),
    meta_pool_(meta_pool.empty() ? pool : meta_pool),
    namespace_(rados_namespace),
    distinct_pools_(meta_pool_ != pool_) {
    util::check(!pool_.empty(), "RADOS storage requires a pool name");
    auto open = [&](const std::string& name, librados::IoCtx& ctx) {
        if (int rc = cluster_->ioctx_create(name.c_str(), ctx); rc < 0)
            util::raise_rte("Failed to open RADOS pool '{}': {} ({})", name, std::strerror(-rc), rc);
        ctx.set_namespace(namespace_);
    };
    open(pool_, data_);
    if (distinct_pools_)
        open(meta_pool_, meta_);
}

std::string RadosClientImpl::describe(PoolKind pool) const {
    return fmt::format("{}/{}", pool == PoolKind::META ? meta_pool_ : pool_, namespace_);
}

folly::Future<RadosReadResult> RadosClientImpl::read(PoolKind pool, const std::string& oid) {
    struct ReadState final : AioState<RadosReadResult> {
        std::shared_ptr<librados::bufferlist> bl = std::make_shared<librados::bufferlist>();
        std::map<std::string, librados::bufferlist> xattrs;
        int read_rval = 0;
        int xattr_rval = 0;

        RadosReadResult result(int rc) override {
            RadosReadResult out;
            out.rc = rc < 0 ? rc : read_rval;
            if (out.rc < 0)
                return out;
            // c_str() is free for the common single-buffer case and makes one contiguous copy otherwise.
            out.bytes.size = bl->length();
            out.bytes.data = out.bytes.size ? reinterpret_cast<const uint8_t*>(bl->c_str()) : nullptr;
            out.bytes.keepalive = bl;
            if (xattr_rval >= 0)
                for (auto& [k, v] : xattrs)
                    out.xattrs.emplace(k, v.to_str());
            return out;
        }
    };
    auto state = std::make_unique<ReadState>();
    librados::ObjectReadOperation op;
    // len 0 == read the whole object, so a read is one round trip with no stat.
    op.read(0, 0, state->bl.get(), &state->read_rval);
    op.set_op_flags2(librados::OP_FADVISE_SEQUENTIAL);
    op.getxattrs(&state->xattrs, &state->xattr_rval);
    return submit(ioctx(pool), oid, std::move(state), op);
}

folly::Future<RadosErrc> RadosClientImpl::write(
        PoolKind pool, const std::string& oid, RadosWriteBuffer buffer, WriteMode mode
) {
    auto state = std::make_unique<RcState>();
    state->keepalive = std::move(buffer.keepalive);
    state->bl = static_bufferlist(buffer.data, buffer.size);

    librados::ObjectWriteOperation op;
    switch (mode) {
    case WriteMode::EXCLUSIVE:
        op.create(true);
        break;
    case WriteMode::MUST_EXIST:
        op.assert_exists();
        break;
    case WriteMode::OVERWRITE:
        break;
    }
    op.write_full(state->bl);
    for (auto& [name, value] : buffer.xattrs) {
        librados::bufferlist v;
        v.append(value);
        op.setxattr(name.c_str(), v);
    }
    return submit(ioctx(pool), oid, std::move(state), op);
}

folly::Future<RadosErrc> RadosClientImpl::stat(PoolKind pool, const std::string& oid, uint64_t* size) {
    struct StatState final : AioState<RadosErrc> {
        uint64_t size = 0;
        int rval = 0;
        uint64_t* out = nullptr;
        RadosErrc result(int rc) override {
            if (rc >= 0 && rval >= 0 && out)
                *out = size;
            return rc < 0 ? rc : rval;
        }
    };
    auto state = std::make_unique<StatState>();
    state->out = size;
    librados::ObjectReadOperation op;
    op.stat(&state->size, nullptr, &state->rval);
    return submit(ioctx(pool), oid, std::move(state), op);
}

folly::Future<RadosErrc> RadosClientImpl::remove(PoolKind pool, const std::string& oid) {
    librados::ObjectWriteOperation op;
    op.remove();
    return submit(ioctx(pool), oid, std::make_unique<RcState>(), op);
}

folly::Future<RadosErrc> RadosClientImpl::omap_set(
        PoolKind pool, const std::string& oid, std::map<std::string, std::string> entries
) {
    std::map<std::string, librados::bufferlist> values;
    for (auto& [k, v] : entries) {
        librados::bufferlist bl;
        bl.append(v);
        values.emplace(k, std::move(bl));
    }
    librados::ObjectWriteOperation op;
    op.create(false); // index shard objects are created on first use
    op.omap_set(values);
    return submit(ioctx(pool), oid, std::make_unique<RcState>(), op);
}

folly::Future<RadosErrc> RadosClientImpl::omap_rm(PoolKind pool, const std::string& oid, std::vector<std::string> keys) {
    librados::ObjectWriteOperation op;
    op.assert_exists(); // don't create empty shard objects just to remove nothing from them
    op.omap_rm_keys(std::set<std::string>(keys.begin(), keys.end()));
    return submit(ioctx(pool), oid, std::make_unique<RcState>(), op);
}

folly::Future<OmapPage> RadosClientImpl::omap_list(
        PoolKind pool, const std::string& oid, const std::string& start_after, const std::string& filter_prefix,
        uint64_t max_entries
) {
    struct OmapState final : AioState<OmapPage> {
        std::map<std::string, librados::bufferlist> vals;
        bool more = false;
        int rval = 0;
        OmapPage result(int rc) override {
            OmapPage page;
            page.rc = rc < 0 ? rc : rval;
            if (page.rc < 0)
                return page;
            page.more = more;
            page.entries.reserve(vals.size());
            for (auto& [k, v] : vals)
                page.entries.emplace_back(k, v.to_str());
            return page;
        }
    };
    auto state = std::make_unique<OmapState>();
    librados::ObjectReadOperation op;
    // filter_prefix is applied by the OSD, so prefix listings only ship matching keys.
    op.omap_get_vals2(start_after, filter_prefix, max_entries, &state->vals, &state->more, &state->rval);
    return submit(ioctx(pool), oid, std::move(state), op);
}

ListPage RadosClientImpl::list_objects(
        PoolKind pool, size_t slice, size_t num_slices, const std::optional<std::string>& cursor, size_t max
) {
    auto& ctx = ioctx(pool);
    librados::ObjectCursor slice_start, slice_end;
    ctx.object_list_slice(ctx.object_list_begin(), ctx.object_list_end(), slice, num_slices, &slice_start, &slice_end);

    librados::ObjectCursor start = slice_start;
    if (cursor)
        util::check(start.from_str(*cursor), "Invalid RADOS list cursor '{}'", *cursor);

    ListPage page;
    std::vector<librados::ObjectItem> items;
    librados::ObjectCursor next;
    // object_list only returns objects in ctx's namespace.
    page.rc = ctx.object_list(start, slice_end, max, {}, &items, &next);
    if (page.rc < 0)
        return page;
    page.names.reserve(items.size());
    for (auto& item : items)
        page.names.push_back(std::move(item.oid));
    if (!(next == slice_end) && !ctx.object_list_is_end(next))
        page.cursor = next.to_str();
    return page;
}

} // namespace arcticdb::storage::rados
