/* Native Ceph RADOS storage backend for ArcticDB. */

#include <arcticdb/storage/rados/rados_cluster_registry.hpp>

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <rados/librados.hpp>

#include <fmt/format.h>

#include <cstring>
#include <mutex>
#include <unistd.h>

namespace arcticdb::storage::rados {

std::string ConnectionParams::cache_key() const {
    std::string key = fmt::format("{}\x1f{}\x1f{}\x1f{}\x1f{}", cluster_name, client_id, conf_path, keyring, op_timeout_s);
    for (const auto& [k, v] : conf_overrides)
        key += fmt::format("\x1f{}={}", k, v);
    return key;
}

namespace {

struct Registry {
    std::mutex mutex;
    pid_t pid = ::getpid();
    std::map<std::string, std::shared_ptr<librados::Rados>> clusters;
};

Registry& registry() {
    // Leaked on purpose: shutting librados down during static destruction races its own threads.
    static auto* r = new Registry();
    return *r;
}

void check_rc(int rc, std::string_view what, const ConnectionParams& params) {
    if (rc < 0) {
        util::raise_rte(
                "RADOS {} failed for cluster '{}' client '{}': {} ({})",
                what,
                params.cluster_name,
                params.client_id,
                std::strerror(-rc),
                rc
        );
    }
}

std::shared_ptr<librados::Rados> connect(const ConnectionParams& params) {
    auto cluster = std::make_shared<librados::Rados>();
    const auto cluster_name = params.cluster_name.empty() ? std::string{"ceph"} : params.cluster_name;
    const auto client_name = "client." + (params.client_id.empty() ? std::string{"admin"} : params.client_id);
    check_rc(cluster->init2(client_name.c_str(), cluster_name.c_str(), 0), "init", params);

    // conf_read_file(nullptr) uses the standard search path ($CEPH_CONF, /etc/ceph/$cluster.conf, ...).
    check_rc(
            cluster->conf_read_file(params.conf_path.empty() ? nullptr : params.conf_path.c_str()),
            "conf_read_file",
            params
    );
    // CEPH_ARGS etc. still apply, as with every other Ceph client.
    cluster->conf_parse_env(nullptr);

    if (!params.keyring.empty())
        check_rc(cluster->conf_set("keyring", params.keyring.c_str()), "conf_set(keyring)", params);
    if (params.op_timeout_s != 0) {
        const auto timeout = std::to_string(params.op_timeout_s);
        check_rc(cluster->conf_set("rados_osd_op_timeout", timeout.c_str()), "conf_set(rados_osd_op_timeout)", params);
        check_rc(cluster->conf_set("rados_mon_op_timeout", timeout.c_str()), "conf_set(rados_mon_op_timeout)", params);
    }
    for (const auto& [k, v] : params.conf_overrides)
        check_rc(cluster->conf_set(k.c_str(), v.c_str()), fmt::format("conf_set({})", k), params);

    check_rc(cluster->connect(), "connect", params);
    log::storage().info("Connected to RADOS cluster '{}' as {}", cluster_name, client_name);
    return cluster;
}

} // namespace

std::shared_ptr<librados::Rados> get_cluster(const ConnectionParams& params) {
    auto& r = registry();
    std::scoped_lock lock(r.mutex);
    if (const auto pid = ::getpid(); pid != r.pid) {
        // Forked child: the inherited handles reference threads that no longer exist. Drop them without shutdown().
        for (auto& [_, cluster] : r.clusters)
            new std::shared_ptr<librados::Rados>(std::move(cluster)); // deliberately leaked
        r.clusters.clear();
        r.pid = pid;
    }
    auto key = params.cache_key();
    if (auto it = r.clusters.find(key); it != r.clusters.end())
        return it->second;

    auto cluster = connect(params);
    r.clusters.emplace(std::move(key), cluster);
    return cluster;
}

} // namespace arcticdb::storage::rados
