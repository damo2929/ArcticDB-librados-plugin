/* Native Ceph RADOS storage backend for ArcticDB.
 *
 * One connected librados::Rados handle per (cluster, client, conf, keyring, overrides) per process. Every library
 * (and every Arctic instance) talking to the same cluster shares the handle, and with it the messenger threads, the
 * OSD sessions and the objecter throttle. Handles are recreated after fork(), since a Rados handle does not survive
 * one.
 */

#pragma once

#include <map>
#include <memory>
#include <string>

#include <rados/librados_fwd.hpp>

namespace arcticdb::storage::rados {

// Everything needed to connect to a cluster. Empty strings mean librados defaults.
struct ConnectionParams {
    std::string cluster_name;
    std::string client_id;
    std::string conf_path;
    std::string keyring;
    std::map<std::string, std::string> conf_overrides;
    uint32_t op_timeout_s = 0;

    [[nodiscard]] std::string cache_key() const;
};

// Returns a connected cluster handle, creating and connecting it on first use. Thread-safe.
std::shared_ptr<librados::Rados> get_cluster(const ConnectionParams& params);

} // namespace arcticdb::storage::rados
