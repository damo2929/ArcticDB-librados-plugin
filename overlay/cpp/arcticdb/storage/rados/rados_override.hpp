/* Native Ceph RADOS storage backend for ArcticDB.
 *
 * Connection settings come from the Arctic URI of the client opening the library, not from the stored library
 * config: they are client specific (conf file, cephx user, keyring) and the keyring path must never be persisted.
 * Layout settings (pool, meta_pool, namespace_prefix, index_shards, unindexed_key_types, chunk_size) are persisted,
 * because every client must agree on them.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/pb_util.hpp>

#include <map>
#include <string>

namespace arcticdb::storage {

class RadosOverride {
    std::string cluster_name_;
    std::string client_id_;
    std::string conf_path_;
    std::string keyring_;
    std::map<std::string, std::string> conf_overrides_;
    uint32_t op_timeout_s_ = 0;

  public:
    [[nodiscard]] std::string cluster_name() const { return cluster_name_; }
    void set_cluster_name(std::string_view v) { cluster_name_ = v; }

    [[nodiscard]] std::string client_id() const { return client_id_; }
    void set_client_id(std::string_view v) { client_id_ = v; }

    [[nodiscard]] std::string conf_path() const { return conf_path_; }
    void set_conf_path(std::string_view v) { conf_path_ = v; }

    [[nodiscard]] std::string keyring() const { return keyring_; }
    void set_keyring(std::string_view v) { keyring_ = v; }

    [[nodiscard]] std::map<std::string, std::string> conf_overrides() const { return conf_overrides_; }
    void set_conf_overrides(std::map<std::string, std::string> v) { conf_overrides_ = std::move(v); }

    [[nodiscard]] uint32_t op_timeout_s() const { return op_timeout_s_; }
    void set_op_timeout_s(uint32_t v) { op_timeout_s_ = v; }

    void modify_storage_config(arcticdb::proto::storage::VariantStorage& storage, bool /*override_https*/) const {
        if (storage.config().Is<arcticdb::proto::rados_storage::Config>()) {
            arcticdb::proto::rados_storage::Config rados_storage;
            storage.config().UnpackTo(&rados_storage);

            rados_storage.set_cluster_name(cluster_name_);
            rados_storage.set_client_id(client_id_);
            rados_storage.set_conf_path(conf_path_);
            rados_storage.set_keyring(keyring_);
            rados_storage.set_op_timeout_s(op_timeout_s_);
            rados_storage.mutable_conf_overrides()->clear();
            rados_storage.mutable_conf_overrides()->insert(conf_overrides_.begin(), conf_overrides_.end());

            util::pack_to_any(rados_storage, *storage.mutable_config());
        }
    }
};

// A stored library config must not carry connection details.
inline bool is_rados_config_ok(const arcticdb::proto::storage::VariantStorage& storage) {
    if (!storage.config().Is<arcticdb::proto::rados_storage::Config>())
        return true;
    arcticdb::proto::rados_storage::Config rados_storage;
    storage.config().UnpackTo(&rados_storage);
    return rados_storage.keyring().empty();
}

} // namespace arcticdb::storage
