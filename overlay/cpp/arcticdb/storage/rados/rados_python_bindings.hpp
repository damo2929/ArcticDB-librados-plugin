/* Native Ceph RADOS storage backend for ArcticDB: Python bindings, registered from storage/python_bindings.cpp. */

#pragma once

#include <arcticdb/storage/rados/rados_override.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace arcticdb::storage::apy {

// Exposes RadosOverride to Python (used by the rados:// adapter) and whether the librados client was compiled in.
inline void register_rados_bindings(pybind11::module& storage) {
    namespace py = pybind11;
    py::class_<RadosOverride>(storage, "RadosOverride")
            .def(py::init<>())
            .def_property("cluster_name", &RadosOverride::cluster_name, &RadosOverride::set_cluster_name)
            .def_property("client_id", &RadosOverride::client_id, &RadosOverride::set_client_id)
            .def_property("conf_path", &RadosOverride::conf_path, &RadosOverride::set_conf_path)
            .def_property("keyring", &RadosOverride::keyring, &RadosOverride::set_keyring)
            .def_property("conf_overrides", &RadosOverride::conf_overrides, &RadosOverride::set_conf_overrides)
            .def_property("op_timeout_s", &RadosOverride::op_timeout_s, &RadosOverride::set_op_timeout_s);
#ifdef ARCTICDB_WITH_RADOS
    storage.attr("RADOS_CLIENT_AVAILABLE") = true;
#else
    storage.attr("RADOS_CLIENT_AVAILABLE") = false;
#endif
}

} // namespace arcticdb::storage::apy
