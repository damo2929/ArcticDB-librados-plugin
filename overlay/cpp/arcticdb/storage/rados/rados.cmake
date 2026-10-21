# Native Ceph RADOS storage backend. Included from cpp/arcticdb/CMakeLists.txt before arcticdb_core_object is created.
#
# The storage, its omap index logic and the mock client are always built, so the upstream storage test suites run the
# RADOS backend (against the mock) in every build. The librados client is only built with ARCTICDB_WITH_RADOS=ON.

if(DEFINED ENV{ARCTICDB_WITH_RADOS})
    set(_arcticdb_with_rados_default $ENV{ARCTICDB_WITH_RADOS})
else()
    set(_arcticdb_with_rados_default OFF)
endif()
option(ARCTICDB_WITH_RADOS "Build the librados client for the native RADOS storage backend." ${_arcticdb_with_rados_default})

list(APPEND arcticdb_srcs
        storage/rados/rados_client_interface.hpp
        storage/rados/rados_storage.hpp
        storage/rados/rados_storage.cpp
        storage/mock/rados_mock_client.hpp
        storage/mock/rados_mock_client.cpp
)

set(ARCTICDB_RADOS_LIBRARIES "")
if(ARCTICDB_WITH_RADOS)
    find_package(Rados REQUIRED)
    list(APPEND arcticdb_srcs
            storage/rados/rados_cluster_registry.hpp
            storage/rados/rados_cluster_registry.cpp
            storage/rados/rados_client_impl.hpp
            storage/rados/rados_client_impl.cpp
    )
    add_compile_definitions(ARCTICDB_WITH_RADOS)
    set(ARCTICDB_RADOS_LIBRARIES Rados::rados)
    message(STATUS "ArcticDB RADOS backend: librados client enabled (${Rados_LIBRARY})")
else()
    message(STATUS "ArcticDB RADOS backend: mock only (set ARCTICDB_WITH_RADOS=ON for librados)")
endif()
