# Find librados (C++ API). Ceph ships neither a pkg-config file nor a CMake package for it.
#
# Debian/Ubuntu: librados-dev libradospp-dev     Fedora/RHEL: librados-devel libradospp-devel
#
# Defines Rados_FOUND, Rados_INCLUDE_DIR, Rados_LIBRARY and the imported target Rados::rados.

find_path(Rados_INCLUDE_DIR NAMES rados/librados.hpp HINTS ENV RADOS_ROOT PATH_SUFFIXES include)
find_library(Rados_LIBRARY NAMES rados HINTS ENV RADOS_ROOT PATH_SUFFIXES lib lib64)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Rados REQUIRED_VARS Rados_LIBRARY Rados_INCLUDE_DIR)

if(Rados_FOUND AND NOT TARGET Rados::rados)
    add_library(Rados::rados UNKNOWN IMPORTED)
    set_target_properties(Rados::rados PROPERTIES
            IMPORTED_LOCATION "${Rados_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${Rados_INCLUDE_DIR}")
endif()

mark_as_advanced(Rados_INCLUDE_DIR Rados_LIBRARY)
