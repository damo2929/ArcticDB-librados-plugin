"""
Native Ceph RADOS storage backend: StorageFixture for the upstream ArcticDB test suites.

The cluster is external (a real cluster, or scripts/minicluster.sh). Point the tests at it with

    ARCTICDB_STORAGE_RADOS=1
    ARCTICDB_RADOS_TEST_URI="rados://<pool>?conf=/path/ceph.conf[&client_id=...&keyring=...]"

Each fixture gets its own namespace prefix inside the pool, so fixtures never see each other's libraries.
"""

import os
import re
import uuid

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap

from .api import *
from arcticdb.adapters.rados_library_adapter import add_rados_library_to_env, parse_rados_uri

RADOS_TEST_URI_ENV = "ARCTICDB_RADOS_TEST_URI"


def with_namespace_prefix(uri: str, prefix: str) -> str:
    match = re.match(r"(?P<head>rados://[^/?]+)(?:/(?P<prefix>[^?]*))?(?P<query>\?.*)?$", uri)
    assert match, f"Invalid RADOS URI {uri}"
    existing = (match["prefix"] or "").strip("/")
    full_prefix = f"{existing}/{prefix}" if existing else prefix
    return f"{match['head']}/{full_prefix}{match['query'] or ''}"


class RadosNamespaceFixture(StorageFixture):
    def __init__(self, base_uri: str, prefix: str):
        super().__init__()
        self.arctic_uri = with_namespace_prefix(base_uri, prefix)
        self._parsed = parse_rados_uri(self.arctic_uri)

    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        add_rados_library_to_env(cfg, lib_name, Defaults.ENV, self._parsed)
        return cfg

    def __exit__(self, exc_type, exc_value, traceback):
        # Libraries own their namespaces, so deleting them (fast_delete) removes everything they wrote.
        with handle_cleanup_exception(self, "Arctic libraries"):
            ac = self.create_arctic()
            for lib in ac.list_libraries():
                ac.delete_library(lib)
        self.slow_cleanup()

    def set_permission(self, *, read: bool, write: bool):
        raise NotImplementedError("Permissions are managed with cephx caps on the cluster")


class ExternalRadosCluster(StorageFixtureFactory):
    """A RADOS cluster whose life-cycle is managed outside the test run."""

    def __init__(self, base_uri: str = None):
        self.base_uri = base_uri or os.environ[RADOS_TEST_URI_ENV]
        self._run_id = uuid.uuid4().hex[:8]
        self._count = 0

    def __str__(self):
        return f"{type(self).__name__}[{self.base_uri}]"

    def create_fixture(self) -> StorageFixture:
        self._count += 1
        return RadosNamespaceFixture(self.base_uri, f"test-{self._run_id}-{self._count}")
