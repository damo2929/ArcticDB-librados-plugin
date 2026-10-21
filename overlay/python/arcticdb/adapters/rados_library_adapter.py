"""
Native Ceph RADOS storage backend for ArcticDB.

URI format::

    rados://<pool>[/<namespace_prefix>][?<options>]

Connection options (taken from the URI of each client, never persisted):

    conf=<path>             ceph.conf path (default: librados search path, $CEPH_CONF)
    client_id=<id>          cephx user without the "client." prefix (default: admin)
    keyring=<path>          keyring path (default: from ceph.conf)
    cluster=<name>          cluster name (default: ceph)
    op_timeout=<seconds>    fail RADOS ops after this long (default: librados default, i.e. wait)
    conf.<option>=<value>   any librados option, e.g. conf.objecter_inflight_op_bytes=1073741824

Layout options (persisted with each library, so every client agrees on them):

    meta_pool=<pool>        replicated (ideally NVMe) pool for refs, versions, locks and the key index
    index_shards=<n>        omap shards per key type for the key index (default 8)
    unindexed=<a,b>         key types (long names) kept out of the key index (default: tdata)
    chunk_size=<bytes>      split segments larger than this across objects (default 64 MiB)

Example::

    Arctic("rados://arcticdb?conf=/etc/ceph/ceph.conf&client_id=arcticdb&meta_pool=arcticdb-meta")
"""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import parse_qsl, unquote

from arcticc.pb2.rados_storage_pb2 import Config as RadosConfig
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter
from arcticdb.config import _DEFAULT_ENV
from arcticdb.encoding_version import EncodingVersion
from arcticdb.exceptions import UserInputException
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.version_store.helper import _add_lib_desc_to_env, get_storage_for_lib_name
from arcticdb_ext.storage import CONFIG_LIBRARY_NAME, RadosOverride, StorageOverride

# URI options split by whether they are persisted with the library (layout) or only apply to this client (connection).
_CONNECTION_OPTIONS = {"conf", "client_id", "keyring", "cluster", "op_timeout"}
_LAYOUT_OPTIONS = {"meta_pool", "index_shards", "unindexed", "chunk_size"}


# Parsed form of a rados:// URI; field names mirror rados_storage.proto Config.
@dataclass
class ParsedRadosUri:
    pool: str
    namespace_prefix: str = ""
    conf_path: str = ""
    client_id: str = ""
    keyring: str = ""
    cluster_name: str = ""
    op_timeout_s: int = 0
    conf_overrides: Dict[str, str] = field(default_factory=dict)
    meta_pool: str = ""
    index_shards: int = 0
    unindexed_key_types: List[str] = field(default_factory=list)
    chunk_size: int = 0


# Parses and validates a rados:// URI. Raises UserInputException for malformed URIs and unknown options.
def parse_rados_uri(uri: str) -> ParsedRadosUri:
    match = re.match(r"rados://(?P<pool>[^/?]+)(?:/(?P<prefix>[^?]*))?(?:\?(?P<query>.*))?$", uri)
    if not match:
        raise UserInputException(f"Invalid RADOS URI {uri!r}. Expected rados://<pool>[/<namespace_prefix>][?options]")
    parsed = ParsedRadosUri(pool=unquote(match["pool"]), namespace_prefix=unquote(match["prefix"] or "").strip("/"))
    for key, value in parse_qsl(match["query"] or "", keep_blank_values=True, strict_parsing=False):
        try:
            if key.startswith("conf."):
                parsed.conf_overrides[key[len("conf.") :]] = value
            elif key == "conf":
                parsed.conf_path = value
            elif key == "client_id":
                parsed.client_id = value[len("client.") :] if value.startswith("client.") else value
            elif key == "keyring":
                parsed.keyring = value
            elif key == "cluster":
                parsed.cluster_name = value
            elif key == "op_timeout":
                parsed.op_timeout_s = int(value)
            elif key == "meta_pool":
                parsed.meta_pool = value
            elif key == "index_shards":
                parsed.index_shards = int(value)
            elif key == "unindexed":
                parsed.unindexed_key_types = [t for t in value.split(",") if t]
            elif key == "chunk_size":
                parsed.chunk_size = int(value)
            else:
                raise UserInputException(
                    f"Unknown RADOS URI option {key!r}. Supported: "
                    f"{sorted(_CONNECTION_OPTIONS | _LAYOUT_OPTIONS)} and conf.<librados option>"
                )
        except ValueError as e:
            raise UserInputException(f"Invalid value for RADOS URI option {key!r}: {value!r}") from e
    return parsed


# Builds the protobuf Config. with_connection=False leaves out client-specific settings (what gets persisted).
def get_rados_proto(parsed: ParsedRadosUri, with_connection: bool) -> RadosConfig:
    cfg = RadosConfig()
    cfg.pool = parsed.pool
    cfg.meta_pool = parsed.meta_pool
    cfg.namespace_prefix = parsed.namespace_prefix
    cfg.index_shards = parsed.index_shards
    cfg.unindexed_key_types.extend(parsed.unindexed_key_types)
    cfg.chunk_size = parsed.chunk_size
    if with_connection:
        cfg.cluster_name = parsed.cluster_name
        cfg.client_id = parsed.client_id
        cfg.conf_path = parsed.conf_path
        cfg.keyring = parsed.keyring
        cfg.op_timeout_s = parsed.op_timeout_s
        cfg.conf_overrides.update(parsed.conf_overrides)
    return cfg


# Adds a RADOS-backed library to an EnvironmentConfigsMap, like helper.add_*_library_to_env for other backends.
def add_rados_library_to_env(cfg, lib_name, env_name, parsed: ParsedRadosUri, description=None, with_connection=True):
    env = cfg.env_by_id[env_name]
    sid, storage = get_storage_for_lib_name(lib_name, env)
    storage.config.Pack(get_rados_proto(parsed, with_connection), type_url_prefix="cxx.arctic.org")
    _add_lib_desc_to_env(env, lib_name, sid, description)


class RadosLibraryAdapter(ArcticLibraryAdapter):
    """
    Store libraries directly in a Ceph RADOS pool through librados, without RGW/S3 in between.

    Supports any URI that begins with `rados://`. See the module docstring for the options.
    """

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("rados://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        self._parsed = parse_rados_uri(uri)
        self._encoding_version = encoding_version
        super().__init__(uri, self._encoding_version)

    def __repr__(self):
        p = self._parsed
        return f"rados(pool={p.pool}, meta_pool={p.meta_pool or p.pool}, namespace_prefix={p.namespace_prefix!r})"

    # The library that stores all other libraries' configs, kept in the same pool under its own namespace.
    @property
    def config_library(self):
        env_cfg = EnvironmentConfigsMap()
        add_rados_library_to_env(env_cfg, CONFIG_LIBRARY_NAME, _DEFAULT_ENV, self._parsed)
        lib = NativeVersionStore.create_store_from_config(
            env_cfg, _DEFAULT_ENV, CONFIG_LIBRARY_NAME, encoding_version=self._encoding_version
        )
        return lib._library

    # Config for a new library created through this Arctic instance.
    def add_library_to_env(self, env_cfg: EnvironmentConfigsMap, name: str):
        add_rados_library_to_env(env_cfg, name, _DEFAULT_ENV, self._parsed)

    # Builds a StorageOverride carrying this client's connection settings, or empty ones (parsed=None) for masking.
    def _override(self, parsed: Optional[ParsedRadosUri]) -> StorageOverride:
        rados_override = RadosOverride()
        if parsed is not None:
            rados_override.cluster_name = parsed.cluster_name
            rados_override.client_id = parsed.client_id
            rados_override.conf_path = parsed.conf_path
            rados_override.keyring = parsed.keyring
            rados_override.op_timeout_s = parsed.op_timeout_s
            rados_override.conf_overrides = dict(parsed.conf_overrides)
        storage_override = StorageOverride()
        storage_override.set_rados_override(rados_override)
        return storage_override

    # Applied when opening a library: connection settings come from this client's URI, not from storage.
    def get_storage_override(self) -> StorageOverride:
        return self._override(self._parsed)

    # Applied before saving a library config: strips connection settings (notably the keyring path).
    def get_masking_override(self) -> StorageOverride:
        return self._override(None)
