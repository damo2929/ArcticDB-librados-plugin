#!/usr/bin/env bash
# Throwaway single-node Ceph cluster (1 mon, 1 mgr, N memstore OSDs) for the RADOS backend integration tests.
#
#   scripts/minicluster.sh up      start it in a background container (host networking)
#   scripts/minicluster.sh down    stop it and delete its data
#   scripts/minicluster.sh env     print the env vars the upstream test suite needs
#
# Creates replicated pools "arcticdb" (data) and "arcticdb-meta" (meta) and a cephx user client.arcticdb that can only
# touch those pools. Everything lives in build/minicluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${MINICLUSTER_NAME:-arcticdb-ceph}"
NUM_OSDS="${MINICLUSTER_OSDS:-3}"
MON_PORT="${MINICLUSTER_MON_PORT:-46789}"
DIR="$ROOT/build/minicluster"
IMAGE="${ARCTICDB_RADOS_IMAGE:-localhost/arcticdb-rados-dev:latest}"

limit_cpus() { echo $(( $(nproc) / 4 > 0 ? $(nproc) / 4 : 1 )); }
limit_mem() { echo "$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 4 ))k"; }

uri() { echo "rados://arcticdb?conf=/work/build/minicluster/ceph.conf&client_id=arcticdb&keyring=/work/build/minicluster/client.arcticdb.keyring&meta_pool=arcticdb-meta"; }

bootstrap() {
    # Runs inside the container, as the invoking user.
    local d=/work/build/minicluster
    rm -rf "$d" && mkdir -p "$d"/{mon,mgr,run,log}
    local fsid
    fsid="$(cat /proc/sys/kernel/random/uuid)"
    cat > "$d/ceph.conf" <<EOF
[global]
fsid = $fsid
mon host = v1:127.0.0.1:$MON_PORT
run dir = $d/run
admin socket = $d/run/\$name.asok
log file = $d/log/\$name.log
keyring = $d/\$name.keyring
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
ms bind msgr2 = false
osd pool default size = 1
osd pool default min size = 1
osd pool default pg autoscale mode = off
mon allow pool size one = true
mon allow pool delete = true
mon warn on pool no redundancy = false
osd objectstore = memstore
memstore device bytes = 2147483648
osd data = $d/osd.\$id
osd crush chooseleaf type = 0
[mon]
mon data = $d/mon/\$cluster-\$id
[mgr]
mgr data = $d/mgr/\$cluster-\$id
EOF
    local conf=(-c "$d/ceph.conf")
    ceph-authtool --create-keyring "$d/mon.keyring" --gen-key -n mon. --cap mon 'allow *' >/dev/null
    ceph-authtool --create-keyring "$d/client.admin.keyring" --gen-key -n client.admin \
        --cap mon 'allow *' --cap osd 'allow *' --cap mgr 'allow *' >/dev/null
    ceph-authtool "$d/mon.keyring" --import-keyring "$d/client.admin.keyring" >/dev/null
    cp "$d/mon.keyring" "$d/mon.a.keyring"
    monmaptool --create --add a "v1:127.0.0.1:$MON_PORT" --fsid "$fsid" "$d/monmap" >/dev/null
    ceph-mon "${conf[@]}" --mkfs -i a --monmap "$d/monmap" --keyring "$d/mon.keyring"
    ceph-mon "${conf[@]}" -i a

    ceph "${conf[@]}" auth get-or-create mgr.x mon 'allow profile mgr' osd 'allow *' mds 'allow *' \
        -o "$d/mgr.x.keyring"
    mkdir -p "$d/mgr/ceph-x"
    ceph-mgr "${conf[@]}" -i x

    for _ in $(seq "$NUM_OSDS"); do
        local uuid id
        uuid="$(cat /proc/sys/kernel/random/uuid)"
        id="$(ceph "${conf[@]}" osd new "$uuid")"
        mkdir -p "$d/osd.$id"
        ceph-osd "${conf[@]}" -i "$id" --no-mon-config --mkfs --mkkey --osd-uuid "$uuid" --keyring "$d/osd.$id.keyring"
        ceph "${conf[@]}" auth add "osd.$id" osd 'allow *' mon 'allow profile osd' mgr 'allow profile osd' \
            -i "$d/osd.$id.keyring"
        ceph-osd "${conf[@]}" -i "$id"
    done

    ceph "${conf[@]}" osd pool create arcticdb 64 64 replicated
    ceph "${conf[@]}" osd pool create arcticdb-meta 16 16 replicated
    ceph "${conf[@]}" osd pool application enable arcticdb arcticdb
    ceph "${conf[@]}" osd pool application enable arcticdb-meta arcticdb
    ceph "${conf[@]}" auth get-or-create client.arcticdb mon 'allow r' \
        osd 'allow rwx pool=arcticdb, allow rwx pool=arcticdb-meta' -o "$d/client.arcticdb.keyring"

    for _ in $(seq 60); do
        if ceph "${conf[@]}" pg stat 2>/dev/null | grep -q "active+clean" &&
            ! ceph "${conf[@]}" pg stat 2>/dev/null | grep -qE "creating|peering|unknown|activating"; then
            echo "minicluster ready"
            return 0
        fi
        sleep 1
    done
    ceph "${conf[@]}" -s
    echo "minicluster did not become healthy" >&2
    return 1
}

case "${1:-}" in
up)
    podman rm -f "$NAME" >/dev/null 2>&1 || true
    # Keep the test cluster to a quarter of the machine.
    podman run -d --name "$NAME" --network host --userns=keep-id -v "$ROOT:/work:Z" -w /work \
        --cpus "$(limit_cpus)" --memory "$(limit_mem)" \
        "$IMAGE" bash -c "scripts/minicluster.sh bootstrap && touch build/minicluster/ready && sleep infinity" >/dev/null
    for _ in $(seq 120); do
        [[ -f "$DIR/ready" ]] && { echo "minicluster up"; "$0" env; exit 0; }
        podman ps --filter "name=$NAME" --format '{{.Status}}' | grep -q Up || break
        sleep 1
    done
    podman logs "$NAME" >&2
    exit 1
    ;;
down)
    podman rm -f "$NAME" >/dev/null 2>&1 || true
    rm -rf "$DIR"
    ;;
bootstrap)
    bootstrap
    ;;
env)
    echo "export ARCTICDB_STORAGE_RADOS=1"
    echo "export ARCTICDB_RADOS_TEST_URI='$(uri)'"
    ;;
*)
    echo "usage: $0 up | down | env" >&2
    exit 2
    ;;
esac
