#!/usr/bin/env bash
# Official-client conformance for rine: exercises the etcd API via etcdctl.
#
# The server under test must already listen on $ENDPOINT (either backend):
#   cargo run -p rine &                                # memory backend
#   RINE_BACKEND=oracle RINE_ORACLE_CONNECT=... cargo run -p rine &
# Then:
#   ENDPOINT=127.0.0.1:2379 ETCDCTL=/path/to/etcdctl bash e2e/etcdctl.sh
#
# Only RPCs the server implements are covered (KV/Watch/Lease/Status/
# MemberList). Alarm/Defrag/Snapshot/Auth/Lock/Election are out of scope.
set -euo pipefail

EP="${ENDPOINT:-127.0.0.1:2379}"
CTL="${ETCDCTL:?set ETCDCTL to the etcdctl binary}"
export ETCDCTL_API=3

PASS=0
fail() {
    echo "FAIL: $1 (expected: [$2], got: [$3])" >&2
    exit 1
}
check() { # check <label> <expected> <actual>
    if [ "$2" != "$3" ]; then
        fail "$1" "$2" "$3"
    fi
    PASS=$((PASS + 1))
    echo "ok: $1"
}

ectl() {
    "$CTL" --endpoints="$EP" "$@"
}

revision() {
    ectl endpoint status -w json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Status"]["header"]["revision"])'
}

echo "== readiness: member list =="
MEMBERS="$(ectl member list)"
echo "$MEMBERS" | grep -q "127.0.0.1:2379" || fail "member list" "127.0.0.1:2379" "$MEMBERS"
PASS=$((PASS + 1)); echo "ok: member list"

echo "== put/get =="
ectl put foo bar >/dev/null
check "get foo" "bar" "$(ectl get foo --print-value-only)"

echo "== prefix range / sort / limit / count =="
ectl put s/b 2 >/dev/null
ectl put s/a 1 >/dev/null
ectl put s/c 3 >/dev/null
check "prefix keys sorted" "s/a
s/b
s/c" "$(ectl get s --prefix --keys-only --order ASCEND --sort-by KEY | grep .)"
check "prefix limit 2" "s/a
s/b" "$(ectl get s --prefix --keys-only --order ASCEND --sort-by KEY --limit 2 | grep .)"
check "prefix count-only" "3" "$(ectl get s --prefix --count-only -w fields | grep '"Count"' | grep -oE '[0-9]+')"
check "prefix values-only" "1
2
3" "$(ectl get s --prefix --print-value-only --order ASCEND --sort-by KEY)"

echo "== txn =="
OUT="$(ectl txn <<EOF
mod("txkey") = "0"

put txkey v1

get txkey

EOF
)"
echo "$OUT" | grep -q "SUCCESS" || fail "txn success" "SUCCESS" "$OUT"
PASS=$((PASS + 1)); echo "ok: txn success"
OUT="$(ectl txn <<EOF
mod("txkey") = "0"

put txkey v1

get txkey

EOF
)"
echo "$OUT" | grep -q "FAILURE" || fail "txn failure" "FAILURE" "$OUT"
PASS=$((PASS + 1)); echo "ok: txn failure"
check "txn value" "v1" "$(ectl get txkey --print-value-only)"

echo "== put --prev-kv =="
ectl put pk v1 --prev-kv >/dev/null
PREV_OUT="$(ectl put pk v2 --prev-kv)"
echo "$PREV_OUT" | grep -q "^v1$" || fail "prev-kv" "v1" "$PREV_OUT"
PASS=$((PASS + 1)); echo "ok: prev-kv"
check "prev-kv current" "v2" "$(ectl get pk --print-value-only)"

echo "== delete =="
check "del foo count" "1" "$(ectl del foo --prev-kv | grep -c '^foo$' || true)"
check "foo gone" "" "$(ectl get foo --print-value-only)"
check "del prefix count" "3" "$(ectl del s --prefix | grep -oE '[0-9]+' | head -1)"

echo "== lease =="
LEASE="$(ectl lease grant 100 | awk '{print $2}')"
ectl put lk lv --lease="$LEASE" >/dev/null
check "leased get" "lv" "$(ectl get lk --print-value-only)"
ectl lease timetolive "$LEASE" --keys | grep -q "lk" || fail "timetolive keys" "lk" "$(ectl lease timetolive "$LEASE" --keys)"
PASS=$((PASS + 1)); echo "ok: timetolive keys"
ectl lease list | grep -q "$LEASE" || fail "lease list" "$LEASE" "$(ectl lease list)"
PASS=$((PASS + 1)); echo "ok: lease list"
ectl lease revoke "$LEASE" >/dev/null
check "revoked key gone" "" "$(ectl get lk --print-value-only)"

echo "== watch replay =="
ectl put w/1 a >/dev/null
ectl put w/2 b >/dev/null
timeout 10 env ETCDCTL_API=3 "$CTL" --endpoints="$EP" watch w --prefix --rev=1 > /tmp/rine-watch.log 2>&1 || true
grep -q "PUT" /tmp/rine-watch.log || fail "watch PUT event" "PUT" "$(cat /tmp/rine-watch.log)"
grep -q "w/1" /tmp/rine-watch.log || fail "watch w/1" "w/1" "$(cat /tmp/rine-watch.log)"
grep -q "w/2" /tmp/rine-watch.log || fail "watch w/2" "w/2" "$(cat /tmp/rine-watch.log)"
PASS=$((PASS + 1)); echo "ok: watch replay"

echo "== compact =="
REV_NOW="$(revision)"
ectl put c1 v1 >/dev/null
ectl compact "$REV_NOW" >/dev/null
check "latest after compact" "v1" "$(ectl get c1 --print-value-only)"
# A revision below the compaction point must be rejected.
if ectl get c1 --rev=1 >/dev/null 2>&1; then
    fail "compacted rev errors" "error" "success"
fi
PASS=$((PASS + 1)); echo "ok: compacted rev errors"

echo "== status =="
ectl endpoint status | grep -q "127.0.0.1:2379" || fail "endpoint status" "endpoint" "$(ectl endpoint status)"
PASS=$((PASS + 1)); echo "ok: endpoint status"

echo "ALL $PASS CHECKS PASSED"
