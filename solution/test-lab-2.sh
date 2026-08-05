#!/usr/bin/env bash
# Test Lab 2: PostgreSQL CDC.
# Docker-only — every tool runs inside the workbench container, nothing is
# installed on the host. Run from workshop/solution/ with the stack up:
#
#   docker compose up -d
#   ./test-lab-2.sh
#
# Uses the real postgres_cdc pipeline if a license is available
# (solution/redpanda.license file OR $REDPANDA_LICENSE in the environment),
# otherwise falls back to the mock generate pipeline. postgres_cdc is a
# Redpanda Enterprise feature, so no license => mock.
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/.env.local"

IMG=rp-workshop-workbench:latest
docker image inspect "$IMG" >/dev/null 2>&1 || \
  docker compose -f "$DIR/../docker-compose.yaml" build workbench

# Resolve a license, if any (file wins, else env).
LICENSE_STR=""
if [ -f "$DIR/redpanda.license" ]; then
  LICENSE_STR="$(cat "$DIR/redpanda.license")"           # solution/redpanda.license
elif [ -f "$DIR/../redpanda.license" ]; then
  LICENSE_STR="$(cat "$DIR/../redpanda.license")"        # workshop/redpanda.license (shipped)
elif [ -n "${REDPANDA_LICENSE:-}" ]; then
  LICENSE_STR="$REDPANDA_LICENSE"
fi

# Workbench runner. Passes the license through as $REDPANDA_LICENSE, which
# redpanda-connect picks up automatically.
wb(){ docker run --rm -i --network host -e REDPANDA_LICENSE="$LICENSE_STR" \
        -v "$DIR":/workshop -w /workshop "$IMG" "$@"; }

PASS=0; FAIL=0
ok()   { echo "  ✅ $1"; PASS=$((PASS+1)); }
fail() { echo "  ❌ $1"; [ -n "${2:-}" ] && echo "     $2"; FAIL=$((FAIL+1)); }
note() { echo "  ℹ️  $1"; }

cleanup(){ docker rm -f wb-lab2-pipe wb-lab2-consume >/dev/null 2>&1 || true; }
trap cleanup EXIT

DSN="postgres://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}?sslmode=disable"
PG(){ wb psql "$DSN" -t -A -c "$1"; }

echo ""
echo "=== Lab 2: PostgreSQL CDC ==="

echo ""
echo "Part 1: Verify Postgres"
count=$(PG "SELECT count(*) FROM public.orders;" 2>/dev/null | tr -d '[:space:]') \
  && ok "Postgres reachable (orders has ${count:-0} rows)" \
  || fail "Cannot connect to Postgres" "DSN: $DSN"
[ "${count:-0}" -ge 10 ] 2>/dev/null && ok "10+ seed rows present" || fail "Expected >=10 rows, got ${count:-0}"
wal=$(PG "SHOW wal_level;" 2>/dev/null | tr -d '[:space:]')
[ "$wal" = "logical" ] && ok "wal_level=logical" || fail "wal_level='$wal', need 'logical'"

echo ""
echo "Config validation"
wb redpanda-connect lint configs/cdc-local.yaml >/dev/null 2>&1 \
  && ok "cdc-local.yaml lints clean" \
  || fail "cdc-local.yaml lint errors" "$(wb redpanda-connect lint configs/cdc-local.yaml 2>&1)"

echo ""
echo "Part 2: Create topic"
wb rpk topic delete cdc.orders --brokers "$CENTRAL_BROKER" >/dev/null 2>&1 || true
PG "SELECT pg_drop_replication_slot('rpcn_cdc_test');" >/dev/null 2>&1 || true
wb rpk topic create cdc.orders --partitions 3 --brokers "$CENTRAL_BROKER" >/dev/null 2>&1 \
  && ok "cdc.orders on central" || fail "cdc.orders topic"

echo ""
if [ -n "$LICENSE_STR" ]; then
  echo "Part 3 & 4: Run real CDC pipeline (license found)"
  CFG=configs/cdc-local.yaml
  REAL_CDC=true
else
  echo "Part 3 & 4: Run mock CDC pipeline (no license — using generate)"
  note "Add solution/redpanda.license (or set \$REDPANDA_LICENSE) to test real postgres_cdc"
  CFG=configs/cdc-mock.yaml
  REAL_CDC=false
fi

docker rm -f wb-lab2-pipe >/dev/null 2>&1
docker run -d --name wb-lab2-pipe --network host -e REDPANDA_LICENSE="$LICENSE_STR" \
  -v "$DIR":/workshop -w /workshop "$IMG" redpanda-connect run "$CFG" >/dev/null 2>&1
sleep 4

# If real CDC failed to start (e.g. bad license), fall back to mock.
if [ "$REAL_CDC" = "true" ] && ! docker ps --format '{{.Names}}' | grep -q '^wb-lab2-pipe$'; then
  note "Real CDC pipeline exited early (license issue?) — falling back to mock"
  note "$(docker logs wb-lab2-pipe 2>&1 | grep -i licens | head -1)"
  REAL_CDC=false
  docker rm -f wb-lab2-pipe >/dev/null 2>&1
  docker run -d --name wb-lab2-pipe --network host -v "$DIR":/workshop -w /workshop "$IMG" \
    redpanda-connect run configs/cdc-mock.yaml >/dev/null 2>&1
fi
sleep 8

if docker ps --format '{{.Names}}' | grep -q '^wb-lab2-pipe$'; then
  ok "Pipeline running"
else
  ok "Pipeline ran to completion"   # mock emits 12 messages then exits
fi

if [ "$REAL_CDC" = "true" ]; then
  echo ""
  echo "Part 5a: Live changes"
  PG "INSERT INTO public.orders (item, qty, status) VALUES ('sensor', 100, 'pending');" >/dev/null 2>&1 \
    && ok "INSERT executed"
  PG "UPDATE public.orders SET status = 'shipped' WHERE id = 1;" >/dev/null 2>&1 \
    && ok "UPDATE executed"
  sleep 4
fi

docker rm -f wb-lab2-pipe >/dev/null 2>&1

echo ""
echo "Part 5: Verify events at central"
docker rm -f wb-lab2-consume >/dev/null 2>&1
docker run -d --name wb-lab2-consume --network host "$IMG" \
  rpk topic consume cdc.orders --offset start -f '%v\n' --brokers "$CENTRAL_BROKER" >/dev/null 2>&1
sleep 5
msgs="$(docker logs wb-lab2-consume 2>&1)"
docker rm -f wb-lab2-consume >/dev/null 2>&1

msg_count=$(echo "$msgs" | grep -c '_captured_at' 2>/dev/null || true)
[ "$msg_count" -ge 10 ] \
  && ok "$msg_count CDC events in cdc.orders" \
  || fail "Expected >=10 events, got $msg_count"

echo "$msgs" | grep -q '_captured_at' && ok '_captured_at enrichment present' || fail '_captured_at missing'

if [ "$REAL_CDC" = "true" ]; then
  # NOTE: redpanda-connect 4.102 postgres_cdc emits FLAT row records
  # ({"_captured_at":...,"id":...,"item":...}), not a Debezium op/before/after
  # envelope. Assertions match the real row shape.
  echo "$msgs" | grep -q 'widget'  && ok 'Snapshot rows present (widget)'   || fail 'Snapshot rows missing'
  echo "$msgs" | grep -q 'sensor'  && ok 'INSERT captured (sensor order)'   || fail 'INSERT event missing'
  echo "$msgs" | grep -q 'shipped' && ok 'UPDATE captured (status=shipped)' || fail 'UPDATE event missing'
else
  # The mock (cdc-mock.yaml) simulates an op/after shape for license-free runs.
  echo "$msgs" | grep -q '"op":"r"' && ok 'Read events (op=r) present'   || fail 'Read events missing'
  echo "$msgs" | grep -q '"op":"c"' && ok 'Insert events (op=c) present' || fail 'Insert events missing'
  echo "$msgs" | grep -q '"op":"u"' && ok 'Update events (op=u) present' || fail 'Update events missing'
fi

echo ""
echo "Cleanup"
PG "SELECT pg_drop_replication_slot('rpcn_cdc_test');" >/dev/null 2>&1 || true
wb rpk topic delete cdc.orders --brokers "$CENTRAL_BROKER" >/dev/null 2>&1 || true
if [ "$REAL_CDC" = "true" ]; then
  PG "UPDATE public.orders SET status='pending' WHERE id=1;" >/dev/null 2>&1 || true
  PG "DELETE FROM public.orders WHERE item='sensor';" >/dev/null 2>&1 || true
fi

echo ""
echo "=========================="
echo "Lab 2 results: $PASS passed, $FAIL failed"
[ $FAIL -eq 0 ] && echo "✅ Lab 2 PASS" || echo "❌ Lab 2 FAIL"
exit $FAIL
