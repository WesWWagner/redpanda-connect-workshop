#!/usr/bin/env bash
# Test Lab 1: Fan-In Replication.
# Docker-only — every tool runs inside the workbench container, nothing is
# installed on the host. Run from workshop/solution/ with the stack up:
#
#   docker compose up -d          # the 3 redpanda + postgres containers
#   ./test-lab-1.sh
#
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/.env.local"

IMG=rp-workshop-workbench:latest
# Build the workbench image on demand (defined by the student compose file).
docker image inspect "$IMG" >/dev/null 2>&1 || \
  docker compose -f "$DIR/../docker-compose.yaml" build workbench

# Run a workshop tool inside the workbench, on the host network so it can reach
# localhost:190xx / :5432 exactly like the host would. -i so piped stdin
# (rpk topic produce) is delivered.
wb(){ docker run --rm -i --network host -v "$DIR":/workshop -w /workshop "$IMG" "$@"; }

PASS=0; FAIL=0
ok()   { echo "  ✅ $1"; PASS=$((PASS+1)); }
fail() { echo "  ❌ $1"; [ -n "${2:-}" ] && echo "     $2"; FAIL=$((FAIL+1)); }

cleanup() {
  docker rm -f wb-lab1-pipe wb-lab1-consume >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_broker() {
  local broker=$1 name=$2
  echo "  Waiting for $name ($broker)..."
  for i in $(seq 1 20); do
    wb rpk cluster info --brokers "$broker" >/dev/null 2>&1 && return 0
    sleep 2
  done
  fail "$name not ready after 40s"
  return 1
}

echo ""
echo "=== Lab 1: Fan-In Replication ==="

echo ""
echo "Prerequisites"
wait_for_broker "$SITE_A_BROKER" "site-a" && ok "site-a reachable"
wait_for_broker "$SITE_B_BROKER" "site-b" && ok "site-b reachable"
wait_for_broker "$CENTRAL_BROKER" "central" && ok "central reachable"

echo ""
echo "Part 1: Create topics"
# Clean slate so re-runs are idempotent (rpk topic create errors if it exists).
wb rpk topic delete fab.events    --brokers "$SITE_A_BROKER"  >/dev/null 2>&1 || true
wb rpk topic delete fab.events    --brokers "$SITE_B_BROKER"  >/dev/null 2>&1 || true
wb rpk topic delete central.events --brokers "$CENTRAL_BROKER" >/dev/null 2>&1 || true
wb rpk group delete fan-in-test   --brokers "$SITE_A_BROKER"  >/dev/null 2>&1 || true
wb rpk group delete fan-in-test   --brokers "$SITE_B_BROKER"  >/dev/null 2>&1 || true
sleep 1
wb rpk topic create fab.events --partitions 3 --brokers "$SITE_A_BROKER" >/dev/null 2>&1 \
  && ok "fab.events on site-a" || fail "fab.events on site-a"
wb rpk topic create fab.events --partitions 3 --brokers "$SITE_B_BROKER" >/dev/null 2>&1 \
  && ok "fab.events on site-b" || fail "fab.events on site-b"
wb rpk topic create central.events --partitions 6 --brokers "$CENTRAL_BROKER" >/dev/null 2>&1 \
  && ok "central.events on central" || fail "central.events on central"

# Start the pipeline first, then produce — avoids stale consumer-group offsets
# from a previous run swallowing the test messages.
echo ""
echo "Part 3 & 4: Run pipeline (in the workbench container)"
docker rm -f wb-lab1-pipe >/dev/null 2>&1
docker run -d --name wb-lab1-pipe --network host -v "$DIR":/workshop -w /workshop "$IMG" \
  redpanda-connect run configs/fan-in-local.yaml >/dev/null 2>&1
sleep 6
if docker ps --format '{{.Names}}' | grep -q '^wb-lab1-pipe$'; then
  ok "Pipeline running"
else
  fail "Pipeline crashed" "$(docker logs wb-lab1-pipe 2>&1 | tail -5)"
  echo "Lab 1 results: $PASS passed, $FAIL failed"
  exit 1
fi

echo ""
echo "Part 2: Produce messages (after pipeline is up)"
for i in $(seq 1 5); do echo "{\"site\":\"a\",\"seq\":$i}"; done \
  | wb rpk topic produce fab.events --brokers "$SITE_A_BROKER" >/dev/null 2>&1 \
  && ok "5 messages → site-a" || fail "Produce to site-a"
for i in $(seq 1 5); do echo "{\"site\":\"b\",\"seq\":$i}"; done \
  | wb rpk topic produce fab.events --brokers "$SITE_B_BROKER" >/dev/null 2>&1 \
  && ok "5 messages → site-b" || fail "Produce to site-b"
sleep 5

echo ""
echo "Part 5: Verify fan-in at central"
docker rm -f wb-lab1-consume >/dev/null 2>&1
docker run -d --name wb-lab1-consume --network host "$IMG" \
  rpk topic consume central.events --offset start -f '%v\n' --brokers "$CENTRAL_BROKER" >/dev/null 2>&1
sleep 8
msgs="$(docker logs wb-lab1-consume 2>&1)"
docker rm -f wb-lab1-consume >/dev/null 2>&1

echo "$msgs" | grep -q '"site":"a"'    && ok 'site-a messages in central'  || fail 'site-a messages missing'
echo "$msgs" | grep -q '"site":"b"'    && ok 'site-b messages in central'  || fail 'site-b messages missing'
echo "$msgs" | grep -q '_source.*site-a' && ok '_source tag: site-a'       || fail '_source tag missing for site-a'
echo "$msgs" | grep -q '_source.*site-b' && ok '_source tag: site-b'       || fail '_source tag missing for site-b'

# Stop pipeline
docker rm -f wb-lab1-pipe >/dev/null 2>&1

echo ""
echo "Cleanup"
wb rpk topic delete fab.events    --brokers "$SITE_A_BROKER"  >/dev/null 2>&1 || true
wb rpk topic delete fab.events    --brokers "$SITE_B_BROKER"  >/dev/null 2>&1 || true
wb rpk topic delete central.events --brokers "$CENTRAL_BROKER" >/dev/null 2>&1 || true
wb rpk group delete fan-in-test   --brokers "$SITE_A_BROKER"  >/dev/null 2>&1 || true
wb rpk group delete fan-in-test   --brokers "$SITE_B_BROKER"  >/dev/null 2>&1 || true

echo ""
echo "=========================="
echo "Lab 1 results: $PASS passed, $FAIL failed"
[ $FAIL -eq 0 ] && echo "✅ Lab 1 PASS" || echo "❌ Lab 1 FAIL"
exit $FAIL
