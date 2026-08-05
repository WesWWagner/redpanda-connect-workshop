# Instructor Guide

This folder is not for students. It contains the local test environment and
delivery notes.

**Docker is the only prerequisite** — for you and for students. Every tool the
labs use (`rpk`, `redpanda-connect`, `psql`) runs inside the *workbench*
container defined by the repo-root `docker-compose.yaml`; the test scripts below
build and drive it for you. Nothing is installed on the host.

---

## Before the Workshop

### 1. Pre-provision infrastructure

Students need three Redpanda Cloud clusters and one Postgres instance. Fill in
real values and distribute `.env` files (or share credentials over a secure
channel):

- **site-a** — simulates Fab Site A
- **site-b** — simulates Fab Site B
- **central** — Central IT hub
- **postgres** — ERP/MES database (pre-loaded with `postgres-init.sql` seed data)

Each student needs a unique `STUDENT_ID` (e.g., their name) to namespace
consumer groups and CDC replication slots so they don't conflict.

### 2. Verify the labs work locally

The bundled Docker stack simulates the whole topology on your laptop:

```bash
cd solution/
docker compose up -d
# wait until all 4 containers report healthy:
docker compose ps

./test-lab-1.sh
./test-lab-2.sh

docker compose down -v
```

Both scripts should exit `0`. On first run they build the workbench image
(a few minutes) and then run every command — rpk, redpanda-connect, psql —
*inside* that container, on the host network, against the local stack. You do
not need any workshop tooling installed.

### 3. License (already handled)

`postgres_cdc` is a Redpanda Enterprise feature and needs a valid
enterprise/trial license. A short-lived trial license ships at the repo root as
`redpanda.license`:

- The **student workbench** mounts it at Connect's default path
  (`/etc/redpanda/redpanda.license`), so `redpanda-connect run` picks it up
  automatically — Lab 2 CDC works with zero setup.
- **`test-lab-2.sh`** finds it (repo-root `redpanda.license`, or
  `solution/redpanda.license`, or `$REDPANDA_LICENSE`) and runs the *real*
  `postgres_cdc` pipeline. With no license it falls back to the mock pipeline.

When the trial expires, drop a fresh `redpanda.license` at the repo root.

---

## Test Environment

Local Docker (`solution/docker-compose.yaml`) simulates the 3-cluster + Postgres
topology:

| Container | Port | Simulates |
|-----------|------|-----------|
| `redpanda-site-a` | 19092 | Fab Site A |
| `redpanda-central` | 39092 | Central IT |
| `redpanda-site-b` | 29092 | Fab Site B |
| `postgres-workshop` | 5432 | ERP/MES DB |

Configs in `solution/configs/` use plain `localhost:port` (no TLS/auth). The
student-facing `configs/` at the repo root use env vars for cloud credentials.

---

## Note: real CDC output shape

Verified against `redpanda-connect` 4.102: the `postgres_cdc` input emits **flat
row records** — e.g. `{"_captured_at":"…","id":1,"item":"widget","qty":5,
"status":"pending"}` — for snapshot reads, inserts, and updates alike. It does
**not** emit a Debezium-style `op` / `before` / `after` / `source` envelope.

Two things in the delivered materials still show the older envelope shape and
should be reconciled if you want byte-for-byte fidelity in the live demo:

- `lab-2-cdc.md` "Expected output" blocks show `{"op":"r",…,"after":{…}}`.
- `configs/cdc-mock.yaml` (the no-license fallback) simulates that `op`/`after`
  shape.

The teaching narrative (snapshot vs. insert vs. update) still holds — just note
that with a real license students see flat rows keyed by the primary key, and
distinguish operations by the row's presence/values rather than an `op` field.

---

## Timing Notes

- Lab 1 runs in ~40 min for most students. The pipeline step (Part 4) is where
  people get stuck — make sure they leave the workbench shell running.
- Lab 2 runs in ~30 min. The replication slot cleanup (dropping
  `rpcn_cdc_$STUDENT_ID`) is easy to forget — remind students at the end.
- The CDC comparison table in lab-2 is designed as a 5-min discussion closer.
  Walk through it verbally.
