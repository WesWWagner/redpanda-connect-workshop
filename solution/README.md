# Instructor Guide

This folder is not for students — it's the automated test harness and delivery
notes. Like the workshop itself, it's fully local and needs only Docker.

---

## Delivering the workshop

There is **nothing to pre-provision** — no cloud clusters, no credentials. Each
student runs the entire workshop on their own laptop from the repo root:

```bash
docker compose up -d
docker compose exec workbench bash
```

`STUDENT_ID` is baked to `local` by the compose file. Because every student runs
their own isolated stack, that shared name never collides between students.

---

## Verify the labs work

Two ways:

**A) Walk the student flow** — `docker compose up -d` at the repo root, open the
workbench, and run Lab 1 + Lab 2 exactly as written.

**B) Automated checks** — this folder has a self-contained stack + scripts that
build the workbench image and run every command (rpk, redpanda-connect, psql)
inside it:

```bash
cd solution/
cp .env.local.example .env.local      # first time only
docker compose up -d                  # wait until all 4 containers are healthy
./test-lab-1.sh
./test-lab-2.sh
docker compose down -v
```

Both scripts should exit `0`.

---

## License

`postgres_cdc` is a Redpanda Enterprise feature and needs a trial/enterprise
license. A trial license ships at the repo root as `redpanda.license`:

- The **workbench** mounts it at `/etc/redpanda/redpanda.license`, so
  `redpanda-connect run` uses it automatically — Lab 2 works with zero setup.
- **`test-lab-2.sh`** finds it (`../redpanda.license`, `solution/redpanda.license`,
  or `$REDPANDA_LICENSE`) and runs the real `postgres_cdc` pipeline; with no
  license it falls back to the mock pipeline.

Replace `redpanda.license` when the trial expires.

---

## Test environment

`solution/docker-compose.yaml` runs a host-port stack the scripts drive over the
host network:

| Container | Port | Role |
|-----------|------|------|
| `redpanda-site-a` | 19092 | Fab Site A |
| `redpanda-site-b` | 29092 | Fab Site B |
| `redpanda-central` | 39092 | Central IT |
| `postgres-workshop` | 5432 | ERP/MES DB |

The student-facing `docker-compose.yaml` at the repo root is separate: it wires
the same topology on one Docker network with service-name addressing and no host
ports.

---

## Note: real CDC output shape

Verified against redpanda-connect 4.102: `postgres_cdc` emits **flat row records**
— `{"_captured_at":…,"id":…,"item":…,"qty":…,"status":…}` — for snapshot reads,
inserts, and updates alike; there is no Debezium `op`/`before`/`after` envelope.
The student `lab-2-cdc.md` reflects this. The only place the older `op` shape
remains is `solution/configs/cdc-mock.yaml`, the license-free fallback that
`test-lab-2.sh` uses when no license is present.

---

## Timing Notes

- Lab 1 runs in ~40 min for most students. The pipeline step (Part 4) is where
  people get stuck — make sure they leave the workbench shell running.
- Lab 2 runs in ~30 min. The replication slot cleanup (dropping
  `rpcn_cdc_$STUDENT_ID`) is easy to forget — remind students at the end.
- The CDC comparison table in lab-2 is designed as a 5-min discussion closer.
  Walk through it verbally.
