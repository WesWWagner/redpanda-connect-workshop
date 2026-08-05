# Lab 2: PostgreSQL CDC

**Duration:** ~35 minutes  
**Level:** Intermediate

## What you'll do
- Connect to a pre-provisioned Postgres database
- Stream changes from `public.orders` into `cdc.orders` on the central cluster
- Insert and update rows to see CDC events in real time
- Compare this approach against Debezium and AWS DMS

```mermaid
flowchart LR
    subgraph PG["🐘  PostgreSQL  (ERP / MES)"]
        direction TB
        WAL["Write-Ahead Log\nwal_level = logical"]
        SLOT["Replication Slot\nrpcn_cdc_&lt;student_id&gt;"]
        WAL -->|"captures every\nINSERT · UPDATE · DELETE"| SLOT
    end

    subgraph RC["⚡  Redpanda Connect"]
        direction TB
        INPUT["postgres_cdc input\nstream_snapshot: true"]
        PROC["mapping processor\n_captured_at = now()"]
        INPUT --> PROC
    end

    subgraph RP["🏢  Redpanda  (Central)"]
        TOPIC[("cdc.orders")]
        subgraph OPS["Event types"]
            direction LR
            R["op: r\nsnapshot row"]
            C["op: c\nINSERT"]
            U["op: u\nUPDATE"]
        end
        TOPIC --- OPS
    end

    SLOT -->|"change stream\nvia logical replication"| INPUT
    PROC -->|"enriched events"| TOPIC

    classDef pg fill:#336791,color:#fff,stroke:#1e4060
    classDef rc fill:#f97316,color:#fff,stroke:#c2410c
    classDef rp fill:#e11d48,color:#fff,stroke:#9f1239
    classDef op fill:#1e293b,color:#f8fafc,stroke:#475569

    class WAL,SLOT pg
    class INPUT,PROC rc
    class TOPIC rp
    class R,C,U op
```

---

> **Run everything in this lab inside the workbench shell** — open it with
> `docker compose exec workbench bash` (see the [README setup](README.md#setup-do-this-first)).
> Your `.env` values are already loaded, so `$PG_HOST`, `$CENTRAL_BROKER`, and
> the rest just work — no `source .env` needed.

## Part 1: Verify Postgres access

```bash
source .env
psql "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}" \
  -c "SELECT count(*) FROM public.orders;"
```

**Expected output:**
```
 count
-------
    10
(1 row)
```

Check that logical replication is enabled:

```bash
psql "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}" \
  -c "SHOW wal_level;"
```

**Expected output:**
```
 wal_level
-----------
 logical
(1 row)
```

> If `wal_level` is not `logical`, tell the instructor — the database needs to be reconfigured.

---

## Part 2: Create the output topic

```bash
source .env
rpk topic create cdc.orders --partitions 3 \
  --brokers $CENTRAL_BROKER --tls-enabled \
  --sasl-mechanism SCRAM-SHA-256 \
  --sasl-username $CENTRAL_USER --sasl-password $CENTRAL_PASSWORD
```

**Expected output:**
```
TOPIC       STATUS
cdc.orders  OK
```

---

## Part 3: Review the CDC pipeline

Open `configs/cdc.yaml`. Key things to notice:

- `postgres_cdc` input uses a **replication slot** — Postgres durably tracks what we've consumed
- `stream_snapshot: true` means we'll get all existing rows first, then live changes
- `slot_name` includes `$STUDENT_ID` so each student has an isolated slot
- Output is `kafka_franz` to your central cluster

> **`postgres_cdc` is a Redpanda Enterprise feature — and the license is
> already wired up for you.** A short-lived trial license ships in this repo as
> `redpanda.license`, and the workbench mounts it at Connect's default license
> path, so `redpanda-connect run` picks it up automatically. Nothing to do.
>
> If the trial has expired you'll see *"this feature requires a valid Redpanda
> Enterprise Edition license"* — just drop a fresh `redpanda.license` into the
> `workshop/` folder (or set `REDPANDA_LICENSE=<key>` in `.env`) and re-run.

Validate:
```bash
redpanda-connect lint configs/cdc.yaml
```

**Expected output:**
```
configs/cdc.yaml [0 errors]
```

---

## Part 4: Run the pipeline

```bash
source .env
redpanda-connect run --env-file .env configs/cdc.yaml
```

**Expected output (initial snapshot):**
```
level=info msg="Launching a Redpanda Connect instance, use CTRL+C to close"
level=info msg="Input type postgres_cdc is now active" path=root.input
level=info msg="Output type kafka_franz is now active" path=root.output
```
Messages start flowing immediately — the snapshot rows arrive first, then live changes.

---

## Part 5: Consume the snapshot

Open a new terminal. Stream from the beginning — the first ~10 messages are the snapshot rows:

```bash
source .env
rpk topic consume cdc.orders --offset start \
  --brokers $CENTRAL_BROKER --tls-enabled \
  --sasl-mechanism SCRAM-SHA-256 \
  --sasl-username $CENTRAL_USER --sasl-password $CENTRAL_PASSWORD
```

> Streams continuously. Press `Ctrl+C` once you've seen ~10 snapshot rows.

**Expected output:**
```json
{"op":"r","source":{"table":"orders"},"before":null,"after":{"id":1,"item":"widget","qty":5,"status":"pending"}}
{"op":"r","source":{"table":"orders"},"after":{"id":2,"item":"gadget","qty":3,"status":"shipped"}}
...
```

`"op":"r"` = read (snapshot). You'll see `"op":"c"` for inserts, `"op":"u"` for updates.

---

## Part 6: Trigger live changes

Insert a new order:

```bash
source .env
psql "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}" \
  -c "INSERT INTO public.orders (item, qty, status) VALUES ('sensor', 100, 'pending');"
```

**Expected output:**
```
INSERT 0 1
```

Now update an existing order:

```bash
psql "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}" \
  -c "UPDATE public.orders SET status = 'shipped' WHERE id = 1;"
```

The pipeline picks up changes in real time. Watch the consume you started in Part 5 — new messages appear within a second. If you stopped it, re-run:

```bash
source .env
rpk topic consume cdc.orders --offset start \
  --brokers $CENTRAL_BROKER --tls-enabled \
  --sasl-mechanism SCRAM-SHA-256 \
  --sasl-username $CENTRAL_USER --sasl-password $CENTRAL_PASSWORD
```

Press `Ctrl+C` once you see the INSERT and UPDATE events:

```json
{"op":"c","after":{"id":11,"item":"sensor","qty":100,"status":"pending"}}
{"op":"u","before":{"id":1,"status":"pending"},"after":{"id":1,"status":"shipped"}}
```

---

## Cleanup

Stop the pipeline: `Ctrl+C`

Drop the replication slot (important — slots hold Postgres WAL disk space):

```bash
source .env
psql "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}" \
  -c "SELECT pg_drop_replication_slot('rpcn_cdc_${STUDENT_ID}');"
```

Delete the topic:

```bash
source .env
rpk topic delete cdc.orders \
  --brokers $CENTRAL_BROKER --tls-enabled \
  --sasl-mechanism SCRAM-SHA-256 \
  --sasl-username $CENTRAL_USER --sasl-password $CENTRAL_PASSWORD
```

---

## What you learned
- Redpanda Connect can stream Postgres WAL directly — no Kafka Connect, no JVM
- Initial snapshot + live changes in a single pipeline
- Replication slots guarantee no changes are missed across restarts
- Each student needs a unique slot name — shared slots cause conflicts

---

## CDC Approach Comparison

| | **Redpanda Connect** | **Debezium** | **AWS DMS** |
|---|---|---|---|
| **Setup** | Single YAML, one binary | Kafka Connect cluster + connector config | Console wizard, managed |
| **JVM required** | No | Yes | No |
| **Schema evolution** | Manual (Bloblang) | Schema Registry integration | Limited |
| **Transforms** | Bloblang (full scripting) | SMTs (limited) | Table mappings |
| **Cost** | Included with Redpanda | Free OSS, ops overhead | Per-DBU billing |
| **Best for** | Lightweight, Redpanda-native pipelines | Complex multi-source, existing Kafka infra | Lift-and-shift migrations |

**When to choose Connect:**  
You already have Redpanda, you want one tool, and you need custom transformation logic.

**When to choose Debezium:**  
You need multi-database fan-out, Schema Registry integration, or already run Kafka Connect.

**When to choose DMS:**  
One-time migration, not ongoing replication. Managed service is worth the cost.
