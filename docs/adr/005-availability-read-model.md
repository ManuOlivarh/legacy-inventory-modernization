# 005 — Availability read model: idempotent projection, freshness SLO, failure handling

Status: accepted, 2026-10-28

## Context

Sales channels need current availability per SKU and location. The source is
`inventory.stock-levels.v1` (ADR 002): absolute values, keyed by
`sku:location`, delivered at least once (ADR 001). Duplicates and replays are
normal, and the database behind the read model will sometimes be down.

## Decisions

### 1. Store and access

`availability-api`, a Spring Boot service with its own Postgres. One row per
`(sku, location)`, written with plain SQL through `JdbcClient`, schema by
Flyway. JPA is rejected: the whole write path is one conditional upsert, and
an ORM would hide the one statement that matters.

`GET /availability/{sku}` returns each location and the total, plus `asOf`
(the ERP time of the newest value served), so a client can see how fresh the
answer is instead of trusting it blindly.

### 2. Idempotency

The upsert only applies when the event is not older than the stored row:

```sql
INSERT ... ON CONFLICT (sku, location) DO UPDATE SET ...
WHERE excluded.source_lsn >= stock_level.source_lsn
```

- A duplicate or a replay of an old event changes nothing.
- `>=` and not `>`: two updates to one row in the same ERP transaction share
  a commit LSN; the later one, which arrives later on the same partition,
  must still apply.
- LSNs are fixed-width hex strings, so text comparison is numeric order.

### 3. Failure handling (blocking retry plus DLQ, no retry topics)

The design first said "retry topics, then DLQ". This replaces it.

| Failure | Handling | Why |
|---|---|---|
| Undeserialisable or invalid event (poison) | Straight to `inventory.stock-levels.v1.availability-api.dlq`, original headers plus the exception | Retrying cannot fix it |
| Database down or timing out (transient) | Blocking retry with exponential backoff, capped at 30 s, no limit on attempts. The partition stalls and lag grows | Sending these to a DLQ during an outage would fill it with healthy events, and someone would then have to replay all of them. Stalling is the backpressure |
| Anything else | 3 attempts with backoff, then DLQ | Unknown errors should not block the partition forever |

**Rejected: non-blocking retry topics.** They reorder events for the same
key, add two or more topics per consumer, and help mainly with errors that
fix themselves per record. Here errors are either permanent (poison) or
affect the whole database (outage); neither benefits.

The translator also gets a DLQ, `inventory.translator.dlq`, for movement
types it does not know. These are written inside its EOS transaction, which
replaces today's warn-and-drop (ADR 002).

A tool to re-inject DLQ records and replay from a point in time is milestone
6 (resilience), not this one.

### 4. Freshness SLO and how it is measured

Proposed targets, to be confirmed or corrected with load test numbers:

| SLO | Target | Measured by |
|---|---|---|
| Freshness | 99% of stock changes visible in the API within 5 s of the ERP commit, over 28 days | Histogram of `now − changedAt` when the row is written, with buckets at 1, 2, 5, 10, 30 s |
| API availability | 99.9% of requests without a 5xx, over 28 days | HTTP server metrics |

Consumer lag in records is exported too, but as a diagnostic, not an SLO: a
lag of 1,000 records means seconds or hours depending on throughput, and
clients care about seconds.

**Limits of the measurement:**

- `changedAt` comes from the ERP server's clock (`GETDATE()` in local time,
  converted by the translator). Clock skew between the ERP and the API shows
  up as freshness error. In Compose they share a clock; in production this
  needs NTP on both, and the README states it.
- An initial snapshot or a re-snapshot (ADR 001) delivers rows whose
  `changedAt` is the last time they changed, possibly days ago. Those land in
  the histogram as days of staleness, although nothing is late. The event
  does not say it came from a snapshot; adding that is the natural first case
  for the schema-evolution milestone. Until then, snapshot windows are
  excluded from the SLO by hand.

**First measurement (2026-10-28, Compose on one laptop, 5 movements/s,
90 s, 451 changes):** 10.6% within 1 s, 30% within 2 s, 91% within 5 s,
100% within 10 s; mean 3.0 s. **The 5 s target is not met.** The likely
dominant cause is SQL Server's CDC capture job, which scans the log every
5 s by default: that alone puts changes 0 to 5 s behind, evenly spread. The
decision between tuning the capture job and relaxing the target is open.

**Decision (2026-10-28): tune the capture job, keep the target.** The CDC
capture job now scans every 1 s (`sys.sp_cdc_add_job @pollinginterval = 1`
in `legacy-erp/sql/04-cdc-and-logins.sql`). This is job configuration a DBA
owns, not a change to the ERP schema or application. The cost is more
frequent log scans on the ERP server; at this load it was not measurable,
and the load test has to confirm it under stress.

**Second measurement (same setup, 120 s, 597 changes):** 18.6% within 1 s,
100% within 2 s; mean 1.3 s. The 5 s target is met with margin.

### 5. Scope: reconciliation moves to its own milestone

The design put the reconciler in milestone 3. It moves to milestone 4, with
its own ADR, because it is the most important evidence in the project and
deserves more than a quarter of a milestone. Reservation-service moves to 5.

## What this does not guarantee

- If the projection database is down, the API is down too. There is no cache
  serving stale data; that would hide the outage behind wrong answers.
- The LSN guard assumes one source log that only moves forward. If the ERP
  database is restored or recreated (ADR 001), its LSNs start lower and the
  projection rejects every new event as stale. Recovery is a rebuild: empty
  the table and replay from a new snapshot.
- Freshness depends on the whole chain. A stopped connector (ADR 001) breaks
  the SLO and the API cannot tell; `asOf` lets clients see it, and the alert
  on the freshness histogram catches it.

## Done when

- Testcontainers tests: duplicates, replays and out-of-order events leave
  the right value; a poison event lands in the DLQ while the next healthy one
  is applied (F4); with Postgres stopped, nothing goes to the DLQ and the
  backlog drains after it returns (a first version of F6).
- Freshness histogram and lag exported on `/actuator/prometheus`.
- `docker compose up` serves `GET /availability/{sku}` from live ERP changes.
