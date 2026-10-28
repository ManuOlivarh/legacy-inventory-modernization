# 001 — Capture ERP changes with Debezium on SQL Server CDC

Status: accepted, 2026-09-17 (recorded after milestone 1)

## Context

The ERP is the system of record and cannot be changed: no triggers, no new
columns, no application hooks. Sales channels need its stock changes within
seconds. Today they get a nightly CSV.

## Options

| Option | Cost | Why not |
|---|---|---|
| Poll tables by `FEC_ULT` | Simple, no extra infrastructure | Misses changes inside the same second, misses deletes, loads the ERP with scans, and `FEC_ULT` is local time with no zone |
| Triggers writing an outbox table | Exact | Changes the ERP schema, which is the one thing we are not allowed to do |
| SQL Server CDC + Debezium | Agent job and a Connect cluster to run | **Chosen** |

## Decision

SQL Server CDC on `INV_ART`, `INV_STK` and `INV_MOV`, read by the Debezium
SQL Server connector on Kafka Connect. CDC is a database feature enabled by
the DBA; the ERP's tables, procedures and application are untouched.

- **Initial snapshot.** `snapshot.mode=initial`: the first start reads the
  current tables, then streams from the log. Snapshot rows arrive with
  `op=r` and the translator treats them like inserts.
- **Restart.** Connect stores the last committed LSN in `_connect-offsets`
  (replication factor 3). A restarted connector resumes from it. Delivery is
  at-least-once: changes between the last offset commit and a crash are read
  again, so every consumer downstream must tolerate duplicates.
- **Least privilege.** Debezium logs in as `debezium`, a reader of the
  captured tables and the `cdc` schema, not `db_owner`. Its password comes
  from an environment config provider, never from the connector JSON.
- **Raw topics are internal.** One topic per table, JSON without schemas.
  Only the translating services may read them (ADR 002).

## What happens if the connector is down for 6 hours

SQL Server keeps change rows until its cleanup job removes them. The
retention is the SQL Server default, 3 days (4320 minutes); this project does
not set it. So:

- **Down 6 hours:** the connector restarts at its stored LSN, reads the
  backlog and catches up. Nothing is lost. Freshness is gone for those 6
  hours plus catch-up time, which the lag metric (milestone 3) must show.
- **Down longer than retention:** the stored LSN no longer exists in the
  change tables. The connector fails on start instead of silently skipping.
  Recovery is a new snapshot, which re-emits current state. Movements made
  during the gap are lost as individual events; stock levels are still
  correct because they are absolute values, not sums.

## Found in practice: offsets that outlive the database

On 2026-10-28 a Compose change recreated the SQL Server container, which has
no volume, so the ERP started again from an empty log. Kafka and Connect kept
their data, including the stored offset: LSN `000000e0:…` from the old
database. The new database was at `0000002e:…`.

Debezium then **skipped every change while reporting the connector as
`RUNNING`**: it waits for the log to pass the stored LSN. The simulator kept
writing, the raw topics stopped growing, and nothing failed. Only the
freshness histogram, with no new observations, gave it away.

The same thing happens in production when the source database is restored
from a backup or failed over to a replica with an older log. Mitigations:

- Freshness alert on "no new observations", not only on slow ones.
- Reconciliation (milestone 4) catches the resulting drift whatever the cause.
- A start-up check that the stored LSN is not ahead of `sys.fn_cdc_get_max_lsn()`
  turns a silent stall into a loud failure. To be decided with the failure drills.
- Recovery: delete the connector's offsets and take a new snapshot.

## Consequences

- Duplicates are normal, not a fault. Idempotent consumers are a
  requirement, not an optimisation.
- The ERP's transaction log grows while CDC is behind. A long outage of
  Connect is also a disk-space risk for the ERP, which is the one system we
  must not hurt. Retention and log size need an alert before this goes near
  production.
- The 6-hour and past-retention cases are claims until failure drill F3
  measures them.
