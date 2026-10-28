# 001. Read ERP changes with Debezium

Date: 2026-09-17

## Context

The ERP is the system of record and we can't change it: no triggers, no new
columns. The online channels need its stock changes in seconds instead of the
nightly CSV.

Polling by `FEC_ULT` was the cheap option, but it misses deletes, misses
changes within the same second, and `FEC_ULT` is local time. Triggers would
have been exact, but they change the ERP schema.

## Decision

Enable SQL Server CDC on `INV_ART`, `INV_STK` and `INV_MOV` and read it with
the Debezium SQL Server connector.

- `snapshot.mode=initial`: first start reads the tables, then streams the log.
- Offsets live in `_connect-offsets` (RF 3). After a restart the connector
  continues from the last committed LSN, so delivery is at least once.
- Debezium uses its own login with read access to the captured tables and
  the `cdc` schema. Its password comes from an env config provider.
- The capture job polls every second (`pollinginterval = 1`). With the
  default of 5 s, 9% of changes missed the freshness SLO (see 004).
- Raw topics are internal. Only the translator reads them (002).

## Connector down for a while

Change rows are kept for 3 days (SQL Server default, not set here).

If the connector is down for 6 hours it restarts from its offset and works
through the backlog. Nothing is lost, it's just late.

If it's down longer than the retention, the stored LSN is gone from the
change tables and the connector fails on start. Recovery is a new snapshot.
The movements in that gap are lost as events, but stock levels come out right
because they're absolute values.

## Incident: offsets newer than the database (2026-10-28)

A Compose change recreated the SQL Server container. It has no volume, so the
ERP came back with an empty log at LSN `0000002e:...`. Kafka Connect still had
the old offset, `000000e0:...`.

The connector stayed `RUNNING` and published nothing. It was waiting for the
log to reach the stored LSN. The simulator kept writing and no component
reported an error. I noticed because the freshness histogram stopped getting
new observations.

The same can happen in production after restoring the ERP from a backup or
failing over to a replica that is behind. What we have or plan against it:

- alert when freshness gets no new observations, not only when it's slow;
- reconciliation against the ERP, which catches it whatever the cause;
- maybe a startup check that the stored LSN isn't ahead of
  `sys.fn_cdc_get_max_lsn()`, to be tried in the failure drills;
- recovery: delete the connector offsets and take a new snapshot.

## Consequences

Duplicates are expected, so every consumer has to be idempotent.

While CDC is behind, the ERP transaction log keeps growing. A long Connect
outage is a disk risk for the ERP, so log size needs an alert before this goes
anywhere near production.
