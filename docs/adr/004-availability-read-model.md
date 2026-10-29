# 004. Availability read model

Date: 2026-10-28

## Context

The channels need current availability per SKU and location. The input is
`inventory.stock-levels.v1` (002), delivered at least once (001), so
duplicates and replays are normal. The database behind the read model will
sometimes be down.

## Decision

`availability-api` keeps one row per `(sku, location)` in its own Postgres,
with plain SQL through `JdbcClient` and Flyway. The write path is a single
upsert; JPA wouldn't add anything there.

`GET /availability/{sku}` returns each location, the totals and `asOf`, the
ERP time of the oldest value in the answer, so a client can see how fresh it is.

### Idempotency

The upsert applies when the event has a newer LSN, or the same LSN with
different values:

```sql
WHERE excluded.source_lsn > stock_level.source_lsn
   OR (excluded.source_lsn = stock_level.source_lsn
       AND (excluded.on_hand, excluded.reserved, excluded.changed_at)
           IS DISTINCT FROM (stock_level.on_hand, stock_level.reserved, stock_level.changed_at))
```

Two updates to the same row in one ERP transaction share a commit LSN, and
the second one has to apply. An exact redelivery doesn't: at first it was a
plain `>=`, and every redelivered event was rewritten and counted again in the
freshness histogram with its old age. LSNs are fixed-width hex, so comparing
them as text gives log order.

### Errors

- A record that can't be decoded or breaks the contract goes straight to
  `inventory.stock-levels.v1.availability-api.dlq`, byte for byte, with the
  exception in the headers.
- If the database is down, the consumer retries with backoff (max 30 s) for
  as long as it takes. The partition stops and lag grows. Sending these to
  the DLQ would fill it with good events that someone would have to replay.
- Anything else: 3 attempts, then DLQ.

I didn't use retry topics. They reorder events for the same key, add topics
per consumer, and our errors are either permanent or take down the whole
database, where they don't help.

### Freshness SLO

99% of stock changes visible in the API within 5 s of the ERP change, over 28
days. Measured with a histogram of `now - changedAt` when the row is written.
API availability: 99.9% of requests without a 5xx.

Consumer lag is exported but isn't an SLO: 1,000 records can be seconds or
hours depending on throughput. Also, Kafka's `records_lag_max` metric stayed
frozen during a large backlog; the real lag had to be read with
`kafka-consumer-groups.sh`.

## Measurements

Compose on one laptop, simulator at 5 movements/s.

| CDC capture job interval | Changes | ≤1 s | ≤2 s | ≤5 s | ≤10 s | Mean |
|---|---|---|---|---|---|---|
| 5 s (SQL Server default) | 451 | 10.6% | 30% | 91% | 100% | 3.0 s |
| 1 s | 597 | 18.6% | 100% | 100% | 100% | 1.3 s |

With the default the SLO wasn't met: the capture job alone adds 0 to 5 s. I
set it to 1 s (001). It's job configuration, not a schema change. It means
more frequent log scans on the ERP; at this load it didn't show, the load test
has to confirm it.

## Limits

- `changedAt` comes from the ERP clock. Clock skew between the ERP and the
  API shows up as freshness error. In Compose they share a clock.
- A snapshot delivers rows with their last change time, possibly days ago,
  and they count as stale in the histogram. Snapshot windows have to be
  excluded from the SLO for now.
- If the projection database is down, the API is down. There's no cache
  serving old values.
- The LSN check assumes the source log only moves forward. If the ERP database
  is restored or recreated, its LSNs start lower and every new event is
  rejected as stale (see the incident in 001). The fix is to rebuild the
  projection from a new snapshot.
