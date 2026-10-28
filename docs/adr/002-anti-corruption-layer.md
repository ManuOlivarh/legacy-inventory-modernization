# 002. Translate raw CDC rows in one Kafka Streams service

Date: 2026-09-17

## Context

Raw CDC rows are the legacy model: padded `CHAR` codes, a quantity whose sign
is in another column, `DATETIME` in local time without zone. If consumers read
them directly, each one has to know all of that.

## Decision

`inventory-translator` is the only reader of the raw topics. It publishes
`StockMoved` (signed quantity) and `StockLevelChanged` (on hand, reserved,
available and the source LSN), both keyed by `sku:location` so each stock
position stays ordered on one partition.

Stock levels are taken from `INV_STK`, not computed by summing movements. A
running sum drifts forever after one lost or repeated event; the absolute
value fixes itself on the next change.

Contracts are Avro files in `contracts/`, registered by a separate step with
`BACKWARD` compatibility. Services run with `auto.register.schemas=false`.

It runs with `exactly_once_v2`.

Debezium SMTs could do part of this, but not the sign or the timezone without
custom plugins inside Connect. Flink would work too, but it's a cluster to
run for a load this size doesn't need.

## What it doesn't guarantee

EOS only covers Kafka to Kafka inside the translator. CDC is at least once,
so a duplicated raw row still becomes a duplicated event, and anything that
writes to a database needs to be idempotent (`movementId`, `sourceLsn`).

Movements with an unknown type go to `inventory.translator.dlq` inside the
same transaction. Other bad rows (for example an unparseable decimal) still
stop the stream thread.
