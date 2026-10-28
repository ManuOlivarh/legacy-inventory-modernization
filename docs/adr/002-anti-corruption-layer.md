# 002 — Translate raw CDC into domain events in one Kafka Streams service

Status: accepted, 2026-09-17 (recorded after milestone 2)

## Context

Raw CDC rows carry the legacy model: padded `CHAR` codes, quantities whose
sign lives in another column, `DATETIME` in local time with no zone. If those
rows become the contract, every consumer re-implements the same quirks and
the legacy model never goes away.

## Options

| Option | Cost | Why not |
|---|---|---|
| Debezium SMTs (single message transforms) | No extra service | Cannot express the movement-type sign or the timezone fix without custom Java plugins inside Connect, which are harder to test and deploy |
| Each consumer translates for itself | Nothing to run | The legacy model spreads to every team, which is the problem we are solving |
| One translating service, Kafka Streams | One more service | **Chosen** |
| Flink | Strong stateful processing | A cluster to operate for a load this size does not need |

## Decision

`inventory-translator` is the only reader of the raw topics. It publishes:

- `inventory.stock-movements.v1` — `StockMoved`, signed quantity.
- `inventory.stock-levels.v1` — `StockLevelChanged`, compacted, with
  `available = onHand − reserved` and the source LSN.

Both are keyed by `sku:location`, so all events for one stock position stay
ordered on one partition.

**Stock levels come from `INV_STK`, not from summing movements.** A sum
drifts forever after one missed or replayed event. Absolute values from the
system of record correct themselves on the next change.

**Contracts are files.** Avro schemas live in `contracts/`, are published by
a deliberate step with `BACKWARD` compatibility, and the service runs with
`auto.register.schemas=false`.

**`processing.guarantee=exactly_once_v2`.** Reading the raw offset and
writing the domain event commit in one Kafka transaction.

## What this does not guarantee

- **No exactly-once from the ERP.** CDC is at-least-once (ADR 001). A
  duplicate raw row becomes a duplicate domain event. EOS here only stops
  the translator from adding duplicates of its own.
- **No exactly-once outside Kafka.** Anything that writes to a database
  must be idempotent. Consumers use `movementId` and `sourceLsn` for that.
- **Unknown movement types are dropped with a warning** today. Milestone 3
  sends them to a dead-letter topic instead.

## Evidence

- `TranslationTopologyTest`: one test per legacy quirk, no broker.
- `TranslatorIT`: the Spring application with EOS against a real broker and
  Schema Registry; a `read_committed` consumer sees the translated events,
  and the registry holds only the published contract versions.
- The full chain from SQL Server through Debezium is exercised by failure
  drills F2 and F3, not by a unit-level test.
