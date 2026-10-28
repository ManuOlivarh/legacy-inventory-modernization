# 003 — Move reservations out of the ERP by SKU ownership, not by request

Status: accepted, 2026-10-28

## Context

Online reservations move from the ERP to `reservation-service` (strangler
fig). The migration needs phases with a measurable share of traffic on the
new system, clear criteria to advance, and a rollback.

Reserved stock is shared state. Whoever takes a reservation for a stock
position must be the only one doing so, or two systems promise the same unit.

## Options

| Option | Phases measurable | Cost | Risk |
|---|---|---|---|
| A. Global switch: everything to the ERP or everything to the new service | No: 0% or 100% | Lowest | One big cutover, the thing strangler fig exists to avoid |
| **B. Ownership per SKU: a table says which system takes each SKU's reservations** | Yes: share of SKUs and of reservation volume | An ownership table, a router, and a handover rule for moving a SKU | Low: each stock position has exactly one owner at any time |
| C. Percentage per request | Yes | Low | Two systems reserve the same stock at once. Rejected |

## Decision

Option B. An ownership table maps `sku → ERP | NEW` with the time of the last
change. Every reservation request is routed by it. Phases move a growing set
of SKUs to `NEW`, starting with low-volume ones; rollback moves them back.

**Handover rule.** A SKU changes owner only when its old owner holds no open
reservations for it, or after those reservations are transferred. Without
this rule a move would briefly leave stock reserved in a system that no
longer answers for it.

The phases, the share at each one, the criteria to advance and the rollback
steps go in `docs/migration-plan.md` with milestone 4.

## Consequences

- Reconciliation has to know the owner of each SKU to know where its
  reserved quantity lives.
- Availability for a SKU owned by `NEW` combines ERP on-hand with
  reservations from the new service. Milestone 4 decides where that sum is
  computed.
- **Open question for milestone 4:** the legacy schema has `STK_RES`, but no
  legacy code path writes it today. The "ERP side" of reservations has to be
  defined (most likely the simulator acting as the old storefront) without
  changing the ERP schema.
