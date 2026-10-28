# 003. Move reservations to the new service by SKU

Date: 2026-10-28

## Context

Online reservations move from the ERP to `reservation-service` in phases.
Reserved stock is shared state: if two systems take reservations for the same
position, they can both promise the same unit.

## Options

| Option | Problem |
|---|---|
| Global switch | 0% or 100%. One big cutover, which is what we're trying to avoid |
| Percentage of requests | Both systems reserve the same stock at the same time |
| Ownership per SKU | Needs an ownership table and a rule for moving a SKU |

## Decision

Ownership per SKU. A table says whether the ERP or the new service takes the
reservations for each SKU, and every request is routed by it. Each phase moves
more SKUs, starting with low-volume ones. Rollback moves them back.

A SKU only changes owner when the old owner has no open reservations for it,
or after they've been transferred.

## Open

Reconciliation will need to know who owns each SKU to know where its reserved
quantity is.

`STK_RES` exists in the ERP but no legacy code writes it. The ERP side of
reservations still has to be defined, probably the simulator acting as the
old storefront, without changing the schema.
