# 004 — Remove the LLM catalog enricher from this project

Status: accepted, 2026-10-28

## Context

The first design included `catalog-enricher`: an LLM turning cryptic ERP
descriptions into structured product attributes, with validation, caching, a
token budget and an evaluation set in CI.

## Options

| Option | Gain | Cost |
|---|---|---|
| Keep it in full | One repository shows LLM work as well | A second story competing with the main one (consistency under failure), about one week of work, and a large overlap with a separate AI gateway project that covers budget, caching and evaluation in depth |
| Keep a minimal version | Some LLM presence | Weak evidence: the parts that make LLM work credible (cost control, evaluation) would be the ones cut |
| **Remove it** | A focused repository; the week goes to reconciliation and failure drills | No LLM work in this repository |

## Decision

Remove it. This project is about capturing changes from a system that cannot
be modified and proving the result is correct when infrastructure fails.

## Consequences

- `INV_ART` is still captured; nothing consumes it yet. It stays because
  reconciliation and the product-facing API may need it, and dropping a table
  from CDC later is cheap.
- The design loses one service, scenario F8 and the Spring AI dependency.
