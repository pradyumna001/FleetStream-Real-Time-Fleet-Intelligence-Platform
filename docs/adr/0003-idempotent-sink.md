# ADR 0003: Deterministic event ids and a MERGE sink

**Status:** accepted

## Context

Structured Streaming's checkpoint guarantees the source is re-read consistently after
a failure. It does not guarantee the sink applied each record exactly once: if Spark
writes a batch and dies before the checkpoint advances, that batch is reprocessed on
restart.

With an appending sink, every row in the replayed batch is duplicated. Nothing errors.
The counts are simply wrong.

## Decision

1. `event_id` is a UUIDv5 over `(vehicle_id, trip_id, sequence)`, computed at the
   source and stable across replays.
2. Silver is written with `MERGE INTO ... ON event_id` inside `foreachBatch`, never
   with an append.

## Why not deduplication alone

`dropDuplicatesWithinWatermark` is used, and it is necessary - without a bounded
window the set of seen ids grows until the job dies. But bounded means the state
*expires*. An event replayed an hour later is no longer remembered and would be
written a second time.

The watermark makes the common case cheap. The MERGE makes correctness permanent.
They are complementary, and the platform needs both.

## Why the id must be deterministic

This is the part that is easy to get wrong. A random UUID per event makes the same
logical reading look like a new record on every replay, and MERGE then inserts it -
behaving exactly like the append it was meant to replace. The determinism is what
makes the merge key meaningful, so the namespace UUID is fixed permanently.

## Costs

MERGE is more expensive than append, so Silver is configured merge-on-read to avoid
rewriting whole files per batch. A duplicate surviving into a single batch would abort
the MERGE ("cannot match multiple source rows"), so `write_batch` also deduplicates
within the batch - making the guarantee local to the write rather than dependent on
upstream state being intact.

## Verification

`make verify` replays events already present in Silver through the real write path and
asserts the row count does not move. That check fails loudly if anyone changes the
sink to an append.
