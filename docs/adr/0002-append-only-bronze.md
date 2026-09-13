# ADR 0002: Bronze is append-only and retains the raw payload

**Status:** accepted

## Context

Bronze could store parsed columns only, or apply light cleaning on the way in. It
could also be compacted and updated freely like any other table.

## Decision

Bronze is strictly append-only, stores the verbatim source payload alongside parsed
columns, and carries Kafka topic, partition and offset. No validation happens there:
a record failing every quality rule is still written.

## Why

**Raw payload.** Parsing is code, and code has bugs. A field mis-parsed for three
weeks is recoverable if the original bytes are kept and unrecoverable otherwise. The
storage cost is real and small; the alternative is a permanent data gap.

**No validation.** The raw layer's job is to preserve what arrived, not to judge it.
Judging belongs in Silver, where a rejection can be recorded with a reason and
replayed. A record dropped at ingestion leaves no trace that it ever existed.

**Append-only is load-bearing, not stylistic.** Silver consumes Bronze as a stream,
and Iceberg's streaming source aborts when it meets a snapshot produced by an
overwrite, delete or MERGE. Any in-place edit of Bronze breaks the Silver query.

## Consequences

Compaction rewrites files and therefore produces exactly such a snapshot. Two things
follow: the Silver reader sets `streaming-skip-overwrite-snapshots`, and compaction is
confined to a scheduled maintenance DAG rather than run ad hoc. This coupling is real
and is documented in the runbook.

Bronze also contains duplicates, because duplicates are what actually arrived. That is
a feature: they are the evidence that deduplication is doing something, and
`make verify` asserts they were present before asserting they were removed.
