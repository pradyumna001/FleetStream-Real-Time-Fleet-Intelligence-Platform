# ADR 0006: Streaming checkpoints on object storage

**Status:** accepted (supersedes an earlier local-volume decision)

## Context

Structured Streaming checkpoints need atomic rename. Three locations were considered.

**A Windows bind mount** is categorically wrong: WSL2 bind mounts do not provide
reliable rename semantics, and the resulting corruption appears as intermittent,
hard-to-diagnose query failures rather than a clean error.

**A Docker named volume** was the original choice - ext4 inside the WSL2 VM, full
POSIX semantics, no host-filesystem translation. It is the textbook-correct answer,
and Spark's own documentation warns against object stores for checkpoints.

**Object storage (`s3a://`)** was initially rejected for exactly that reason.

## What forced the change

The Silver query streams *from* an Iceberg table. Iceberg's streaming source writes
its offset metadata into the checkpoint directory using the catalog's `S3FileIO` - not
through Spark's filesystem layer. Handed a `file:` URI it fails immediately:

```
ValidationException: Invalid S3 URI, cannot determine scheme:
file:/checkpoints/silver-transform/sources/0/offsets/0
```

The checkpoint location must therefore be a URI that **both** Spark's filesystem layer
and Iceberg's FileIO accept. That is `s3a://`.

## Decision

All streaming checkpoints live at `s3a://fleetstream/checkpoints/<job>`. The Spark
image carries `hadoop-aws` and the matching AWS SDK so Spark's own writer can use the
scheme, alongside `iceberg-aws-bundle` for Iceberg's FileIO.

## On the atomic-rename objection

The hazard that warning describes is two writers racing on one checkpoint. Exactly one
query owns each checkpoint path, so there is no race to lose. MinIO is strongly
consistent, as is S3 since 2020. This is also what managed Spark services do against
real S3.

The objection is legitimate in general and does not apply to this configuration. Both
halves of that sentence matter: the constraint is real, and so is the reason it is
survivable here.

## Costs

Two S3 clients on the classpath, with the version-alignment risk that implies -
`hadoop-aws` must match the Hadoop bundled in the Spark image (3.3.4) and the AWS SDK
it was compiled against. Those versions are pinned as build arguments with a comment
explaining why.
