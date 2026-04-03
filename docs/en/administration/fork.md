---
title: Volume Fork (Checkpoint & Restore)
sidebar_position: 9
---

:::tip
Volume fork is available in JuiceFS community edition. It requires no downtime on the source volume and copies no data at fork time.
:::

A **fork** creates a new, fully independent JuiceFS volume from an existing one. At the moment of forking:

- **No data is copied.** The fork shares all pre-fork object blocks with the source via a lease-based protection scheme.
- **Metadata is cloned instantly.** The fork gets its own metadata database with a complete snapshot of the source's directory tree, file attributes, and chunk layout at that point in time.
- **Both volumes are immediately independent.** New writes on either side go to separate chunk ID spaces, so they never collide.

The primary use cases are:

| Use case | What you do |
|----------|-------------|
| **Checkpoint** | Fork before a risky operation (migration, bulk delete, schema change). Roll back by mounting the fork. |
| **Parallel experiments** | Fork once, run different workloads on each branch, compare results. |
| **Test/staging from production** | Fork a production volume to get a consistent copy with zero data movement. |
| **Multi-level checkpoints** | Fork a fork to create nested checkpoints. |

## Prerequisites

- The source volume must be accessible (its metadata URL and object storage credentials).
- The destination metadata URL must point to an **empty** metadata store (a fresh Redis DB, an empty SQLite file, etc.).
- Both the source and fork share the same object storage bucket — no extra bucket is needed.

## Quick start

```shell
# Create a fork
juicefs fork redis://src-host/1 redis://dst-host/2 --name my-checkpoint

# List active forks of the source volume
juicefs fork list redis://src-host/1

# Mount the fork as a normal volume
juicefs mount redis://dst-host/2 /mnt/checkpoint

# When done, destroy the fork and release its lease
juicefs destroy redis://dst-host/2 <FORK-UUID>
juicefs fork release redis://src-host/1 --fork-name my-checkpoint
```

## Commands

### `juicefs fork create` (or `juicefs fork`)

Forks the source volume into the destination.

```shell
juicefs fork <SRC-META-URL> <DST-META-URL> [flags]
# or equivalently:
juicefs fork create <SRC-META-URL> <DST-META-URL> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | `<src>-fork-<uuid8>` | Human-readable name for this fork. Must be unique among active forks of the source. |
| `--threads` | `10` | Threads used for the internal metadata dump/load pipe. |

**What it does:**

1. Reads the source's current `nextChunk` and `nextInode` counters (the *fork base*).
2. Dumps the source metadata and loads it into the destination in a streaming pipe — no temp file.
3. Assigns the fork a new UUID and offsets its `nextChunk`/`nextInode` counters by `forkIndex × 2^40` so new IDs from the fork never collide with the source or sibling forks.
4. Writes a **lease object** into the shared bucket at `<bucket>/<volumeName>/_forks/<forkUUID>/lease`. This lease prevents the source's GC from reclaiming pre-fork objects.
5. Sets a `forkProtectBelow` counter in both the source and fork metadata databases so their GC daemons protect pre-fork chunk IDs even without reading the bucket.

### `juicefs fork dump`

Reserves a fork lease first, then writes a metadata dump and a sidecar manifest.

```shell
juicefs fork dump <SRC-META-URL> --path <DUMP-FILE> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--path` | (required) | Dump file path. The manifest is written to `<path>.fork.json`. |
| `--name` | `<src>-fork-<uuid8>` | Human-readable fork name stored in the lease and manifest. |
| `--threads` | `10` | Threads used for metadata dump. |
| `--binary` | `false` | Dump metadata in binary format instead of JSON. |
| `--keep-secret-key` | `true` | Keep object-storage secrets in the dump. |

**What it does:**

1. Reads source `nextChunk` / `nextInode` and computes `forkIndex`.
2. Writes the lease immediately at `<bucket>/<volumeName>/_forks/<forkUUID>/lease`, so GC protection starts before dump completes.
3. Persists source protection counters (`forkProtectBelow`, and `forkProtectRearm` when needed).
4. Writes metadata dump to `--path` and sidecar manifest to `<path>.fork.json`.

If dump fails after the lease is written, JuiceFS attempts to roll back the lease and, when no active leases remain, advances `forkProtectCleared` to a value greater than `forkProtectRearm`.

### `juicefs fork load`

Consumes a deferred fork dump + manifest and creates the destination fork DB.

```shell
juicefs fork load <DST-META-URL> --path <DUMP-FILE> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--path` | (required) | Dump file path. Manifest is read from `<path>.fork.json`. |
| `--threads` | `10` | Threads used when loading binary dump format. |

**What it does:**

1. Reads `<path>.fork.json` and validates format/version.
2. Verifies destination metadata is empty.
3. Loads the dump into destination (`LoadMeta` for JSON, `LoadMetaV2` for binary).
4. Patches destination UUID and diverged counters:
   - `nextChunk = forkBaseChunk + forkIndex * 2^40`
   - `nextInode = forkBaseInode + forkIndex * 2^40`
5. Marks shared storage (`forkSharedStorage=1`) and writes `forkProtectBelow` in destination DB.

### `juicefs fork list`

Lists all active fork leases of a volume.

```shell
juicefs fork list <SRC-META-URL>
```

Example output:

```
Active forks of myvol:

  Fork UUID                             Fork Name                 Base Chunk    Created At
  ------------------------------------------------------------------------------------------
  a1b2c3d4-...                          my-checkpoint             4194304       2025-06-01T10:00:00Z
  e5f6a7b8-...                          experiment-branch         4194304       2025-06-02T14:30:00Z
```

### `juicefs fork release`

Removes the fork lease from the bucket. Run this **after** destroying the fork volume.

```shell
juicefs fork release <SRC-META-URL> --fork-name <name>
```

Once the last lease is released, GC on the source can reclaim space that was only needed by the fork:

```shell
juicefs gc redis://src-host/1 --delete
```

## Typical workflows

### Checkpoint before a risky operation

```shell
# 1. Create a checkpoint fork
juicefs fork redis://prod/1 sqlite3:///tmp/checkpoint.db --name pre-migration

# 2. Perform the risky operation on the production volume
#    ... (migrations, bulk deletes, etc.)

# 3a. Success — destroy the checkpoint and free space
juicefs destroy sqlite3:///tmp/checkpoint.db <CHECKPOINT-UUID>
juicefs fork release redis://prod/1 --fork-name pre-migration
juicefs gc redis://prod/1 --delete

# 3b. Failure — restore by treating the checkpoint as the new production volume
#    (swap metadata URLs in your mount config, or dump and reload)
juicefs mount sqlite3:///tmp/checkpoint.db /mnt/restored
```

### Nested checkpoints (fork of fork)

```shell
# Fork production → branch-a
juicefs fork redis://prod/1 redis://branch-a/1 --name branch-a

# Fork branch-a → checkpoint-a (before a bulk operation on branch-a)
juicefs fork redis://branch-a/1 redis://ckpt-a/1 --name ckpt-a

# All three volumes remain independently consistent; GC on any of them
# will not delete objects needed by any other active branch.
```

### Create a staging environment from production

```shell
# Fork production to staging (zero data copy)
juicefs fork redis://prod/1 redis://staging/1 --name staging-$(date +%Y%m%d)

# Mount staging
juicefs mount redis://staging/1 /mnt/staging --no-usage-report

# Developers work on /mnt/staging — production is unaffected
# When done, destroy staging and release the lease
juicefs destroy redis://staging/1 <UUID>
juicefs fork release redis://prod/1 --fork-name staging-$(date +%Y%m%d)
```

## Destroying a fork

Use `juicefs destroy` on the fork's metadata URL. Because the fork shares object storage with the source, `destroy` on a fork **only wipes the fork's metadata database** — it does not touch any objects in the bucket.

```shell
# Destroy the fork (metadata only — objects are not deleted)
juicefs destroy redis://fork-host/2 <FORK-UUID>

# Release the lease so the source GC can reclaim pre-fork space
juicefs fork release redis://src-host/1 --fork-name my-checkpoint
```

:::warning
Do **not** `juicefs destroy` the **source** volume while it has active fork leases. The source objects are still referenced by the forks. `destroy` will refuse with an error if active leases are detected.
:::

## GC and space reclamation

JuiceFS GC (`juicefs gc`) is fork-aware:

- While a fork lease exists, GC on the source skips any object whose chunk ID is ≤ the fork's base chunk — those objects are still referenced by the fork.
- GC on the fork similarly skips pre-fork objects, so running `juicefs gc --delete` on the fork will not corrupt the source or sibling forks.
- Once all fork leases are released, a full GC on the source reclaims all unreferenced pre-fork objects.

```shell
# Reclaim space after all forks have been destroyed and released
juicefs gc redis://src-host/1 --delete
```

## How fork differs from clone and dump/load

JuiceFS has three operations that can produce what looks like a "copy" of data. They solve very different problems:

### `juicefs clone` — subtree copy within one volume

`juicefs clone` copies a file or directory tree **within the same mounted volume**. Source and destination are POSIX paths on the same mount point:

```shell
juicefs clone /jfs/src /jfs/dst
```

- Works entirely within **one metadata database** and **one mount point**.
- Cannot produce a separate, independently mountable volume.
- The destination path lives in the same filesystem namespace as the source — same JuiceFS volume, same metadata engine URL.
- Suitable for duplicating a subtree for local experiments, not for creating a separate branch of the whole volume.

### `juicefs dump` + `juicefs load` — metadata-only restore, no GC protection

`juicefs dump` serializes all metadata (inodes, directories, chunk mappings) to a JSON/binary file. `juicefs load` reads that file into a new empty metadata database:

```shell
juicefs dump redis://src/1 meta-backup.json
juicefs load redis://dst/2 meta-backup.json
```

This produces a new independently mountable volume — but with two critical differences from fork:

1. **No GC protection.** After load, both the source DB and the restored DB point at the same object blocks in the bucket. There is no lease between them. If GC runs on the source (`juicefs gc --delete`), it will delete objects that the restored volume still references, corrupting it. The two volumes can only safely coexist if you never run GC on either, or if you also copy all the object data to a separate bucket.

2. **Proportional to file count, not instant.** Dump serializes every inode and every chunk mapping. For a large volume (millions of files), this takes minutes to hours and produces a large intermediate file.

dump/load is the right tool for **disaster recovery** (restoring a backed-up metadata snapshot to a fresh cluster) or for **migrating between metadata engine types** (e.g. Redis → TiKV). It is not designed for creating live branches.

If you need a deferred workflow with the same GC safety as `fork create`, use:

```shell
juicefs fork dump <SRC-META-URL> --path <dump>
juicefs fork load <DST-META-URL> --path <dump>
```

This keeps the lease protection model while allowing the fork DB to be created later.

### `juicefs fork` — the only safe multi-volume branch

Fork is the only operation that produces a **separate mountable volume with a safe shared-object relationship**:

| | `clone` | `dump`+`load` | `fork` |
|---|---------|--------------|--------|
| Produces separate volume | No | Yes | Yes |
| GC-safe shared objects | N/A | **No** | **Yes** |
| Speed | Fast (CoW) | Proportional to file count | Fast (metadata pipe only) |
| Metadata engine scope | Single DB | Any → Any | Any → Any |
| Supports nested branches | No | No | Yes |
| Live GC on either volume | N/A | Unsafe | Safe |

The safety comes from the **lease object** written to the shared bucket at fork time. As long as the lease exists, GC on either the source or the fork will skip pre-fork objects. This means you can run `juicefs gc --delete` on the source at any time without corrupting the fork, and vice versa.

## Limitations and notes

- **Same object storage bucket.** The fork must use the same bucket and prefix as the source. The separation is in the metadata only.
- **Fork name must be unique** among the active forks of a source volume. Duplicate names are rejected at creation time.
- **Concurrent fork creation** from the same source at the exact same moment may produce the same fork index. Avoid running `juicefs fork` twice simultaneously against the same source.
- **Plain `dump`/`load`** (not `fork dump`/`fork load`) produces a standalone volume without fork leases and `forkSharedStorage` sentinel. Treat it as a new independent volume and manage object storage separately.
- **No automatic sync.** A fork is a point-in-time snapshot; changes on the source after the fork point are not propagated to the fork, and vice versa.
