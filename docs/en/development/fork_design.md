# JuiceFS Volume Fork — Design Document

## Overview

`juicefs fork` creates an independent copy of a volume that shares pre-fork object data
with the original via copy-on-write semantics. After forking, both volumes operate
completely independently — different metadata databases, different object namespaces for
new writes — with no coordination required at runtime.

The key insight is that the object storage prefix infrastructure already exists in
production (`cmd/format.go` applies `WithPrefix(blob, format.Name+"/")` to every
volume). Two volumes with different names already write to separate object paths.
The fork command is essentially wiring together existing primitives.

---

## Data Integrity Guarantees

### Pre-Fork Data — User-Visible Behaviour

The most important thing to get right: **each volume owns its own view of the
filesystem**. What one volume does to a file is never visible on another.

| Operation | On the volume that did it | On all other volumes |
|---|---|---|
| Delete a pre-fork file | File is gone immediately — not listed, not accessible, no future operations affected | File still exists, unchanged, fully accessible |
| Overwrite a pre-fork file | New content visible immediately | Original pre-fork content unchanged |
| Rename a pre-fork file | Rename visible immediately | File still at original path |
| `stat`, `ls`, any metadata op | Reflects that volume's current state | Reflects that volume's own state independently |

Deleting a pre-fork file from the original works **exactly like deleting any file**.
It disappears from the original immediately — not listed, not stat-able, not readable,
no interference with any future operations. The fact that the underlying chunk objects
are retained in the bucket for the fork's benefit is a **storage-layer implementation
detail, completely invisible to all users on all volumes**.

Similarly, a fork has no knowledge of or access to anything the original does after
the fork point. The original is just another independent volume.

### Pre-Fork Data — Storage Layer (implementation detail)

The chunk objects backing pre-fork files physically remain in the bucket until every
fork that references them is destroyed. This is transparent to users — it only affects
**how much storage you're billed for**, not what any user can see or do.

- Pre-fork objects are never GC'd while any fork lease is alive
- Once all forks are destroyed and their leases released, the original's GC reclaims
  those objects normally
- This is the explicit cost of zero-copy fork creation

### Post-Fork Data (diverged objects)

- Writes on one volume are **completely invisible** to all others
- Each volume is fully independent: different inodes, different chunk objects, different
  metadata DB
- GC on any volume only reclaims that volume's own post-fork objects
- No ordering guarantees across volumes — they are completely decoupled after fork time

### Multi-Fork Guarantee

- N forks of the same original are all independent of each other
- A fork of a fork has the same guarantees as a fork of the original
- Destroying any one fork does not affect the others
- Original and forks are peers — there is no "primary" after the fork point

### Explicit Non-Guarantees

- No cross-volume consistency — diverged writes are invisible to other forks
- No automatic merge or reconciliation
- No causal ordering of operations across volumes
- No point-in-time atomicity during fork — if the source is being written to while
  `juicefs fork` runs the dump/load pipeline, the fork captures a fuzzy snapshot.
  Quiesce the source first, or accept this limitation.

---

## How Object Storage Isolation Already Works

Every JuiceFS volume already namespaces its objects under its volume name:

```go
// cmd/format.go
blob = object.WithPrefix(blob, format.Name+"/")
```

So for two volumes `vol-orig` and `vol-fork` on the same bucket:

```
gs://mybucket/vol-orig/chunks/0/1/1001_0_4194304   ← original's new writes
gs://mybucket/vol-fork/chunks/0/1/1001_0_4194304   ← fork's new writes (different prefix)
```

Pre-fork objects live under `vol-orig/chunks/...`. After forking, the fork volume writes
all new chunks under `vol-fork/chunks/...`. The two namespaces never collide.

**This means post-fork write isolation is already solved. No new code needed for the
write path.**

---

## The Two Problems That Need Solving

### Problem 1: GC Safety for Pre-Fork Objects

After fork, both `meta-original.db` and `meta-forked.db` have `chunk_ref` rows for
pre-fork slices. These rows are independent — each DB has its own `refs` counter.

When a file is deleted on the original:

1. `meta-original.db` decrements `chunk_ref(id=5001)` → `refs=0`
2. GC runs → sees `refs <= 0` → calls `deleteSlice(5001)` → **deletes
   `vol-orig/chunks/.../5001_...` from the bucket**
3. The fork's `meta-forked.db` still has `refs=1` for slice 5001 and expects the
   object to exist
4. Fork reads that file → **object not found — data loss**

The GC query in `pkg/meta/sql.go:3431`:

```go
s.Where("refs <= 0").Find(&cks)
```

has no awareness that other forks may still reference a slice.

### Problem 2: Inode and Chunk ID Collision

After `dump` + `load`, both DBs have identical `nextInode` and `nextChunk` counters.
New inodes allocated by the original and the fork will have the same numeric IDs.
New chunk objects will be written to paths derived from those IDs.

Since the volume-name prefix already separates the object paths, ID collision does not
cause object-level corruption — but it does mean metadata is ambiguous if any
cross-volume tooling ever inspects raw IDs.

Advancing the fork's counters is cheap insurance and keeps the design clean.

---

## Solution Design

### Component 1 — Fork Lease (GC Safety)

When a fork is created, write a small marker object to the bucket:

```
<srcVolumeName>/_forks/<forkUUID>/lease
```

Content (JSON):

```json
{
  "forkUUID": "b3d9a1c2-...",
  "forkName": "vol-fork",
  "sourceName": "vol-orig",
  "sourceUUID": "a1b2c3d4-...",
  "forkBaseChunk": 8192,
  "forkBaseInode": 4096,
  "forkIndex": 1,
  "createdAt": "2026-03-30T10:00:00Z"
}
```

`forkBaseChunk` is the value of `nextChunk` in the source DB at fork time. Any slice
with `id <= forkBaseChunk` is a pre-fork slice and must not be GC'd while leases exist.

**GC modification** (`cmd/gc.go` + `pkg/meta/base.go`):

Before the cleanup loop in `doCleanupSlices`, perform one `LIST` call:

```go
leases, _ := blob.ListAll(ctx, "_forks/", "", false)
hasFork := len(leases) > 0

if hasFork {
    minBase := loadMinForkBaseChunk(leases)  // lowest forkBaseChunk across all leases
}
```

Then in the slice deletion loop:

```go
for _, ck := range cks {
    if hasFork && ck.Id <= minBase {
        continue  // pre-fork slice, protected by lease
    }
    m.deleteSlice(ck.Id, ck.Size)
}
```

The `LIST` result is cached for the duration of one GC cycle — not checked per slice.
Cost: one extra object storage LIST call per GC run.

**Lease removal**: When a fork is destroyed (`juicefs destroy` on the fork's meta +
explicit `--release-fork-lease` flag), delete the lease object. When zero leases exist
under `<srcVolumeName>/_forks/`, pre-fork GC resumes normally on the original.

**Fork GC is already safe**: The fork volume only ever touches `<forkName>/chunks/...`.
It never lists or deletes objects under `<srcName>/chunks/...`. No change needed.

### Component 2 — Counter Divergence

After loading the dump into the fork's DB, advance its counters to a non-overlapping
range:

```
forkIndex = number of existing leases + 1  (1-based)

fork.nextChunk = forkBaseChunk + forkIndex * (1 << 40)
fork.nextInode = forkBaseInode + forkIndex * (1 << 40)
```

`1 << 40` is ~1.1 trillion. At the current JuiceFS default batch size of 1024, a volume
would need to write ~1 billion files to approach this limit — well beyond any realistic
workload.

This is written directly to the fork's DB after `LoadMeta` completes, using the same
counter channel pattern in `pkg/meta/sql.go:4888`:

```go
chs[5] <- &counter{"nextChunk", forkNextChunk}
chs[5] <- &counter{"nextInode", forkNextInode}
```

For multiple forks of the same original, each gets a unique `forkIndex`, so their
counter ranges never overlap with each other or with the original.

### Component 3 — `juicefs fork` Commands

```
juicefs fork create <src-meta> <dst-meta> [--name <fork-name>]
```

**Full execution flow**:

```
1.  Load source Format from src-meta
2.  Validate dst-meta is empty (same check as juicefs load)
3.  Read forkBaseChunk = current "nextChunk" counter from src-meta
    Read forkBaseInode = current "nextInode" counter from src-meta
4.  Generate forkUUID = uuid.New()
5.  List existing leases in bucket to determine forkIndex
6.  Pipe: dump src-meta | load dst-meta
        (reuse existing dump/load pipeline — no temp file required)
7.  Patch dst-meta counters:
        nextChunk = forkBaseChunk + forkIndex * (1 << 40)
        nextInode = forkBaseInode + forkIndex * (1 << 40)
8.  Patch dst-meta Format:
        Name = fork-name (or auto: "<srcName>-fork-<shortUUID>")
        UUID = forkUUID
        (Bucket, Storage, credentials unchanged — same bucket, same backend)
9.  Write lease object to bucket:
        <srcName>/_forks/<forkUUID>/lease  (JSON content above)
10. Print success: both volumes ready to mount independently
```

**Cost**: one `dump` + `load` (metadata only, no object reads), one lease write.
No object copying. No data transfer. Sub-second for small volumes; scales with metadata
size only.

### Deferred flow: `fork dump` + `fork load`

To split fork creation into two stages without losing GC safety:

```
juicefs fork dump <src-meta> --path <dump-file> [--name <fork-name>] [--binary]
juicefs fork load <dst-meta> --path <dump-file>
```

`fork dump` performs lease reservation first, then writes metadata dump + sidecar manifest:

```
<dump-file>.fork.json
```

Manifest schema:

```json
{
  "version": 1,
  "dumpPath": "meta.dump",
  "dumpFormat": "json|binary",
  "createdAt": "RFC3339",
  "sourceName": "...",
  "sourceUUID": "...",
  "forkUUID": "...",
  "forkName": "...",
  "forkBaseChunk": 123,
  "forkBaseInode": 456,
  "forkIndex": 1,
  "forkCounterOffset": 1099511627776
}
```

`fork load` reads this manifest, loads metadata into an empty destination DB, then patches:

- `UUID = forkUUID`
- `nextChunk = forkBaseChunk + forkIndex * forkCounterOffset`
- `nextInode = forkBaseInode + forkIndex * forkCounterOffset`
- `forkSharedStorage = 1`
- `forkProtectBelow = forkBaseChunk`

If `fork dump` fails after lease creation, it attempts to delete the lease and
advances `forkProtectCleared` beyond `forkProtectRearm` when no leases remain.

### Component 4 — Fix `prefix.go` Copy Bug

The `Copy()` method in `pkg/object/prefix.go` does not apply the prefix:

```go
// current — bug
func (p *withPrefix) Copy(ctx context.Context, dst, src string) error {
    return p.os.Copy(ctx, dst, src)  // prefix missing on both dst and src
}

// fixed
func (p *withPrefix) Copy(ctx context.Context, dst, src string) error {
    return p.os.Copy(ctx, p.prefix+dst, p.prefix+src)
}
```

This does not block the fork feature (fork never calls `Copy` on pre-fork objects) but
must be fixed to avoid silent misbehavior in any future intra-volume copy operations
going through the prefix wrapper.

---

## Files to Change

| File | Change | Estimated Size |
|---|---|---|
| `cmd/fork.go` | New command: fork flow, lease write, counter patch | ~200 lines |
| `cmd/juicefs.go` | Register `fork` command | ~3 lines |
| `pkg/meta/base.go` | `hasForkLeases()` helper, guard in `doCleanupSlices` | ~30 lines |
| `cmd/gc.go` | Pass blob to GC so lease LIST is possible; apply guard | ~20 lines |
| `pkg/object/prefix.go` | Fix `Copy()` to apply prefix | 2 lines |

**No changes to**:

- `Format` struct — volume name is already the object prefix
- Write path, read path, or mount logic
- Dump or load internals
- Any existing metadata engine (SQL, Redis, TiKV)

---

## Multi-Fork Counter Layout

For a source volume with `forkBaseChunk = B` and `forkBaseInode = I`:

```
original:   inodes [1 .. I],        chunks [1 .. B]          (grows upward from base)
fork-1:     inodes [I+1T .. I+2T),  chunks [B+1T .. B+2T)    (1T = 1<<40)
fork-2:     inodes [I+2T .. I+3T),  chunks [B+2T .. B+3T)
fork-N:     inodes [I+N*T .. ...),  chunks [B+N*T .. ...)
```

A fork of a fork uses the fork's current counters as its own base, then adds the
same offset. The lease chain (`sourceName` → `forkName`) is recorded in each lease,
so GC on any ancestor volume can find the full set of dependents.

---

## GC Interaction — Full Scenarios

| Scenario | What Happens |
|---|---|
| Original deletes pre-fork file, no forks | `refs=0` → GC deletes slice normally |
| Original deletes pre-fork file, 1 fork alive | `refs=0` → lease found → GC skips slice |
| Fork deletes pre-fork file | `refs=0` in fork DB → fork GC runs → slice is under `<srcName>/` prefix, **fork GC never touches it** — safe |
| All forks destroyed, leases removed | GC on original resumes normally, cleans up all `refs<=0` slices |
| Fork deletes post-fork file | Slice under `<forkName>/` prefix, `refs=0` → fork GC deletes it — fully independent |
| Original and fork both overwrite same pre-fork file | Each writes a new slice under its own prefix; pre-fork slice ref decrements in each DB independently; GC safe per above |

---

## Operational Notes

### Listing forks of a volume

```bash
juicefs fork list <any-peer-meta>
# reads <volumeName>/_forks/ from bucket, prints lease metadata for each fork
# can be run from any volume sharing the bucket — source, fork, or fork-of-fork
```

### Destroying a fork safely

```bash
juicefs destroy <fork-meta>                            # destroys fork metadata + objects under forkName/
# then release the lease from any peer sharing the bucket:
juicefs fork release <any-peer-meta> --fork-name <name>  # removes the lease from the bucket
```

Releasing the lease without destroying the fork first leaves orphaned objects but does
not cause data corruption on the original — it just re-enables GC for pre-fork slices.

### Storage cost over time

Pre-fork objects under `<srcName>/chunks/...` are frozen as long as any lease exists.
The original volume cannot reclaim space from pre-fork deletions until all forks are
released. This is the explicit trade-off for zero-cost fork creation.

Post-fork writes on each volume are GC'd independently and normally.

---

## What This Is Not

- Not a snapshot system — there is no point-in-time consistency guarantee during an
  active fork operation (use `--quiesce` flag or quiesce the source externally first)
- Not a replication or sync mechanism — diverged writes are not reconciled
- Not a backup — use `juicefs dump` for backups
