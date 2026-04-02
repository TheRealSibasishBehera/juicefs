---
title: Volume Fork — Design & Internals
sidebar_position: 10
---

This document describes the internal design of the JuiceFS volume fork feature: how zero-copy forking works, how chunk ID spaces are kept isolated, how GC protection is maintained across the fork tree, and how the various failure modes are handled.

For the user-facing guide see [Volume Fork (Checkpoint & Restore)](../administration/fork.md).

---

## Overview

A JuiceFS volume is split into two independent layers:

- **Object storage** — the actual data blocks, addressed by `<chunkID>_<blockIndex>_<blockSize>`.
- **Metadata database** — the directory tree, file attributes, and the mapping from file extents to chunk IDs.

A fork exploits this separation: at fork time, **only the metadata is copied**. The fork gets its own metadata database containing a full snapshot of the source's directory tree and chunk mappings. Both the source and the fork then point at the same object blocks for all pre-fork data. New writes on either side allocate chunk IDs from non-overlapping ranges, so the objects they write never collide.

```
                    ┌──────────────────────────────────────────┐
                    │         Shared Object Storage             │
                    │  chunks/myvol/<chunkID>_<idx>_<size>     │
                    └────────────┬──────────────┬──────────────┘
                                 │ pre-fork      │ pre-fork
                    ┌────────────▼──┐        ┌──▼────────────┐
                    │  Source DB    │        │   Fork DB      │
                    │ new IDs start │        │ new IDs start  │
                    │ at baseChunk  │        │ at base+1×2^40 │
                    └───────────────┘        └────────────────┘
```

---

## Chunk ID space partitioning

JuiceFS assigns sequential chunk IDs from a `nextChunk` counter. If both the source and fork share a counter space, new writes from either side would collide in the object namespace.

At fork time the source's current `nextChunk` value becomes the **fork base** (`B`). The fork's counter is offset by `forkIndex × forkCounterOffset` where `forkCounterOffset = 1<<40` (≈ 1.1 trillion):

```
Source:   nextChunk starts at B         (organic growth from B upward)
Fork 1:   nextChunk starts at B + 1×2^40
Fork 2:   nextChunk starts at B + 2×2^40
Fork 3:   nextChunk starts at B + 3×2^40
...
```

Any organic growth within a single volume would need to generate 1.1 trillion chunks before it could overflow into the next partition — unreachable in practice. The same offset is applied to `nextInode`.

**`forkCounterOffset`** is defined in `cmd/fork.go`:

```go
const forkCounterOffset = int64(1) << 40
```

---

## The fork lease

The key mechanism preventing GC from deleting pre-fork objects is the **lease object** stored in the shared bucket:

```
<bucket>/<volumeName>/_forks/<forkUUID>/lease
```

The lease is a small JSON document (`ForkLease` struct):

```json
{
  "forkUUID":      "a1b2c3d4-...",
  "forkName":      "my-checkpoint",
  "sourceName":    "myvol",
  "sourceUUID":    "...",
  "forkBaseChunk": 4194304,
  "forkBaseInode": 131072,
  "forkIndex":     1,
  "createdAt":     "2025-06-01T10:00:00Z"
}
```

**Why the bucket, not a metadata counter?** The lease lives in the bucket so that any process — including GC running on the source with no knowledge of the fork's metadata URL — can discover it. GC only has access to the source's metadata DB and the bucket. Storing the lease in the bucket makes it universally visible.

All lease objects for a volume are under `<volumeName>/_forks/` so a single `ListAll` prefix scan finds them all.

---

## GC protection: two layers

GC has two code paths that can delete pre-fork objects, and both must be protected.

### Layer 1 — `deleteSlice_` (metadata-driven deletions)

When files are deleted or compacted, the metadata engine calls `deleteSlice_(id, size)` to queue the chunk for object deletion. This path runs on live mounts and on the GC worker.

Protection is via the in-memory `forkProtectBelow` field in `baseMeta`. If `id <= forkProtectBelow`, the deletion is silently skipped:

```go
func (m *baseMeta) deleteSlice_(id uint64, size uint32) {
    if protect := m.ForkProtect(); protect > 0 && id <= protect {
        return // fork-protected, skip
    }
    // ... proceed with deletion
}
```

`forkProtectBelow` is loaded at `NewSession` from the `forkProtectBelow` counter in the metadata DB, and refreshed lazily (at most once per second) on subsequent calls to `ForkProtect()`.

### Layer 2 — object scan in `juicefs gc`

GC's main loop scans all objects in the bucket and marks as "leaked" any object whose chunk ID does not appear in any slice in the metadata. For pre-fork objects that the **fork** deleted, these objects are genuinely absent from the source's metadata — but they must not be deleted because the fork may still reference them (and so may sibling forks).

Protection here is via a local variable `gcForkProtect` computed at the start of GC:

```go
// Combined threshold: max of DB counter and max lease base across all active forks
if gcForkProtect > 0 && uint64(cid) <= gcForkProtect {
    skipped.IncrInt64(obj.Size())
    continue
}
```

`gcForkProtect` is the **maximum** of:
1. The `forkProtectBelow` counter in the source's metadata DB (covers offline/DB-only state).
2. The `ForkBaseChunk` from every active lease in the bucket (authoritative, covers all levels of the fork tree).

Using **max** (not min) across all leases is critical — a fork-of-fork creates a lease with a higher base chunk than the first-level fork. GC on the source must skip objects all the way up to the highest base in the tree.

---

## Equal privilege design

Both the source and the fork can delete pre-fork files. A naive design where only the source has `forkProtectBelow` would leave the fork unprotected: the fork's `deleteSlice_` would happily delete pre-fork chunks, corrupting the source.

The equal-privilege fix: `forkProtectBelow` is written into **both** the source and the fork's metadata DB during `fork create`:

```
source DB:  forkProtectBelow = max(existing, forkBaseChunk)
fork DB:    forkProtectBelow = forkBaseChunk
```

This gives the fork symmetric GC safety — it cannot accidentally corrupt the source by running its own GC or deleting its own pre-fork files.

---

## Fork-of-fork (multi-level checkpoints)

When fork-a forks into ckpt-a:

1. fork-a's `nextChunk` at that moment becomes ckpt-a's base (call it `B2`, where `B2 > B`).
2. ckpt-a's lease is written with `ForkBaseChunk = B2`.
3. fork-a's `forkProtectBelow` must be raised to `B2` (previously it was `B`), because fork-a's GC now needs to protect ckpt-a's referenced objects too.

The condition in `forkCreate`:

```go
if existErr == nil && existing < forkBaseChunk {
    srcMeta.SetCounter("forkProtectBelow", forkBaseChunk)
}
```

This advances the threshold only forward — it never lowers it.

**Cross-sibling GC:** If source has two forks (fork-a with base `B1`, fork-b with base `B2` where `B2 > B1`), running GC on fork-a would see fork-b's objects (IDs ≈ `B + 2×2^40`) as leaked — those IDs are absent from fork-a's metadata. But both forks' leases are in the bucket under the same volume name. GC reads all leases, takes the max base, and protects up to that value. Fork-a's GC will not delete fork-b's chunks.

---

## Protection state machine: `forkProtectCleared` and `forkProtectRearm`

Metadata counters can only advance (they are append-only by convention). This creates a problem when:

1. All forks are released → protection is cleared (`forkProtectCleared = 1`).
2. A new fork is created → protection must be re-enabled.

Setting `forkProtectBelow` back to a higher value works for the threshold, but `forkProtectCleared = 1` still signals "ignore the threshold". We can't decrement `forkProtectCleared` to 0.

Solution: a third counter `forkProtectRearm`. GC treats protection as **active** if:

```
forkProtectBelow > 0  AND  NOT (forkProtectCleared > 0 AND forkProtectRearm <= forkProtectCleared)
```

State transitions:

| Event | Counter changes | Protection state |
|-------|----------------|-----------------|
| First fork created | `forkProtectBelow = B`, `cleared = 0`, `rearm = 0` | Active (`cleared = 0`) |
| All forks released | `cleared = 1` | Inactive (`cleared=1, rearm=0 ≤ cleared`) |
| New fork after full release | `rearm = cleared + 1 = 2` | Active again (`rearm=2 > cleared=1`) |
| Another full release | `cleared = 2` | Inactive (`cleared=2, rearm=2 ≤ cleared`) |

The bucket leases are always the authoritative source. The DB counters are a fast-path optimization to avoid a bucket scan on every `deleteSlice_` call.

---

## `forkSharedStorage` sentinel

When `juicefs destroy` is run against a fork, it must not delete the shared objects in the bucket (those are owned by the source). The fork's metadata DB contains a counter:

```
forkSharedStorage = 1
```

`destroy` reads this counter before doing anything destructive. If it is non-zero, `destroy` only wipes the metadata DB and skips all object deletions. The user is reminded to run `juicefs fork release` on the source afterward.

Conversely, if `destroy` is run against the **source** and active fork leases exist, `destroy` refuses with an error:

```
This volume has active fork leases (see 'juicefs fork list').
Destroying it would corrupt all forked volumes.
Release all forks first with 'juicefs fork release', then retry.
```

---

## Fork create: step-by-step

```
fork create SRC DST --name my-fork
│
├─ 1. Load SRC format (name, UUID, storage config)
├─ 2. Check DST is empty (no existing format)
├─ 3. Read SRC nextChunk → forkBaseChunk
│      Read SRC nextInode → forkBaseInode
├─ 4. List existing leases in bucket → compute forkIndex
├─ 5. Check for duplicate --name
├─ 6. Pipe: DumpMeta(SRC) → LoadMeta(DST)
│      (full metadata snapshot, ~seconds for large volumes)
├─ 7. Patch DST:
│      - New UUID (random)
│      - nextChunk = forkBaseChunk + forkIndex × 2^40
│      - nextInode = forkBaseInode + forkIndex × 2^40
│      - forkSharedStorage = 1
├─ 8. Write lease JSON to bucket:
│      <volumeName>/_forks/<forkUUID>/lease
└─ 9. Write protection counters:
       SRC DB: forkProtectBelow = max(existing, forkBaseChunk)
       SRC DB: forkProtectRearm = forkProtectCleared + 1  (if cleared > 0)
       DST DB: forkProtectBelow = forkBaseChunk
```

---

## Fork release: step-by-step

```
fork release SRC --fork-name my-fork
│
├─ 1. List leases → find the lease matching --fork-name
├─ 2. Delete lease object from bucket
├─ 3. Re-read remaining leases
│      If 0 remaining:
│        SRC DB: forkProtectCleared = 1
│        (user prompted to run juicefs gc --delete)
│      If N remaining:
│        log new minimum base chunk for info
└─ done
```

---

## Fork dump/load (deferred): step-by-step

```
fork dump SRC --path meta.dump [--name my-fork] [--binary]
│
├─ 1. Load SRC format and read forkBaseChunk / forkBaseInode
├─ 2. List existing leases → compute forkIndex
├─ 3. Generate forkUUID and forkName
├─ 4. Write lease JSON immediately:
│      <volumeName>/_forks/<forkUUID>/lease
├─ 5. Update source protection counters:
│      - forkProtectBelow = max(existing, forkBaseChunk)
│      - forkProtectRearm = forkProtectCleared + 1 (if needed)
├─ 6. Dump metadata to --path (json or binary)
└─ 7. Write sidecar manifest: <path>.fork.json
```

```
fork load DST --path meta.dump
│
├─ 1. Read and validate <path>.fork.json
├─ 2. Ensure DST metadata is empty
├─ 3. Load dump into DST (LoadMeta / LoadMetaV2)
├─ 4. Patch DST:
│      - UUID = forkUUID
│      - nextChunk = forkBaseChunk + forkIndex × 2^40
│      - nextInode = forkBaseInode + forkIndex × 2^40
│      - forkSharedStorage = 1
│      - forkProtectBelow = forkBaseChunk
└─ 5. Shutdown DST meta to flush SQLite WAL/checkpoint
```

Manifest keys:

`version`, `dumpPath`, `dumpFormat`, `createdAt`, `sourceName`, `sourceUUID`,
`forkUUID`, `forkName`, `forkBaseChunk`, `forkBaseInode`, `forkIndex`,
`forkCounterOffset`.

---

## Metadata dump/load pipe

`fork create` uses an in-memory pipe rather than a temp file to pass the metadata dump from source to destination:

```go
pr, pw := io.Pipe()
go func() {
    srcMeta.DumpMeta(pw, ...)
    pw.CloseWithError(err)
}()
dstMeta.LoadMeta(pr)
```

This avoids writing a potentially-large JSON/binary dump to disk. For very large volumes the dump is still bounded by the thread count (`--threads` flag) rather than by disk space.

---

## Object storage layout

After a fork, the bucket looks like:

```
<bucket>/
  myvol/                         ← shared volume prefix (source and fork both use this)
    chunks/
      0/0/
        1234_0_4194304            ← pre-fork block, referenced by source AND fork
        1234_1_1048576            ← pre-fork block
      1/0/
        1099511627777_0_4194304   ← post-fork block written by fork-1 (ID ≈ B + 1×2^40)
    _forks/
      a1b2c3d4-.../
        lease                     ← fork lease JSON
```

Both the source and the fork resolve object paths using the same `<volumeName>/chunks/` prefix — this is why both must use the same bucket and why `forkSharedStorage` is needed to prevent `destroy` from wiping shared objects.

---

## Failure modes and recovery

| Scenario | Effect | Recovery |
|----------|--------|----------|
| `fork create` crashes after lease write but before counter writes | Lease exists, DST may be partially loaded or unprotected | Delete the lease manually (`juicefs fork release`), repeat fork |
| `fork dump` fails after lease write | Temporary protection may remain | Command tries to delete lease and, if no leases remain, sets `forkProtectCleared=1`; rerun `fork dump` |
| `fork release` crashes after lease delete | Lease gone, `forkProtectCleared` may not be set | Manually set `forkProtectCleared = 1` in source DB via `juicefs config`, then GC |
| Source DB counter `forkProtectBelow` lost (DB reset) | GC falls back to bucket scan on next run — leases still protect objects | No action needed; GC reads leases from bucket |
| Fork DB counter `forkProtectBelow` lost | Fork's `deleteSlice_` loses DB protection | GC on the fork still reads bucket leases and skips protected objects; live-mount deleteSlice_ has no DB fallback — re-set counter via `juicefs config` |
| Orphan lease (fork DB wiped, lease not released) | Pre-fork objects are permanently protected on the source | Manually delete the lease object from the bucket at `<vol>/_forks/<uuid>/lease` |
