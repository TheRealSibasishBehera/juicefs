/*
 * JuiceFS, Copyright 2024 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

// forkLeasePrefix is the object key prefix under which lease markers are written.
// Full path: <volumeName>/_forks/<forkUUID>/lease
const forkLeasePrefix = "_forks/"

// forkCounterOffset is added to source counters when initialising the fork's
// ID space.  1<<40 ≈ 1.1 trillion — unreachable by organic growth.
const forkCounterOffset = int64(1) << 40

// ForkLease is the JSON document stored as the lease object in the bucket.
type ForkLease struct {
	ForkUUID      string `json:"forkUUID"`
	ForkName      string `json:"forkName"`
	SourceName    string `json:"sourceName"`
	SourceUUID    string `json:"sourceUUID"`
	ForkBaseChunk int64  `json:"forkBaseChunk"`
	ForkBaseInode int64  `json:"forkBaseInode"`
	ForkIndex     int64  `json:"forkIndex"`
	CreatedAt     string `json:"createdAt"`
}

func cmdFork() *cli.Command {
	return &cli.Command{
		Name:     "fork",
		Category: "ADMIN",
		Usage:    "Fork a volume into an independent copy sharing pre-fork data",
		Description: `
Fork creates a new independent volume from an existing one. The fork shares
pre-fork object data with the source via a lease-based copy-on-write scheme:
no data is copied at fork time. Both volumes operate independently after the
fork point — different metadata databases, different object namespaces for
new writes.

Pre-fork objects are protected from GC on the source volume as long as the
fork lease exists. Destroy the fork and release its lease to allow the source
volume's GC to reclaim that space.

Examples:
  # Fork a volume
  $ juicefs fork redis://localhost/1 redis://localhost/2 --name myvol-fork

  # List active forks of a volume
  $ juicefs fork list redis://localhost/1

  # Release a fork lease (run after juicefs destroy on the fork)
  $ juicefs fork release redis://localhost/1 --fork-name myvol-fork`,
		Subcommands: []*cli.Command{
			{
				Name:      "create",
				Action:    forkCreate,
				Usage:     "Fork a volume",
				ArgsUsage: "SRC-META-URL DST-META-URL",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "name",
						Usage: "name for the forked volume (default: <srcName>-fork-<shortUUID>)",
					},
					&cli.IntFlag{
						Name:  "threads",
						Value: 10,
						Usage: "number of threads for the internal metadata dump/load",
					},
				},
			},
			{
				Name:      "list",
				Action:    forkList,
				Usage:     "List active forks of a volume",
				ArgsUsage: "SRC-META-URL",
			},
			{
				Name:      "release",
				Action:    forkRelease,
				Usage:     "Release a fork lease (run after destroying the fork)",
				ArgsUsage: "SRC-META-URL",
			},
		},
		// Allow `juicefs fork SRC DST` as a shorthand for `juicefs fork create SRC DST`
		Action:    forkCreate,
		ArgsUsage: "SRC-META-URL DST-META-URL",
		// Flags are defined here at the parent level so reorderOptions can see them
		// (it only descends one level).  All subcommands share these flags.
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "name",
				Usage: "name for the forked volume (default: <srcName>-fork-<shortUUID>)",
			},
			&cli.IntFlag{
				Name:  "threads",
				Value: 10,
				Usage: "number of threads for the internal metadata dump/load",
			},
			&cli.StringFlag{
				Name:  "fork-name",
				Usage: "name of the fork whose lease should be released (for 'release' subcommand)",
			},
		},
	}
}

// ---------------------------------------------------------------------------
// fork create
// ---------------------------------------------------------------------------

func forkCreate(ctx *cli.Context) error {
	setup0(ctx, 2, 2)
	srcURI := ctx.Args().Get(0)
	dstURI := ctx.Args().Get(1)
	removePassword(srcURI)
	removePassword(dstURI)

	// 1. Load source format
	srcMeta := meta.NewClient(srcURI, meta.DefaultConf())
	defer srcMeta.Shutdown() //nolint:errcheck
	srcFormat, err := srcMeta.Load(true)
	if err != nil {
		return fmt.Errorf("load source format: %w", err)
	}

	// 2. Ensure destination is empty (same guard as juicefs load)
	dstMeta := meta.NewClient(dstURI, meta.DefaultConf())
	defer dstMeta.Shutdown() //nolint:errcheck
	if existingFmt, err := dstMeta.Load(false); err == nil {
		return fmt.Errorf("destination %s is already used by volume %s — use an empty metadata store",
			utils.RemovePassword(dstURI), existingFmt.Name)
	}

	// 3. Read source counters before touching anything
	forkBaseChunk, err := srcMeta.GetCounter("nextChunk")
	if err != nil {
		return fmt.Errorf("read nextChunk from source: %w", err)
	}
	forkBaseInode, err := srcMeta.GetCounter("nextInode")
	if err != nil {
		return fmt.Errorf("read nextInode from source: %w", err)
	}

	// 4. Determine fork index by counting existing leases in the bucket
	blob, err := createStorage(*srcFormat)
	if err != nil {
		return fmt.Errorf("connect to object storage: %w", err)
	}
	existingLeases, err := listForkLeases(ctx.Context, blob, srcFormat.Name)
	if err != nil {
		return fmt.Errorf("list existing fork leases: %w", err)
	}
	// forkIndex determines the chunk/inode ID space offset for this fork.
	// It is 1-based: source uses offset 0, first fork uses offset 1, etc.
	// NOTE: two concurrent `juicefs fork` invocations against the same source
	// can compute the same forkIndex (no distributed lock exists). The docs
	// warn against this. The UUID assigned to each fork ensures lease objects
	// never collide, but the chunk ID ranges would overlap. Avoid running
	// fork create twice simultaneously against the same source.
	forkIndex := int64(len(existingLeases)) + 1 // 1-based

	// 5. Build fork identifiers
	forkUUID := uuid.New().String()
	forkName := ctx.String("name")
	if forkName == "" {
		forkName = srcFormat.Name + "-fork-" + forkUUID[:8]
	}

	// Reject duplicate fork names — two leases with the same name make
	// 'fork release --fork-name' and 'fork list' output ambiguous.
	for _, l := range existingLeases {
		if l.ForkName == forkName {
			return fmt.Errorf("a fork named %q already exists under volume %s (UUID %s); choose a different --name",
				forkName, srcFormat.Name, l.ForkUUID)
		}
	}

	// 6. Dump source metadata and load into destination (pipe, no temp file)
	logger.Infof("Forking %s → %s (fork index %d)...", srcFormat.Name, forkName, forkIndex)
	pr, pw := io.Pipe()

	dumpErr := make(chan error, 1)
	go func() {
		err := srcMeta.DumpMeta(pw, 1, ctx.Int("threads"), false, false, false)
		_ = pw.CloseWithError(err)
		dumpErr <- err
	}()

	if err := dstMeta.LoadMeta(pr); err != nil {
		// Close pr so the dump goroutine unblocks and exits cleanly.
		_ = pr.CloseWithError(err)
		<-dumpErr
		return fmt.Errorf("load metadata into destination: %w", err)
	}
	if err := <-dumpErr; err != nil {
		return fmt.Errorf("dump source metadata: %w", err)
	}

	// 7. Patch destination: new UUID, diverged counters.
	// IMPORTANT: keep dstFormat.Name == srcFormat.Name so the fork uses the
	// same object-storage prefix (<bucket>/<name>/) as the source.  Both
	// volumes share the same chunk objects; isolation is purely in the
	// metadata (separate DB).  The human-readable fork label lives only in
	// the lease object, not in the format.
	dstFormat, err := dstMeta.Load(false)
	if err != nil {
		return fmt.Errorf("read destination format after load: %w", err)
	}
	dstFormat.UUID = forkUUID
	if err := dstMeta.Init(dstFormat, true /* force overwrite */); err != nil {
		return fmt.Errorf("patch destination format: %w", err)
	}

	newNextChunk := forkBaseChunk + forkIndex*forkCounterOffset
	newNextInode := forkBaseInode + forkIndex*forkCounterOffset
	if err := dstMeta.SetCounter("nextChunk", newNextChunk); err != nil {
		return fmt.Errorf("patch nextChunk: %w", err)
	}
	if err := dstMeta.SetCounter("nextInode", newNextInode); err != nil {
		return fmt.Errorf("patch nextInode: %w", err)
	}

	// Mark the fork's metadata DB so juicefs destroy knows this volume
	// shares object storage with its source and must NOT delete objects.
	// We encode the source UUID as a hash into the counter (UUID chars are
	// hex so this is safe as a positive int64 checksum used as a sentinel).
	// Any non-zero value of "forkSharedStorage" signals shared-storage fork.
	if err := dstMeta.SetCounter("forkSharedStorage", 1); err != nil {
		logger.Warnf("mark fork as shared-storage: %v (non-fatal)", err)
	}

	// 8. Write lease to bucket
	lease := ForkLease{
		ForkUUID:      forkUUID,
		ForkName:      forkName,
		SourceName:    srcFormat.Name,
		SourceUUID:    srcFormat.UUID,
		ForkBaseChunk: forkBaseChunk,
		ForkBaseInode: forkBaseInode,
		ForkIndex:     forkIndex,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	if err := writeForkLease(ctx.Context, blob, srcFormat.Name, forkUUID, lease); err != nil {
		return fmt.Errorf("write fork lease: %w", err)
	}

	// 9. Persist the fork protection threshold in BOTH the source and fork DBs.
	//
	// Equal-privilege design: both source and fork share pre-fork chunks from
	// the same bucket namespace. Either side can delete files (including pre-fork
	// ones), so both must protect the shared chunk IDs from deletion.
	//
	// Source DB: any live source mount or GC reads this at startup.
	// SetCounter only advances. We always try to advance to forkBaseChunk so
	// that the source's GC knows to protect all chunks up to this fork point.
	// For a first fork: 0 → forkBaseChunk.
	// For a fork-of-fork: the source is fork-a whose existing threshold may be
	// lower (set when fork-a was itself created). We must raise it to forkBaseChunk
	// so fork-a's GC protects its own post-fork-a chunks that this new child needs.
	existing, existErr := srcMeta.GetCounter("forkProtectBelow")
	logger.Infof("forkProtectBelow in source DB: %d (err: %v)", existing, existErr)
	if existErr == nil && existing < forkBaseChunk {
		if setErr := srcMeta.SetCounter("forkProtectBelow", forkBaseChunk); setErr != nil {
			logger.Warnf("persist forkProtectBelow in source: %v (non-fatal, GC will fall back to bucket)", setErr)
		} else {
			logger.Infof("Persisted forkProtectBelow=%d in source metadata DB", forkBaseChunk)
		}
	}
	// If a previous round of forks was fully released, forkProtectCleared was
	// set to 1 to signal GC that protection was disabled.  A new fork being
	// created must re-arm protection, so we must reset that sentinel.
	// We can't decrement a counter, so we use a separate "rearm" counter:
	// forkProtectRearm tracks how many new-fork cycles have started since the
	// last cleared signal.  GC treats protection as active if
	// rearmCount > clearedCount (i.e., a new fork was created after last clear).
	clearedVal, _ := srcMeta.GetCounter("forkProtectCleared")
	if clearedVal > 0 {
		// Advance rearm counter to be strictly greater than cleared counter,
		// signalling GC that a new fork exists and protection is active again.
		rearmVal, _ := srcMeta.GetCounter("forkProtectRearm")
		if rearmVal <= clearedVal {
			if setErr := srcMeta.SetCounter("forkProtectRearm", clearedVal+1); setErr != nil {
				logger.Warnf("reset fork protection rearm counter: %v (non-fatal)", setErr)
			} else {
				logger.Infof("Re-armed fork protection in source DB (cleared=%d, rearm=%d)", clearedVal, clearedVal+1)
			}
		}
	}
	// Fork DB: without its own threshold the fork's deleteSlice_ would have no
	// protection against deleting shared pre-fork chunks, corrupting the source
	// and any sibling forks. This gives the fork symmetric GC safety.
	if setErr := dstMeta.SetCounter("forkProtectBelow", forkBaseChunk); setErr != nil {
		logger.Warnf("persist forkProtectBelow in fork: %v (non-fatal)", setErr)
	} else {
		logger.Infof("Persisted forkProtectBelow=%d in fork metadata DB", forkBaseChunk)
	}

	// dstMeta.Shutdown() is called via defer above, which flushes the SQLite
	// WAL before the process exits, making the .db file self-contained.

	logger.Infof("Fork created successfully:")
	logger.Infof("  Source:     %s (%s)", srcFormat.Name, srcFormat.UUID)
	logger.Infof("  Fork:       %s (%s)", forkName, forkUUID)
	logger.Infof("  BaseChunk:  %d  →  fork starts at %d", forkBaseChunk, newNextChunk)
	logger.Infof("  BaseInode:  %d  →  fork starts at %d", forkBaseInode, newNextInode)
	logger.Infof("Mount the fork with: juicefs mount %s <mountpoint>", utils.RemovePassword(dstURI))
	return nil
}

// ---------------------------------------------------------------------------
// fork list
// ---------------------------------------------------------------------------

func forkList(ctx *cli.Context) error {
	setup0(ctx, 1, 1)
	srcURI := ctx.Args().Get(0)
	removePassword(srcURI)

	srcMeta := meta.NewClient(srcURI, meta.DefaultConf())
	srcFormat, err := srcMeta.Load(true)
	if err != nil {
		return fmt.Errorf("load source format: %w", err)
	}

	blob, err := createStorage(*srcFormat)
	if err != nil {
		return fmt.Errorf("connect to object storage: %w", err)
	}

	leases, err := listForkLeases(ctx.Context, blob, srcFormat.Name)
	if err != nil {
		return fmt.Errorf("list fork leases: %w", err)
	}

	if len(leases) == 0 {
		fmt.Printf("No active forks for volume %s\n", srcFormat.Name)
		return nil
	}

	fmt.Printf("Active forks of %s:\n\n", srcFormat.Name)
	fmt.Printf("  %-36s  %-24s  %-12s  %s\n", "Fork UUID", "Fork Name", "Base Chunk", "Created At")
	fmt.Printf("  %s\n", strings.Repeat("-", 90))
	for _, l := range leases {
		fmt.Printf("  %-36s  %-24s  %-12d  %s\n",
			l.ForkUUID, l.ForkName, l.ForkBaseChunk, l.CreatedAt)
	}
	return nil
}

// ---------------------------------------------------------------------------
// fork release
// ---------------------------------------------------------------------------

func forkRelease(ctx *cli.Context) error {
	setup0(ctx, 1, 1)
	srcURI := ctx.Args().Get(0)
	removePassword(srcURI)

	srcMeta := meta.NewClient(srcURI, meta.DefaultConf())
	srcFormat, err := srcMeta.Load(true)
	if err != nil {
		return fmt.Errorf("load source format: %w", err)
	}

	blob, err := createStorage(*srcFormat)
	if err != nil {
		return fmt.Errorf("connect to object storage: %w", err)
	}

	forkName := ctx.String("fork-name")
	if forkName == "" {
		return fmt.Errorf("--fork-name is required for 'fork release'")
	}
	leases, err := listForkLeases(ctx.Context, blob, srcFormat.Name)
	if err != nil {
		return fmt.Errorf("list fork leases: %w", err)
	}

	var target *ForkLease
	for i := range leases {
		if leases[i].ForkName == forkName {
			target = &leases[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("no active fork lease found for fork name %q under volume %s", forkName, srcFormat.Name)
	}

	leaseKey := forkLeaseKey(srcFormat.Name, target.ForkUUID)
	if err := blob.Delete(ctx.Context, leaseKey); err != nil {
		return fmt.Errorf("delete lease object %s: %w", leaseKey, err)
	}

	logger.Infof("Released fork lease for %q (UUID %s) from volume %s",
		forkName, target.ForkUUID, srcFormat.Name)

	// Re-read remaining leases to recalculate the protection threshold.
	remaining, err := listForkLeases(ctx.Context, blob, srcFormat.Name)
	if err != nil {
		logger.Warnf("re-list fork leases after release: %v", err)
	} else if len(remaining) == 0 {
		// No more active leases — set the "cleared" flag so any process
		// reading forkProtectBelow from the DB knows to ignore it.
		if setErr := srcMeta.SetCounter("forkProtectCleared", 1); setErr != nil {
			logger.Warnf("set forkProtectCleared: %v", setErr)
		}
		logger.Infof("All fork leases released — GC on %s can now reclaim pre-fork objects", srcFormat.Name)
		logger.Infof("Run `juicefs gc %s --delete` to reclaim space", utils.RemovePassword(srcURI))
	} else {
		// Compute new minimum for remaining leases
		newMin := remaining[0].ForkBaseChunk
		for _, l := range remaining[1:] {
			if l.ForkBaseChunk < newMin {
				newMin = l.ForkBaseChunk
			}
		}
		logger.Infof("%d fork lease(s) still active; minimum base chunk is %d", len(remaining), newMin)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Lease helpers
// ---------------------------------------------------------------------------

// forkLeaseKey returns the object key for a fork's lease.
// Format: _forks/<forkUUID>/lease   (relative to the volume prefix)
func forkLeaseKey(volumeName, forkUUID string) string {
	// The blob is already wrapped with WithPrefix(volumeName+"/") by
	// createStorage, so we only need the path relative to that prefix.
	_ = volumeName // kept for documentation clarity
	return forkLeasePrefix + forkUUID + "/lease"
}

func writeForkLease(ctx context.Context, blob object.ObjectStorage, volumeName, forkUUID string, lease ForkLease) error {
	data, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	key := forkLeaseKey(volumeName, forkUUID)
	return blob.Put(ctx, key, strings.NewReader(string(data)))
}

func listForkLeases(ctx context.Context, blob object.ObjectStorage, volumeName string) ([]ForkLease, error) {
	objs, err := object.ListAll(ctx, object.WithPrefix(blob, forkLeasePrefix), "", "", true, false)
	if err != nil {
		return nil, err
	}
	var leases []ForkLease
	for obj := range objs {
		if obj == nil {
			break
		}
		if obj.IsDir() || !strings.HasSuffix(obj.Key(), "/lease") {
			continue
		}
		r, err := blob.Get(ctx, forkLeasePrefix+obj.Key(), 0, -1)
		if err != nil {
			logger.Warnf("read fork lease %s: %v", obj.Key(), err)
			continue
		}
		data, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			logger.Warnf("read fork lease data %s: %v", obj.Key(), err)
			continue
		}
		var l ForkLease
		if err := json.Unmarshal(data, &l); err != nil {
			logger.Warnf("parse fork lease %s: %v", obj.Key(), err)
			continue
		}
		leases = append(leases, l)
	}
	return leases, nil
}

// LoadForkLeaseInfo is exported so gc.go can call it without import cycles.
// It returns the minimum and maximum forkBaseChunk across all active leases,
// and whether any leases exist at all.
//
// minBaseChunk: the earliest fork-point — used to set the in-memory
//
//	forkProtectBelow for the metadata-deletion path (deleteSlice_).
//
// maxBaseChunk: the latest fork-point across ALL leases (including fork-of-fork
//
//	leases).  Used as the object-scan protection threshold: any object
//	whose chunk ID is <= maxBaseChunk might be referenced by some live
//	fork at any level of the fork tree, so it must not be deleted.
func LoadForkLeaseInfo(ctx context.Context, blob object.ObjectStorage, volumeName string) (minBaseChunk, maxBaseChunk int64, hasLeases bool, err error) {
	leases, err := listForkLeases(ctx, blob, volumeName)
	if err != nil || len(leases) == 0 {
		return 0, 0, false, err
	}
	minBaseChunk = leases[0].ForkBaseChunk
	maxBaseChunk = leases[0].ForkBaseChunk
	for _, l := range leases[1:] {
		if l.ForkBaseChunk < minBaseChunk {
			minBaseChunk = l.ForkBaseChunk
		}
		if l.ForkBaseChunk > maxBaseChunk {
			maxBaseChunk = l.ForkBaseChunk
		}
	}
	return minBaseChunk, maxBaseChunk, true, nil
}
