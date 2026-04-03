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
	"os"
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

const (
	forkManifestVersion = 1
	forkDumpFormatJSON  = "json"
	forkDumpFormatBin   = "binary"
)

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

// ForkManifest is written as a sidecar JSON at <dump-path>.fork.json.
type ForkManifest struct {
	Version           int    `json:"version"`
	DumpPath          string `json:"dumpPath"`
	DumpFormat        string `json:"dumpFormat"`
	CreatedAt         string `json:"createdAt"`
	SourceName        string `json:"sourceName"`
	SourceUUID        string `json:"sourceUUID"`
	ForkUUID          string `json:"forkUUID"`
	ForkName          string `json:"forkName"`
	ForkBaseChunk     int64  `json:"forkBaseChunk"`
	ForkBaseInode     int64  `json:"forkBaseInode"`
	ForkIndex         int64  `json:"forkIndex"`
	ForkCounterOffset int64  `json:"forkCounterOffset"`
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
				Name:      "dump",
				Action:    forkDump,
				Usage:     "Reserve a fork lease and dump source metadata for deferred fork creation",
				ArgsUsage: "SRC-META-URL",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "path",
						Usage: "path of the metadata dump file",
					},
					&cli.StringFlag{
						Name:  "name",
						Usage: "name for the forked volume (default: <srcName>-fork-<shortUUID>)",
					},
					&cli.IntFlag{
						Name:  "threads",
						Value: 10,
						Usage: "number of threads for the metadata dump/load",
					},
					&cli.BoolFlag{
						Name:  "binary",
						Usage: "dump metadata into a binary file (different from original JSON format)",
					},
					&cli.BoolFlag{
						Name:  "keep-secret-key",
						Value: true,
						Usage: "keep secret keys intact in the dump file",
					},
				},
			},
			{
				Name:      "load",
				Action:    forkLoad,
				Usage:     "Load a deferred fork dump into destination metadata",
				ArgsUsage: "DST-META-URL",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "path",
						Usage: "path of the metadata dump file",
					},
					&cli.IntFlag{
						Name:  "threads",
						Value: 10,
						Usage: "number of threads to load binary metadata",
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
			&cli.StringFlag{
				Name:  "path",
				Usage: "path of the metadata dump file (for 'dump'/'load' subcommands)",
			},
			&cli.BoolFlag{
				Name:  "binary",
				Usage: "dump metadata into a binary file (for 'dump' subcommand)",
			},
			&cli.BoolFlag{
				Name:  "keep-secret-key",
				Value: true,
				Usage: "keep secret keys intact in dump file (for 'dump' subcommand)",
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
	forkName := forkStringFlag(ctx, "name")
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
		err := srcMeta.DumpMeta(pw, 1, forkIntFlag(ctx, "threads"), false, false, false)
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

	// 9. Persist the fork protection threshold in BOTH source and fork DBs.
	persistSourceForkProtection(srcMeta, forkBaseChunk)
	persistForkProtection(dstMeta, forkBaseChunk)

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
// fork dump
// ---------------------------------------------------------------------------

func forkDump(ctx *cli.Context) (err error) {
	setup0(ctx, 1, 1)
	srcURI := ctx.Args().Get(0)
	removePassword(srcURI)

	dumpPath := forkStringFlag(ctx, "path")
	if dumpPath == "" {
		return fmt.Errorf("--path is required for 'fork dump'")
	}
	threads := normalizeForkThreads(forkIntFlag(ctx, "threads"))
	isBinary := forkBoolFlag(ctx, "binary")

	srcMeta := meta.NewClient(srcURI, meta.DefaultConf())
	defer srcMeta.Shutdown() //nolint:errcheck
	srcFormat, err := srcMeta.Load(true)
	if err != nil {
		return fmt.Errorf("load source format: %w", err)
	}

	forkBaseChunk, err := srcMeta.GetCounter("nextChunk")
	if err != nil {
		return fmt.Errorf("read nextChunk from source: %w", err)
	}
	forkBaseInode, err := srcMeta.GetCounter("nextInode")
	if err != nil {
		return fmt.Errorf("read nextInode from source: %w", err)
	}

	blob, err := createStorage(*srcFormat)
	if err != nil {
		return fmt.Errorf("connect to object storage: %w", err)
	}
	existingLeases, err := listForkLeases(ctx.Context, blob, srcFormat.Name)
	if err != nil {
		return fmt.Errorf("list existing fork leases: %w", err)
	}
	forkIndex := int64(len(existingLeases)) + 1 // 1-based

	forkUUID := uuid.New().String()
	forkName := forkStringFlag(ctx, "name")
	if forkName == "" {
		forkName = srcFormat.Name + "-fork-" + forkUUID[:8]
	}
	for _, l := range existingLeases {
		if l.ForkName == forkName {
			return fmt.Errorf("a fork named %q already exists under volume %s (UUID %s); choose a different --name",
				forkName, srcFormat.Name, l.ForkUUID)
		}
	}

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
	defer func() {
		if err == nil {
			return
		}
		rollbackErr := rollbackForkLeaseAfterDumpFailure(ctx.Context, srcMeta, blob, srcFormat.Name, forkUUID)
		if rollbackErr != nil {
			logger.Warnf("rollback lease after failed fork dump: %v", rollbackErr)
		}
	}()

	persistSourceForkProtection(srcMeta, forkBaseChunk)

	if err := dumpMeta(srcMeta, dumpPath, threads, forkBoolFlag(ctx, "keep-secret-key"), false, false, isBinary); err != nil {
		return fmt.Errorf("dump metadata to %s: %w", dumpPath, err)
	}

	manifest := ForkManifest{
		Version:           forkManifestVersion,
		DumpPath:          dumpPath,
		DumpFormat:        forkDumpFormatJSON,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339),
		SourceName:        srcFormat.Name,
		SourceUUID:        srcFormat.UUID,
		ForkUUID:          forkUUID,
		ForkName:          forkName,
		ForkBaseChunk:     forkBaseChunk,
		ForkBaseInode:     forkBaseInode,
		ForkIndex:         forkIndex,
		ForkCounterOffset: forkCounterOffset,
	}
	if isBinary {
		manifest.DumpFormat = forkDumpFormatBin
	}
	manifestPath := forkManifestPath(dumpPath)
	if err := writeForkManifest(manifestPath, manifest); err != nil {
		return fmt.Errorf("write fork manifest %s: %w", manifestPath, err)
	}

	logger.Infof("Fork dump created successfully:")
	logger.Infof("  Source:      %s (%s)", srcFormat.Name, srcFormat.UUID)
	logger.Infof("  Fork:        %s (%s)", forkName, forkUUID)
	logger.Infof("  Dump:        %s (%s)", dumpPath, manifest.DumpFormat)
	logger.Infof("  Manifest:    %s", manifestPath)
	logger.Infof("Run: juicefs fork load <DST-META-URL> --path %s", dumpPath)
	return nil
}

// ---------------------------------------------------------------------------
// fork load
// ---------------------------------------------------------------------------

func forkLoad(ctx *cli.Context) (err error) {
	setup0(ctx, 1, 1)
	dstURI := ctx.Args().Get(0)
	removePassword(dstURI)

	dumpPath := forkStringFlag(ctx, "path")
	if dumpPath == "" {
		return fmt.Errorf("--path is required for 'fork load'")
	}
	manifestPath := forkManifestPath(dumpPath)
	threads := normalizeForkThreads(forkIntFlag(ctx, "threads"))

	manifest, err := readForkManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("read fork manifest %s: %w", manifestPath, err)
	}
	if manifest.DumpPath != "" && manifest.DumpPath != dumpPath {
		logger.Warnf("manifest dumpPath=%q differs from --path=%q", manifest.DumpPath, dumpPath)
	}

	dstMeta := meta.NewClient(dstURI, meta.DefaultConf())
	closed := false
	defer func() {
		if !closed {
			_ = dstMeta.Shutdown()
		}
	}()

	if existingFmt, err := dstMeta.Load(false); err == nil {
		return fmt.Errorf("destination %s is already used by volume %s — use an empty metadata store",
			utils.RemovePassword(dstURI), existingFmt.Name)
	}

	r, err := open(dumpPath, "", "")
	if err != nil {
		return fmt.Errorf("open dump file %s: %w", dumpPath, err)
	}
	defer r.Close() //nolint:errcheck

	switch manifest.DumpFormat {
	case forkDumpFormatBin:
		opt := &meta.LoadOption{Threads: threads}
		if err := dstMeta.LoadMetaV2(meta.WrapContext(ctx.Context), r, opt); err != nil {
			return fmt.Errorf("load binary dump: %w", err)
		}
	case forkDumpFormatJSON:
		if err := dstMeta.LoadMeta(r); err != nil {
			return fmt.Errorf("load json dump: %w", err)
		}
	default:
		return fmt.Errorf("unsupported dump format %q in manifest", manifest.DumpFormat)
	}

	dstFormat, err := dstMeta.Load(false)
	if err != nil {
		return fmt.Errorf("read destination format after load: %w", err)
	}
	dstFormat.UUID = manifest.ForkUUID
	if err := dstMeta.Init(dstFormat, true /* force overwrite */); err != nil {
		return fmt.Errorf("patch destination format: %w", err)
	}

	offset := manifest.ForkCounterOffset
	if offset <= 0 {
		offset = forkCounterOffset
	}
	newNextChunk := manifest.ForkBaseChunk + manifest.ForkIndex*offset
	newNextInode := manifest.ForkBaseInode + manifest.ForkIndex*offset
	if err := dstMeta.SetCounter("nextChunk", newNextChunk); err != nil {
		return fmt.Errorf("patch nextChunk: %w", err)
	}
	if err := dstMeta.SetCounter("nextInode", newNextInode); err != nil {
		return fmt.Errorf("patch nextInode: %w", err)
	}
	if err := dstMeta.SetCounter("forkSharedStorage", 1); err != nil {
		logger.Warnf("mark fork as shared-storage: %v (non-fatal)", err)
	}
	persistForkProtection(dstMeta, manifest.ForkBaseChunk)

	if err := dstMeta.Shutdown(); err != nil {
		return fmt.Errorf("shutdown destination metadata: %w", err)
	}
	closed = true

	logger.Infof("Fork loaded successfully:")
	logger.Infof("  Source:      %s (%s)", manifest.SourceName, manifest.SourceUUID)
	logger.Infof("  Fork:        %s (%s)", manifest.ForkName, manifest.ForkUUID)
	logger.Infof("  BaseChunk:   %d  →  fork starts at %d", manifest.ForkBaseChunk, newNextChunk)
	logger.Infof("  BaseInode:   %d  →  fork starts at %d", manifest.ForkBaseInode, newNextInode)
	logger.Infof("Mount the fork with: juicefs mount %s <mountpoint>", utils.RemovePassword(dstURI))
	return nil
}

func normalizeForkThreads(threads int) int {
	if threads <= 0 {
		logger.Warnf("Invalid threads number %d, reset to 1", threads)
		return 1
	}
	return threads
}

func forkStringFlag(ctx *cli.Context, name string) string {
	for _, c := range ctx.Lineage() {
		if c != nil && c.IsSet(name) {
			return c.String(name)
		}
	}
	return ctx.String(name)
}

func forkIntFlag(ctx *cli.Context, name string) int {
	for _, c := range ctx.Lineage() {
		if c != nil && c.IsSet(name) {
			return c.Int(name)
		}
	}
	return ctx.Int(name)
}

func forkBoolFlag(ctx *cli.Context, name string) bool {
	for _, c := range ctx.Lineage() {
		if c != nil && c.IsSet(name) {
			return c.Bool(name)
		}
	}
	return ctx.Bool(name)
}

func persistSourceForkProtection(srcMeta meta.Meta, forkBaseChunk int64) {
	existing, existErr := srcMeta.GetCounter("forkProtectBelow")
	logger.Infof("forkProtectBelow in source DB: %d (err: %v)", existing, existErr)
	if existErr == nil && existing < forkBaseChunk {
		if setErr := srcMeta.SetCounter("forkProtectBelow", forkBaseChunk); setErr != nil {
			logger.Warnf("persist forkProtectBelow in source: %v (non-fatal, GC will fall back to bucket)", setErr)
		} else {
			logger.Infof("Persisted forkProtectBelow=%d in source metadata DB", forkBaseChunk)
		}
	}

	// If all previous forks were released, re-arm protection for this new fork.
	clearedVal, _ := srcMeta.GetCounter("forkProtectCleared")
	if clearedVal > 0 {
		rearmVal, _ := srcMeta.GetCounter("forkProtectRearm")
		if rearmVal <= clearedVal {
			if setErr := srcMeta.SetCounter("forkProtectRearm", clearedVal+1); setErr != nil {
				logger.Warnf("reset fork protection rearm counter: %v (non-fatal)", setErr)
			} else {
				logger.Infof("Re-armed fork protection in source DB (cleared=%d, rearm=%d)", clearedVal, clearedVal+1)
			}
		}
	}
}

func persistForkProtection(forkMeta meta.Meta, forkBaseChunk int64) {
	if setErr := forkMeta.SetCounter("forkProtectBelow", forkBaseChunk); setErr != nil {
		logger.Warnf("persist forkProtectBelow in fork: %v (non-fatal)", setErr)
	} else {
		logger.Infof("Persisted forkProtectBelow=%d in fork metadata DB", forkBaseChunk)
	}
}

func nextForkProtectCleared(cleared, rearm int64) int64 {
	target := cleared + 1
	if rearm >= target {
		target = rearm + 1
	}
	return target
}

func nextForkProtectRearm(cleared, rearm int64) (int64, bool) {
	if rearm > cleared {
		return 0, false
	}
	return cleared + 1, true
}

func ensureForkProtectionRearmed(srcMeta meta.Meta, reason string) {
	for i := 0; i < 3; i++ {
		clearedVal, err := srcMeta.GetCounter("forkProtectCleared")
		if err != nil {
			logger.Warnf("read forkProtectCleared %s: %v", reason, err)
			return
		}
		rearmVal, err := srcMeta.GetCounter("forkProtectRearm")
		if err != nil {
			logger.Warnf("read forkProtectRearm %s: %v", reason, err)
			return
		}
		target, need := nextForkProtectRearm(clearedVal, rearmVal)
		if !need {
			return
		}
		if setErr := srcMeta.SetCounter("forkProtectRearm", target); setErr != nil {
			// Concurrent updates can race this monotonic set; retry.
			if strings.Contains(setErr.Error(), "must be greater than current") {
				continue
			}
			logger.Warnf("set forkProtectRearm=%d %s: %v", target, reason, setErr)
			return
		}
		logger.Infof("Re-armed fork protection in source DB (cleared=%d, rearm=%d)", clearedVal, target)
		return
	}
	logger.Warnf("failed to re-arm fork protection %s after retries", reason)
}

func markForkProtectionCleared(srcMeta meta.Meta, reason string) {
	clearedVal, err := srcMeta.GetCounter("forkProtectCleared")
	if err != nil {
		logger.Warnf("read forkProtectCleared %s: %v", reason, err)
		return
	}
	rearmVal, err := srcMeta.GetCounter("forkProtectRearm")
	if err != nil {
		logger.Warnf("read forkProtectRearm %s: %v", reason, err)
		return
	}

	target := nextForkProtectCleared(clearedVal, rearmVal)
	if setErr := srcMeta.SetCounter("forkProtectCleared", target); setErr != nil {
		logger.Warnf("set forkProtectCleared=%d %s: %v", target, reason, setErr)
		return
	}
	logger.Infof("Marked fork protection cleared in source DB (cleared=%d, rearm=%d, new=%d)",
		clearedVal, rearmVal, target)
}

func markForkProtectionClearedWithLeaseRecheck(ctx context.Context, srcMeta meta.Meta, blob object.ObjectStorage, sourceName, reason string) {
	markForkProtectionCleared(srcMeta, reason)

	remaining, err := listForkLeases(ctx, blob, sourceName)
	if err != nil {
		// Be conservative on uncertainty: keep protection armed.
		logger.Warnf("re-list fork leases %s: %v; conservatively re-arming protection", reason, err)
		ensureForkProtectionRearmed(srcMeta, reason+" (lease recheck failed)")
		return
	}
	if len(remaining) > 0 {
		logger.Warnf("detected %d active lease(s) %s after clearing; re-arming protection", len(remaining), reason)
		ensureForkProtectionRearmed(srcMeta, reason+" (active lease detected)")
	}
}

func rollbackForkLeaseAfterDumpFailure(ctx context.Context, srcMeta meta.Meta, blob object.ObjectStorage, sourceName, forkUUID string) error {
	leaseKey := forkLeaseKey(sourceName, forkUUID)
	if err := blob.Delete(ctx, leaseKey); err != nil {
		return fmt.Errorf("delete lease object %s: %w", leaseKey, err)
	}
	logger.Infof("Rolled back lease %s after failed fork dump", leaseKey)

	remaining, err := listForkLeases(ctx, blob, sourceName)
	if err != nil {
		return fmt.Errorf("re-list fork leases after rollback: %w", err)
	}
	if len(remaining) == 0 {
		// No active leases remain. Mark protection as cleared.
		markForkProtectionClearedWithLeaseRecheck(ctx, srcMeta, blob, sourceName, "after rollback")
	}
	return nil
}

func forkManifestPath(dumpPath string) string {
	return dumpPath + ".fork.json"
}

func writeForkManifest(path string, manifest ForkManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func readForkManifest(path string) (ForkManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ForkManifest{}, err
	}
	var manifest ForkManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return ForkManifest{}, err
	}
	if manifest.Version != forkManifestVersion {
		return ForkManifest{}, fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}
	if manifest.DumpFormat != forkDumpFormatJSON && manifest.DumpFormat != forkDumpFormatBin {
		return ForkManifest{}, fmt.Errorf("unsupported dump format %q", manifest.DumpFormat)
	}
	if manifest.ForkUUID == "" || manifest.ForkName == "" || manifest.SourceName == "" || manifest.SourceUUID == "" {
		return ForkManifest{}, fmt.Errorf("manifest missing required fork/source identifiers")
	}
	if manifest.ForkIndex <= 0 {
		return ForkManifest{}, fmt.Errorf("invalid forkIndex %d in manifest", manifest.ForkIndex)
	}
	return manifest, nil
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

	forkName := forkStringFlag(ctx, "fork-name")
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
		markForkProtectionClearedWithLeaseRecheck(ctx.Context, srcMeta, blob, srcFormat.Name, "after release")
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
