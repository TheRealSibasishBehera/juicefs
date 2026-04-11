/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
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

// End-to-end tests for flush-before-fork.
//
// These tests mount a real FUSE volume, write files via the OS, call
// juicefs fork via Main(), then mount the fork and verify content.
//
// Flow for each test:
//   1. mountTemp  → source mount at /tmp/jfs-unit-test
//   2. Write files via os.OpenFile / os.Write (no fsync)
//   3. Main(fork create ...) — dumps metadata while source is mounted
//   4. umountTemp — tear down source (so we can mount the fork)
//   5. mount fork at /tmp/jfs-fork-test
//   6. Read files from fork, assert content
//   7. umount fork
//
// EXPECTED TO FAIL today: fork does not flush the source mount's write
// buffer before dumping metadata.
//
// Requirements:
//   - Redis at 127.0.0.1:6379
//   - FUSE available
//   - Run: sudo go test ./cmd/ -run TestForkE2E -v -timeout 120s

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const forkDstMeta = "redis://127.0.0.1:6379/12"
const forkMountPoint = "/tmp/jfs-fork-test"

func resetForkMeta() {
	opt, _ := redis.ParseURL(forkDstMeta)
	rdb := redis.NewClient(opt)
	_ = rdb.FlushDB(context.Background())
	rdb.Close()
}

// mountForkVolume mounts the fork volume. The source must be unmounted first
// because the test harness shares the HTTP metrics mux.
func mountForkVolume(t *testing.T) {
	t.Helper()
	os.MkdirAll(forkMountPoint, 0777)
	ResetHttp()
	os.Setenv("JFS_SUPERVISOR", "test")
	go func() {
		_ = Main([]string{"", "mount", "--enable-xattr",
			forkDstMeta, forkMountPoint,
			"--attr-cache", "0", "--entry-cache", "0", "--dir-entry-cache", "0",
			"--no-usage-report"})
	}()
	time.Sleep(3 * time.Second)

	inode, err := utils.GetFileInode(forkMountPoint)
	require.NoError(t, err, "get fork mount inode")
	require.Equal(t, uint64(1), inode, "fork mount must have root inode 1")
}

func umountForkVolume(t *testing.T) {
	t.Helper()
	_ = Main([]string{"", "umount", forkMountPoint})
}

// ---------------------------------------------------------------------------
// TestForkE2E_FsyncedFilePresent
//
// Baseline: if the app calls fsync, the fork always has the data.
//
// STATUS: PASS today.
// ---------------------------------------------------------------------------

func TestForkE2E_FsyncedFilePresent(t *testing.T) {
	var bucket string
	mountTemp(t, &bucket, nil, nil)

	fpath := filepath.Join(testMountPoint, "synced.dat")
	payload := make([]byte, 512<<10)
	for i := range payload {
		payload[i] = 'S'
	}
	require.NoError(t, os.WriteFile(fpath, payload, 0644))

	resetForkMeta()
	err := Main([]string{"", "fork", "create", testMeta, forkDstMeta, "--name", "test-fork"})
	require.NoError(t, err, "fork create")

	umountTemp(t)

	mountForkVolume(t)
	defer umountForkVolume(t)

	content, err := os.ReadFile(filepath.Join(forkMountPoint, "synced.dat"))
	require.NoError(t, err)
	require.Equal(t, payload, content, "fork of fsynced file must be complete")
}

// ---------------------------------------------------------------------------
// TestForkE2E_SourceIntactAfterFork
//
// Verify forking does not corrupt the source. Write more data after fork.
//
// STATUS: PASS today.
// ---------------------------------------------------------------------------

func TestForkE2E_SourceIntactAfterFork(t *testing.T) {
	var bucket string
	mountTemp(t, &bucket, nil, nil)
	defer umountTemp(t)

	fpath := filepath.Join(testMountPoint, "source.dat")
	payload := make([]byte, 256<<10)
	for i := range payload {
		payload[i] = 'A'
	}
	require.NoError(t, os.WriteFile(fpath, payload, 0644))

	resetForkMeta()
	err := Main([]string{"", "fork", "create", testMeta, forkDstMeta, "--name", "test-fork"})
	require.NoError(t, err)

	// Write more to source after fork.
	extra := make([]byte, 256<<10)
	for i := range extra {
		extra[i] = 'B'
	}
	f, err := os.OpenFile(fpath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(extra)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// Verify source has both writes.
	srcContent, err := os.ReadFile(fpath)
	require.NoError(t, err)
	require.Equal(t, append(payload, extra...), srcContent,
		"source must be intact after fork")
}

// ---------------------------------------------------------------------------
// Tests that exercise the FIX: fork with --mountpoint
//
// These pass --mountpoint to fork create / fork dump, which triggers
// flushViaMountpoint() before the metadata dump. The unflushed writes get
// committed to the metadata DB and appear in the fork.
//
// STATUS: MUST PASS today (the fix is implemented).
// ---------------------------------------------------------------------------

func TestForkE2E_WithMountpoint_Create(t *testing.T) {
	var bucket string
	mountTemp(t, &bucket, nil, nil)

	fpath := filepath.Join(testMountPoint, "unflushed.dat")
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = 'M'
	}

	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(payload)
	require.NoError(t, err)
	// No fsync. Data is in the write buffer.

	// Fork WITH --mountpoint — triggers flush before dump.
	resetForkMeta()
	err = Main([]string{"", "fork", "create", testMeta, forkDstMeta,
		"--name", "test-fork", "--mountpoint", testMountPoint})
	require.NoError(t, err, "fork create with --mountpoint must succeed")

	f.Close()
	umountTemp(t)

	mountForkVolume(t)
	defer umountForkVolume(t)

	content, err := os.ReadFile(filepath.Join(forkMountPoint, "unflushed.dat"))
	require.NoError(t, err, "read file from fork")
	require.Equal(t, payload, content,
		"fork with --mountpoint must contain unflushed data")
}

func TestForkE2E_WithMountpoint_DeferredDump(t *testing.T) {
	var bucket string
	mountTemp(t, &bucket, nil, nil)

	fpath := filepath.Join(testMountPoint, "deferred.dat")
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = 'N'
	}

	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write(payload)
	require.NoError(t, err)
	// No fsync.

	// fork dump WITH --mountpoint.
	dumpPath := filepath.Join(t.TempDir(), "meta.dump")
	err = Main([]string{"", "fork", "dump", testMeta, "--path", dumpPath,
		"--name", "test-fork", "--mountpoint", testMountPoint})
	require.NoError(t, err, "fork dump with --mountpoint")

	f.Close()
	umountTemp(t)

	// fork load (no --mountpoint needed).
	resetForkMeta()
	err = Main([]string{"", "fork", "load", forkDstMeta, "--path", dumpPath})
	require.NoError(t, err, "fork load")

	mountForkVolume(t)
	defer umountForkVolume(t)

	content, err := os.ReadFile(filepath.Join(forkMountPoint, "deferred.dat"))
	require.NoError(t, err)
	require.Equal(t, payload, content,
		"deferred fork with --mountpoint must contain unflushed data")
}

func TestForkE2E_WithMountpoint_MultipleFiles(t *testing.T) {
	var bucket string
	mountTemp(t, &bucket, nil, nil)

	const numFiles = 5
	const fileSize = 128 << 10

	handles := make([]*os.File, numFiles)
	payloads := make([][]byte, numFiles)

	for i := range numFiles {
		name := fmt.Sprintf("file_%d.dat", i)
		fpath := filepath.Join(testMountPoint, name)
		payloads[i] = make([]byte, fileSize)
		for j := range payloads[i] {
			payloads[i][j] = byte('A' + i)
		}
		f, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f.Write(payloads[i])
		require.NoError(t, err)
		handles[i] = f // no fsync, no close
	}

	// Fork WITH --mountpoint.
	resetForkMeta()
	err := Main([]string{"", "fork", "create", testMeta, forkDstMeta,
		"--name", "test-fork", "--mountpoint", testMountPoint})
	require.NoError(t, err)

	for _, f := range handles {
		f.Close()
	}
	umountTemp(t)

	mountForkVolume(t)
	defer umountForkVolume(t)

	for i := range numFiles {
		name := fmt.Sprintf("file_%d.dat", i)
		content, err := os.ReadFile(filepath.Join(forkMountPoint, name))
		require.NoError(t, err, "read %s from fork", name)
		require.Equal(t, payloads[i], content,
			"fork with --mountpoint must contain data for %s", name)
	}
}
