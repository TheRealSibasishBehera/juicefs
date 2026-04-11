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

package vfs

// Tests for flush-before-fork correctness.
//
// The core problem: juicefs fork dumps metadata from the source volume's DB
// into a new volume. But the VFS write buffer (dataWriter) holds slice data
// not yet committed to the metadata DB via commitThread → m.Write(). Fork
// reads the DB directly, so unflushed writes are silently absent.
//
// Test categories:
//
//   FAIL-today tests assert the CORRECT behaviour (fork should have the data).
//   They FAIL because the bug exists. Once flush-before-fork is implemented,
//   they must PASS.
//
//   PASS-today tests exercise the fix path (explicit FlushAll before dump) or
//   baseline scenarios. They must PASS before and after the fix.
//
// Run:
//   go test ./pkg/vfs/ -run TestFork -v -count=1

import (
	"bytes"
	"log"
	"os"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

func createTestVFSForFork(t *testing.T) (*VFS, meta.Meta, object.ObjectStorage) {
	t.Helper()
	mp := "/jfs"
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mp
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{
		Name:        "test-fork-src",
		UUID:        uuid.New().String(),
		Storage:     "mem",
		BlockSize:   4096,
		Compression: "lz4",
		DirStats:    true,
	}
	if err := m.Init(format, true); err != nil {
		log.Fatalf("init meta: %s", err)
	}
	conf := &Config{
		Meta:   metaConf,
		Format: *format,
		Chunk: &chunk.Config{
			BlockSize:   format.BlockSize * 1024,
			Compress:    format.Compression,
			MaxUpload:   2,
			MaxDownload: 200,
			BufferSize:  30 << 20,
			CacheSize:   10 << 20,
			CacheDir:    "memory",
		},
		FuseOpts: &FuseOptions{},
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": format.Name}, registry))
	store := chunk.NewCachedStore(blob, *conf.Chunk, registry)
	v := NewVFS(conf, m, store, registerer, registry)
	return v, m, blob
}

// forkViaDump performs the metadata dump→load cycle that juicefs fork does.
// Returns the fork VFS so callers can read files from it.
func forkViaDump(t *testing.T, srcMeta meta.Meta, blob object.ObjectStorage) *VFS {
	t.Helper()

	// Dump source metadata.
	var dumpBuf bytes.Buffer
	err := srcMeta.DumpMeta(&dumpBuf, 1, 1, false, false, false)
	require.NoError(t, err, "DumpMeta")

	// Create a fresh destination meta and load the dump.
	mp := "/jfs-fork"
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mp
	dstMeta := meta.NewClient("memkv://", metaConf)
	require.NoError(t, dstMeta.Reset(), "reset fork meta")
	require.NoError(t, dstMeta.LoadMeta(bytes.NewReader(dumpBuf.Bytes())), "LoadMeta")

	dstFormat, err := dstMeta.Load(false)
	require.NoError(t, err, "load fork format")

	conf := &Config{
		Meta:   metaConf,
		Format: *dstFormat,
		Chunk: &chunk.Config{
			BlockSize:   dstFormat.BlockSize * 1024,
			Compress:    dstFormat.Compression,
			MaxUpload:   2,
			MaxDownload: 200,
			BufferSize:  30 << 20,
			CacheSize:   10 << 20,
			CacheDir:    "memory",
		},
		FuseOpts: &FuseOptions{},
	}
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": dstFormat.Name}, registry))
	// Share the SAME object storage — fork reads pre-fork chunks from source.
	store := chunk.NewCachedStore(blob, *conf.Chunk, registry)
	return NewVFS(conf, dstMeta, store, registerer, registry)
}

// readForkFile opens a file by inode from a VFS and returns its content.
// Returns (content, exists). Never silently swallows errors.
func readForkFile(t *testing.T, v *VFS, ino Ino) (content []byte, exists bool) {
	t.Helper()
	ctx := NewLogContext(meta.Background())

	attr, e := v.GetAttr(ctx, ino, 0)
	if e == syscall.ENOENT {
		return nil, false
	}
	require.Zero(t, e, "GetAttr inode %d", ino)

	size := int(attr.Attr.Length)
	if size == 0 {
		return []byte{}, true // file exists but is zero-length
	}

	_, fh, e := v.Open(ctx, ino, syscall.O_RDONLY)
	require.Zero(t, e, "Open inode %d", ino)
	defer v.Release(ctx, ino, fh)

	buf := make([]byte, size)
	n, e := v.Read(ctx, ino, buf, 0, fh)
	require.Zero(t, e, "Read inode %d", ino)
	return buf[:n], true
}

// hasCommittedSlices checks if the metadata DB has any chunk slice refs for
// the given inode+chunkIndex.
func hasCommittedSlices(t *testing.T, m meta.Meta, ino Ino, chunkIdx uint32) bool {
	t.Helper()
	var slices []meta.Slice
	e := m.Read(meta.Background(), ino, chunkIdx, &slices)
	require.Zero(t, e, "meta.Read for slices")
	return len(slices) > 0
}

// writeFile creates a file, writes payload, and returns the inode and handle.
// Deliberately does NOT flush, fsync, or close.
func writeFile(t *testing.T, v *VFS, parentIno Ino, name string, payload []byte) (ino Ino, fh uint64) {
	t.Helper()
	ctx := NewLogContext(meta.Background())
	fe, fh, e := v.Create(ctx, parentIno, name, 0644, 0, uint32(os.O_WRONLY))
	require.Zero(t, e, "create %s", name)
	e = v.Write(ctx, fe.Inode, payload, 0, fh)
	require.Zero(t, e, "write %s", name)
	return fe.Inode, fh
}

// ---------------------------------------------------------------------------
// FlushAll unit tests
// ---------------------------------------------------------------------------

// TestFlushAll_CommitsSlicesToMetadata verifies the full flush chain:
// upload → commitThread → m.Write → slice refs appear in metadata DB.
//
// STATUS: PASS today.
func TestFlushAll_CommitsSlicesToMetadata(t *testing.T) {
	v, m, _ := createTestVFSForFork(t)

	payload := bytes.Repeat([]byte("F"), 256<<10)
	ino, _ := writeFile(t, v, 1, "pending.txt", payload)

	assert.False(t, hasCommittedSlices(t, m, ino, 0),
		"pre-condition: no slices before flush")

	require.NoError(t, v.writer.FlushAll())

	assert.True(t, hasCommittedSlices(t, m, ino, 0),
		"after FlushAll, slices must be committed to metadata DB")
}

// TestFlushAll_NoWriters verifies FlushAll on an idle mount returns OK.
//
// STATUS: PASS today.
func TestFlushAll_NoWriters(t *testing.T) {
	v, _, _ := createTestVFSForFork(t)
	require.NoError(t, v.writer.FlushAll())
}

// TestFlushAll_MultipleFiles verifies all open writers are drained, not just one.
//
// STATUS: PASS today.
func TestFlushAll_MultipleFiles(t *testing.T) {
	v, m, _ := createTestVFSForFork(t)

	const n = 5
	inodes := make([]Ino, n)
	for i := range n {
		name := "file" + string(rune('0'+i))
		inodes[i], _ = writeFile(t, v, 1, name, bytes.Repeat([]byte{byte('A' + i)}, 128<<10))
	}

	require.NoError(t, v.writer.FlushAll())

	for i, ino := range inodes {
		assert.True(t, hasCommittedSlices(t, m, ino, 0),
			"file %d: slices must be committed after FlushAll", i)
	}
}

// ---------------------------------------------------------------------------
// Fork correctness: fix path + baselines
// ---------------------------------------------------------------------------

// TestFork_FlushThenDump_Create is the acceptance test: FlushAll before
// DumpMeta produces a complete fork.
//
// STATUS: PASS today.
func TestFork_FlushThenDump_Create(t *testing.T) {
	v, m, blob := createTestVFSForFork(t)

	payload := bytes.Repeat([]byte("Y"), 1<<20)
	ino, _ := writeFile(t, v, 1, "important.dat", payload)

	// THE FIX: flush before dump.
	require.NoError(t, v.writer.FlushAll())
	require.True(t, hasCommittedSlices(t, m, ino, 0),
		"after FlushAll, slices must be in metadata")

	forkVFS := forkViaDump(t, m, blob)

	content, exists := readForkFile(t, forkVFS, ino)
	require.True(t, exists)
	require.Equal(t, payload, content,
		"after FlushAll, fork must contain complete data")
}

// TestFork_FlushThenDump_Deferred is the acceptance test for the deferred path.
//
// STATUS: PASS today.
func TestFork_FlushThenDump_Deferred(t *testing.T) {
	v, m, blob := createTestVFSForFork(t)

	payload := bytes.Repeat([]byte("E"), 512<<10)
	ino, _ := writeFile(t, v, 1, "data.bin", payload)

	require.NoError(t, v.writer.FlushAll())

	forkVFS := forkViaDump(t, m, blob)

	content, exists := readForkFile(t, forkVFS, ino)
	require.True(t, exists)
	require.Equal(t, payload, content,
		"after FlushAll, deferred fork must contain complete data")
}

// TestFork_SourceIntactAfterFlush verifies flush for fork does not corrupt
// the source. Post-flush writes to the source must not leak into the fork.
//
// STATUS: PASS today.
func TestFork_SourceIntactAfterFlush(t *testing.T) {
	v, m, blob := createTestVFSForFork(t)
	ctx := NewLogContext(meta.Background())

	firstWrite := bytes.Repeat([]byte("A"), 256<<10)
	secondWrite := bytes.Repeat([]byte("B"), 256<<10)

	fe, fh, e := v.Create(ctx, 1, "source.dat", 0644, 0, uint32(os.O_RDWR))
	require.Zero(t, e)
	e = v.Write(ctx, fe.Inode, firstWrite, 0, fh)
	require.Zero(t, e)

	// Flush + dump (the fork snapshot point).
	require.NoError(t, v.writer.FlushAll())
	forkVFS := forkViaDump(t, m, blob)

	// Second write AFTER the dump.
	e = v.Write(ctx, fe.Inode, secondWrite, uint64(len(firstWrite)), fh)
	require.Zero(t, e)
	e = v.Fsync(ctx, fe.Inode, 1, fh)
	require.Zero(t, e)
	v.Release(ctx, fe.Inode, fh)

	// Source has both writes.
	srcContent, srcExists := readForkFile(t, v, fe.Inode)
	require.True(t, srcExists)
	require.Equal(t, append(firstWrite, secondWrite...), srcContent)

	// Fork has only the first write.
	forkContent, forkExists := readForkFile(t, forkVFS, fe.Inode)
	require.True(t, forkExists)
	require.Equal(t, firstWrite, forkContent,
		"fork must not contain writes that happened after the dump")
}

// TestFork_AlreadyFsynced verifies fork works when the app already fsynced.
// This is the baseline — must work with or without the flush feature.
//
// STATUS: PASS today.
func TestFork_AlreadyFsynced(t *testing.T) {
	v, m, blob := createTestVFSForFork(t)
	ctx := NewLogContext(meta.Background())

	payload := bytes.Repeat([]byte("S"), 256<<10)
	ino, fh := writeFile(t, v, 1, "synced.dat", payload)

	e := v.Fsync(ctx, ino, 1, fh)
	require.Zero(t, e)
	v.Release(ctx, ino, fh)

	// Fork WITHOUT FlushAll — should work because app already fsynced.
	forkVFS := forkViaDump(t, m, blob)

	content, exists := readForkFile(t, forkVFS, ino)
	require.True(t, exists)
	require.Equal(t, payload, content)
}

// TestFork_EmptyFile verifies fork handles empty files.
//
// STATUS: PASS today.
func TestFork_EmptyFile(t *testing.T) {
	v, m, blob := createTestVFSForFork(t)
	ctx := NewLogContext(meta.Background())

	fe, fh, e := v.Create(ctx, 1, "empty.dat", 0644, 0, uint32(os.O_WRONLY))
	require.Zero(t, e)
	v.Release(ctx, fe.Inode, fh)

	forkVFS := forkViaDump(t, m, blob)

	content, exists := readForkFile(t, forkVFS, fe.Inode)
	require.True(t, exists, "empty file inode must exist in fork")
	require.Empty(t, content, "empty file must have zero-length content in fork")
}

// TestFork_FlushAllMultipleFiles verifies that FlushAll+fork captures all
// files when multiple files are open and unflushed.
//
// STATUS: PASS today.
func TestFork_FlushAllMultipleFiles(t *testing.T) {
	v, m, blob := createTestVFSForFork(t)

	const numFiles = 5
	const fileSize = 128 << 10

	payloads := make([][]byte, numFiles)
	inodes := make([]Ino, numFiles)
	for i := range numFiles {
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, fileSize)
		name := "file" + string(rune('0'+i))
		inodes[i], _ = writeFile(t, v, 1, name, payloads[i])
	}

	require.NoError(t, v.writer.FlushAll())

	forkVFS := forkViaDump(t, m, blob)

	for i, ino := range inodes {
		content, exists := readForkFile(t, forkVFS, ino)
		require.True(t, exists, "file %d must exist in fork", i)
		require.Equal(t, payloads[i], content,
			"file %d must have complete data in fork", i)
	}
}

// TestFork_TruncatedFileAfterWrite verifies that if a file is written then
// truncated (without flush), the fork sees the truncated state.
//
// STATUS: FAIL today (same root cause — truncate calls flush internally but
// the truncated-to-zero file may still have buffered state).
func TestFork_TruncatedFileAfterWrite(t *testing.T) {
	v, m, blob := createTestVFSForFork(t)
	ctx := NewLogContext(meta.Background())

	payload := bytes.Repeat([]byte("T"), 256<<10)
	ino, fh := writeFile(t, v, 1, "trunc.dat", payload)

	// Truncate to half — this internally flushes the write buffer for this file.
	var attr meta.Attr
	e := v.Truncate(ctx, ino, int64(128<<10), fh, &attr)
	require.Zero(t, e)

	// Fork without explicit FlushAll.
	forkVFS := forkViaDump(t, m, blob)

	content, exists := readForkFile(t, forkVFS, ino)
	require.True(t, exists)
	// After truncate, the file should be 128KiB. The first 128KiB of payload.
	require.Equal(t, payload[:128<<10], content,
		"fork of truncated file must reflect the truncated size and content")
}
