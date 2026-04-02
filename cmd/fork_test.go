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

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestForkManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.dump.fork.json")
	want := ForkManifest{
		Version:           forkManifestVersion,
		DumpPath:          "/tmp/meta.dump",
		DumpFormat:        forkDumpFormatJSON,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339),
		SourceName:        "src-vol",
		SourceUUID:        "11111111-1111-1111-1111-111111111111",
		ForkUUID:          "22222222-2222-2222-2222-222222222222",
		ForkName:          "src-vol-fork",
		ForkBaseChunk:     1024,
		ForkBaseInode:     2048,
		ForkIndex:         2,
		ForkCounterOffset: forkCounterOffset,
	}

	if err := writeForkManifest(path, want); err != nil {
		t.Fatalf("writeForkManifest: %v", err)
	}

	got, err := readForkManifest(path)
	if err != nil {
		t.Fatalf("readForkManifest: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("manifest mismatch: want %+v, got %+v", want, got)
	}
}

func TestReadForkManifestValidation(t *testing.T) {
	dir := t.TempDir()

	t.Run("invalid version", func(t *testing.T) {
		path := filepath.Join(dir, "bad-version.json")
		manifest := ForkManifest{
			Version:       2,
			DumpFormat:    forkDumpFormatJSON,
			SourceName:    "src",
			SourceUUID:    "src-uuid",
			ForkUUID:      "fork-uuid",
			ForkName:      "fork-name",
			ForkBaseChunk: 1,
			ForkBaseInode: 1,
			ForkIndex:     1,
		}
		if err := writeForkManifest(path, manifest); err != nil {
			t.Fatalf("writeForkManifest: %v", err)
		}
		_, err := readForkManifest(path)
		if err == nil || !strings.Contains(err.Error(), "unsupported manifest version") {
			t.Fatalf("expected unsupported version error, got %v", err)
		}
	})

	t.Run("invalid dump format", func(t *testing.T) {
		path := filepath.Join(dir, "bad-format.json")
		manifest := ForkManifest{
			Version:       forkManifestVersion,
			DumpFormat:    "yaml",
			SourceName:    "src",
			SourceUUID:    "src-uuid",
			ForkUUID:      "fork-uuid",
			ForkName:      "fork-name",
			ForkBaseChunk: 1,
			ForkBaseInode: 1,
			ForkIndex:     1,
		}
		if err := writeForkManifest(path, manifest); err != nil {
			t.Fatalf("writeForkManifest: %v", err)
		}
		_, err := readForkManifest(path)
		if err == nil || !strings.Contains(err.Error(), "unsupported dump format") {
			t.Fatalf("expected unsupported dump format error, got %v", err)
		}
	})

	t.Run("invalid fork index", func(t *testing.T) {
		path := filepath.Join(dir, "bad-index.json")
		manifest := ForkManifest{
			Version:       forkManifestVersion,
			DumpFormat:    forkDumpFormatJSON,
			SourceName:    "src",
			SourceUUID:    "src-uuid",
			ForkUUID:      "fork-uuid",
			ForkName:      "fork-name",
			ForkBaseChunk: 1,
			ForkBaseInode: 1,
			ForkIndex:     0,
		}
		if err := writeForkManifest(path, manifest); err != nil {
			t.Fatalf("writeForkManifest: %v", err)
		}
		_, err := readForkManifest(path)
		if err == nil || !strings.Contains(err.Error(), "invalid forkIndex") {
			t.Fatalf("expected invalid forkIndex error, got %v", err)
		}
	})
}

func TestForkManifestPathAndThreads(t *testing.T) {
	if got, want := forkManifestPath("/tmp/meta.dump"), "/tmp/meta.dump.fork.json"; got != want {
		t.Fatalf("forkManifestPath: want %q, got %q", want, got)
	}
	if got := normalizeForkThreads(0); got != 1 {
		t.Fatalf("normalizeForkThreads(0): expected 1, got %d", got)
	}
	if got := normalizeForkThreads(8); got != 8 {
		t.Fatalf("normalizeForkThreads(8): expected 8, got %d", got)
	}
}
