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

	"github.com/juicedata/juicefs/pkg/meta"
)

// TestRewrapCredentials verifies that source credentials, sealed under the
// source UUID, end up decryptable under the fork UUID after rewrap — and
// that the recovered plaintext matches the original.
//
// Regression: an earlier implementation copied srcFormat.SecretKey (still
// ciphertext — Load() does not decrypt) into dstFormat and called Encrypt().
// That produced a double-sealed blob; the fork mount could not unwrap it, so
// every chunk GET failed with SignatureDoesNotMatch.
func TestRewrapCredentials(t *testing.T) {
	const plainSecret = "F/GHRL3VF1woj/gUJjgMmZUvZoYDmAARjQs82+C1"
	const plainEncrypt = "encrypt-key-plaintext"
	const plainSession = "session-token-plaintext"
	const srcUUID = "11111111-1111-1111-1111-111111111111"
	const forkUUID = "22222222-2222-2222-2222-222222222222"

	src := &meta.Format{
		Name:         "src",
		UUID:         srcUUID,
		SecretKey:    plainSecret,
		EncryptKey:   plainEncrypt,
		SessionToken: plainSession,
	}
	if err := src.Encrypt(); err != nil {
		t.Fatalf("seal source: %v", err)
	}
	if !src.KeyEncrypted {
		t.Fatal("source not marked KeyEncrypted after Encrypt")
	}
	srcSealedLen := len(src.SecretKey)

	// Mimic what forkCreate sees: dstFormat comes from Load() after LoadMeta,
	// whose DumpMeta set SecretKey="removed".
	dst := &meta.Format{
		Name:         "src", // fork keeps the source Name (shared prefix)
		UUID:         "placeholder-to-be-overwritten",
		SecretKey:    "removed",
		EncryptKey:   "removed",
		SessionToken: "removed",
		KeyEncrypted: true,
	}

	if err := rewrapCredentials(src, dst, forkUUID); err != nil {
		t.Fatalf("rewrapCredentials: %v", err)
	}

	if dst.UUID != forkUUID {
		t.Fatalf("dst.UUID: want %q, got %q", forkUUID, dst.UUID)
	}
	if !dst.KeyEncrypted {
		t.Fatal("dst not marked KeyEncrypted after rewrap")
	}
	if dst.SecretKey == src.SecretKey {
		t.Fatal("dst.SecretKey identical to src.SecretKey — not resealed")
	}
	// A correctly-sealed dst has the same ciphertext length as src (same
	// plaintext, same GCM overhead).  A double-sealed dst is ~68 bytes
	// larger (base64 of 12-byte nonce + ciphertext + 16-byte tag).
	if len(dst.SecretKey) > srcSealedLen+10 {
		t.Fatalf("dst.SecretKey length %d > src length %d + 10: looks double-encrypted",
			len(dst.SecretKey), srcSealedLen)
	}

	if err := dst.Decrypt(); err != nil {
		t.Fatalf("decrypt dst under fork UUID: %v", err)
	}
	if dst.SecretKey != plainSecret {
		t.Fatalf("decrypted SecretKey: want %q, got %q", plainSecret, dst.SecretKey)
	}
	if dst.EncryptKey != plainEncrypt {
		t.Fatalf("decrypted EncryptKey: want %q, got %q", plainEncrypt, dst.EncryptKey)
	}
	if dst.SessionToken != plainSession {
		t.Fatalf("decrypted SessionToken: want %q, got %q", plainSession, dst.SessionToken)
	}
}

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

func TestNextForkProtectCleared(t *testing.T) {
	tests := []struct {
		name    string
		cleared int64
		rearm   int64
		want    int64
	}{
		{name: "both zero", cleared: 0, rearm: 0, want: 1},
		{name: "rearm ahead", cleared: 1, rearm: 2, want: 3},
		{name: "equal counters", cleared: 5, rearm: 5, want: 6},
		{name: "cleared already ahead", cleared: 7, rearm: 3, want: 8},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := nextForkProtectCleared(tc.cleared, tc.rearm); got != tc.want {
				t.Fatalf("nextForkProtectCleared(%d,%d): want %d, got %d", tc.cleared, tc.rearm, tc.want, got)
			}
		})
	}
}

func TestNextForkProtectRearm(t *testing.T) {
	tests := []struct {
		name       string
		cleared    int64
		rearm      int64
		wantTarget int64
		wantNeed   bool
	}{
		{name: "already rearmed", cleared: 5, rearm: 6, wantNeed: false},
		{name: "equal counters", cleared: 5, rearm: 5, wantTarget: 6, wantNeed: true},
		{name: "rearm behind", cleared: 8, rearm: 2, wantTarget: 9, wantNeed: true},
		{name: "both zero", cleared: 0, rearm: 0, wantTarget: 1, wantNeed: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotTarget, gotNeed := nextForkProtectRearm(tc.cleared, tc.rearm)
			if gotNeed != tc.wantNeed {
				t.Fatalf("nextForkProtectRearm(%d,%d): want need=%v, got %v", tc.cleared, tc.rearm, tc.wantNeed, gotNeed)
			}
			if gotNeed && gotTarget != tc.wantTarget {
				t.Fatalf("nextForkProtectRearm(%d,%d): want target=%d, got %d", tc.cleared, tc.rearm, tc.wantTarget, gotTarget)
			}
		})
	}
}
