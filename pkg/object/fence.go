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

package object

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"syscall"
	"time"
)

// FenceState is the JSON document stored at the sentinel object in the bucket.
type FenceState struct {
	Generation     int64    `json:"generation"`
	FencedSessions []string `json:"fenced_sessions"`
}

// FencedStorage wraps an ObjectStorage and blocks write operations when the
// mount's session is fenced. Reads always pass through. The fence state is
// checked by polling a sentinel object in the bucket at a configurable interval.
//
// When fenced, Put/Delete/Copy/CreateMultipartUpload/UploadPart/CompleteUpload
// return syscall.EROFS. When the fence is lifted (session removed from the
// sentinel or generation reset), writes resume automatically.
type FencedStorage struct {
	ObjectStorage
	fenced    atomic.Bool
	sessionID string
	fenceKey  string
	interval  time.Duration
	stop      chan struct{}
}

// NewFencedStorage wraps the given storage with fence checking.
// sessionID identifies this mount (used to match against fenced_sessions).
// fenceKey is the object key for the sentinel (e.g., "_fence").
// interval is the polling interval (e.g., 5 * time.Second).
func NewFencedStorage(inner ObjectStorage, sessionID, fenceKey string, interval time.Duration) *FencedStorage {
	fs := &FencedStorage{
		ObjectStorage: inner,
		sessionID:     sessionID,
		fenceKey:      fenceKey,
		interval:      interval,
		stop:          make(chan struct{}),
	}
	go fs.pollLoop()
	return fs
}

// IsFenced returns true if writes are currently blocked.
func (fs *FencedStorage) IsFenced() bool {
	return fs.fenced.Load()
}

// Stop stops the polling goroutine.
func (fs *FencedStorage) Stop() {
	close(fs.stop)
}

func (fs *FencedStorage) pollLoop() {
	ticker := time.NewTicker(fs.interval)
	defer ticker.Stop()

	// Check immediately on start.
	fs.checkFence()

	for {
		select {
		case <-ticker.C:
			fs.checkFence()
		case <-fs.stop:
			return
		}
	}
}

func (fs *FencedStorage) checkFence() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r, err := fs.ObjectStorage.Get(ctx, fs.fenceKey, 0, -1)
	if err != nil {
		// Sentinel doesn't exist or unreadable — not fenced.
		// If we can't reach storage, writes will fail at Put anyway.
		logger.Debugf("fence: cannot read sentinel %s: %s", fs.fenceKey, err)
		fs.fenced.Store(false)
		return
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		logger.Warnf("fence: cannot read sentinel body %s: %s", fs.fenceKey, err)
		fs.fenced.Store(false)
		return
	}

	var state FenceState
	if err := json.Unmarshal(data, &state); err != nil {
		logger.Warnf("fence: invalid sentinel JSON: %s", err)
		fs.fenced.Store(false)
		return
	}

	fenced := false
	for _, s := range state.FencedSessions {
		if s == fs.sessionID {
			fenced = true
			break
		}
	}

	wasFenced := fs.fenced.Load()
	fs.fenced.Store(fenced)

	if fenced && !wasFenced {
		logger.Warnf("fence: this session (%s) has been fenced at generation %d — writes blocked",
			fs.sessionID, state.Generation)
	} else if !fenced && wasFenced {
		logger.Infof("fence: this session (%s) is no longer fenced — writes resumed", fs.sessionID)
	}
}

func (fs *FencedStorage) writeErr() error {
	return fmt.Errorf("fenced: write operations blocked for session %s: %w",
		fs.sessionID, syscall.EROFS)
}

// --- Write methods: blocked when fenced ---

func (fs *FencedStorage) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	if fs.fenced.Load() {
		return fs.writeErr()
	}
	return fs.ObjectStorage.Put(ctx, key, in, getters...)
}

func (fs *FencedStorage) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	if fs.fenced.Load() {
		return fs.writeErr()
	}
	return fs.ObjectStorage.Delete(ctx, key, getters...)
}

func (fs *FencedStorage) Copy(ctx context.Context, dst, src string) error {
	if fs.fenced.Load() {
		return fs.writeErr()
	}
	return fs.ObjectStorage.Copy(ctx, dst, src)
}

func (fs *FencedStorage) Create(ctx context.Context) error {
	if fs.fenced.Load() {
		return fs.writeErr()
	}
	return fs.ObjectStorage.Create(ctx)
}

func (fs *FencedStorage) CreateMultipartUpload(ctx context.Context, key string) (*MultipartUpload, error) {
	if fs.fenced.Load() {
		return nil, fs.writeErr()
	}
	return fs.ObjectStorage.CreateMultipartUpload(ctx, key)
}

func (fs *FencedStorage) UploadPart(ctx context.Context, key string, uploadID string, num int, body []byte) (*Part, error) {
	if fs.fenced.Load() {
		return nil, fs.writeErr()
	}
	return fs.ObjectStorage.UploadPart(ctx, key, uploadID, num, body)
}

func (fs *FencedStorage) UploadPartCopy(ctx context.Context, key string, uploadID string, num int, srcKey string, off, size int64) (*Part, error) {
	if fs.fenced.Load() {
		return nil, fs.writeErr()
	}
	return fs.ObjectStorage.UploadPartCopy(ctx, key, uploadID, num, srcKey, off, size)
}

func (fs *FencedStorage) CompleteUpload(ctx context.Context, key string, uploadID string, parts []*Part) error {
	if fs.fenced.Load() {
		return fs.writeErr()
	}
	return fs.ObjectStorage.CompleteUpload(ctx, key, uploadID, parts)
}

// --- Read methods: always pass through ---
// Get, Head, List, ListAll, ListUploads, AbortUpload, String, Limits
// are inherited from the embedded ObjectStorage and always work.
