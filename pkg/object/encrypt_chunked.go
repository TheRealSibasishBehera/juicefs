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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	defaultChunkSize = 8 << 20 // 8 MiB
	encMagic         = "JENC"
	encVersion       = 1
	encHeaderSize    = 4 + 2 + 4 // magic + version + chunk_size
)

// chunkedEncrypted is an ObjectStorage wrapper that encrypts data in streaming chunked.
// Chunked encryption format:
// [4 bytes: magic "JENC"][2 bytes: version=1][4 bytes: chunk_size]
// [4 bytes: chunk0_len][chunk0_ciphertext]
// [4 bytes: chunk1_len][chunk1_ciphertext]
// ...
type chunkedEncrypted struct {
	ObjectStorage
	enc       Encryptor
	chunkSize int
}

func NewChunkedEncrypted(o ObjectStorage, enc Encryptor) ObjectStorage {
	return &chunkedEncrypted{o, enc, defaultChunkSize}
}

func (e *chunkedEncrypted) String() string {
	return fmt.Sprintf("%s(encrypted-chunked)", e.ObjectStorage)
}

func encodeHeader(chunkSize int) []byte {
	buf := make([]byte, encHeaderSize)
	copy(buf[:4], encMagic)
	binary.BigEndian.PutUint16(buf[4:6], encVersion)
	binary.BigEndian.PutUint32(buf[6:10], uint32(chunkSize))
	return buf
}

func decodeHeader(data []byte) (chunkSize int, err error) {
	if len(data) < encHeaderSize {
		return 0, errors.New("encrypted data too short for header")
	}
	if string(data[:4]) != encMagic {
		return 0, errors.New("invalid encryption magic")
	}
	ver := binary.BigEndian.Uint16(data[4:6])
	if ver != encVersion {
		return 0, fmt.Errorf("unsupported encryption version: %d", ver)
	}
	chunkSize = int(binary.BigEndian.Uint32(data[6:10]))
	if chunkSize <= 0 {
		return 0, fmt.Errorf("invalid chunk size: %d", chunkSize)
	}
	return chunkSize, nil
}

func (e *chunkedEncrypted) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	r, err := e.ObjectStorage.Get(ctx, key, 0, -1, getters...)
	if err != nil {
		return nil, err
	}

	header := make([]byte, encHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		r.Close()
		return nil, fmt.Errorf("Decrypt: read header: %s", err)
	}
	_, err = decodeHeader(header)
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("Decrypt: %s", err)
	}
	dr := &chunkDecryptReader{r: r, enc: e.enc}
	if off > 0 {
		if _, err := io.CopyN(io.Discard, dr, off); err != nil && err != io.EOF {
			dr.Close()
			return nil, err
		}
	}
	if limit >= 0 {
		return &limitedReadCloser{
			Reader: io.LimitReader(dr, limit),
			Closer: dr,
		}, nil
	}
	return dr, nil
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}

type chunkDecryptReader struct {
	r   io.ReadCloser
	enc Encryptor
	buf []byte
}

func (r *chunkDecryptReader) Read(p []byte) (int, error) {
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(r.r, lenBuf[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return 0, fmt.Errorf("Decrypt: truncated chunk length")
		}
		return 0, err
	}
	chunkLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	ciphertext := make([]byte, chunkLen)
	if _, err := io.ReadFull(r.r, ciphertext); err != nil {
		return 0, fmt.Errorf("Decrypt: read chunk: %s", err)
	}
	plain, err := r.enc.Decrypt(ciphertext)
	if err != nil {
		return 0, fmt.Errorf("Decrypt: %s", err)
	}
	n := copy(p, plain)
	if n < len(plain) {
		r.buf = plain[n:]
	}
	return n, nil
}

func (r *chunkDecryptReader) Close() error { return r.r.Close() }

func (e *chunkedEncrypted) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- e.ObjectStorage.Put(ctx, key, pr, getters...)
	}()

	writeErr := e.encryptStream(in, pw)
	if writeErr != nil {
		pw.CloseWithError(writeErr)
		<-errCh
		return writeErr
	}
	pw.Close()
	return <-errCh
}

func (e *chunkedEncrypted) encryptStream(in io.Reader, w io.Writer) error {
	if _, err := w.Write(encodeHeader(e.chunkSize)); err != nil {
		return err
	}
	buf := make([]byte, e.chunkSize)
	lenBuf := make([]byte, 4)
	for {
		n, readErr := io.ReadFull(in, buf)
		if n > 0 {
			ciphertext, err := e.enc.Encrypt(buf[:n])
			if err != nil {
				return err
			}
			binary.BigEndian.PutUint32(lenBuf, uint32(len(ciphertext)))
			if _, err := w.Write(lenBuf); err != nil {
				return err
			}
			if _, err := w.Write(ciphertext); err != nil {
				return err
			}
		}
		if readErr != nil {
			if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
				return nil
			}
			return readErr
		}
	}
}

var _ ObjectStorage = (*chunkedEncrypted)(nil)
