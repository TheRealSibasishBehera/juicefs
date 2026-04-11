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
	"fmt"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

func cmdFlush() *cli.Command {
	return &cli.Command{
		Name:      "flush",
		Action:    flush,
		Category:  "TOOL",
		Usage:     "Flush all buffered writes of a mounted volume to storage",
		ArgsUsage: "MOUNTPOINT",
		Description: `
Examples:
$ juicefs flush /mnt/jfs`,
	}
}

func flush(ctx *cli.Context) error {
	setup0(ctx, 1, 1)
	mp := ctx.Args().Get(0)
	return flushViaMountpoint(mp)
}

func flushViaMountpoint(mp string) error {
	f, err := openController(mp)
	if err != nil {
		return fmt.Errorf("open control file for %s: %w", mp, err)
	}
	defer f.Close()

	headerLen := uint32(4 + 4)
	contentSize := uint32(0)
	wb := utils.NewBuffer(headerLen + contentSize)
	wb.Put32(meta.FlushAll)
	wb.Put32(contentSize)

	if _, err = f.Write(wb.Bytes()); err != nil {
		return fmt.Errorf("write message: %s", err)
	}

	if _, errno := readProgress(f, func(count, total uint64) {}); errno != 0 {
		return fmt.Errorf("flush failed: %v", errno)
	}
	return nil
}
