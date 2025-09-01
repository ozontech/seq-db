// This code has been copied and modified from:
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/21c06e86db16cb9df191db107697164608382b6e/lib/fs/fs_unix.go#L22

package util

import (
	"errors"
	"os"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

func MustSyncPath(path string) {
	d, err := os.Open(path)
	if err != nil {
		logger.Panic("cannot open file for fsync", zap.Error(err))
	}
	if err = d.Sync(); err != nil {
		_ = d.Close()
		logger.Panic("cannot flush path to storage", zap.String("path", path), zap.Error(err))
	}

	if err = d.Close(); err != nil {
		logger.Panic("cannot close path", zap.String("path", path), zap.Error(err))
	}
}

func MustRemoveFileByPath(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Panic(
			"cannot remove file by path",
			zap.String("path", path),
			zap.Error(err),
		)
	}
}
