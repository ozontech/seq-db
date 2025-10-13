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
	if err := SyncPath(path); err != nil {
		logger.Panic("cannot sync path", zap.String("path", path), zap.Error(err))
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

func SyncPath(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}

	if err := d.Close(); err != nil {
		return err
	}
	return nil
}

// RemoveFile safely removes a file with logging
// Handles cases where the file already doesn't exist
func RemoveFile(file string) {
	if err := os.Remove(file); err == nil {
		logger.Info("remove file", zap.String("filename", file))
	} else if !os.IsNotExist(err) {
		logger.Error("file removing error", zap.Error(err))
	}
}
