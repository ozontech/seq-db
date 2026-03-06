// This code has been copied and modified from:
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/21c06e86db16cb9df191db107697164608382b6e/lib/fs/fs_unix.go#L22

package util

import (
	"errors"
	"os"
	"path"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

func MustSyncPath(dirPath string) {
	if err := SyncPath(dirPath); err != nil {
		logger.Panic("cannot sync path", zap.String("path", dirPath), zap.Error(err))
	}
}

func MustRemoveFileByPath(fpath string) {
	if err := os.Remove(fpath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Panic(
			"cannot remove file by path",
			zap.String("path", fpath),
			zap.Error(err),
		)
	}
}

func SyncPath(dirPath string) error {
	d, err := os.Open(dirPath)
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

func MustWriteFileAtomic(fpath string, data []byte, tmpFileExt string) {
	fpathTmp := fpath + tmpFileExt

	f, err := os.Create(fpathTmp)
	if err != nil {
		logger.Panic("can't create file", zap.Error(err))
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Panic("can't close file", zap.Error(err))
		}
	}()

	if _, err := f.Write(data); err != nil {
		logger.Panic("can't write to file", zap.Error(err))
	}

	if err := f.Sync(); err != nil {
		logger.Panic("can't sync file", zap.Error(err))
	}

	if err := os.Rename(fpathTmp, fpath); err != nil {
		logger.Panic("can't rename file", zap.Error(err))
	}

	absFpath, err := filepath.Abs(fpath)
	if err != nil {
		logger.Panic("can't get absolute path", zap.String("path", fpath), zap.Error(err))
	}
	dir := path.Dir(absFpath)
	MustFsyncFile(dir)
}

func MustFsyncFile(fpath string) {
	dirFile, err := os.Open(fpath)
	if err != nil {
		logger.Panic("can't open dir", zap.Error(err))
	}
	if err := dirFile.Sync(); err != nil {
		logger.Panic("can't sync dir", zap.Error(err))
	}
	if err := dirFile.Close(); err != nil {
		logger.Panic("can't close dir", zap.Error(err))
	}
}

// MustCreateDir creates directory at dirPath.
// Handles the case when directory already exists.
func MustCreateDir(dirPath string) {
	err := os.MkdirAll(dirPath, 0o777)
	if err != nil && !os.IsExist(err) {
		logger.Panic("can't create file", zap.Error(err))
	}
}

// VisitFilesWithExt traverses all the files with `ext` extension in `des` directory and calls a `cb` func for each of files.
func VisitFilesWithExt(des []os.DirEntry, ext string, cb func(name string) error) error {
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		if path.Ext(name) != ext {
			continue
		}
		if err := cb(name); err != nil {
			return err
		}
	}
	return nil
}
