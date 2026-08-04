// This code has been copied and modified from:
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/21c06e86db16cb9df191db107697164608382b6e/lib/fs/fs_unix.go#L22

package util

import (
	"errors"
	"fmt"
	"io"
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

func MustWriteFileAtomic(fpath string, data []byte, perm os.FileMode, tmpFileExt string) {
	if err := WriteFileAtomic(fpath, data, perm, tmpFileExt); err != nil {
		logger.Panic("can't write file atomic", zap.String("path", fpath), zap.Error(err))
	}
}

// atomicWrite safely writes data to file using atomic replacement pattern
func WriteFileAtomic(fpath string, data []byte, perm os.FileMode, tmpFileExt string) error {
	tmpPath := fpath + tmpFileExt
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	defer func() {
		if f != nil {
			f.Close()
		}
		if err != nil {
			os.Remove(tmpPath)
		}
	}()

	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	if err = f.Sync(); err != nil {
		return fmt.Errorf("sync data: %w", err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	f = nil // mark as closed so defer doesn't close again

	if err = os.Rename(tmpPath, fpath); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}

	if err = SyncPath(filepath.Dir(fpath)); err != nil { // also sync parent directory
		return fmt.Errorf("sync dir: %w", err)
	}

	return nil
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

// CopyFile copies a file (overwrites if already exists)
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
