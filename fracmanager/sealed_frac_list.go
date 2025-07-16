package fracmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
)

const defaultFilePermission = 0o660

type sealedFracList struct {
	dataDir  string
	fullPath string
	fileName string

	fracListMu sync.RWMutex
	fracList   map[string]*frac.Info
	version    uint64 // if we increment the counter every second it will take 31 billion years (quite enough)

	saveMu       sync.Mutex
	savedVersion atomic.Uint64
}

func NewSealedFracList(filePath string) *sealedFracList {
	fc := &sealedFracList{
		fracList:   make(map[string]*frac.Info),
		fracListMu: sync.RWMutex{},
		fullPath:   filePath,
		fileName:   filepath.Base(filePath),
		dataDir:    filepath.Dir(filePath),
		version:    1,
	}

	return fc
}

func NewFracListFromDisk(filePath string) *sealedFracList {
	fc := NewSealedFracList(filePath)
	fc.LoadFromDisk(filePath)
	return fc
}

// LoadFromDisk loads the contents of the fraction list file to the in-memory map
func (fc *sealedFracList) LoadFromDisk(fileName string) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		logger.Info("frac-list read error, empty list will be created",
			zap.Error(err),
			zap.String("filename", fileName),
		)
		return
	}

	err = json.Unmarshal(content, &fc.fracList)
	if err != nil {
		logger.Warn("can't unmarshal frac-list, new frac-list will be created later on",
			zap.Error(err),
		)
		return
	}

	logger.Info("frac-list loaded from disk",
		zap.String("filename", fileName),
		zap.Int("list_entries", len(fc.fracList)),
	)
}

// AddFraction adds a new entry to the in-memory FracList
func (fc *sealedFracList) AddFraction(name string, info *frac.Info) {
	fc.fracListMu.Lock()
	defer fc.fracListMu.Unlock()

	fc.version++
	fc.fracList[name] = info
}

// RemoveFraction removes a fraction from FracList
// The data is synced with the disk on SyncWithDisk call
func (fc *sealedFracList) RemoveFraction(name string) {
	fc.fracListMu.Lock()
	defer fc.fracListMu.Unlock()

	fc.version++
	delete(fc.fracList, name)
}

// GetFracInfo returns fraction info and a flag that indicates
// whether the data is present in the map
func (fc *sealedFracList) GetFracInfo(name string) (*frac.Info, bool) {
	fc.fracListMu.RLock()
	defer fc.fracListMu.RUnlock()

	el, ok := fc.fracList[name]
	return el, ok
}

func (fc *sealedFracList) getContentWithVersion() (uint64, []byte, error) {
	fc.fracListMu.RLock()
	defer fc.fracListMu.RUnlock()

	if fc.version == fc.savedVersion.Load() {
		return 0, nil, nil // no changes
	}

	content, err := json.Marshal(fc.fracList)
	if err != nil {
		return 0, nil, err
	}
	return fc.version, content, nil
}

// SyncWithDisk synchronizes the contents of the in-memory map
// with the file on the disk, if any changes were made (fractions added/deleted)
func (fc *sealedFracList) SyncWithDisk() error {
	curVersion, content, err := fc.getContentWithVersion()
	if err != nil {
		return fmt.Errorf("can't get frac-list content: %w", err)
	}

	if curVersion == 0 { // not need to save
		return nil
	}

	if err := fc.SaveListToDisk(curVersion, content); err != nil {
		return fmt.Errorf("can't save frac-list: %w", err)
	}

	return nil
}

func (fc *sealedFracList) SaveListToDisk(version uint64, content []byte) error {
	fc.saveMu.Lock()
	defer fc.saveMu.Unlock()

	savedVersion := fc.savedVersion.Load()
	if version <= savedVersion {
		logger.Info("frac-list already saved",
			zap.Uint64("version_to_save", version),
			zap.Uint64("saved_version", savedVersion))
		return nil
	}

	// we use unique temporary file
	//  * for atomic content changing
	//  * protect origin file from writing interruption
	//  * and to avoid race when writing (we can have several independent writers running at the same time, see tools/distribution/distribution.go)
	tmp, err := os.CreateTemp(fc.dataDir, fc.fileName+".")
	if err != nil {
		return fmt.Errorf("can't save frac-list: %w", err)
	}

	err = tmp.Chmod(defaultFilePermission)
	if err != nil {
		return fmt.Errorf("can't change frac-list file permission: %w", err)
	}

	if _, err = tmp.Write(content); err != nil {
		return fmt.Errorf("can't save frac-list: %w", err)
	}

	if err = os.Rename(tmp.Name(), fc.fullPath); err != nil {
		return fmt.Errorf("can't rename tmp to actual frac-list: %w", err)
	}

	fc.savedVersion.Store(version)
	logger.Info("frac-list saved to disk",
		zap.String("filepath", fc.fullPath),
		zap.Uint64("version", version))
	return nil
}
