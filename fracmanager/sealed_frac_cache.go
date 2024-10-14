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

// sealedFracCache is a
type sealedFracCache struct {
	dataDir  string
	fullPath string
	fileName string

	fracCacheMu sync.RWMutex
	fracCache   map[string]*frac.Info
	version     uint64 // if we increment the counter every second it will take 31 billion years (quite enough)

	saveMu       sync.Mutex
	savedVersion atomic.Uint64
}

func NewSealedFracCache(filePath string) *sealedFracCache {
	fc := &sealedFracCache{
		fracCache:   make(map[string]*frac.Info),
		fracCacheMu: sync.RWMutex{},
		fullPath:    filePath,
		fileName:    filepath.Base(filePath),
		dataDir:     filepath.Dir(filePath),
		version:     1,
	}

	return fc
}

func NewFracCacheFromDisk(filePath string) *sealedFracCache {
	fc := NewSealedFracCache(filePath)
	fc.LoadFromDisk(filePath)
	return fc
}

// LoadFromDisk loads the contents of the fraction cache file to the in-memory map
func (fc *sealedFracCache) LoadFromDisk(fileName string) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		logger.Info("frac-cache read error, empty cache will be created",
			zap.Error(err),
			zap.String("filename", fileName),
		)
		return
	}

	err = json.Unmarshal(content, &fc.fracCache)
	if err != nil {
		logger.Warn("can't unmarshal frac cache, new frac cache will be created later on",
			zap.Error(err),
		)

		return
	}

	logger.Info("frac cache loaded from disk",
		zap.String("filename", fileName),
		zap.Int("cache_entries", len(fc.fracCache)),
	)
}

// AddFraction adds a new entry to the in-memory FracCache
func (fc *sealedFracCache) AddFraction(name string, info *frac.Info) {
	fc.fracCacheMu.Lock()
	defer fc.fracCacheMu.Unlock()

	fc.version++
	fc.fracCache[name] = info
}

// RemoveFraction removes a fraction from FracCache
// The data is synced with the disk on SyncWithDisk call
func (fc *sealedFracCache) RemoveFraction(name string) {
	fc.fracCacheMu.Lock()
	defer fc.fracCacheMu.Unlock()

	fc.version++
	delete(fc.fracCache, name)
}

// GetFracInfo returns fraction info and a flag that indicates
// whether the data is present in the map
func (fc *sealedFracCache) GetFracInfo(name string) (*frac.Info, bool) {
	fc.fracCacheMu.RLock()
	defer fc.fracCacheMu.RUnlock()

	el, ok := fc.fracCache[name]
	return el, ok
}

func (fc *sealedFracCache) getContentWithVersion() (uint64, []byte, error) {
	fc.fracCacheMu.RLock()
	defer fc.fracCacheMu.RUnlock()

	if fc.version == fc.savedVersion.Load() {
		return 0, nil, nil // no changes
	}

	content, err := json.Marshal(fc.fracCache)
	if err != nil {
		return 0, nil, err
	}
	return fc.version, content, nil
}

// SyncWithDisk synchronizes the contents of the in-memory map
// with the file on the disk, if any changes were made (fractions added/deleted)
func (fc *sealedFracCache) SyncWithDisk() error {
	curVersion, content, err := fc.getContentWithVersion()
	if err != nil {
		return fmt.Errorf("can't get frac cache content: %w", err)
	}

	if curVersion == 0 { // not need to save
		return nil
	}

	if err := fc.SaveCacheToDisk(curVersion, content); err != nil {
		return fmt.Errorf("can't save frac cache: %w", err)
	}

	return nil
}

func (fc *sealedFracCache) SaveCacheToDisk(version uint64, content []byte) error {
	fc.saveMu.Lock()
	defer fc.saveMu.Unlock()

	savedVersion := fc.savedVersion.Load()
	if version <= savedVersion {
		logger.Info("cache already saved",
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
		return fmt.Errorf("can't save frac cache: %w", err)
	}

	err = tmp.Chmod(defaultFilePermission)
	if err != nil {
		return fmt.Errorf("can't change frac cache file permission: %w", err)
	}

	if _, err = tmp.Write(content); err != nil {
		return fmt.Errorf("can't save frac cache: %w", err)
	}

	if err = os.Rename(tmp.Name(), fc.fullPath); err != nil {
		return fmt.Errorf("can't rename tmp to actual frac cache: %w", err)
	}

	fc.savedVersion.Store(version)
	logger.Info("frac cache saved to disk",
		zap.String("filepath", fc.fullPath),
		zap.Uint64("version", version))
	return nil
}
