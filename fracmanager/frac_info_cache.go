package fracmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

const defaultFilePermission = 0o660

// infoJSON is a temporary struct for JSON marshaling/unmarshaling
// that always stores From and To in milliseconds for backward compatibility
type infoJSON struct {
	*common.Info
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

// MarshalJSON implements custom JSON marshaling to always store From and To in milliseconds
func (e *infoJSON) MarshalJSON() ([]byte, error) {
	// Use type alias to avoid infinite recursion
	type Alias common.Info
	return json.Marshal(&struct {
		From uint64 `json:"from"`
		To   uint64 `json:"to"`
		*Alias
	}{
		From:  uint64(seq.MIDToMillis(e.Info.From)),
		To:    seq.MIDToCeilingMillis(e.Info.To),
		Alias: (*Alias)(e.Info),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling to convert From and To from milliseconds to nanoseconds
func (e *infoJSON) UnmarshalJSON(data []byte) error {
	e.Info = &common.Info{}

	// Use type alias to avoid infinite recursion
	type Alias common.Info
	tmp := &struct {
		From uint64 `json:"from"`
		To   uint64 `json:"to"`
		*Alias
	}{
		Alias: (*Alias)(e.Info),
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	e.Info.From = seq.MillisToMID(tmp.From)
	e.Info.To = seq.MillisToMID(tmp.To)
	return nil
}

type fracInfoCache struct {
	dataDir  string
	fullPath string
	fileName string

	mu      sync.RWMutex
	cache   map[string]*common.Info
	version uint64 // if we increment the counter every second it will take 31 billion years (quite enough)

	saveMu       sync.Mutex
	savedVersion atomic.Uint64
}

func NewFracInfoCache(filePath string) *fracInfoCache {
	fc := &fracInfoCache{
		cache:    make(map[string]*common.Info),
		mu:       sync.RWMutex{},
		fullPath: filePath,
		fileName: filepath.Base(filePath),
		dataDir:  filepath.Dir(filePath),
		version:  1,
	}

	return fc
}

func NewFracInfoCacheFromDisk(filePath string) *fracInfoCache {
	fc := NewFracInfoCache(filePath)
	fc.LoadFromDisk(filePath)
	return fc
}

// LoadFromDisk loads the contents of the fraction cache file to the in-memory map.
func (fc *fracInfoCache) LoadFromDisk(fileName string) {
	content, err := os.ReadFile(fileName)
	if err != nil {
		logger.Info("frac-cache read error, empty cache will be created",
			zap.Error(err),
			zap.String("filename", fileName),
		)
		return
	}

	cacheJSON := make(map[string]*infoJSON)
	err = json.Unmarshal(content, &cacheJSON)
	if err != nil {
		logger.Warn("can't unmarshal frac-cache, new frac-cache will be created later on",
			zap.Error(err),
		)
		return
	}
	for frac, entry := range cacheJSON {
		fc.cache[frac] = entry.Info
	}
	logger.Info("frac-cache loaded from disk",
		zap.String("filename", fileName),
		zap.Int("cache_entries", len(fc.cache)),
	)
}

// Add adds a new entry to the in-memory [sealedFracCache].
func (fc *fracInfoCache) Add(info *common.Info) {
	name := info.Name()

	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.version++
	fc.cache[name] = info
}

// Remove removes a fraction from [sealedFracCache].
// The data is synced with the disk on [sealedFracCache.SyncWithDisk] call.
func (fc *fracInfoCache) Remove(name string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.version++
	delete(fc.cache, name)
}

// Get returns fraction info and a flag that indicates
// whether the data is present in the map.
func (fc *fracInfoCache) Get(name string) (*common.Info, bool) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	el, ok := fc.cache[name]
	return el, ok
}

func (fc *fracInfoCache) getContentWithVersion() (uint64, []byte, error) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	if fc.version == fc.savedVersion.Load() {
		return 0, nil, nil // no changes
	}

	cacheJSON := make(map[string]*infoJSON, len(fc.cache))
	for k, v := range fc.cache {
		cacheJSON[k] = &infoJSON{Info: v}
	}

	content, err := json.Marshal(cacheJSON)
	if err != nil {
		return 0, nil, err
	}
	return fc.version, content, nil
}

// SyncWithDisk synchronizes the contents of the in-memory map
// with the file on the disk, if any changes were made (fractions added/deleted).
func (fc *fracInfoCache) SyncWithDisk() error {
	curVersion, content, err := fc.getContentWithVersion()
	if err != nil {
		return fmt.Errorf("can't get frac-cache content: %w", err)
	}

	if curVersion == 0 { // not need to save
		return nil
	}

	if err := fc.SaveCacheToDisk(curVersion, content); err != nil {
		return fmt.Errorf("can't save frac-cache: %w", err)
	}

	return nil
}

func (fc *fracInfoCache) SaveCacheToDisk(version uint64, content []byte) error {
	fc.saveMu.Lock()
	defer fc.saveMu.Unlock()

	savedVersion := fc.savedVersion.Load()
	if version <= savedVersion {
		logger.Info("frac-cache already saved",
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
		return fmt.Errorf("can't save frac-cache: %w", err)
	}

	err = tmp.Chmod(defaultFilePermission)
	if err != nil {
		return fmt.Errorf("can't change frac-cache file permission: %w", err)
	}

	if _, err = tmp.Write(content); err != nil {
		return fmt.Errorf("can't save frac-cache: %w", err)
	}

	if err = os.Rename(tmp.Name(), fc.fullPath); err != nil {
		return fmt.Errorf("can't rename tmp to actual frac-cache: %w", err)
	}

	fc.savedVersion.Store(version)
	logger.Info("frac-cache saved to disk",
		zap.String("filepath", fc.fullPath),
		zap.Uint64("version", version))
	return nil
}
