package fracmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ozontech/seq-db/util"
)

const StateFile = "storage-state.json"

type StorageState struct {
	CapacityExceeded bool `json:"capacity_exceeded"` // Storage capacity exceeded flag
}

// StateManager manages storage state with thread safety
type StateManager struct {
	mu       sync.RWMutex
	current  StorageState
	filePath string
	synced   bool
}

// NewStateManager creates a new storage state manager
func NewStateManager(dataDir string, defaultState StorageState) (*StateManager, error) {
	sm := &StateManager{
		filePath: filepath.Join(dataDir, StateFile),
		current:  defaultState,
	}

	err := sm.load()
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return sm, nil
}

// IsCapacityExceeded returns storage capacity exceeded flag
func (m *StateManager) IsCapacityExceeded() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current.CapacityExceeded
}

// setCapacityExceeded sets storage capacity exceeded flag
func (m *StateManager) setCapacityExceeded(exceeded bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current.CapacityExceeded != exceeded {
		m.current.CapacityExceeded = exceeded
		m.synced = false
	}

	return m.save()
}

func (m *StateManager) load() error {
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		return err
	}
	var state StorageState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	m.synced = true
	m.current = state
	return nil
}

func (m *StateManager) save() error {
	if m.synced {
		return nil
	}

	data, err := json.Marshal(m.current)
	if err != nil {
		return err
	}
	return atomicWrite(m.filePath, data, 0o644)
}

// atomicWrite safely writes data to file using atomic replacement pattern
func atomicWrite(path string, data []byte, perm os.FileMode) error {
	tmpPath := path + ".tmp"
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

	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}

	if err = util.SyncPath(filepath.Dir(path)); err != nil { // also sync parent directory
		return fmt.Errorf("sync dir: %w", err)
	}

	return nil
}
