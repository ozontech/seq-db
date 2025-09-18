package fracmanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const StateFile = "storage-state.json"

type StorageState struct {
	CapacityExceeded bool `json:"capacity_exceeded"` // Storage capacity exceeded flag
}

// StateManager manages storage state with thread safety
type StateManager struct {
	mu       sync.Mutex
	current  StorageState
	filePath string
}

// NewStateManager creates a new storage state manager
// dataDir - directory for storing state file
// defaultState - default state if file doesn't exist
func NewStateManager(dataDir string, defaultState StorageState) (*StateManager, error) {
	sm := &StateManager{
		filePath: filepath.Join(dataDir, StateFile),
	}
	err := sm.init(defaultState)
	return sm, err
}

// IsCapacityExceeded returns storage capacity exceeded flag
func (m *StateManager) IsCapacityExceeded() bool {
	m.mu.Lock()         // Lock for thread safety
	defer m.mu.Unlock() // Ensure unlock
	return m.current.CapacityExceeded
}

// SetCapacityExceeded sets storage capacity exceeded flag
func (m *StateManager) SetCapacityExceeded(exceeded bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current.CapacityExceeded == exceeded {
		return nil
	}

	m.current.CapacityExceeded = exceeded
	return m.save()
}

func (m *StateManager) init(defaultState StorageState) error {
	err := m.load()
	if os.IsNotExist(err) {
		m.current = defaultState
		return m.save()
	}
	return err
}

func (m *StateManager) load() error {
	data, err := os.ReadFile(m.filePath) // Read file
	if err != nil {
		return err
	}
	var state StorageState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	m.current = state
	return nil
}

func (m *StateManager) save() error {
	data, err := json.Marshal(m.current)
	if err != nil {
		return err
	}
	return atomicWrite(m.filePath, data, 0644)
}

// atomicWrite safely writes data to file using atomic replacement pattern
// Ensures data integrity with proper synchronization and cleanup
func atomicWrite(path string, data []byte, perm os.FileMode) error {
	// Create temporary file
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	defer func() {
		if f != nil {
			f.Close() // Close file if still open
		}
		if err != nil {
			os.Remove(tmpPath) // Remove temp file on error
		}
	}()

	// Write data to temporary file
	if _, err = f.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	// Sync data to disk (force write from cache)
	if err = f.Sync(); err != nil {
		return fmt.Errorf("sync data: %w", err)
	}

	// Close file handle
	if err = f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	f = nil // Mark as closed so defer doesn't close again

	// Atomic replacement using rename (atomic operation on most filesystems)
	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}

	// Sync parent directory (optional - for critical operations only)
	if err = syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync dir: %w", err)
	}

	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	return d.Sync()
}
