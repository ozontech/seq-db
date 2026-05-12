package fracmanager

import (
	"encoding/json"
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
	return util.WriteFileAtomic(m.filePath, data, 0o644, ".txt")
}
