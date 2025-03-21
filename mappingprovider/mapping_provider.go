package mappingprovider

import (
	"context"
	"crypto/sha256"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
)

const defaultUpdatePeriod = 30 * time.Second

type Option func(*MappingProvider)

func WithUpdatePeriod(up time.Duration) Option {
	return func(p *MappingProvider) {
		p.updatePeriod = up
	}
}

func WithMapping(m seq.Mapping) Option {
	return func(p *MappingProvider) {
		p.mapping = m
		p.rawMapping = seq.NewRawMapping(m)
	}
}

func WithIndexAllFields(enabled bool) Option {
	return func(p *MappingProvider) {
		p.indexAllFields = enabled
	}
}

type MappingProvider struct {
	filePath       string
	updatePeriod   time.Duration
	checksum       [sha256.Size]byte
	mapping        seq.Mapping
	rawMapping     *seq.RawMapping
	mu             sync.RWMutex
	indexAllFields bool
}

func New(
	filePath string,
	opts ...Option,
) (*MappingProvider, error) {
	p := &MappingProvider{
		filePath:     filePath,
		updatePeriod: defaultUpdatePeriod,
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.mapping != nil {
		return p, nil
	}

	if p.indexAllFields {
		return p, nil
	}

	if err := p.initMapping(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *MappingProvider) GetMapping() seq.Mapping {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.indexAllFields {
		return nil
	}

	return p.mapping
}

func (p *MappingProvider) GetRawMapping() *seq.RawMapping {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.rawMapping
}

// WatchUpdates periodically reads the contents at filePath and when it detects changes it updates the mapping
func (p *MappingProvider) WatchUpdates(ctx context.Context) {
	logger.Info("starting mapping file watcher", zap.String("file", p.filePath))

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("mapping file watcher closed")
				return
			case <-time.After(p.updatePeriod):
				p.reloadMapping()
			}
		}
	}()
}

func (p *MappingProvider) initMapping() error {
	data, err := os.ReadFile(p.filePath)
	if err != nil {
		return err
	}

	p.checksum = p.calcChecksum(data)

	mapping, err := seq.ReadMapping(data)
	if err != nil {
		return err
	}

	p.mapping = mapping
	p.rawMapping = seq.NewRawMapping(mapping)

	return nil
}

func (p *MappingProvider) reloadMapping() {
	logger.Debug("checking mapping file for updates...", zap.String("file", p.filePath))

	data, err := os.ReadFile(p.filePath)
	if err != nil {
		logger.Error("error opening mapping file", zap.Error(err))
		return
	}

	newChecksum := p.calcChecksum(data)
	if newChecksum == p.checksum {
		logger.Debug("no mapping updates")
		return
	}

	newMapping, err := seq.ReadMapping(data)
	if err != nil {
		logger.Error("read new mapping error", zap.Error(err))
		return
	}

	p.updateMapping(newMapping)

	p.checksum = newChecksum
	logger.Info("mapping updated", zap.String("file", p.filePath))
}

func (p *MappingProvider) updateMapping(m seq.Mapping) {
	newRawMapping := seq.NewRawMapping(m)

	p.mu.Lock()
	defer p.mu.Unlock()

	p.mapping = m
	p.rawMapping = newRawMapping
}

func (p *MappingProvider) calcChecksum(data []byte) [sha256.Size]byte {
	return sha256.Sum256(data)
}
