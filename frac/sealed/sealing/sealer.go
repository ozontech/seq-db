package sealing

import (
	"errors"
	"iter"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

// Source interface defines the contract for data sources that can be sealed.
// Provides access to all necessary data components for index creation.
type Source interface {
	Info() *common.Info                                   // Fraction metadata information
	IDsBlocks(size int) iter.Seq2[[]seq.ID, []seq.DocPos] // Ordered sequence of document IDs and their positions, divided into blocks
	TokenBlocks(size int) iter.Seq[[][]byte]              // Ordered sequence of tokens divided into blocks
	Fields() iter.Seq2[string, uint32]                    // Ordered sequence of fields with their max field's TID value
	TokenLIDs() iter.Seq[[]uint32]                        // Ordered sequence of sorted token LIDs
	BlocksOffsets() []uint64                              // Offsets of DocBlock's in the doc file
	LastError() error                                     // Last error encountered during data retrieval
}

// Seal is the main entry point for sealing a fraction.
// It performs the complete sealing process:
// 1. Creates the index file structure
// 2. Writes all index blocks with compression
// 3. Builds PreloadedData structures for fast initialization of sealed fraction
// 4. Handles file system operations and error recovery
//
// Parameters:
//   - src: Data source providing all fraction data
//   - params: Sealing parameters including compression levels
//
// Returns:
//   - *sealed.PreloadedData: Preloaded data structures for initialization of sealed fraction
//   - error: Any error encountered during the sealing process
func Seal(src Source, params common.SealParams) (*sealed.PreloadedData, error) {
	start := time.Now()
	info := src.Info()

	// Validate that we're not sealing an empty fraction
	if info.To == 0 {
		return nil, errors.New("sealing of an empty active fraction is not supported")
	}

	// Create temporary index file (will be renamed on success)
	indexFile, err := os.Create(info.Path + consts.IndexTmpFileSuffix)
	if err != nil {
		return nil, err
	}

	// Create index sealer and write the index structure
	indexSealer := NewIndexSealer(params)
	if err := indexSealer.WriteIndex(indexFile, src); err != nil {
		return nil, err
	}

	// Ensure data is flushed to disk
	if err := indexFile.Sync(); err != nil {
		return nil, err
	}

	// Get final file size for metadata
	stat, err := indexFile.Stat()
	if err != nil {
		return nil, err
	}
	info.IndexOnDisk = uint64(stat.Size())

	// Close file before renaming
	if err := indexFile.Close(); err != nil {
		return nil, err
	}

	// Atomically rename temporary file to final name
	if err := os.Rename(indexFile.Name(), info.Path+consts.IndexFileSuffix); err != nil {
		return nil, err
	}

	// Ensure directory metadata is synced to disk
	util.MustSyncPath(filepath.Dir(info.Path))

	// Build preloaded data structure for fast query access
	preloaded := sealed.PreloadedData{
		Info:       info,
		TokenTable: indexSealer.TokenTable(),
		BlocksData: sealed.BlocksData{
			IDsTable:      indexSealer.IDsTable(),
			LIDsTable:     indexSealer.LIDsTable(),
			BlocksOffsets: src.BlocksOffsets(),
		},
	}

	// Log successful sealing operation
	logger.Info(
		"fraction sealed",
		zap.String("fraction", filepath.Dir(info.Path)),
		zap.Float64("time_spent_s", util.DurationToUnit(time.Since(start), "s")),
	)
	return &preloaded, nil
}
