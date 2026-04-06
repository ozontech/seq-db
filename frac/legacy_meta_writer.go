package frac

import (
	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/storage"
)

// LegacyMetaWriter is MetaWriter for the legacy *.meta files. Converts new storage.WalBlock block type to
// storage.DocBlock
type LegacyMetaWriter struct {
	fw *storage.FileWriter
}

func NewLegacyMetaWriter(fw *storage.FileWriter) *LegacyMetaWriter {
	return &LegacyMetaWriter{fw: fw}
}

func (l *LegacyMetaWriter) Write(data storage.WalBlock, sw *stopwatch.Stopwatch) (int64, error) {
	docBlock := storage.PackWalBlockToDocBlock(data, nil)
	return l.fw.Write(docBlock, sw)
}

func (l *LegacyMetaWriter) Stop() {
	l.fw.Stop()
}
