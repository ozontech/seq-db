package fracmanager

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

// fracManifest represents a manifest of fraction files
// Contains information about the presence of various file types for a specific fraction
type fracManifest struct {
	basePath  string // base path to fraction files (without extension)
	hasDocs   bool   // presence of main documents file
	hasIndex  bool   // presence of index file
	hasMeta   bool   // presence of meta-information
	hasSdocs  bool   // presence of sorted documents
	hasRemote bool   // presence of remote fraction

	// Deletion marker file flags
	hasDocsDel  bool // documents deletion marker
	hasSdocsDel bool // sorted documents deletion marker
	hasIndexDel bool // index deletion marker

	// Temporary file flags
	hasIndexTmp bool // temporary index file
	hasSdocsTmp bool // temporary sorted documents file
}

// AddExtension adds information about a file with the specified extension
// Called during filesystem analysis to build a complete picture of fraction files
func (m *fracManifest) AddExtension(ext string) error {
	switch ext {
	case consts.DocsFileSuffix:
		m.hasDocs = true
	case consts.MetaFileSuffix:
		m.hasMeta = true
	case consts.SdocsFileSuffix:
		m.hasSdocs = true
	case consts.IndexFileSuffix:
		m.hasIndex = true
	case consts.RemoteFractionSuffix:
		m.hasRemote = true

	case consts.DocsDelFileSuffix:
		m.hasDocsDel = true
	case consts.SdocsDelFileSuffix:
		m.hasSdocsDel = true
	case consts.IndexDelFileSuffix:
		m.hasIndexDel = true

	case consts.IndexTmpFileSuffix:
		m.hasIndexTmp = true
	case consts.SdocsTmpFileSuffix:
		m.hasSdocsTmp = true

	default:
		return fmt.Errorf("unknown fraction file type %s", ext)
	}
	return nil
}

// fracStage represents the lifecycle stage of a fraction
// Used to determine fraction state and corresponding cleanup operations
type fracStage int

const (
	fracStageActive  fracStage = iota // active fraction - accepts new data
	fracStageSealed                   // sealed fraction - read-only
	fracStageRemote                   // remote fraction - data stored in external storage
	fracStageZombie                   // partially deleted fraction - requires cleanup
	fracStageUnknown                  // unknown state - requires analysis
)

// Stage determines the current stage of the fraction based on file presence
// Key method for making fraction management decisions
func (m *fracManifest) Stage() fracStage {
	if m.hasRemote {
		return fracStageRemote
	}
	if m.hasIndex && (m.hasSdocs || m.hasDocs) {
		return fracStageSealed
	}
	if m.hasMeta && m.hasDocs {
		return fracStageActive
	}
	if m.hasDocsDel || m.hasIndexDel || m.hasSdocsDel {
		return fracStageZombie
	}
	return fracStageUnknown
}

func removeDocs(m *fracManifest) {
	if m.hasDocs {
		util.RemoveFile(m.basePath + consts.DocsFileSuffix)
		m.hasDocs = false
	}
}

func removeSdocs(m *fracManifest) {
	if m.hasDocs {
		util.RemoveFile(m.basePath + consts.SdocsFileSuffix)
		m.hasSdocs = false
	}
}

func removeMeta(m *fracManifest) {
	if m.hasMeta {
		util.RemoveFile(m.basePath + consts.MetaFileSuffix)
		m.hasMeta = false
	}
}

func removeIndex(m *fracManifest) {
	if m.hasIndex {
		util.RemoveFile(m.basePath + consts.IndexFileSuffix)
		m.hasIndex = false
	}
}

func removeIndexDel(m *fracManifest) {
	if m.hasIndexDel {
		util.RemoveFile(m.basePath + consts.IndexDelFileSuffix)
		m.hasIndexDel = false
	}
}

func removeSdocsDel(m *fracManifest) {
	if m.hasSdocsDel {
		util.RemoveFile(m.basePath + consts.SdocsDelFileSuffix)
		m.hasSdocsDel = false
	}
}

func removeDocsDel(m *fracManifest) {
	if m.hasDocsDel {
		util.RemoveFile(m.basePath + consts.DocsDelFileSuffix)
		m.hasDocsDel = false
	}
}

func removeIndexTmp(m *fracManifest) {
	if m.hasIndexTmp {
		util.RemoveFile(m.basePath + consts.IndexTmpFileSuffix)
		m.hasIndexTmp = false
	}
}

func removeSdocsTmp(m *fracManifest) {
	if m.hasSdocsTmp {
		util.RemoveFile(m.basePath + consts.SdocsTmpFileSuffix)
		m.hasSdocsTmp = false
	}
}

// analyzeFiles analyzes fraction files and groups them by fraction ID
// Creates manifests that represent the complete state of each fraction
func analyzeFiles(files []string) ([]*fracManifest, error) {
	ids := make([]string, 0, len(files))
	manifests := make(map[string]*fracManifest)

	for _, file := range files {
		basePath, ext, id, err := parseFilePath(file)
		if err != nil {
			return nil, err
		}

		manifest, exists := manifests[id]
		if !exists {
			manifest = &fracManifest{basePath: basePath}
			manifests[id] = manifest
			ids = append(ids, id)
		}

		if err := manifest.AddExtension(ext); err != nil {
			return nil, err
		}
	}

	sort.Strings(ids) // sort by identifiers

	// Filter valid fractions
	return filterValid(ids, manifests)
}

// filterValid filters valid fractions and handles invalid ones
// Removes partially deleted and unknown fractions
func filterValid(ids []string, manifests map[string]*fracManifest) ([]*fracManifest, error) {
	validated := make([]*fracManifest, 0, len(manifests))
	for _, id := range ids {
		manifest := manifests[id]
		if manifest == nil {
			return nil, errors.New("inconsistent fraction file analysis")
		}

		switch manifest.Stage() {
		case fracStageUnknown:
			logger.Error("unknown fraction stage", zap.String("fraction", id), zap.Any("manifest", manifest))
			fractionLoadErrors.Inc()
			continue
		case fracStageZombie:
			logger.Warn("cleaning up partially deleted fraction files", zap.String("base_path", manifest.basePath))
			removeAllFiles(manifest.basePath)
			continue
		}

		cleanupFrac(manifest)
		validated = append(validated, manifest)
	}
	return validated, nil
}

// cleanupFrac performs cleanup of unnecessary files depending on fraction stage
// Called after stage determination to optimize storage
func cleanupFrac(m *fracManifest) {
	switch m.Stage() {
	case fracStageRemote:
		cleanupRemoteFrac(m)
	case fracStageSealed:
		cleanupSealedFrac(m)
	}
	cleanupTemporary(m) // always clean up temporary files
}

// cleanupRemoteFrac cleans files for remote fractions
// Removes local file copies since data is stored remotely
func cleanupRemoteFrac(m *fracManifest) {
	removeMeta(m)
	removeDocs(m)
	removeSdocs(m)
	removeIndex(m)
	removeIndexDel(m)
}

// cleanupSealedFrac cleans files for sealed fractions
// Removes redundant files after finishing work with the fraction
func cleanupSealedFrac(m *fracManifest) {
	removeMeta(m)
	if m.hasSdocs {
		removeDocs(m) // remove orig docs, keep sorted
	}
}

// cleanupTemporary cleans temporary and marker files
// Executed for all fractions regardless of stage
func cleanupTemporary(m *fracManifest) {
	removeSdocsDel(m)
	removeDocsDel(m)
	removeIndexTmp(m)
	removeSdocsTmp(m)
}

// removeAllFiles completely removes all fraction files
// Used for cleaning up partially deleted or corrupted fractions
func removeAllFiles(basePath string) {
	// Remove main files first, then deletion markers to preserve deletion intent
	util.RemoveFile(basePath + consts.IndexFileSuffix)
	util.RemoveFile(basePath + consts.DocsFileSuffix)
	util.RemoveFile(basePath + consts.SdocsFileSuffix)
	util.RemoveFile(basePath + consts.MetaFileSuffix)

	util.RemoveFile(basePath + consts.IndexDelFileSuffix)
	util.RemoveFile(basePath + consts.DocsDelFileSuffix)
	util.RemoveFile(basePath + consts.SdocsDelFileSuffix)
	util.RemoveFile(basePath + consts.SdocsTmpFileSuffix)
	util.RemoveFile(basePath + consts.IndexTmpFileSuffix)
}

// parseFilePath extracts components from a fraction file path
// Returns base path, extension, and fraction identifier
// Used for grouping files by fractions
func parseFilePath(filePath string) (string, string, string, error) {
	filename := filepath.Base(filePath)

	nameLen := len(filename)
	prefixLen := len(fileBasePattern)

	if nameLen < prefixLen || filename[:prefixLen] != fileBasePattern {
		return "", "", "", fmt.Errorf("invalid fraction file name %s", filePath)
	}

	suffix := extractSuffix(filename)
	id := filename[prefixLen : nameLen-len(suffix)]
	return filePath[:len(filePath)-len(suffix)], suffix, id, nil
}

// extractSuffix extracts file extension (suffix) from filename
// Helper function for analyzing fraction file names
func extractSuffix(filename string) string {
	i := strings.Index(filename, ".")
	if i < 0 {
		return ""
	}
	return filename[i:]
}
