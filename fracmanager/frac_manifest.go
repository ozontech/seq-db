package fracmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

// fracManifest represents a manifest of fraction files
// Contains information about the presence of various file types for a specific fraction
type fracManifest struct {
	basePath      string // base path to fraction files (without extension)
	hasDocs       bool   // presence of main documents file
	hasWal        bool   // presence of WAL with meta
	hasIndex      bool   // presence of index file
	hasSdocs      bool   // presence of sorted documents
	hasRemote     bool   // presence of remote fraction (legacy)
	hasRemoteInfo bool   // presence of .remote-info

	// Presence of ._remote-info when offloading was interrupted
	hasRemoteInfoTmp bool

	// Split index file flags
	hasInfo    bool
	hasToken   bool
	hasOffsets bool
	hasID      bool
	hasLID     bool

	// Deletion marker file flags
	hasDocsDel  bool // documents deletion marker
	hasSdocsDel bool // sorted documents deletion marker
	hasIndexDel bool // index deletion marker

	hasCompactionPlan bool
}

// hasAllIndexFiles reports whether all 5 split index files are present.
func (m *fracManifest) hasAllIndexFiles() bool {
	return m.hasInfo && m.hasToken && m.hasOffsets && m.hasID && m.hasLID || m.hasIndex
}

func (m *fracManifest) hasDocsFile() bool {
	return m.hasSdocs || m.hasDocs
}

func (m *fracManifest) hasRemoteFile() bool {
	return m.hasRemote || m.hasRemoteInfo
}

// AddExtension adds information about a file with the specified extension
// Called during filesystem analysis to build a complete picture of fraction files
func (m *fracManifest) AddExtension(ext string) error {
	switch ext {
	case consts.DocsFileSuffix:
		m.hasDocs = true
	case consts.WalFileSuffix:
		m.hasWal = true
	case consts.SdocsFileSuffix:
		m.hasSdocs = true
	case consts.IndexFileSuffix:
		m.hasIndex = true
	case consts.RemoteFractionInfoSuffix:
		m.hasRemoteInfo = true
	case consts.RemoteFractionSuffix:
		m.hasRemote = true

	case consts.InfoFileSuffix:
		m.hasInfo = true
	case consts.TokenFileSuffix:
		m.hasToken = true
	case consts.OffsetsFileSuffix:
		m.hasOffsets = true
	case consts.IDFileSuffix:
		m.hasID = true
	case consts.LIDFileSuffix:
		m.hasLID = true

	case consts.DocsDelFileSuffix:
		m.hasDocsDel = true
	case consts.SdocsDelFileSuffix:
		m.hasSdocsDel = true
	case consts.IndexDelFileSuffix:
		m.hasIndexDel = true

	case consts.CompactionPlan:
		m.hasCompactionPlan = true

	case consts.RemoteFractionInfoTmpSuffix:
		m.hasRemoteInfoTmp = true

	case consts.IndexTmpFileSuffix, consts.InfoTmpFileSuffix,
		consts.TokenTmpFileSuffix, consts.OffsetsTmpFileSuffix,
		consts.IDTmpFileSuffix, consts.LIDTmpFileSuffix,
		consts.DocsTmpFileSuffix, consts.SdocsTmpFileSuffix:

		// Just handle temporary files (which were not commited).
		// We will just drop them in all possible cases.

	default:
		return fmt.Errorf("unknown fraction file type %s", ext)
	}
	return nil
}

// fracStage represents the lifecycle stage of a fraction
// Used to determine fraction state and corresponding cleanup operations
type fracStage int

const (
	fracStageUnknown fracStage = iota // unknown state - requires analysis
	fracStageActive                   // active fraction - accepts new data
	fracStageSealed                   // sealed fraction - read-only
	fracStageRemote                   // remote fraction - data stored in external storage
	fracStageZombie                   // partially deleted fraction - requires cleanup
)

// Stage determines the current stage of the fraction based on file presence
// Key method for making fraction management decisions
func (m *fracManifest) Stage() fracStage {
	if m.hasRemoteFile() {
		return fracStageRemote
	}
	if m.hasAllIndexFiles() && m.hasDocsFile() {
		return fracStageSealed
	}
	if m.hasWal && m.hasDocs {
		return fracStageActive
	}
	if m.hasDocsDel || m.hasSdocsDel || m.hasIndexDel {
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
	if m.hasSdocs {
		util.RemoveFile(m.basePath + consts.SdocsFileSuffix)
		m.hasSdocs = false
	}
}

func removeWal(m *fracManifest) {
	if m.hasWal {
		util.RemoveFile(m.basePath + consts.WalFileSuffix)
		m.hasWal = false
	}
}

func removeCompactionPlan(m *fracManifest) {
	util.RemoveFile(m.basePath + consts.CompactionPlan)
	m.hasCompactionPlan = false
}

func removeIndexFiles(m *fracManifest) {
	for _, suffix := range []string{
		consts.InfoFileSuffix,
		consts.TokenFileSuffix,
		consts.OffsetsFileSuffix,
		consts.IDFileSuffix,
		consts.LIDFileSuffix,
		consts.IndexFileSuffix,
	} {
		util.RemoveFile(m.basePath + suffix)
	}
	m.hasInfo = false
	m.hasToken = false
	m.hasOffsets = false
	m.hasID = false
	m.hasLID = false
	m.hasIndex = false
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
	for _, suffix := range []string{
		consts.IndexTmpFileSuffix,
		consts.InfoTmpFileSuffix,
		consts.TokenTmpFileSuffix,
		consts.OffsetsTmpFileSuffix,
		consts.IDTmpFileSuffix,
		consts.LIDTmpFileSuffix,
	} {
		util.RemoveFile(m.basePath + suffix)
	}
}

func removeRemoteTmp(m *fracManifest) {
	if m.hasRemoteInfoTmp {
		// TODO: Clean S3 zombies
		util.RemoveFile(m.basePath + consts.RemoteFractionInfoTmpSuffix)
		m.hasRemoteInfoTmp = false
	}
}

func removeSdocsTmp(m *fracManifest) {
	util.RemoveFile(m.basePath + consts.SdocsTmpFileSuffix)
}

func removeDocsTmp(m *fracManifest) {
	util.RemoveFile(m.basePath + consts.DocsTmpFileSuffix)
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

	sort.Strings(ids)
	return filterValid(ids, manifests)
}

// filterValid filters valid fractions and handles invalid ones
// Removes partially deleted and unknown fractions
func filterValid(ids []string, manifests map[string]*fracManifest) ([]*fracManifest, error) {
	// We need to drop stale (compacted) fractions first.
	ids, err := dropCompacted(ids, manifests)
	if err != nil {
		return nil, err
	}

	validated := make([]*fracManifest, 0, len(manifests))

	for _, id := range ids {
		manifest := manifests[id]
		if manifest == nil {
			return nil, errors.New("inconsistent fraction file analysis")
		}

		switch manifest.Stage() {
		case fracStageUnknown:
			// Processing partially compacted fraction.
			if manifest.hasCompactionPlan {
				logger.Warn(
					"dropping partially compacted fraction",
					zap.String("base_path", manifest.basePath),
				)
				removeAllFiles(manifest.basePath)
				continue
			}

			logger.Error("unknown fraction stage", zap.Object("manifest", manifest))
			fractionLoadErrors.Inc()
			removeAllFiles(manifest.basePath)
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

func dropCompacted(ids []string, manifests map[string]*fracManifest) ([]string, error) {
	type plan struct {
		Participants []string `json:"participants"`
	}

	filtered := make(map[string]struct{})

	for _, id := range ids {
		filtered[id] = struct{}{}
	}

	for _, id := range ids {
		m := manifests[id]
		if m == nil {
			return nil, errors.New("inconsistent fraction file analysis")
		}

		skip := !m.hasCompactionPlan ||
			m.Stage() != fracStageSealed

		if skip {
			continue
		}

		f, err := os.Open(m.basePath + consts.CompactionPlan)
		if err != nil {
			return nil, err
		}
		defer f.Close() //nolint

		var p plan
		if err := json.NewDecoder(f).Decode(&p); err != nil {
			// Well, we cannot decode compaction plan so let's drop whatever the result is.

			logger.Warn(
				"dropping possibly correctly compacted fraction: cannot decode compaction plan",
				zap.Error(err),
				zap.String("base_path", m.basePath),
			)

			delete(filtered, id)
			removeAllFiles(m.basePath)
			continue
		}

		for _, pname := range p.Participants {
			pid := pname[len(fileBasePattern):]

			pm := manifests[pid]
			if pm == nil {
				// NOTE(dkharms): It is possible that compaction participants
				// were dropped but the plan itself was not deleted.
				continue
			}

			logger.Warn(
				"dropping fraction: it was merged into another one",
				zap.Error(err),
				zap.String("merged_base_path", m.basePath),
				zap.String("participant_base_path", pm.basePath),
			)

			delete(filtered, pid)
			removeAllFiles(pm.basePath)
		}
	}

	return slices.Collect(maps.Keys(filtered)), nil
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
	removeWal(m)
	removeDocs(m)
	removeSdocs(m)
	removeIndexFiles(m)
}

// cleanupSealedFrac cleans files for sealed fractions
// Removes redundant files after finishing work with the fraction
func cleanupSealedFrac(m *fracManifest) {
	removeWal(m)
	removeCompactionPlan(m)
	if m.hasSdocs {
		removeDocs(m) // remove orig docs, but keeping sorted
	}
}

// cleanupTemporary cleans temporary and marker files
// Executed for all fractions regardless of stage
func cleanupTemporary(m *fracManifest) {
	removeSdocsDel(m)
	removeDocsDel(m)
	removeIndexTmp(m)
	removeRemoteTmp(m)
	removeDocsTmp(m)
	removeSdocsTmp(m)
}

// removeAllFiles completely removes all fraction files
// Used for cleaning up partially deleted or corrupted fractions
func removeAllFiles(basePath string) {
	for _, suffix := range []string{
		consts.DocsFileSuffix, consts.DocsDelFileSuffix, consts.DocsTmpFileSuffix,
		consts.SdocsFileSuffix, consts.SdocsDelFileSuffix, consts.SdocsTmpFileSuffix,
		consts.IndexFileSuffix, consts.IndexDelFileSuffix, consts.IndexTmpFileSuffix,
		consts.RemoteFractionInfoSuffix, consts.RemoteFractionInfoTmpSuffix,

		consts.InfoFileSuffix, consts.InfoTmpFileSuffix,
		consts.TokenFileSuffix, consts.TokenTmpFileSuffix,
		consts.OffsetsFileSuffix, consts.OffsetsTmpFileSuffix,
		consts.IDFileSuffix, consts.IDTmpFileSuffix,
		consts.LIDFileSuffix, consts.LIDTmpFileSuffix,

		consts.WalFileSuffix,
		consts.CompactionPlan,
	} {
		util.RemoveFile(basePath + suffix)
	}
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

func (f *fracManifest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("basePath", f.basePath)
	enc.AddBool("hasDocs", f.hasDocs)
	enc.AddBool("hasWal", f.hasWal)
	enc.AddBool("hasIndex", f.hasIndex)
	enc.AddBool("hasSdocs", f.hasSdocs)
	enc.AddBool("hasRemote", f.hasRemote)
	enc.AddBool("hasRemoteInfo", f.hasRemoteInfo)

	enc.AddBool("hasInfo", f.hasInfo)
	enc.AddBool("hasToken", f.hasToken)
	enc.AddBool("hasOffsets", f.hasOffsets)
	enc.AddBool("hasID", f.hasID)
	enc.AddBool("hasLID", f.hasLID)

	enc.AddBool("hasDocsDel", f.hasDocsDel)
	enc.AddBool("hasSdocsDel", f.hasSdocsDel)
	enc.AddBool("hasIndexDel", f.hasIndexDel)

	return nil
}
