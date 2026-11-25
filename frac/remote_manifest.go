package frac

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

const (
	remoteManifestMagic   = "SEQR"
	remoteManifestVersion = 1
)

type remoteFileRole string

const (
	remoteFileDocs    remoteFileRole = "docs"
	remoteFileInfo    remoteFileRole = "info"
	remoteFileTokens  remoteFileRole = "tokens"
	remoteFileOffsets remoteFileRole = "offsets"
	remoteFileIDs     remoteFileRole = "ids"
	remoteFileLIDs    remoteFileRole = "lids"
	remoteFileIndex   remoteFileRole = "index"
)

var (
	legacyRemoteFileRoles = map[remoteFileRole]struct{}{
		remoteFileDocs:  {},
		remoteFileIndex: {},
	}

	splitRemoteFileRoles = map[remoteFileRole]struct{}{
		remoteFileDocs:    {},
		remoteFileInfo:    {},
		remoteFileTokens:  {},
		remoteFileOffsets: {},
		remoteFileIDs:     {},
		remoteFileLIDs:    {},
	}
)

type remoteManifestFile struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

type remoteManifest struct {
	Version uint32                                `json:"version"`
	Info    *common.Info                          `json:"info"`
	Files   map[remoteFileRole]remoteManifestFile `json:"files"`
}

func newRemoteManifest(info *common.Info, files map[remoteFileRole]storage.ImmutableFile) (*remoteManifest, error) {
	if info == nil {
		return nil, errors.New("remote manifest info is nil")
	}

	infoCopy := *info
	m := &remoteManifest{
		Version: remoteManifestVersion,
		Info:    &infoCopy,
		Files:   make(map[remoteFileRole]remoteManifestFile, len(files)),
	}

	for role, file := range files {
		if file == nil {
			return nil, fmt.Errorf("remote manifest file %q is nil", role)
		}
		stat, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat remote manifest file %q: %w", role, err)
		}
		m.Files[role] = remoteManifestFile{
			Key:  filepath.Base(file.Name()),
			Size: stat.Size(),
		}
	}

	if err := m.validate(); err != nil {
		return nil, err
	}
	m.normalizeInfo()

	return m, nil
}

func (m *remoteManifest) file(role remoteFileRole) (remoteManifestFile, bool) {
	file, ok := m.Files[role]
	return file, ok
}

func (m *remoteManifest) keys() []string {
	keys := make([]string, 0, len(m.Files))
	for _, file := range m.Files {
		keys = append(keys, file.Key)
	}
	slices.Sort(keys)
	return keys
}

func (m *remoteManifest) normalizeInfo() {
	m.Info.MetaOnDisk = 0
	m.Info.InfoOnDisk = 0

	if m.Info.BinaryDataVer < config.BinaryDataV3 {
		m.Info.IndexOnDisk = uint64(m.Files[remoteFileIndex].Size)
		return
	}

	info := m.Files[remoteFileInfo]
	m.Info.InfoOnDisk = uint64(info.Size)
	m.Info.IndexOnDisk = m.Info.InfoOnDisk

	for _, role := range []remoteFileRole{
		remoteFileTokens,
		remoteFileOffsets,
		remoteFileIDs,
		remoteFileLIDs,
	} {
		m.Info.IndexOnDisk += uint64(m.Files[role].Size)
	}
}

func (m *remoteManifest) marshal() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode remote manifest: %w", err)
	}

	return append([]byte(remoteManifestMagic), data...), nil
}

func (m *remoteManifest) validate() error {
	if m.Version != remoteManifestVersion {
		return fmt.Errorf("unsupported remote manifest version %d", m.Version)
	}
	if m.Info == nil {
		return errors.New("remote manifest info is nil")
	}

	required := m.requiredFiles()
	if len(m.Files) != len(required) {
		return fmt.Errorf("remote manifest contains %d files, expected %d", len(m.Files), len(required))
	}

	seenKeys := make(map[string]struct{}, len(m.Files))
	for role, file := range m.Files {
		if _, ok := required[role]; !ok {
			return fmt.Errorf("remote manifest file role %q is not allowed", role)
		}
		if file.Key == "" {
			return fmt.Errorf("remote manifest file %q has empty key", role)
		}
		if file.Size < 0 {
			return fmt.Errorf("remote manifest file %q has negative size", role)
		}
		if _, ok := seenKeys[file.Key]; ok {
			return fmt.Errorf("remote manifest contains duplicate key %q", file.Key)
		}
		seenKeys[file.Key] = struct{}{}
	}

	for role := range required {
		if _, ok := m.file(role); !ok {
			return fmt.Errorf("remote manifest is missing %q", role)
		}
	}

	return nil
}

func (m *remoteManifest) requiredFiles() map[remoteFileRole]struct{} {
	if m.Info.BinaryDataVer < config.BinaryDataV3 {
		return legacyRemoteFileRoles
	}
	return splitRemoteFileRoles
}

func parseRemoteManifest(data []byte) (*remoteManifest, error) {
	if !hasRemoteManifestMagic(data) {
		return nil, errors.New("remote manifest magic mismatch")
	}

	var m remoteManifest
	if err := json.Unmarshal(data[len(remoteManifestMagic):], &m); err != nil {
		return nil, fmt.Errorf("decode remote manifest: %w", err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	m.normalizeInfo()
	return &m, nil
}

func hasRemoteManifestMagic(data []byte) bool {
	return len(data) >= len(remoteManifestMagic) && string(data[:len(remoteManifestMagic)]) == remoteManifestMagic
}

func writeRemoteManifest(path string, m *remoteManifest) error {
	data, err := m.marshal()
	if err != nil {
		return err
	}

	// The old path may be a hard link to .info. Unlink it before recreating the
	// file so replacing a pending remote marker cannot truncate the shared .info inode.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return err
	}

	written := false
	defer func() {
		if !written {
			_ = f.Close()
			_ = os.Remove(path)
		}
	}()

	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := util.SyncPath(filepath.Dir(path)); err != nil {
		return err
	}

	written = true

	return nil
}
