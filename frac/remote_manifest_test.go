package frac

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

func testRemoteManifest(legacy bool) *remoteManifest {
	version := config.CurrentFracVersion
	if legacy {
		version = config.BinaryDataV2
	}

	m := &remoteManifest{
		Version: remoteManifestVersion,
		Info: &common.Info{
			Path:          "/data/seq-db-test",
			BinaryDataVer: version,
			DocsTotal:     42,
			From:          seq.MillisToMID(1),
			To:            seq.MillisToMID(2),
			MetaOnDisk:    100,
			IndexOnDisk:   999,
		},
		Files: map[remoteFileRole]remoteManifestFile{
			remoteFileDocs: {Key: "seq-db-test.sdocs", Size: 100},
		},
	}

	if legacy {
		m.Files[remoteFileIndex] = remoteManifestFile{Key: "seq-db-test.index", Size: 500}
		return m
	}

	m.Files[remoteFileInfo] = remoteManifestFile{Key: "seq-db-test.info", Size: 10}
	m.Files[remoteFileTokens] = remoteManifestFile{Key: "seq-db-test.tokens", Size: 20}
	m.Files[remoteFileOffsets] = remoteManifestFile{Key: "seq-db-test.offsets", Size: 30}
	m.Files[remoteFileIDs] = remoteManifestFile{Key: "seq-db-test.ids", Size: 40}
	m.Files[remoteFileLIDs] = remoteManifestFile{Key: "seq-db-test.lids", Size: 50}

	return m
}

func TestRemoteManifestRoundTrip(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "split", true: "legacy"}[legacy], func(t *testing.T) {
			m := testRemoteManifest(legacy)
			data, err := m.marshal()
			require.NoError(t, err)
			require.True(t, hasRemoteManifestMagic(data))

			again, err := m.marshal()
			require.NoError(t, err)
			assert.Equal(t, data, again)

			parsed, err := parseRemoteManifest(data)
			require.NoError(t, err)

			assert.Equal(t, m.Files, parsed.Files)
			assert.Equal(t, uint32(42), parsed.Info.DocsTotal)
			assert.Equal(t, seq.MillisToMID(1), parsed.Info.From)
			assert.Equal(t, seq.MillisToMID(2), parsed.Info.To)
			assert.Zero(t, parsed.Info.MetaOnDisk)

			if legacy {
				assert.Zero(t, parsed.Info.InfoOnDisk)
				assert.Equal(t, uint64(500), parsed.Info.IndexOnDisk)
			} else {
				assert.Equal(t, uint64(10), parsed.Info.InfoOnDisk)
				assert.Equal(t, uint64(150), parsed.Info.IndexOnDisk)
			}
		})
	}
}

func TestRemoteManifestValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*remoteManifest)
	}{
		{
			name:   "version",
			mutate: func(m *remoteManifest) { m.Version++ },
		},
		{
			name:   "nil info",
			mutate: func(m *remoteManifest) { m.Info = nil },
		},
		{
			name: "negative size",
			mutate: func(m *remoteManifest) {
				file := m.Files[remoteFileDocs]
				file.Size = -1
				m.Files[remoteFileDocs] = file
			},
		},
		{
			name: "empty key",
			mutate: func(m *remoteManifest) {
				file := m.Files[remoteFileDocs]
				file.Key = ""
				m.Files[remoteFileDocs] = file
			},
		},
		{
			name: "unknown role",
			mutate: func(m *remoteManifest) {
				delete(m.Files, remoteFileDocs)
				m.Files["unknown"] = remoteManifestFile{Key: "unknown", Size: 1}
			},
		},
		{
			name: "duplicate key",
			mutate: func(m *remoteManifest) {
				m.Files[remoteFileIndex] = remoteManifestFile{Key: m.Files[remoteFileDocs].Key, Size: 1}
			},
		},
		{
			name:   "missing role",
			mutate: func(m *remoteManifest) { delete(m.Files, remoteFileDocs) },
		},
		{
			name: "legacy role",
			mutate: func(m *remoteManifest) {
				delete(m.Files, remoteFileDocs)
				m.Files[remoteFileIndex] = remoteManifestFile{Key: "other.index", Size: 1}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testRemoteManifest(false)
			tt.mutate(m)
			_, err := m.marshal()
			require.Error(t, err)
		})
	}
}

func TestParseRemoteManifestRejectsMalformedInput(t *testing.T) {
	_, err := parseRemoteManifest([]byte(remoteManifestMagic + "{"))
	require.Error(t, err)

	_, err = parseRemoteManifest([]byte("unknown"))
	require.Error(t, err)
}

func TestRemoteManifestDrivesFileOpening(t *testing.T) {
	m := testRemoteManifest(false)
	manifestFile := m.Files[remoteFileTokens]
	manifestFile.Key = "custom/nested/tokens"
	m.Files[remoteFileTokens] = manifestFile

	r := Remote{manifest: m}
	opened, err := r.openRemoteFile(consts.TokenFileSuffix, true)
	require.NoError(t, err)
	assert.Equal(t, "custom/nested/tokens", opened.Name())

	stat, err := opened.Stat()
	require.NoError(t, err)

	assert.Equal(t, int64(20), stat.Size())
}

func TestRemoteManifestDrivesDeletionKeys(t *testing.T) {
	m := testRemoteManifest(false)
	for role, file := range m.Files {
		file.Key = "custom/" + file.Key
		m.Files[role] = file
	}

	r := Remote{BaseFileName: "/data/seq-db-test", manifest: m}
	assert.Equal(t, m.keys(), r.remoteKeys())
	assert.NotContains(t, r.remoteKeys(), "seq-db-test.docs")
	assert.NotContains(t, r.remoteKeys(), "seq-db-test.sdocs")
}

func TestWriteRemoteManifestReplacesLegacyHardLink(t *testing.T) {
	base := filepath.Join(t.TempDir(), "seq-db-test")
	infoPath := base + consts.InfoFileSuffix
	pendingPath := base + consts.RemoteFractionTmpSuffix
	originalInfo := []byte("original info")

	require.NoError(t, os.WriteFile(infoPath, originalInfo, 0o666))
	require.NoError(t, os.Link(infoPath, pendingPath))
	require.NoError(t, writeRemoteManifest(pendingPath, testRemoteManifest(false)))

	info, err := os.ReadFile(infoPath)
	require.NoError(t, err)
	assert.Equal(t, originalInfo, info)

	pending, err := os.ReadFile(pendingPath)
	require.NoError(t, err)
	_, err = parseRemoteManifest(pending)
	require.NoError(t, err)

	infoStat, err := os.Stat(infoPath)
	require.NoError(t, err)
	pendingStat, err := os.Stat(pendingPath)
	require.NoError(t, err)
	assert.False(t, os.SameFile(infoStat, pendingStat))
}

func TestRemoteManifestErrorIsNotHiddenByCache(t *testing.T) {
	base := filepath.Join(t.TempDir(), "seq-db-test")
	data := append([]byte(remoteManifestMagic), '{')
	require.NoError(t, os.WriteFile(base+consts.RemoteFractionSuffix, data, 0o666))

	r := Remote{BaseFileName: base, info: &common.Info{IndexOnDisk: 1}}
	loaded, err := r.tryLoadLocal(true)
	require.Error(t, err)
	assert.False(t, loaded)
	assert.Nil(t, r.manifest)
}

func TestTryLoadLocalMissingFile(t *testing.T) {
	r := Remote{BaseFileName: filepath.Join(t.TempDir(), "fraction")}

	loaded, err := r.tryLoadLocal(false)

	require.NoError(t, err)
	require.False(t, loaded)
	assert.Nil(t, r.manifest)
}

func TestTryLoadLocalLegacyInfoOverridesCache(t *testing.T) {
	base := filepath.Join(t.TempDir(), "seq-db-test")
	localInfo := &common.Info{
		Path:          base,
		BinaryDataVer: config.BinaryDataV2,
		DocsTotal:     42,
	}
	blockInfo := sealed.BlockInfo{Info: localInfo}
	data := blockInfo.Pack(nil)
	require.NoError(t, os.WriteFile(base+consts.RemoteFractionSuffix, data, 0o666))

	cachedInfo := &common.Info{
		Path:          base,
		BinaryDataVer: config.BinaryDataV2,
		DocsTotal:     100,
		IndexOnDisk:   4096,
	}
	r := Remote{BaseFileName: base, info: cachedInfo}

	loaded, err := r.tryLoadLocal(true)
	require.NoError(t, err)
	require.True(t, loaded)
	assert.Equal(t, localInfo.DocsTotal, r.info.DocsTotal)
	assert.NotSame(t, cachedInfo, r.info)
	assert.Nil(t, r.manifest)
}

func TestNewRemoteZombieUsesManifestKeys(t *testing.T) {
	for _, suffix := range []string{
		consts.RemoteFractionDeletingSuffix,
		consts.RemoteFractionTmpSuffix,
	} {
		t.Run(suffix, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "seq-db-test")

			manifest := testRemoteManifest(false)
			manifest.Info.Path = base
			manifest.Files[remoteFileDocs] = remoteManifestFile{Key: "custom-docs", Size: 100}
			manifest.Files[remoteFileInfo] = remoteManifestFile{Key: "custom-info", Size: 10}
			manifest.Files[remoteFileTokens] = remoteManifestFile{Key: "custom-tokens", Size: 20}
			manifest.Files[remoteFileOffsets] = remoteManifestFile{Key: "custom-offsets", Size: 30}
			manifest.Files[remoteFileIDs] = remoteManifestFile{Key: "custom-ids", Size: 40}
			manifest.Files[remoteFileLIDs] = remoteManifestFile{Key: "custom-lids", Size: 50}
			require.NoError(t, writeRemoteManifest(base+suffix, manifest))

			r := NewRemoteZombie(t.Context(), base, nil, nil)

			assert.Equal(t, manifest.Info.DocsTotal, r.info.DocsTotal)
			assert.Equal(t, manifest.keys(), r.remoteKeys())
		})
	}
}

func TestNewRemoteZombieChecksAllMarkers(t *testing.T) {
	base := filepath.Join(t.TempDir(), "seq-db-test")
	require.NoError(t, os.WriteFile(base+consts.RemoteFractionDeletingSuffix, nil, 0o666))

	manifest := testRemoteManifest(false)
	manifest.Info.Path = base
	manifest.Files[remoteFileDocs] = remoteManifestFile{Key: "custom-docs", Size: 100}
	manifest.Files[remoteFileInfo] = remoteManifestFile{Key: "custom-info", Size: 10}
	manifest.Files[remoteFileTokens] = remoteManifestFile{Key: "custom-tokens", Size: 20}
	manifest.Files[remoteFileOffsets] = remoteManifestFile{Key: "custom-offsets", Size: 30}
	manifest.Files[remoteFileIDs] = remoteManifestFile{Key: "custom-ids", Size: 40}
	manifest.Files[remoteFileLIDs] = remoteManifestFile{Key: "custom-lids", Size: 50}
	require.NoError(t, writeRemoteManifest(base+consts.RemoteFractionTmpSuffix, manifest))

	r := NewRemoteZombie(t.Context(), base, nil, nil)

	assert.Equal(t, manifest.keys(), r.remoteKeys())
}

func TestNewRemoteZombieWithoutManifestUsesLegacyKeys(t *testing.T) {
	base := filepath.Join(t.TempDir(), "seq-db-test")
	require.NoError(t, os.WriteFile(base+consts.RemoteFractionTmpSuffix, nil, 0o666))

	r := NewRemoteZombie(t.Context(), base, nil, nil)

	assert.Nil(t, r.manifest)
	assert.Equal(t, base, r.info.Path)
	assert.Equal(t, []string{
		filepath.Base(base) + consts.DocsFileSuffix,
		filepath.Base(base) + consts.SdocsFileSuffix,
		filepath.Base(base) + consts.IndexFileSuffix,
		filepath.Base(base) + consts.InfoFileSuffix,
		filepath.Base(base) + consts.TokenFileSuffix,
		filepath.Base(base) + consts.OffsetsFileSuffix,
		filepath.Base(base) + consts.IDFileSuffix,
		filepath.Base(base) + consts.LIDFileSuffix,
	}, r.remoteKeys())
}

func TestOffloadUsesManifestKeys(t *testing.T) {
	manifest := testRemoteManifest(false)
	dir := t.TempDir()

	files := make(map[remoteFileRole]storage.ImmutableFile, len(manifest.Files))
	for role, manifestFile := range manifest.Files {
		path := filepath.Join(dir, "subdir", manifestFile.Key)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, make([]byte, manifestFile.Size), 0o644))

		file, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })

		files[role] = file
	}

	fraction := &Sealed{
		BaseFileName: filepath.Join(dir, "seq-db-test"),
		info:         manifest.Info,
	}
	uploader := &recordingUploader{}
	require.NoError(t, fraction.offloadFiles(t.Context(), uploader, files))

	wantKeys := manifest.keys()
	slices.Sort(wantKeys)
	uploadedKeys := slices.Clone(uploader.keys)
	slices.Sort(uploadedKeys)
	assert.Equal(t, wantKeys, uploadedKeys)
}

type recordingUploader struct {
	mu   sync.Mutex
	keys []string
}

func (u *recordingUploader) Upload(_ context.Context, key string, _ storage.ImmutableFile) error {
	u.mu.Lock()
	u.keys = append(u.keys, key)
	u.mu.Unlock()

	return nil
}
