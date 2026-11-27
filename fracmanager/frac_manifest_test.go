package fracmanager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/consts"
)

func TestFracManifestStage(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  fracStage
	}{
		{
			name: "empty",
		},
		{
			name:  "active",
			files: []string{consts.WalFileSuffix, consts.DocsFileSuffix},
			want:  fracStageActive,
		},
		{
			name:  "sealed",
			files: []string{consts.SdocsFileSuffix, consts.InfoFileSuffix, consts.TokenFileSuffix, consts.OffsetsFileSuffix, consts.IDFileSuffix, consts.LIDFileSuffix},
			want:  fracStageSealed,
		},
		{
			name:  "sealed legacy",
			files: []string{consts.SdocsFileSuffix, consts.IndexFileSuffix},
			want:  fracStageSealed,
		},
		{
			name:  "remote",
			files: []string{consts.RemoteFractionSuffix},
			want:  fracStageRemote,
		},
		{
			// Offloading interrupted, local files are intact: offloading is retried.
			name:  "offload intent with local files",
			files: []string{consts.RemoteFractionTmpSuffix, consts.SdocsFileSuffix, consts.InfoFileSuffix, consts.TokenFileSuffix, consts.OffsetsFileSuffix, consts.IDFileSuffix, consts.LIDFileSuffix},
			want:  fracStageSealed,
		},
		{
			// Offloading interrupted, local files are gone: remote zombie.
			name:  "offload intent without local files",
			files: []string{consts.RemoteFractionTmpSuffix},
			want:  fracStageZombieRemote,
		},
		{
			name:  "offload intent with docs renamed",
			files: []string{consts.RemoteFractionTmpSuffix, consts.SdocsDelFileSuffix, consts.InfoFileSuffix, consts.TokenFileSuffix, consts.OffsetsFileSuffix, consts.IDFileSuffix, consts.LIDFileSuffix},
			want:  fracStageZombieRemote,
		},
		{
			name:  "offload intent with docs renamed legacy",
			files: []string{consts.RemoteFractionTmpSuffix, consts.DocsDelFileSuffix, consts.IndexFileSuffix},
			want:  fracStageZombieRemote,
		},
		{
			name:  "offload intent with partial index",
			files: []string{consts.RemoteFractionTmpSuffix, consts.SdocsFileSuffix, consts.TokenFileSuffix, consts.OffsetsFileSuffix, consts.IDFileSuffix, consts.LIDFileSuffix},
			want:  fracStageZombieRemote,
		},
		{
			// Local files are gone but deletion was interrupted.
			name:  "deleting marker",
			files: []string{consts.RemoteFractionDeletingSuffix},
			want:  fracStageZombieRemote,
		},
		{
			// Partially deleted: deletion markers rename data files before removal.
			name:  "zombie",
			files: []string{consts.DocsDelFileSuffix},
			want:  fracStageZombie,
		},
		{
			name:  "unknown",
			files: []string{consts.InfoFileSuffix},
			want:  fracStageUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fracManifest{basePath: "seq-db-xxx"}
			for _, ext := range tt.files {
				require.NoError(t, m.AddExtension(ext))
			}
			assert.Equal(t, tt.want, m.Stage())
		})
	}
}

func TestReplacementRecovery(t *testing.T) {
	tests := []struct {
		name        string
		outputFiles []string
		plan        string
		want        []string
	}{
		{
			name:        "ready output",
			outputFiles: replacementRecoverySealedFiles,
			plan:        "valid",
			want:        []string{"output"},
		},
		{
			name:        "partial output",
			outputFiles: []string{consts.InfoFileSuffix},
			plan:        "valid",
			want:        []string{"source-a", "source-b"},
		},
		{
			name:        "corrupt plan",
			outputFiles: replacementRecoverySealedFiles,
			plan:        "corrupt",
			want:        []string{"source-a", "source-b"},
		},
		{
			name: "plan only",
			plan: "valid",
			want: []string{"source-a", "source-b"},
		},
		{
			name:        "temporary plan",
			outputFiles: replacementRecoverySealedFiles,
			plan:        "temporary",
			want:        []string{"output", "source-a", "source-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			sources := []string{"source-a", "source-b"}
			for _, id := range sources {
				base := filepath.Join(dir, fileBasePattern+id)
				writeReplacementRecoveryFiles(t, base, replacementRecoverySealedFiles)
			}

			outputBase := filepath.Join(dir, fileBasePattern+"output")
			writeReplacementRecoveryFiles(t, outputBase, tt.outputFiles)

			switch tt.plan {
			case "valid":
				require.NoError(t, writeReplacementPlan(outputBase, []string{
					fileBasePattern + "source-a",
					fileBasePattern + "source-b",
				}))
			case "corrupt":
				require.NoError(t, os.WriteFile(outputBase+consts.ReplacementPlan, []byte("{"), defaultFilePermission))
			case "temporary":
				require.NoError(t, os.WriteFile(outputBase+consts.ReplacementPlanTmp, []byte("{}"), defaultFilePermission))
			}

			files, err := filepath.Glob(filepath.Join(dir, fileBasePattern+"*"))
			require.NoError(t, err)
			manifests, err := analyzeFiles(files)
			require.NoError(t, err)

			got := make([]string, len(manifests))
			for i, m := range manifests {
				got[i] = filepath.Base(m.basePath)
			}
			want := make([]string, len(tt.want))
			for i, id := range tt.want {
				want[i] = fileBasePattern + id
			}
			require.ElementsMatch(t, want, got)

			wantSet := make(map[string]struct{}, len(tt.want))
			for _, id := range tt.want {
				wantSet[id] = struct{}{}
			}

			for _, id := range sources {
				base := filepath.Join(dir, fileBasePattern+id)
				if _, ok := wantSet[id]; ok {
					require.FileExists(t, base+consts.DocsFileSuffix)
				} else {
					requireReplacementRecoveryFilesAbsent(t, base)
				}
			}

			if _, ok := wantSet["output"]; ok {
				require.FileExists(t, outputBase+consts.DocsFileSuffix)
			} else {
				requireReplacementRecoveryFilesAbsent(t, outputBase)
			}
			require.NoFileExists(t, outputBase+consts.ReplacementPlan)
			require.NoFileExists(t, outputBase+consts.ReplacementPlanTmp)
		})
	}
}

var replacementRecoverySealedFiles = []string{
	consts.DocsFileSuffix,
	consts.InfoFileSuffix,
	consts.TokenFileSuffix,
	consts.OffsetsFileSuffix,
	consts.IDFileSuffix,
	consts.LIDFileSuffix,
}

var replacementRecoveryAllFiles = []string{
	consts.DocsFileSuffix,
	consts.InfoFileSuffix,
	consts.TokenFileSuffix,
	consts.OffsetsFileSuffix,
	consts.IDFileSuffix,
	consts.LIDFileSuffix,
	consts.ReplacementPlan,
	consts.ReplacementPlanTmp,
}

func writeReplacementRecoveryFiles(t *testing.T, base string, suffixes []string) {
	t.Helper()
	for _, suffix := range suffixes {
		require.NoError(t, os.WriteFile(base+suffix, []byte{}, defaultFilePermission))
	}
}

func requireReplacementRecoveryFilesAbsent(t *testing.T, base string) {
	t.Helper()
	for _, suffix := range replacementRecoveryAllFiles {
		require.NoFileExists(t, base+suffix)
	}
}
