package fracmanager

import (
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
