package config

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidation(t *testing.T) {
	base := `storage:
  data_dir: /seq-db-data
  frac_size: 16MiB
  total_size: 10GiB

mapping:
  path: /configs/mapping.yaml

resources:
  cache_size: 2GiB

limits:
  query_rate: 1024
  search_requests: 1024
  bulk_requests: 128
  inflight_bulks: 128
  doc_size: 1MiB
`

	baseCfg := createCfgFile(t, base)

	tests := []struct {
		name      string
		cfg       string
		env       map[string]string
		expectErr bool
	}{
		{
			name:      "Invalid storage.sealing_queue_len 1",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_STORAGE_SEALING_QUEUE_LEN": "-1"},
			expectErr: true,
		},
		{
			name:      "Valid storage.sealing_queue_len 2",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_STORAGE_SEALING_QUEUE_LEN": "0"},
			expectErr: false,
		},
		{
			name:      "Valid storage.sealing_queue_len 3",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_STORAGE_SEALING_QUEUE_LEN": "100"},
			expectErr: false,
		},

		{
			name:      "Invalid offloading.queue_size_percent 1",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_OFFLOADING_QUEUE_SIZE_PERCENT": "-1"},
			expectErr: true,
		},
		{
			name:      "Invalid offloading.queue_size_percent 2",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_OFFLOADING_QUEUE_SIZE_PERCENT": "100.1"},
			expectErr: true,
		},
		{
			name:      "Valid offloading.queue_size_percent 3",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_OFFLOADING_QUEUE_SIZE_PERCENT": "0"},
			expectErr: false,
		},
		{
			name:      "Valid offloading.queue_size_percent 4",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_OFFLOADING_QUEUE_SIZE_PERCENT": "100"},
			expectErr: false,
		},
		{
			name:      "Valid offloading.queue_size_percent 5",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_OFFLOADING_QUEUE_SIZE_PERCENT": "50"},
			expectErr: false,
		},
		{
			name:      "Invalid sealing.lids.block_size",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_SEALING_LIDS_BLOCK_SIZE": "-1"},
			expectErr: true,
		},
		{
			name:      "Negative sealing.lids.bitmap_threshold",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_SEALING_LIDS_BITMAP_THRESHOLD": "-1"},
			expectErr: true,
		},
		{
			name:      "Valid sealing.lids.block_size",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_SEALING_LIDS_BLOCK_SIZE": "8192"},
			expectErr: false,
		},
		{
			name: "Valid sealing.lids.bitmap_threshold",
			cfg: createCfgFile(t, base+`
sealing:
  lids:
    block_size: 100
    bitmap_threshold: 100
`),
			expectErr: false,
		},
		{
			name: "Invalid sealing.lids.bitmap_threshold",
			cfg: createCfgFile(t, base+`
sealing:
  lids:
    block_size: 100
    bitmap_threshold: 101
`),
			expectErr: true,
		},
		{
			name: "Invalid sealing.tokens.block_size",
			cfg: createCfgFile(t, base+`
sealing:
  tokens:
    block_size: -1B
`),
			expectErr: true,
		},
		{
			name: "Valid sealing.tokens.freq_threshold_percentage",
			cfg: createCfgFile(t, base+`
sealing:
  tokens:
    freq_threshold_percentage: 0.005
`),
			expectErr: false,
		},
		{
			name: "Valid large sealing.tokens.freq_threshold_percentage",
			cfg: createCfgFile(t, base+`
sealing:
  tokens:
    freq_threshold_percentage: 99.5
`),
			expectErr: false,
		},
		{
			name: "Valid max value for sealing.tokens.freq_threshold_percentage",
			cfg: createCfgFile(t, base+`
sealing:
  tokens:
    freq_threshold_percentage: 100.0
`),
			expectErr: false,
		},
		{
			name: "Invalid negative sealing.tokens.freq_threshold_percentage",
			cfg: createCfgFile(t, base+`
sealing:
  tokens:
    freq_threshold_percentage: -0.005
`),
			expectErr: true,
		},
		{
			name: "Invalid large sealing.tokens.freq_threshold_percentage",
			cfg: createCfgFile(t, base+`
sealing:
  tokens:
    freq_threshold_percentage: 100.03
`),
			expectErr: true,
		},
		{
			name:      "Valid sealing.tokens.block_size",
			cfg:       baseCfg,
			env:       map[string]string{"SEQDB_SEALING_TOKENS_BLOCK_SIZE": "32KiB"},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			c, err := Parse(tt.cfg)
			assert.NoError(t, err)

			res := c.Validate("store")
			if tt.expectErr {
				assert.Error(t, res)
			} else {
				assert.NoError(t, res)
			}
		})
	}

}

func createCfgFile(t *testing.T, data string) string {
	f := path.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(f, []byte(data), 0o666)
	assert.NoError(t, err)

	abs, err := filepath.Abs(f)
	assert.NoError(t, err)
	return abs
}
