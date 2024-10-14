package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"lukechampine.com/frand"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

var baseTmpDir string

func MakeTouchTotal() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "touch_total",
		Help:      "",
	}, []string{"layer"})
}

func MakeHitsTotal() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_store",
		Subsystem: "cache",
		Name:      "hits_total",
		Help:      "",
	}, []string{"layer"})
}

func CreateDir(path string) {
	err := os.MkdirAll(path, 0o777)
	if err != nil {
		panic(err)
	}
}

func RemoveDir(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		panic(err)
	}
}

func RecreateDir(path string) {
	RemoveDir(path)
	CreateDir(path)
}

func GetBaseTestTmpDir() string {
	if baseTmpDir == "" {
		var err error
		if baseTmpDir, err = os.MkdirTemp("", "seq-db"); err != nil {
			panic(err)
		}
	}
	return baseTmpDir
}

func GetTestTmpDir(t *testing.T) string {
	return filepath.Join(GetBaseTestTmpDir(), t.Name())
}

func IDs(ids ...int) []seq.ID {
	r := make([]seq.ID, 0)
	for _, n := range ids {
		r = append(r, seq.SimpleID(n))
	}

	return r
}

func RandomString(minLen, maxLen int) string {
	size := frand.Intn(maxLen-minLen+1) + minLen
	res := make([]byte, size)
	for i := range res {
		res[i] = byte(frand.Intn(26)) + 'a'
	}
	return string(res)
}

func LoadMapping() query.Mapping {
	if m, err := query.LoadMapping(TestDataDir + "/mappings/logging.yaml"); err != nil {
		return m
	}

	return nil
}
