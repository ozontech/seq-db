package common

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"lukechampine.com/frand"
)

var baseTmpDir string

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

var tmpDirMu = sync.Mutex{}

func CreateTempDir() string {
	tmpDirMu.Lock()
	defer tmpDirMu.Unlock()

	if baseTmpDir == "" {
		var err error
		if baseTmpDir, err = os.MkdirTemp("", "seq-db"); err != nil {
			panic(err)
		}
	}
	return baseTmpDir
}

func GetTestTmpDir(t *testing.T) string {
	return filepath.Join(CreateTempDir(), t.Name())
}

func RandomString(minLen, maxLen int) string {
	size := frand.Intn(maxLen-minLen+1) + minLen
	res := make([]byte, size)
	for i := range res {
		res[i] = byte(frand.Intn(26)) + 'a'
	}
	return string(res)
}
