package common

import (
	"path/filepath"
	"runtime"
)

const (
	MockHTTPPortStart = 6060
	StorePortStart    = 9991
	IngestorPortStart = 9891
	Localhost         = "127.0.0.1"
)

var (
	_, b, _, _  = runtime.Caller(0)
	ProjectRoot = filepath.Join(filepath.Dir(b), "..", "..")
	TestsDir    = filepath.Join(ProjectRoot, "tests")

	TestDataDir = filepath.Join(TestsDir, "data")
)
