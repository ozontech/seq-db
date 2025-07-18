package suites

import (
	"path/filepath"

	"github.com/stretchr/testify/suite"

	"github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tests/setup"
)

type Base struct {
	suite.Suite

	SuiteName string
	DataDir   string

	// can be nil, if tests don't require env
	Config *setup.TestingEnvConfig
}

func NewBase(config *setup.TestingEnvConfig) *Base {
	return &Base{Config: config}
}

func (s *Base) getDataDir(suiteName, testName string) string {
	name := ""
	if s.Config != nil {
		name = s.Config.Name
	}
	return filepath.Join(common.CreateTempDir(), suiteName+"_"+name+"_"+testName)
}

func (s *Base) RecreateDataDir() {
	common.RecreateDir(s.DataDir)
}

func (s *Base) BeforeTest(suiteName, testName string) {
	s.SuiteName = suiteName
	s.DataDir = s.getDataDir(suiteName, testName)
	if s.Config != nil {
		s.Config.DataDir = s.DataDir
	}
	s.RecreateDataDir()
}

func (s *Base) AfterTest(_, _ string) {
	common.RemoveDir(s.DataDir)
}

func (s *Base) TearDownSuite() {
}
