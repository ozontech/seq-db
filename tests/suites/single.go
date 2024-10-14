package suites

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storeapi"
	"github.com/ozontech/seq-db/tests/common"
	"github.com/ozontech/seq-db/tests/setup"
)

// Single suite for testing with 1 ingestor and 1 store
type Single struct {
	Base
	Env *setup.TestingEnv
}

// -- useful functions --

func (s *Single) Store() *storeapi.Store {
	if len(s.Env.ColdStores) > 0 {
		return s.Env.ColdStores[0][0]
	}
	return s.Env.HotStores[0][0]
}

func (s *Single) Ingestor() *setup.Ingestor {
	return s.Env.Ingestors[0]
}

func (s *Single) Bulk(docs []string) {
	setup.Bulk(s.T(), s.Env.IngestorBulkAddr(), docs)
	s.Env.WaitIdle()
}

func (s *Single) SearchDocs(query string, size int, order seq.DocsOrder) []string {
	_, docs1, _, err := s.Env.Search(query, size, setup.WithOrder(order))
	s.Require().NoError(err)
	r1 := common.ToStringSlice(docs1)

	_, docs2, _, err := s.Env.Search(query, size, setup.WithTotal(false), setup.WithOrder(order))
	s.Require().NoError(err)
	r2 := common.ToStringSlice(docs2)

	s.Require().Equal(r1, r2)

	if order.IsReverse() {
		slices.Reverse(r1)
	}

	return r1
}

func (s *Single) AssertDocsEqual(originalDocs []string, indexes []int, foundDocs []string) {
	if !s.Assert().Equal(len(indexes), len(foundDocs)) {
		if len(indexes) < len(foundDocs) {
			fmt.Printf("foundDocs:\n%v\n", strings.Join(foundDocs, "\n"))
		} else {
			fmt.Println("expectedDocs:")
			for _, ind := range indexes {
				fmt.Println(originalDocs[ind])
			}
		}
	}
	for i, doc := range foundDocs {
		if i < len(indexes) {
			s.Assert().Equalf(originalDocs[indexes[i]], doc, "docs not the same at %d index, expected doc index %d", i, indexes[i])
		}
	}
}

func (s *Single) AssertSearch(query string, originalDocs []string, indexes []int) {
	s.AssertDocsEqual(originalDocs, indexes, s.SearchDocs(query, math.MaxInt32, seq.DocsOrderDesc))
	s.AssertDocsEqual(originalDocs, indexes, s.SearchDocs(query, math.MaxInt32, seq.DocsOrderAsc))
}

type FractionEnv int

const (
	ActiveEnv FractionEnv = iota
	SealedEnv
	RestartedEnv
)

var AllFracEnvs = map[FractionEnv]bool{
	ActiveEnv:    true,
	SealedEnv:    true,
	RestartedEnv: true,
}

func (s *Single) RunFracEnvs(envs map[FractionEnv]bool, stopOnFail bool, f func()) {
	if envs[ActiveEnv] {
		// no setup
		if !stopOnFail || !s.T().Failed() {
			f()
		}
	}
	if envs[SealedEnv] {
		s.Env.SealAll()
		if !stopOnFail || !s.T().Failed() {
			f()
		}
	}
	if envs[RestartedEnv] {
		s.RestartStore()
		if !stopOnFail || !s.T().Failed() {
			f()
		}
	}
}

func (s *Single) RestartStore() {
	// if store is already stopped will just start
	s.Env.StopStore()
	s.Env.HotStores, _ = setup.MakeStores(s.Config, 1, false)
}

func (s *Single) RestartIngestor() {
	// if ingestor is already stopped will just start
	s.Env.StopIngestor()
	s.Env.Ingestors = setup.MakeIngestors(s.Config, [][]string{}, s.Env.Ingestor().Config.Search.ReadStores.Shards)
}

// -- setup --

func NewSingle(cfg *setup.TestingEnvConfig) *Single {
	return &Single{
		Base: *NewBase(cfg),
	}
}

func (s *Single) BeforeTest(suiteName, testName string) {
	s.Base.BeforeTest(suiteName, testName)
	s.Env = setup.NewTestingEnv(s.Config)
}

func (s *Single) AfterTest(suiteName, testName string) {
	s.Env.StopAll()
	s.Base.AfterTest(suiteName, testName)
}

func SingleEnvs() []*setup.TestingEnvConfig {
	envs := []*setup.TestingEnvConfig{
		{
			Name:          "Basic",
			IngestorCount: 1,
			HotShards:     1,
			HotFactor:     1,
		},
	}
	return envs
}
