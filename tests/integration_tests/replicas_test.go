package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/proxy/search"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/tests/setup"
)

const (
	defaultDocFmt = `{"service":"a", "id":%d}`
)

func init() {
	os.Setenv("LOG_LEVEL", "fatal")
}

type ReplicasEnv struct {
	*setup.TestingEnv
}

func NewReplicaEnv(t *testing.T, config setup.TestingEnvConfig) ReplicasEnv {
	dir, err := os.MkdirTemp(os.TempDir(), "seqdb-replicas-*")
	if err != nil {
		t.Fatal(err)
	}
	config.DataDir = dir
	config.IngestorCount = 1
	config.StartStorePort = 1025 + rand.Intn(math.MaxInt16-1025)
	config.StartIngestorPort = 1025 + rand.Intn(math.MaxInt16-1025)

	env := ReplicasEnv{setup.NewTestingEnv(&config)}

	var s *stores.Stores
	if env.Config.HotFactor > 0 {
		s = env.Ingestor().Config.Search.HotStores
	} else {
		s = env.Ingestor().Config.Search.ReadStores
	}

	for _, replicaSets := range env.HotStores {
		var shards []string
		for _, replica := range replicaSets {
			shards = append(shards, replica.GrpcAddr())
		}
		s.Shards = append(s.Shards, shards)
	}
	return env
}

func (s *ReplicasEnv) tearDown() {
	s.StopAll()
	os.RemoveAll(s.Config.DataDir)
}

func init() {
	logger.SetLevel(zapcore.FatalLevel)
}

func TestAllUp(t *testing.T) {
	t.Parallel()
	env := NewReplicaEnv(t, setup.TestingEnvConfig{
		HotShards: 2,
		HotFactor: 2,
	})
	defer env.tearDown()

	expected := env.bulk(t)
	err := env.checkAll(t, expected)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReplicaDown(t *testing.T) {
	t.Parallel()
	env := NewReplicaEnv(t, setup.TestingEnvConfig{
		HotShards: 2,
		HotFactor: 2,
	})
	defer env.tearDown()

	env.HotStores[1][0].Stop()

	expected := env.bulk(t)
	err := env.checkAll(t, expected)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPartialResponse(t *testing.T) {
	t.Parallel()
	env := NewReplicaEnv(t, setup.TestingEnvConfig{
		HotShards: 2,
		HotFactor: 2,
	})
	defer env.tearDown()

	expected := env.bulk(t)

	env.HotStores[1][0].Stop()

	err := env.checkAll(t, expected)
	if err != nil {
		t.Fatal(err)
	}
}

func TestShardDown(t *testing.T) {
	t.Parallel()
	env := NewReplicaEnv(t, setup.TestingEnvConfig{
		HotShards: 2,
		HotFactor: 2,
	})
	defer env.tearDown()

	expected := env.bulk(t)

	env.HotStores[1][0].Stop()
	env.HotStores[1][1].Stop()

	err := env.checkAll(t, expected)
	if !errors.Is(err, consts.ErrPartialResponse) {
		t.Fatalf("expecting ErrPartialResponse, got %v", err)
	}
}

func (s *ReplicasEnv) bulk(t *testing.T) map[string]int {
	result := map[string]int{}
	for i := 0; i < s.HotStores.CountInstances(); i++ {
		doc := fmt.Sprintf(defaultDocFmt, i)
		result[doc]++
		b := setup.GenBuffer([]string{doc})
		r, err := http.Post(s.IngestorBulkAddr(), "", b)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode != 200 {
			t.Fatal("bad status code", r.StatusCode)
		}
	}
	s.WaitIdle()
	return result
}

func (s *ReplicasEnv) searchStores() (map[string]int, error) {
	result := map[string]int{}

	ingestor := s.Ingestor()

	_, docsStream, _, err := ingestor.SearchIngestor.Search(
		context.Background(),
		&search.SearchRequest{
			Explain:     false,
			Q:           []byte(`service:a`),
			Offset:      0,
			Size:        1000,
			Interval:    0,
			From:        0,
			To:          math.MaxUint64,
			WithTotal:   true,
			ShouldFetch: true,
		},
	)
	if err != nil {
		return result, err
	}
	for doc, err := docsStream.Next(); err == nil; doc, err = docsStream.Next() {
		result[string(doc.Data)]++
	}
	return result, nil
}

func (s *ReplicasEnv) checkAll(t *testing.T, expected map[string]int) error {
	s.WaitIdle()
	docs, err := s.searchStores()
	if err != nil {
		return err
	}
	for one := range expected {
		if _, ok := docs[one]; !ok {
			t.Fatalf("doc %s not found", one)
		}
	}
	return nil
}
