package bare_store

import (
	"context"
	"time"

	"github.com/ozontech/seq-db/seq"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	api "github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/storeapi"
)

type BareStore interface {
	Stop()
	Bulk(req *api.BulkRequest) error
	Search(req *api.SearchRequest) error
}

type bareStoreImpl struct {
	fracManager *fracmanager.FracManager
	impl        *storeapi.GrpcV1
}

func NewBareStore(config *storeapi.StoreConfig, mapping seq.Mapping) BareStore {
	s := &bareStoreImpl{}

	s.fracManager = fracmanager.NewFracManager(&config.FracManager)
	if err := s.fracManager.Load(context.Background()); err != nil {
		logger.Panic("fracmanager load error", zap.Error(err))
	}
	s.fracManager.Start()

	s.impl = storeapi.NewGrpcV1(config.API, s.fracManager, mapping)

	return s
}

func (s *bareStoreImpl) Stop() {
	s.fracManager.WaitIdle()
	s.fracManager.Stop()
}

func (s *bareStoreImpl) Bulk(req *api.BulkRequest) error {
	_, err := s.impl.Bulk(context.Background(), req)
	return err
}

func (s *bareStoreImpl) Search(req *api.SearchRequest) error {
	_, err := s.impl.Search(context.Background(), req)
	return err
}

type suppressErrors struct {
	BareStore
}

func SuppressErrors(store BareStore) BareStore {
	return &suppressErrors{
		BareStore: store,
	}
}

func (i *suppressErrors) Bulk(req *api.BulkRequest) error {
	_ = i.BareStore.Bulk(req)
	return nil
}

func (i *suppressErrors) Search(req *api.SearchRequest) error {
	_ = i.BareStore.Search(req)
	return nil
}

type MockStore struct {
	Sleep time.Duration
}

func (s *MockStore) Stop() {}

func (s *MockStore) Bulk(*api.BulkRequest) error {
	time.Sleep(s.Sleep)
	return nil
}

func (s *MockStore) Search(*api.SearchRequest) error {
	time.Sleep(s.Sleep)
	return nil
}
