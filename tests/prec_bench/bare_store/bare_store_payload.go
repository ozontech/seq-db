package bare_store

import (
	"sync"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/tests/prec_bench/bench"
)

type BareStorePayloadConfig struct {
	PreloadParallel int
	BulkParallel    int
	SearchParallel  int
}

func (c *BareStorePayloadConfig) GetPreloadParallel() int {
	parallel := c.PreloadParallel
	if parallel == 0 {
		return 1
	}
	return parallel
}

func (c *BareStorePayloadConfig) GetBulkParallel() int {
	parallel := c.BulkParallel
	if parallel == 0 {
		return 1
	}
	return parallel
}

func (c *BareStorePayloadConfig) GetSearchParallel() int {
	parallel := c.SearchParallel
	if parallel == 0 {
		return 1
	}
	return parallel
}

type BareStorePayload struct {
	PreloadBulkRequests <-chan *storeapi.BulkRequest
	BulkRequests        <-chan *storeapi.BulkRequest
	SearchRequests      <-chan *storeapi.SearchRequest
	store               BareStore
	config              *BareStorePayloadConfig
	bulkMeter           bench.Meter
	searchMeter         bench.Meter
	wgWorkers           sync.WaitGroup
}

func NewBench(
	config *BareStorePayloadConfig,
	store BareStore,
	bulkMeter bench.Meter,
	searchMeter bench.Meter,
) *BareStorePayload {
	p := &BareStorePayload{
		store:       store,
		config:      config,
		bulkMeter:   bulkMeter,
		searchMeter: searchMeter,
	}
	return p
}

func (p *BareStorePayload) Start(cleaner *bench.Cleaner) {
	if p.PreloadBulkRequests != nil {
		parallel := p.config.GetPreloadParallel()
		p.wgWorkers.Add(parallel)
		for i := 0; i < parallel; i++ {
			go p.processBulk(p.PreloadBulkRequests)
		}
	}
	p.wgWorkers.Wait()

	cleaner.WgTeardown.Add(1)
	if p.BulkRequests != nil {
		parallel := p.config.GetBulkParallel()
		p.wgWorkers.Add(parallel)
		for i := 0; i < parallel; i++ {
			if p.bulkMeter != nil {
				go p.processBulkWithMeter()
			} else {
				go p.processBulk(p.BulkRequests)
			}
		}
	}
	if p.SearchRequests != nil {
		parallel := p.config.GetSearchParallel()
		p.wgWorkers.Add(parallel)
		for i := 0; i < parallel; i++ {
			if p.searchMeter != nil {
				go p.processSearchWithMeter()
			} else {
				go p.processSearch()
			}
		}
	}

	go func() {
		p.wgWorkers.Wait()
		p.store.Stop()
		cleaner.WgTeardown.Done()
	}()
}

func (p *BareStorePayload) processBulk(requests <-chan *storeapi.BulkRequest) {
	for r := range requests {
		p.doBulk(r)
	}
	p.wgWorkers.Done()
}

func (p *BareStorePayload) processSearch() {
	for r := range p.SearchRequests {
		p.doSearch(r)
	}
	p.wgWorkers.Done()
}

func (p *BareStorePayload) processBulkWithMeter() {
	for r := range p.BulkRequests {
		p.bulkMeter.Step()
		p.doBulk(r)
	}
	p.wgWorkers.Done()
}

func (p *BareStorePayload) processSearchWithMeter() {
	for r := range p.SearchRequests {
		p.searchMeter.Step()
		p.doSearch(r)
	}
	p.wgWorkers.Done()
}

func (p *BareStorePayload) doBulk(r *storeapi.BulkRequest) {
	if err := p.store.Bulk(r); err != nil {
		logger.Panic("bulk error", zap.Error(err))
	}
}

func (p *BareStorePayload) doSearch(r *storeapi.SearchRequest) {
	if err := p.store.Search(r); err != nil {
		logger.Panic("search error", zap.Error(err))
	}
}
