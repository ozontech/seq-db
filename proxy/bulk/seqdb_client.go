package bulk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cep21/circuit/v3"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/network/circuitbreaker"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/proxy/stores"
	"github.com/ozontech/seq-db/util"
)

type SeqDBClient struct {
	hotStores   bulkStores
	writeStores bulkStores
}

func NewSeqDBClient(hots, colds *stores.Stores, bulkCircuit circuitbreaker.Config, clients map[string]storeapi.StoreApiClient) *SeqDBClient {
	hotStores := newBulkStores("bulk_hot", hots.Shards, clients, bulkCircuit)
	writeStores := newBulkStores("bulk_write", colds.Shards, clients, bulkCircuit)

	return &SeqDBClient{
		hotStores:   hotStores,
		writeStores: writeStores,
	}
}

func (i *SeqDBClient) StoreDocuments(ctx context.Context, count int, docs, metas []byte) error {
	req := storeapi.BulkRequest{
		Count: int64(count),
		Docs:  docs,
		Metas: metas,
	}

	writeStatus := newBulkWriteStatus(i.hotStores.shardsCnt, i.hotStores.replicasCnt, i.writeStores.shardsCnt, i.writeStores.replicasCnt)

	for n := 0; n < consts.BulkMaxTries; n++ {
		startAttempt := time.Now()
		err := i.storeDocs(ctx, &req, writeStatus)

		if err == nil {
			metric.IngestorBulkSendAttemptDurationSeconds.Observe(time.Since(startAttempt).Seconds())
			break
		}
		metric.IngestorBulkAttemptErrorDurationSeconds.Observe(time.Since(startAttempt).Seconds())

		if n == consts.BulkMaxTries-1 {
			return fmt.Errorf("too many bulk retries: count=%d, last err=%w", n, err)
		}
		m := n * 100
		time.Sleep(time.Millisecond * time.Duration(m))
	}
	return nil
}

func (i *SeqDBClient) storeDocs(ctx context.Context, req *storeapi.BulkRequest, bulkWS *bulkWriteStatus) error {
	// TODO: need to make a normal commit process, to not write data to long term in case of error in hot
	if !bulkWS.coldWritten {
		if err := i.sendBulkToStores(ctx, req, i.writeStores.shards, bulkWS.writeStoresWS); err != nil {
			return fmt.Errorf("failed to send to long term stores: %w", err)
		}
		bulkWS.coldWritten = true
	} else {
		metric.IngestorBulkSkipCold.Inc()
	}
	if err := i.sendBulkToStores(ctx, req, i.hotStores.shards, bulkWS.hotStoresWS); err != nil {
		return fmt.Errorf("failed to send to hot stores: %w", err)
	}
	return nil
}

func (i *SeqDBClient) sendBulkToStores(
	ctx context.Context,
	req *storeapi.BulkRequest,
	shards []shard,
	storesWS *storesWriteStatus,
) error {
	if len(shards) == 0 {
		return nil
	}

	idx := util.IdxShuffle(len(shards))

	var err error
	for n := 0; n < len(shards); n++ {
		shardIdx := idx[n]
		shard := shards[shardIdx]
		shardWS := storesWS.getShard(shardIdx)
		if err = shard.Bulk(ctx, req, shardWS); err == nil {
			break
		}

		if !isOpenCircuitBreakerError(err) {
			logger.Error("can't store in the shard", zap.Error(err), zap.Int("shard", shardIdx))
		}
	}

	if err != nil {
		return fmt.Errorf("failed to send bulk to all shards, last error is: %w", err)
	}

	return nil
}

func isOpenCircuitBreakerError(err error) bool {
	var circuitBreakerErr circuit.Error
	return errors.As(err, &circuitBreakerErr) && circuitBreakerErr.CircuitOpen()
}

type bulkStores struct {
	shardsCnt   int
	replicasCnt int
	shards      []shard
}

func newBulkStores(breakerPrefix string, hosts [][]string, clients map[string]storeapi.StoreApiClient, config circuitbreaker.Config) bulkStores {
	bs := bulkStores{
		shardsCnt: len(hosts),
		shards:    make([]shard, 0, len(hosts)),
	}
	for i, replicaHosts := range hosts {
		breaker := circuitbreaker.New(fmt.Sprintf("%s-shard-%d", breakerPrefix, i), config)
		bs.shards = append(bs.shards, newShard(replicaHosts, clients, breaker))
		bs.replicasCnt = len(replicaHosts)
	}
	return bs
}

type replica struct {
	host   string
	client storeapi.StoreApiClient
}

type shard struct {
	replicas []replica
	breaker  *circuitbreaker.CircuitBreaker
}

func newShard(replicaHosts []string, clients map[string]storeapi.StoreApiClient, breaker *circuitbreaker.CircuitBreaker) shard {
	replicas := make([]replica, 0, len(replicaHosts))
	for _, host := range replicaHosts {
		replicas = append(replicas, replica{
			host:   host,
			client: clients[host],
		})
	}
	return shard{
		replicas: replicas,
		breaker:  breaker,
	}
}

func (s *shard) Bulk(ctx context.Context, req *storeapi.BulkRequest, writtenReplicas []bool) error {
	return s.breaker.Execute(ctx, func(ctx context.Context) error {
		wg := sync.WaitGroup{}
		hostErrors := make([]error, len(s.replicas))

		for i, replica := range s.replicas {
			replicaIdx := i
			replica := replica
			if len(writtenReplicas) > 0 && writtenReplicas[replicaIdx] {
				metric.IngestorBulkSkipShard.Inc()
				continue
			}
			wg.Add(1)
			go func() {
				hostErr := sendBulkToHost(ctx, replica, req)
				if hostErr != nil {
					metric.BulkErrors.Add(1)
					hostErrors[replicaIdx] = hostErr
				} else if writtenReplicas != nil {
					writtenReplicas[replicaIdx] = true
				}
				wg.Done()
			}()
		}
		wg.Wait()

		return multierr.Combine(hostErrors...)
	})
}

func sendBulkToHost(ctx context.Context, replica replica, req *storeapi.BulkRequest) error {
	_, err := replica.client.Bulk(ctx, req, grpc.MaxCallRecvMsgSize(256*consts.MB), grpc.MaxCallSendMsgSize(256*consts.MB))
	if err != nil {
		return fmt.Errorf("can't receive bulk acceptance: host=%s, err=%w", replica.host, err)
	}

	return nil
}
