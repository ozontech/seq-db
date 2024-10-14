// package main implements a simple stress tool
// search is done over last hour only
// fetched is done over configurable period of time
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tracing"
)

const (
	reqTimeout           = time.Second * 20
	searchTpl            = `message:readyz OR (NOT message:%s AND message:%s)` // it is just whole lots of this 'readyz' in logs...
	fetchMaxBacklogHours = 10                                                  // do not go deeper in history for fetch, loop over instead
	tickerTime           = time.Second * 2                                     // stats ticker
	pageLen              = 150
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	rand.Seed(time.Now().UnixNano())

	endpoint := flag.String("endpoint", "127.0.0.1", "ip address : port")
	workers := flag.Int("workers", 1, "")
	mode := flag.String("mode", "search", "search/fetch/bulk")
	flag.Parse()

	log.Printf("Endpoint: %s workers: %d, performing %s", *endpoint, *workers, *mode)

	ctx, cancel := context.WithCancel(context.Background())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT)

	go func() {
		<-quit
		log.Println("Got signal, quitting")
		cancel()
	}()

	conn, err := grpc.DialContext(
		ctx,
		*endpoint,
		grpc.WithInsecure(),
		grpc.WithStatsHandler(&tracing.ClientHandler{}),
	)

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := storeapi.NewStoreApiClient(conn)

	switch *mode {
	case "search":
		stressSearch(ctx, client, *workers)
	case "fetch":
		stressFetch(ctx, client, *workers)
	default:
		log.Printf("Mode %s not implemented", *mode)
	}

}

func stressSearch(ctx context.Context, client storeapi.StoreApiClient, workers int) {
	var counter, errors, found, reported int64
	var lastErr atomic.Value

	go func() {
		ticker := time.NewTicker(tickerTime)
		for range ticker.C {
			log.Printf("Requests sent: %d, docs found: %d, ids reported:%d, errors: %d, last error: %v\n", atomic.LoadInt64(&counter), atomic.LoadInt64(&found), atomic.LoadInt64(&reported), atomic.LoadInt64(&errors), lastErr.Load())
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go searchWorker(ctx, &wg, &counter, &errors, &found, &reported, &lastErr, client)
	}
	wg.Wait()
}

func stressFetch(ctx context.Context, client storeapi.StoreApiClient, workers int) {
	var counter, errors, done int64
	var lastErr atomic.Value

	startTime := atomic.Value{}
	startTime.Store(time.Now())

	go func() {
		ticker := time.NewTicker(time.Second * 2)
		for range ticker.C {
			log.Printf("Requests sent: %d, done: %d, errors: %d, last error: %v\n", atomic.LoadInt64(&counter), atomic.LoadInt64(&done), atomic.LoadInt64(&errors), lastErr)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go fetchWorker(ctx, &wg, &counter, &errors, &done, &startTime, &lastErr, client)
	}
	wg.Wait()

}

func searchWorker(ctx context.Context, wg *sync.WaitGroup, counter, errors, total, reported *int64, lastErr *atomic.Value, client storeapi.StoreApiClient) {
	defer wg.Done()

	now := time.Now()
	from := now.Add(-time.Hour).UnixMilli()
	to := now.UnixMilli()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			req := newSearchReq(from, to, pageLen)
			atomic.AddInt64(counter, 1)

			qprs, err := doSearch(ctx, client, req)
			if err != nil {
				lastErr.Store(err)
				atomic.AddInt64(errors, 1)
				continue
			}
			atomic.AddInt64(total, int64(qprs.Total))
			atomic.AddInt64(reported, int64(len(qprs.IdSources)))
		}
	}
}

func prevHourSearch(ctx context.Context, client storeapi.StoreApiClient, from time.Time) (*storeapi.SearchResponse, error) {
	to := from.Add(time.Hour).UnixMilli()
	req := newSearchReq(from.UnixMilli(), to, math.MaxInt)
	return doSearch(ctx, client, req)
}

func fetchWorker(ctx context.Context, wg *sync.WaitGroup, counter, errors, done *int64, startTime, lastErr *atomic.Value, client storeapi.StoreApiClient) {
	defer wg.Done()

	var start time.Time

	var err error
	step := pageLen
	strIDs := make([]string, 0, step)

	var qprs *storeapi.SearchResponse
	var from, remains int

	for {
		select {
		case <-ctx.Done():
			return
		default:
			atomic.AddInt64(counter, 1)
			strIDs = strIDs[:0]

			if qprs == nil || remains == 0 {
				for {
					try := startTime.Load().(time.Time)
					if try.Before(time.Now().Add(-time.Hour * fetchMaxBacklogHours)) {
						start = time.Now().Add(-time.Hour)
					} else {
						start = try.Add(-time.Hour)
					}
					if startTime.CompareAndSwap(try, start) {
						break
					}
				}
				qprs, err = prevHourSearch(ctx, client, start)
				if err != nil {
					lastErr.Store(err)
					atomic.AddInt64(errors, 1)
					continue
				}
				remains = len(qprs.IdSources)
				from = 0
			}

			var i int
			for i = 0; i < step && i+from < len(qprs.IdSources); i++ {
				id := qprs.IdSources[i+from].Id
				strIDs = append(strIDs, seq.ID{
					MID: seq.MID(id.Mid),
					RID: seq.RID(id.Rid),
				}.String())
			}

			from += i
			remains -= i

			fFetched, err := doFetch(ctx, client, &storeapi.FetchRequest{
				Ids:     strIDs,
				Explain: false,
			})

			if err != nil {
				lastErr.Store(err) // not concurrent safe but we don't care
				atomic.AddInt64(errors, 1)
				continue
			}

			atomic.AddInt64(done, int64(fFetched))
		}
	}
}

func doSearch(ctx context.Context, client storeapi.StoreApiClient, req *storeapi.SearchRequest) (*storeapi.SearchResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, reqTimeout)
	defer cancel()

	return client.Search(ctx, req, grpc.MaxCallRecvMsgSize(256*consts.MB), grpc.MaxCallSendMsgSize(256*consts.MB))
}

func doFetch(ctx context.Context, client storeapi.StoreApiClient, req *storeapi.FetchRequest) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, reqTimeout)
	defer cancel()

	stream, err := client.Fetch(ctx, req, grpc.MaxCallRecvMsgSize(256*consts.MB), grpc.MaxCallSendMsgSize(256*consts.MB))
	if err != nil {
		return 0, err
	}

	var fetched int
	for {
		_, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fetched, err
		}

		fetched++
	}
	return fetched, nil
}

func newSearchReq(from, to, size int64) *storeapi.SearchRequest {
	query := fmt.Sprintf(searchTpl, "nonexistent"+strconv.Itoa(rand.Int()), "nonexistent"+strconv.Itoa(rand.Int()))

	req := storeapi.SearchRequest{
		Query:       query,
		From:        from,
		To:          to,
		Size:        size,
		Offset:      0,
		Interval:    to - from,
		Aggregation: "",
		Explain:     false,
	}

	return &req
}
