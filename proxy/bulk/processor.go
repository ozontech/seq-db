package bulk

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

const (
	rollingAverageItems   = 200
	resetBufferThreshold  = 2
	resetBufferMultiplier = 1.5
)

var (
	bulkTimeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "seq_db_ingestor",
		Subsystem: "bulk",
		Name:      "time_errors_total",
		Help:      "errors for time rules violation in events",
	}, []string{"cause"})

	parseErrors  = bulkTimeErrors.WithLabelValues("parse_error")
	delays       = bulkTimeErrors.WithLabelValues("delay")
	futureDelays = bulkTimeErrors.WithLabelValues("future_delay")
)

// processor accumulates meta and docs from a single bulk
// returns bulk request ready to be sent to store
type processor struct {
	indexer *indexer

	decoder  *insaneJSON.Root
	meta     []byte
	metasAvg *metric.RollingAverage
	docs     []byte
	docsAvg  *metric.RollingAverage

	proxyIndex  uint64
	drift       time.Duration
	futureDrift time.Duration

	maxBufCap   int
	maxPoolSize int
}

func newBulkProcessor(indexer *indexer, drift, futureDrift time.Duration, index uint64, maxBufCap, maxPoolSize int) *processor {
	return &processor{
		indexer:     indexer,
		decoder:     insaneJSON.Spawn(),
		meta:        []byte{},
		metasAvg:    metric.NewRollingAverage(rollingAverageItems),
		docs:        []byte{},
		docsAvg:     metric.NewRollingAverage(rollingAverageItems),
		proxyIndex:  index,
		drift:       drift,
		futureDrift: futureDrift,
		maxBufCap:   maxBufCap,
		maxPoolSize: maxPoolSize,
	}
}

func (p *processor) Process(doc []byte, requestTime time.Time) error {
	defer func() {
		if p.decoder.PoolSize() > p.maxPoolSize {
			p.decoder.ReleasePoolMem()
		}
		if p.decoder.BuffCap() > p.maxBufCap {
			p.decoder.ReleaseBufMem()
		}
	}()

	if err := p.decoder.DecodeBytes(doc); err != nil {
		return err
	}
	docTime, timeField := extractDocTime(p.decoder.Node, requestTime)
	docDelay := requestTime.Sub(docTime)
	if timeField == nil {
		// couldn't parse given event time
		parseErrors.Inc()
	} else if documentDelayed(docDelay, p.drift, p.futureDrift) {
		updateDocTime(p.decoder, timeField, requestTime.UTC(), docTime)
		docTime = requestTime
		doc = p.decoder.Encode(doc[:0])
	}

	p.appendDoc(doc)

	id := seq.NewID(docTime, (rand.Uint64()<<16)+p.proxyIndex)

	p.indexer.Index(p.decoder.Node, id, uint32(len(doc)), p.marshalAppendMeta)

	return nil
}

func documentDelayed(docDelay, drift, futureDrift time.Duration) bool {
	delayed := false
	if docDelay > drift {
		delays.Inc()
		delayed = true
	}
	if docDelay < 0 && -docDelay > futureDrift {
		futureDelays.Inc()
		delayed = true
	}
	return delayed
}

func (p *processor) appendDoc(docs []byte) {
	p.docs = binary.LittleEndian.AppendUint32(p.docs, uint32(len(docs)))
	p.docs = append(p.docs, docs...)
}

func extractDocTime(node *insaneJSON.Node, requestTime time.Time) (time.Time, []string) {
	for _, field := range consts.TimeFields {
		timeVal := node.Dig(field...).AsBytes()
		if len(timeVal) == 0 {
			continue
		}

		for _, f := range consts.TimeFormats {
			var t time.Time
			var ok bool
			if f == consts.ESTimeFormat {
				// Fallback to optimized es time parsing.
				t, ok = parseESTime(util.ByteToStringUnsafe(timeVal))
			} else {
				var err error
				t, err = time.Parse(f, util.ByteToStringUnsafe(timeVal))
				ok = err == nil
			}
			if ok {
				return t, field
			}
		}
	}
	defaultTime := requestTime
	return defaultTime, nil
}

// parseESTime parses time in "2006-01-02 15:04:05.999" format.
// It is copied and modified stdlib function time.parseRFC3339.
func parseESTime(t string) (time.Time, bool) {
	if len(t) < len("2006-01-02 15:04:05") {
		return time.Time{}, false
	}

	ok := true
	parseUint := func(s string, from, to uint) uint {
		x := uint(0)
		for _, c := range []byte(s) {
			if c < '0' || c > '9' {
				ok = false
				return 0
			}
			x = x*10 + uint(c) - '0'
		}
		if x < from || x > to {
			ok = false
			return 0
		}
		return x
	}

	year := parseUint(t[0:4], 0, 9999) // Parse YYYY
	month := parseUint(t[5:7], 1, 12)  // Parse MM
	// Day in a month will be checked in the Date function.
	day := parseUint(t[8:10], 1, 31)     // Parse DD
	hour := parseUint(t[11:13], 0, 23)   // Parse HH
	minute := parseUint(t[14:16], 0, 59) // Parse mm
	second := parseUint(t[17:19], 0, 59) // Parse ss
	if !ok || !(t[4] == '-' && t[7] == '-' && t[10] == ' ' && t[13] == ':' && t[16] == ':') {
		return time.Time{}, false
	}

	t = t[19:]
	nsecs := uint(0)
	if t != "" {
		if t[0] != '.' || len(t) == 1 {
			return time.Time{}, false
		}
		t = t[1:]

		// Parse nanoseconds.
		multi := uint(math.Pow10(9 - len(t)))
		if multi == 0 {
			multi = 1
		}
		nsecs = parseUint(t, 0, 999999999) * multi
		if !ok {
			return time.Time{}, false
		}
	}

	return time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), int(nsecs), time.UTC), true
}

func updateDocTime(root *insaneJSON.Root, fieldName []string, now, origTime time.Time) {
	setInsaneJSONValue(root, consts.OriginalTimeFieldName, origTime.Format(time.RFC3339Nano))
	setInsaneJSONValue(root, fieldName, now.Format(time.RFC3339Nano))
}

func setInsaneJSONValue(root *insaneJSON.Root, path []string, v string) {
	n := root.Dig(path...)
	if !n.IsNil() {
		n.MutateToString(v)
		return
	}

	n = root.Node
	for _, k := range path {
		if n.Dig(k).IsNil() || !n.Dig(k).IsObject() {
			n = n.AddField(k).MutateToObject()
			continue
		}
		n = n.Dig(k)
	}

	n.MutateToString(v)
}

func (p *processor) Cleanup() {
	p.docsAvg.Append(len(p.docs))
	if avg := p.docsAvg.Get(); p.docsAvg.Filled() && int(avg*resetBufferThreshold) < cap(p.docs) {
		p.docs = make([]byte, int(avg*resetBufferMultiplier))
	}
	p.docs = p.docs[:0]

	p.metasAvg.Append(len(p.meta))
	if avg := p.metasAvg.Get(); p.metasAvg.Filled() && int(avg*resetBufferThreshold) < cap(p.meta) {
		p.meta = make([]byte, int(avg*resetBufferMultiplier))
	}
	p.meta = p.meta[:0]
}

func (p *processor) marshalAppendMeta(meta frac.MetaData) {
	metaLenPosition := len(p.meta)
	p.meta = append(p.meta, make([]byte, 4)...)
	p.meta = meta.MarshalBinaryTo(p.meta)
	// Metadata length = len(slice after append) - len(slice before append).
	metaLen := uint32(len(p.meta) - metaLenPosition - 4)
	// Put metadata length before metadata bytes.
	binary.LittleEndian.PutUint32(p.meta[metaLenPosition:], metaLen)
}
