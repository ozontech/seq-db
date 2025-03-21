package bulk

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/tokenizer"
	"github.com/ozontech/seq-db/util"
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
	proxyIndex  uint64
	drift       time.Duration
	futureDrift time.Duration

	indexer *indexer
	decoder *insaneJSON.Root
}

func init() {
	// Disable cache for the Dig() method.
	insaneJSON.MapUseThreshold = math.MaxInt32
}

func newBulkProcessor(mapping seq.Mapping, tokenizers map[seq.TokenizerType]tokenizer.Tokenizer, drift, futureDrift time.Duration, index uint64) *processor {
	return &processor{
		proxyIndex:  index,
		drift:       drift,
		futureDrift: futureDrift,
		indexer: &indexer{
			tokenizers: tokenizers,
			mapping:    mapping,
			metas:      []frac.MetaData{},
		},
		decoder: insaneJSON.Spawn(),
	}
}

var errNotAnObject = errors.New("not an object")

func (p *processor) Process(doc []byte, requestTime time.Time) ([]byte, []frac.MetaData, error) {
	err := p.decoder.DecodeBytes(doc)
	if err != nil {
		return nil, nil, err
	}
	if !p.decoder.IsObject() {
		return nil, nil, errNotAnObject
	}
	docTime, timeField := extractDocTime(p.decoder.Node, requestTime)
	docDelay := requestTime.Sub(docTime)
	if timeField == nil {
		// couldn't parse given event time
		parseErrors.Inc()
	} else if documentDelayed(docDelay, p.drift, p.futureDrift) {
		docTime = requestTime
	}

	id := seq.NewID(docTime, (rand.Uint64()<<16)+p.proxyIndex)

	p.indexer.Index(p.decoder.Node, id, uint32(len(doc)))

	return doc, p.indexer.Metas(), nil
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
