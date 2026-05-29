package compaction

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/alecthomas/units"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
)

type fraction interface {
	Info() *common.Info
}

const (
	// TODO(dkharms): Move this options to config.
	compactionTick   = time.Second
	compactionWindow = 24 * time.Hour
)

type task struct {
	bin        time.Time
	filename   string
	snapshot   *fracmanager.CompactionSnapshot
	onComplete func(*sealed.PreloadedData, error)
}

type planner struct {
	wg   sync.WaitGroup
	ctx  context.Context
	done chan struct{}

	fm    *fracmanager.FracManager
	tasks chan task

	mu sync.RWMutex
	// inflight tracks active compactions for each time-bin.
	// We cannot have concurrent compactions within one time-bin for correctness purposes.
	inflight map[time.Time]struct{}

	stats map[time.Time]int
}

func NewPlanner(ctx context.Context, fm *fracmanager.FracManager) *planner {
	p := planner{
		ctx:  ctx,
		done: make(chan struct{}),

		fm: fm,

		tasks: make(chan task),

		inflight: make(map[time.Time]struct{}),
		stats:    make(map[time.Time]int),
	}

	p.init()
	return &p
}

func (p *planner) init() {
	p.wg.Go(func() {
		t := time.NewTicker(compactionTick)

		for {
			select {
			case <-p.ctx.Done():
				close(p.tasks)
				return

			case <-p.done:
				close(p.tasks)
				return

			case <-t.C:
				task, ok := p.pick()
				if !ok {
					continue
				}

				select {
				case p.tasks <- task:
				case <-time.NewTimer(time.Second).C:
					// If all executor workers are busy for some long period of time,
					// we want to drop the task because it might contain stale decision.
				}
			}
		}
	})
}

func (p *planner) close() {
	close(p.done)
}

func (p *planner) pick() (task, bool) {
	fractions := p.fm.SealedFractionsSnapshot()
	snapshot := make([]fraction, len(fractions))

	for i := range fractions {
		snapshot[i] = fractions[i]
	}

	bins := p.distribute(compactionWindow, snapshot)
	times := p.prioritize(bins)

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, t := range times {
		if _, ok := p.inflight[t]; ok {
			// NOTE(dkharms): Currently we allow only one on-going compaction
			// per each time-bin however this might be not the best idea.
			// So, I will revise it later.
			continue
		}

		// TODO(dkharms): Move this options to config.
		picked := strategySTCS{
			mergeTrigger:     4,
			mergeFanIn:       32,
			mergeFanOutSize:  128 * uint64(units.MiB),
			bucketLowerbound: 0.5,
			bucketUpperbound: 1.5,
		}.Pick(bins[t].fracs)

		if len(picked) == 0 {
			// No candidates were found.
			continue
		}

		csnapshot, err := p.fm.ClaimForCompaction(names(picked))
		if err != nil {
			continue
		}

		p.inflight[t] = struct{}{}

		return task{
			bin: t,

			filename: p.fm.FractionName(),
			snapshot: csnapshot,

			onComplete: func(s *sealed.PreloadedData, err error) {
				p.mu.Lock()
				defer p.mu.Unlock()
				delete(p.inflight, t)

				if err != nil {
					logger.Error(
						"failed to compact fractions",
						zap.Error(err),
						zap.Any("snapshot", names(csnapshot.Fractions())),
					)
					return
				}

				if s == nil {
					logger.Info(
						"compaction did not produce fraction",
						zap.Any("snapshot", names(csnapshot.Fractions())),
					)
					return
				}

				// TODO(dkharms): Is it fine to substitute and delete?
				// We need somehow substitute and delete atomically.
				p.fm.SubstituteWithSealed(s, csnapshot)
				csnapshot.Destroy()
			},
		}, true
	}

	return task{}, false
}

type timestampBin struct {
	t     time.Time
	fracs []fraction
}

func (p *planner) distribute(window time.Duration, fracs []fraction) map[time.Time]timestampBin {
	bins := make(map[time.Time]timestampBin)

	for _, f := range fracs {
		from, to := f.Info().From.Time(), f.Info().To.Time()

		// Do not handle fractions which have
		// too wide date-range.
		if to.Sub(from) > window {
			continue
		}

		bin := from.Truncate(window)
		tb := bins[bin]

		tb.t = bin
		tb.fracs = append(tb.fracs, f)

		bins[bin] = tb
	}

	return bins
}

func (p *planner) prioritize(bins map[time.Time]timestampBin) []time.Time {
	// NOTE(dkharms): What other strategies we can use here?
	// (*) Prioritize by change rate;
	// (*) Prioritize by amount of fractions;

	ordered := slices.Collect(maps.Keys(bins))

	// Order timestamp-bins by the change-rate.
	// We will prioritize bins with higher change rate.
	slices.SortFunc(ordered, func(x, y time.Time) int {
		xold, xnew := p.stats[x], len(bins[x].fracs)
		yold, ynew := p.stats[y], len(bins[y].fracs)
		xchange, ychange := xnew-xold, ynew-yold

		p.stats[x], p.stats[y] = xnew, ynew

		if xchange == ychange {
			return -x.Compare(y)
		}

		return -(xchange - ychange)
	})

	return ordered
}

func names[T interface{ Info() *common.Info }, S ~[]T](fracs S) []string {
	fnames := make([]string, len(fracs))
	for i := range fracs {
		fnames[i] = fracs[i].Info().Name()
	}
	return fnames
}
