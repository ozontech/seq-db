package compaction

import (
	"cmp"
	"context"
	"maps"
	"math/bits"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

type Config struct {
	Enabled bool

	MergeTrigger    int
	MergeFanIn      int
	MergeFanOutSize uint64

	BucketLowerbound float64
	BucketUpperbound float64

	Workers      int
	TimeWindow   time.Duration
	TickInterval time.Duration
}

type fraction interface {
	Info() *common.Info
}

type task struct {
	bin        time.Time
	bucketSize string

	filename string
	snapshot *fracmanager.CompactionSnapshot

	onComplete func(*sealed.PreloadedData, error)
}

type planner struct {
	cfg Config

	ctx    context.Context
	cancel func()

	wg sync.WaitGroup

	fm    *fracmanager.FracManager
	tasks chan task
}

func NewPlanner(ctx context.Context, fm *fracmanager.FracManager, cfg Config) *planner {
	ctx, cancel := context.WithCancel(ctx)

	p := planner{
		cfg: cfg,

		ctx:    ctx,
		cancel: cancel,

		fm:    fm,
		tasks: make(chan task),
	}

	if cfg.Enabled {
		p.init()
		return &p
	}

	return &p
}

func (p *planner) init() {
	p.wg.Go(func() {
		t := time.NewTicker(p.cfg.TickInterval)

		for {
			select {
			case <-p.ctx.Done():
				close(p.tasks)
				return

			case <-t.C:
				task, ok := p.pick()
				if !ok {
					compactionSkipped.Inc()
					continue
				}

				select {
				case p.tasks <- task:
				case <-time.NewTimer(time.Second).C:
					p.fm.ReleaseSnapshot(task.snapshot)
					// If all executor workers are busy for some long period of time,
					// we want to drop the task because it might contain stale decision.
					compactionSkipped.Inc()
				}
			}
		}
	})
}

func (p *planner) stop() {
	if !p.cfg.Enabled {
		close(p.tasks)
	}

	p.cancel()
	p.wg.Wait()
}

func (p *planner) pick() (task, bool) {
	fractions := p.fm.SealedFractionsSnapshot()
	snapshot := make([]fraction, len(fractions))

	for i := range fractions {
		snapshot[i] = fractions[i]
	}

	bins := p.distribute(p.cfg.TimeWindow, snapshot)
	compactionBins.Set(float64(len(bins)))
	times := p.prioritize(bins)

	for _, t := range times {
		picked := strategySTCS{
			mergeTrigger:     p.cfg.MergeTrigger,
			mergeFanIn:       p.cfg.MergeFanIn,
			mergeFanOutSize:  p.cfg.MergeFanOutSize,
			bucketLowerbound: p.cfg.BucketLowerbound,
			bucketUpperbound: p.cfg.BucketUpperbound,
		}.Pick(bins[t])

		if len(picked.fracs) == 0 {
			// No candidates were found.
			continue
		}

		bucketSize := util.SizeStr(ceilPowerOfTwo(picked.sizeAvg))
		csnapshot, err := p.fm.ClaimForCompaction(names(picked.fracs))
		if err != nil {
			continue
		}

		return task{
			bin:        t,
			bucketSize: bucketSize,

			filename: p.fm.FractionName(),
			snapshot: csnapshot,

			onComplete: func(s *sealed.PreloadedData, err error) {
				if err != nil {
					compactionResultTotal.WithLabelValues(bucketSize, "error").Inc()

					logger.Error(
						"failed to compact fractions",
						zap.Error(err),
						zap.Any("snapshot", names(csnapshot.Fractions())),
					)

					p.fm.ReleaseSnapshot(csnapshot)
					return
				}

				if s == nil {
					logger.Info(
						"compaction did not produce fraction",
						zap.Any("snapshot", names(csnapshot.Fractions())),
					)

					p.fm.ReleaseSnapshot(csnapshot)
					return
				}

				compactionResultTotal.WithLabelValues(bucketSize, "success").Inc()

				p.fm.SubstituteWithSealed(s, csnapshot)
				csnapshot.Destroy()

				// We have destroyed all sealed fractions which participated
				// in compaction and now stale. So we can drop compaction plan.
				util.RemoveFile(s.Info.Path + consts.CompactionPlan)
			},
		}, true
	}

	return task{}, false
}

func (p *planner) distribute(window time.Duration, fracs []fraction) map[time.Time][]fraction {
	bins := make(map[time.Time][]fraction)

	for _, f := range fracs {
		// TODO(dkharms): Group by time-range fraction cover.
		//
		// Once we implement timestamp-binning, we need to group fractions into bins
		// not by creation time, but by time-range they cover.
		creation := time.UnixMilli(int64(f.Info().CreationTime))

		bin := creation.Truncate(window)
		bins[bin] = append(bins[bin], f)
	}

	return bins
}

func (p *planner) prioritize(bins map[time.Time][]fraction) []time.Time {
	ordered := slices.Collect(maps.Keys(bins))

	// Prioritize bins with the most fractions above target since they hurt search the most.
	// Older bins are preferred on ties since they have been sitting above target longer.
	slices.SortFunc(ordered, func(x, y time.Time) int {
		xcount := len(bins[x])
		ycount := len(bins[y])
		if xcount == ycount {
			return -x.Compare(y)
		}
		return -cmp.Compare(xcount, ycount)
	})

	return ordered
}

func names[T fraction, S ~[]T](fracs S) []string {
	fnames := make([]string, len(fracs))
	for i := range fracs {
		fnames[i] = fracs[i].Info().Name()
	}
	return fnames
}

func ceilPowerOfTwo(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	return 1 << bits.Len64(v-1)
}
