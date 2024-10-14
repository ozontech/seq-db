package fracmanager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
)

type fracInfo struct {
	base        string
	hasDocs     bool
	hasDocsDel  bool
	hasIndex    bool
	hasIndexDel bool
	hasMeta     bool
}

type loader struct {
	config          *Config
	reader          *disk.Reader
	cacheMaintainer *CacheMaintainer
	indexWorkers    *frac.IndexWorkers
	fracCache       *sealedFracCache
}

func NewLoader(
	config *Config,
	reader *disk.Reader,
	cacheMaintainer *CacheMaintainer,
	indexWorkers *frac.IndexWorkers,
	fracCache *sealedFracCache,
) *loader {
	return &loader{
		config:          config,
		reader:          reader,
		cacheMaintainer: cacheMaintainer,
		indexWorkers:    indexWorkers,
		fracCache:       fracCache,
	}
}

func (t *loader) load(ctx context.Context) ([]*fracRef, []activeRef, error) {
	fracIDs, infos := t.makeInfos(t.getFileList())
	sort.Strings(fracIDs)

	if t.config.FracLoadLimit > 0 {
		logger.Info("preloading fractions", zap.Uint64("limit", t.config.FracLoadLimit))
		if len(fracIDs) > int(t.config.FracLoadLimit) {
			fracIDs = fracIDs[len(fracIDs)-int(t.config.FracLoadLimit):]
		}
	}

	infosList := t.filterInfos(fracIDs, infos)
	cnt := len(infosList)

	cachedFracs := 0
	uncachedFracs := 0
	fracs := make([]*fracRef, 0, cnt)
	actives := make([]*frac.Active, 0)

	diskFracCache := NewFracCacheFromDisk(filepath.Join(t.config.DataDir, consts.FracCacheFileSuffix))
	ts := time.Now()

	for i, info := range infosList {
		if info.hasMeta {
			actives = append(actives, frac.NewActive(info.base, t.config.ShouldRemoveMeta, t.indexWorkers, t.reader, t.cacheMaintainer.CreateDocBlockCache()))
		} else {
			cachedFracInfo, ok := diskFracCache.GetFracInfo(filepath.Base(info.base))
			if ok {
				cachedFracs++
			} else {
				uncachedFracs++
			}

			sealed := frac.NewSealed(info.base, t.reader, t.cacheMaintainer.CreateSealedIndexCache(), t.cacheMaintainer.CreateDocBlockCache(), cachedFracInfo)
			fracs = append(fracs, &fracRef{instance: sealed})

			stats := sealed.Info()
			t.fracCache.AddFraction(stats.Name(), stats)
		}

		if time.Since(ts) >= time.Second || i == len(infosList)-1 {
			ts = time.Now()
			p := 100 * (i + 1) / cnt
			logger.Info(
				"preloading",
				zap.String("progress", fmt.Sprintf("%d%%", p)),
				zap.Int("fracs_total", cnt),
				zap.Int("fracs_loaded", i+1),
			)
		}
	}

	logger.Info("fractions list created", zap.Int("cached", cachedFracs), zap.Int("uncached", uncachedFracs))

	logger.Info("replaying active fractions", zap.Int("count", len(actives)))
	notSealed := make([]activeRef, 0)
	for _, a := range actives {
		if err := a.ReplayBlocks(ctx); err != nil {
			return nil, nil, fmt.Errorf("while replaying blocks: %w", err)
		}
		if a.Info().DocsTotal == 0 { // skip empty
			removeFractionFiles(a.BaseFileName)
			continue
		}
		active := activeRef{
			frac: a,
			ref:  &fracRef{instance: a},
		}
		fracs = append(fracs, active.ref)
		notSealed = append(notSealed, active)
	}

	return fracs, notSealed, nil
}

func (t *loader) getFileList() []string {
	filePatten := fmt.Sprintf("%s*", fileBasePattern)
	pattern := filepath.Join(t.config.DataDir, filePatten)

	files, err := filepath.Glob(pattern)
	if err != nil {
		logger.Panic("todo")
	}
	return files
}

func removeFractionFiles(base string) {
	removeFile(base + consts.IndexFileSuffix) // first delete files without del suffix
	removeFile(base + consts.DocsFileSuffix)  // to preserve the info about fractions
	removeFile(base + consts.MetaFileSuffix)  // that should be deleted

	removeFile(base + consts.IndexDelFileSuffix)
	removeFile(base + consts.DocsDelFileSuffix)
}

func removeFile(file string) {
	if err := os.Remove(file); err == nil {
		logger.Info("remove file", zap.String("filename", file))
	} else if !os.IsNotExist(err) {
		logger.Error("file removing error", zap.Error(err))
	}
}

func (t *loader) filterInfos(fracIDs []string, infos map[string]*fracInfo) []*fracInfo {
	infoList := make([]*fracInfo, 0)

	for _, id := range fracIDs {
		info := infos[id]
		if info == nil {
			logger.Panic("frac loader has gone crazy")
		}

		if info.hasDocsDel || info.hasIndexDel {
			// storage has terminated in the middle of fraction deletion so continue this process
			logger.Info("cleaning up partially deleted fraction files", zap.String("file", info.base))
			removeFractionFiles(info.base)
			continue
		}

		if !info.hasDocs {
			metric.FractionLoadErrors.Inc()
			logger.Error("fraction doesn't have .docs file, skipping", zap.Any("frac_info", info))
			continue
		}

		if info.hasMeta || info.hasIndex {
			infoList = append(infoList, info)
			continue
		}

		if t.noValidDoc(info) {
			metric.FractionLoadErrors.Inc()
			logger.Error("fraction has .docs file without .meta and could not be read, deleting as invalid",
				zap.String("fraction_id", id),
			)
			_ = os.Remove(info.base + consts.DocsFileSuffix)
			continue
		}

		logger.Fatal("fraction has valid docs but no .index or .meta file", zap.String("fraction_id", id))
	}
	return infoList
}

// noValidDoc return true if disk.Reader.ReadDocBlock fails
// this captures cases when doc file size is zero, or doc file header is invalid
func (t *loader) noValidDoc(info *fracInfo) (invalid bool) {
	docFile, err := os.Open(info.base + consts.DocsFileSuffix)
	defer docFile.Close()

	if err != nil {
		return true
	}

	defer func() {
		if recover() != nil {
			invalid = true
		}
	}()
	readTask := t.reader.ReadDocBlock(docFile, 0, 0, nil)
	return readTask.Err != nil
}

func (t *loader) makeInfos(files []string) ([]string, map[string]*fracInfo) {
	fracIDs := make([]string, 0, len(files))
	infos := make(map[string]*fracInfo)
	for _, file := range files {
		base, suffix, fracID := t.extractInfo(file)
		if suffix == consts.IndexTmpFileSuffix {
			continue
		}

		info, ok := infos[fracID]
		if !ok {
			info = &fracInfo{base: base}
			infos[fracID] = info
			fracIDs = append(fracIDs, fracID)
		}

		switch suffix {
		case consts.DocsFileSuffix:
			info.hasDocs = true
		case consts.IndexFileSuffix:
			info.hasIndex = true
		case consts.DocsDelFileSuffix:
			info.hasDocsDel = true
		case consts.IndexDelFileSuffix:
			info.hasIndexDel = true
		case consts.MetaFileSuffix:
			info.hasMeta = true
		default:
			logger.Fatal("unknown file", zap.String("file", file))
		}
	}

	return fracIDs, infos
}

func (t *loader) extractInfo(file string) (string, string, string) {
	base := filepath.Base(file)

	if len(base) < len(fileBasePattern) {
		logger.Panic("wrong docs file", zap.String("file", file))
	}

	if base[:len(fileBasePattern)] != fileBasePattern {
		logger.Panic("wrong docs file", zap.String("file", file))
	}

	suffix := getSuffix(base)
	fracID := base[len(fileBasePattern) : len(base)-len(suffix)]

	return file[:len(file)-len(suffix)], suffix, fracID
}

func getSuffix(str string) string {
	for i, c := range str {
		if c == '.' {
			return str[i:]
		}
	}
	return ""
}
