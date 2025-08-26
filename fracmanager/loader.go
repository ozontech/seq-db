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
	hasSdocs    bool
	hasSdocsDel bool
	hasRemote   bool
}

type loader struct {
	config       *Config
	fracProvider *fractionProvider
	fracCache    *sealedFracCache

	cachedFracs   int
	uncachedFracs int
}

func NewLoader(
	config *Config, fracProvider *fractionProvider,
	fracCache *sealedFracCache,
) *loader {
	return &loader{
		config:       config,
		fracProvider: fracProvider,
		fracCache:    fracCache,
	}
}

func (l *loader) load(ctx context.Context) ([]*frac.Active, []*frac.Sealed, []*frac.Remote, error) {
	fracIDs, infos := l.makeInfos(l.getFileList())
	sort.Strings(fracIDs)

	if l.config.FracLoadLimit > 0 {
		logger.Info("preloading fractions", zap.Uint64("limit", l.config.FracLoadLimit))
		if len(fracIDs) > int(l.config.FracLoadLimit) {
			fracIDs = fracIDs[len(fracIDs)-int(l.config.FracLoadLimit):]
		}
	}

	infosList := l.filterInfos(fracIDs, infos)
	cnt := len(infosList)

	actives := make([]*frac.Active, 0)
	remote := make([]*frac.Remote, 0, cnt)
	sealed := make([]*frac.Sealed, 0, cnt)

	diskFracCache := NewFracCacheFromDisk(filepath.Join(l.config.DataDir, consts.FracCacheFileSuffix))
	ts := time.Now()

	for i, info := range infosList {
		if l.config.OffloadingEnabled && info.hasRemote {
			remote = append(remote, l.loadRemoteFrac(ctx, diskFracCache, info))
		}

		if info.hasSdocs && info.hasIndex {
			if info.hasMeta {
				removeFile(info.base + consts.MetaFileSuffix)
			}
			if info.hasDocs {
				removeFile(info.base + consts.DocsFileSuffix)
			}
			sealed = append(sealed, l.loadSealedFrac(diskFracCache, info))
		} else if !info.hasRemote {
			if info.hasMeta {
				actives = append(actives, l.fracProvider.NewActive(info.base))
			} else {
				sealed = append(sealed, l.loadSealedFrac(diskFracCache, info))
			}
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

	logger.Info("fractions list created", zap.Int("cached", l.cachedFracs), zap.Int("uncached", l.uncachedFracs))

	return actives, sealed, remote, nil
}

func (l *loader) loadSealedFrac(diskFracCache *sealedFracCache, info *fracInfo) *frac.Sealed {
	listedInfo, ok := diskFracCache.GetFracInfo(filepath.Base(info.base))
	if ok {
		l.cachedFracs++
	} else {
		l.uncachedFracs++
	}

	sealed := l.fracProvider.NewSealed(info.base, listedInfo)

	stats := sealed.Info()
	l.fracCache.AddFraction(stats.Name(), stats)
	return sealed
}

func (l *loader) loadRemoteFrac(ctx context.Context, diskFracCache *sealedFracCache, info *fracInfo) *frac.Remote {
	listedInfo, ok := diskFracCache.GetFracInfo(filepath.Base(info.base))
	if ok {
		l.cachedFracs++
	} else {
		l.uncachedFracs++
	}

	remote := l.fracProvider.NewRemote(ctx, info.base, listedInfo)

	stats := remote.Info()
	l.fracCache.AddFraction(stats.Name(), stats)

	return remote
}

func (l *loader) getFileList() []string {
	filePatten := fmt.Sprintf("%s*", fileBasePattern)
	pattern := filepath.Join(l.config.DataDir, filePatten)

	files, err := filepath.Glob(pattern)
	if err != nil {
		logger.Panic("todo")
	}
	return files
}

func removeFractionFiles(base string) {
	removeFile(base + consts.IndexFileSuffix) // first delete files without del suffix
	removeFile(base + consts.DocsFileSuffix)  // to preserve the info about fractions
	removeFile(base + consts.SdocsFileSuffix) // that should be deleted
	removeFile(base + consts.MetaFileSuffix)

	removeFile(base + consts.IndexDelFileSuffix)
	removeFile(base + consts.DocsDelFileSuffix)
	removeFile(base + consts.SdocsDelFileSuffix)
}

func removeFile(file string) {
	if err := os.Remove(file); err == nil {
		logger.Info("remove file", zap.String("filename", file))
	} else if !os.IsNotExist(err) {
		logger.Error("file removing error", zap.Error(err))
	}
}

func (l *loader) filterInfos(fracIDs []string, infos map[string]*fracInfo) []*fracInfo {
	infoList := make([]*fracInfo, 0)

	for _, id := range fracIDs {
		info := infos[id]
		if info == nil {
			logger.Panic("frac loader has gone crazy")
		}

		if info.hasRemote {
			infoList = append(infoList, info)
			continue
		}

		if info.hasDocsDel || info.hasIndexDel || info.hasSdocsDel {
			// storage has terminated in the middle of fraction deletion so continue this process
			logger.Info("cleaning up partially deleted fraction files", zap.String("file", info.base))
			removeFractionFiles(info.base)
			continue
		}

		if !info.hasDocs && !info.hasSdocs {
			metric.FractionLoadErrors.Inc()
			logger.Error("fraction doesn't have .docs/.sdocs file, skipping", zap.String("file", info.base))
			continue
		}

		if info.hasMeta || info.hasIndex {
			infoList = append(infoList, info)
			continue
		}

		logger.Fatal("fraction has valid docs but no .index or .meta file", zap.String("fraction_id", id), zap.Any("info", info))
	}
	return infoList
}

func (l *loader) makeInfos(files []string) ([]string, map[string]*fracInfo) {
	fracIDs := make([]string, 0, len(files))
	infos := make(map[string]*fracInfo)
	for _, file := range files {
		base, suffix, fracID := l.extractInfo(file)
		if suffix == consts.IndexTmpFileSuffix || suffix == consts.SdocsTmpFileSuffix {
			continue
		}

		info, ok := infos[fracID]
		if !ok {
			info = &fracInfo{base: base}
			infos[fracID] = info
			fracIDs = append(fracIDs, fracID)
		}

		logger.Info("new file", zap.String("file", file))

		switch suffix {
		case consts.DocsFileSuffix:
			info.hasDocs = true
		case consts.DocsDelFileSuffix:
			info.hasDocsDel = true
		case consts.SdocsFileSuffix:
			info.hasSdocs = true
		case consts.SdocsDelFileSuffix:
			info.hasSdocsDel = true
		case consts.IndexFileSuffix:
			info.hasIndex = true
		case consts.IndexDelFileSuffix:
			info.hasIndexDel = true
		case consts.MetaFileSuffix:
			info.hasMeta = true
		case consts.RemoteFractionSuffix:
			info.hasRemote = true
		default:
			logger.Fatal("unknown file", zap.String("file", file))
		}
	}

	return fracIDs, infos
}

func (l *loader) extractInfo(file string) (string, string, string) {
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
