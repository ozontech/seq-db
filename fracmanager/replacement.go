package fracmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/util"
)

var ErrReplacementClaim = errors.New("replacement claim failed")

type replacementPlan struct {
	Participants []string `json:"participants"`
}

type Replacement struct {
	lc           *lifecycleManager
	producedPath string
	claimed      []*refCountedSealed

	mu        sync.Mutex
	finalized bool
}

func (fm *FracManager) StartReplacement(producedPath string, sourceNames []string) (*Replacement, error) {
	claimed, err := fm.lc.registry.claimForReplacement(sourceNames)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReplacementClaim, err)
	}

	if err := writeReplacementPlan(producedPath, sourceNames); err != nil {
		fm.lc.registry.releaseReplacement(claimed)
		removeAllFiles(producedPath)
		return nil, err
	}

	return &Replacement{
		lc:           fm.lc,
		producedPath: producedPath,
		claimed:      claimed,
	}, nil
}

func (r *Replacement) Fractions() []*frac.Sealed {
	result := make([]*frac.Sealed, len(r.claimed))
	for i, f := range r.claimed {
		result[i] = f.Sealed
	}
	return result
}

func (r *Replacement) Release() {
	r.mu.Lock()
	if r.finalized {
		r.mu.Unlock()
		return
	}
	r.finalized = true
	r.mu.Unlock()

	r.lc.registry.releaseReplacement(r.claimed)
	removeAllFiles(r.producedPath)
}

func (r *Replacement) Commit(preloaded *sealed.PreloadedData) error {
	if preloaded == nil || preloaded.Info == nil {
		return fmt.Errorf("replacement produced fraction info is required")
	}
	if filepath.Clean(preloaded.Info.Path) != filepath.Clean(r.producedPath) {
		return fmt.Errorf(
			"replacement produced path mismatch: got %q, want %q",
			preloaded.Info.Path,
			r.producedPath,
		)
	}

	r.mu.Lock()
	if r.finalized {
		r.mu.Unlock()
		return fmt.Errorf("replacement is already finalized")
	}
	r.finalized = true
	r.mu.Unlock()

	produced := r.lc.provider.NewSealedPreloaded(r.producedPath, preloaded)
	r.lc.registry.replaceWithSealed(produced, r.claimed...)

	consumedNames := make([]string, len(r.claimed))
	for i, f := range r.claimed {
		consumedNames[i] = f.Info().Name()
	}
	r.lc.infoCache.Replace(preloaded.Info, consumedNames...)

	for _, f := range r.claimed {
		f.Destroy()
	}
	util.RemoveFile(r.producedPath + consts.ReplacementPlan)
	util.RemoveFile(r.producedPath + consts.ReplacementPlanTmp)

	return nil
}

func writeReplacementPlan(filename string, participants []string) error {
	tmp := filename + consts.ReplacementPlanTmp
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultFilePermission)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)

	p := replacementPlan{Participants: append([]string(nil), participants...)}
	if err := json.NewEncoder(f).Encode(p); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return util.DurableRenameFile(tmp, filename+consts.ReplacementPlan)
}
