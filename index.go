package bff

import (
	"sync"

	"golang.org/x/sync/errgroup"
)

type Verifier interface {
	Verify(id uint64, terms [][]byte) (bool, error)
}

// Index - search index implementation using Bloom filters.
type Index struct {
	mux       sync.RWMutex
	nParallel int
	filters   *BloomFiltersController
	verifier  Verifier
	data      [][]uint64
	ids       []uint64
}

func NewIndex(nParallel int, verifier Verifier, opts ...option) *Index {
	return &Index{
		nParallel: nParallel,
		filters:   New(64, opts...),
		verifier:  verifier,
	}
}

func (idx *Index) Add(id uint64, searchTerms [][]byte) {
	filterData := idx.filters.Acquire()
	for _, term := range searchTerms {
		idx.filters.Add(filterData, term)
	}

	idx.mux.Lock()
	idx.data = append(idx.data, filterData)
	idx.ids = append(idx.ids, id)
	idx.mux.Unlock()
}

func (idx *Index) Lookup(searchTerms [][]byte) ([]uint64, error) {
	idx.mux.RLock()
	defer idx.mux.RUnlock()

	var (
		res    []uint64
		resMux sync.Mutex
		eg     errgroup.Group
	)

	size := len(idx.data) / idx.nParallel
	parallel := idx.nParallel
	if size == 0 {
		size = len(idx.data)
		parallel = 1
	}
	for k := 0; k < parallel; k++ {
		k := k

		eg.Go(func() error {
			scanData := idx.data[k*size : (k+1)*size]
			scanIDs := idx.ids[k*size : (k+1)*size]

			hits := make([]uint64, 0, size)
			for i := range scanData {
				ok := true
				for _, term := range searchTerms {
					if len(term) == 0 {
						continue
					}
					if !idx.filters.MayContain(scanData[i], term) {
						ok = false
						break
					}
				}
				if ok {
					hits = append(hits, scanIDs[i])
				}
			}

			verifiedHits := make([]uint64, 0, len(hits))
			for _, id := range hits {
				ok, err := idx.verifier.Verify(id, searchTerms)
				if err != nil {
					return err
				}

				if !ok {
					continue
				}

				verifiedHits = append(verifiedHits, id)
			}

			if len(verifiedHits) > 0 {
				resMux.Lock()
				res = append(res, verifiedHits...)
				resMux.Unlock()
			}

			return nil
		})
	}

	return res, eg.Wait()
}
