package bff

import "runtime"

type (
	// SearchEngine performs indexing and lookup operations for objects of type T
	SearchEngine[T, Q any] struct {
		termsExtractor TermsExtractor[T, Q]
		index          *Index
	}
	TermsExtractor[T, Q any] interface {
		ExtractValueTerms(input T) [][]byte
		ExtractQueryTerms(input Q) [][]byte
	}
)

func NewSearchEngine[T, Q any](
	termsExtractor TermsExtractor[T, Q],
	verifier Verifier,
	indexOptions ...option,
) *SearchEngine[T, Q] {
	return &SearchEngine[T, Q]{
		termsExtractor: termsExtractor,
		index:          NewIndex(runtime.NumCPU(), verifier, indexOptions...),
	}
}

func (se *SearchEngine[T, Q]) Index(id uint64, input T) {
	se.index.Add(id, se.termsExtractor.ExtractValueTerms(input))
}

func (se *SearchEngine[T, Q]) Lookup(query Q) ([]uint64, error) {
	return se.index.Lookup(se.termsExtractor.ExtractQueryTerms(query))
}
