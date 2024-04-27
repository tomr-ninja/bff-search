package bff

import "github.com/OneOfOne/xxhash"

type Options struct {
	CreateMapperFunc func(size, seed int) func([]byte) int
	NMappers         int
}

type option func(*Options)

var WithCustomMapper = func(f func(size, seed int) func([]byte) int) option {
	return func(opts *Options) {
		opts.CreateMapperFunc = f
	}
}

var WithNMappers = func(nMappers int) option {
	return func(opts *Options) {
		opts.NMappers = nMappers
	}
}

// WithErrorRate - calculate NMappers using expectations for number of elements inserted and false-positive error rate.
// See https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
var WithErrorRate = func(float64) option {
	panic("not implemented")
}

func setup(opts []option) *Options {
	res := &Options{}
	for _, opt := range opts {
		opt(res)
	}

	if res.CreateMapperFunc == nil {
		res.CreateMapperFunc = defaultCreateMapperFunc
	}
	if res.NMappers == 0 {
		panic("number of mappers cannot be 0")
	}

	return res
}

func defaultCreateMapperFunc(size, seed int) func([]byte) int {
	hash := xxhash.NewS64(uint64(seed))

	return func(in []byte) int {
		hash.Reset()
		_, _ = hash.Write(in)

		return int(hash.Sum64() % uint64(size))
	}
}
