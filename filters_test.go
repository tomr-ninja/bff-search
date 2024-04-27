package bff_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tomr-ninja/bff-search"
)

func TestBloomFilter(t *testing.T) {
	words := []string{"hello", "world", "foo", "bar", "baz", "golang", "java"}

	type testCase struct {
		size     int
		nMappers int
		words    []string
	}
	testCases := []testCase{
		{size: 1, nMappers: 1, words: words},
		{size: 5, nMappers: 1, words: words},
		{size: 5, nMappers: 2, words: words},
		{size: 5, nMappers: 3, words: words},
		{size: 10, nMappers: 1, words: words},
		{size: 10, nMappers: 2, words: words},
		{size: 10, nMappers: 3, words: words},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			runTest(t, tc.size, tc.nMappers, tc.words)
		})
	}
}

func runTest(t *testing.T, size, nMappers int, words []string) {
	bloom := bff.New(size, bff.WithNMappers(nMappers))
	data := bloom.Acquire()
	defer bloom.Release(data)

	for _, word := range words {
		bloom.Add(data, []byte(word))
	}
	for _, word := range words {
		t.Run(word, func(t *testing.T) {
			assert.True(t, bloom.MayContain(data, []byte(word)))
		})
	}
}
