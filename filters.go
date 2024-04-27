package bff

import (
	"fmt"
	"strings"
)

type BloomFiltersController struct {
	size    int
	mappers []func([]byte) int
	pool    *dataPool
}

func New(size int, opts ...option) *BloomFiltersController {
	options := setup(opts)

	return &BloomFiltersController{
		size:    size,
		mappers: createMappers(size, options.NMappers, options.CreateMapperFunc),
		pool:    newDataPool(size/64 + 1),
	}
}

// Size - number of bits
func (bf *BloomFiltersController) Size() int {
	return bf.size
}

// Acquire - get a slice from pool / create new to use as a storage for a bloom filter.
func (bf *BloomFiltersController) Acquire() []uint64 {
	return bf.pool.Get()
}

// Release - free v for future use
func (bf *BloomFiltersController) Release(v []uint64) {
	bf.pool.Put(v)
}

// PreAllocate - pre-create several slices and put them into pool in advance to make Acquire() faster
func (bf *BloomFiltersController) PreAllocate(n int) {
	buf := make([][]uint64, n)
	for i := 0; i < n; i++ {
		buf[i] = bf.Acquire()
	}
	for i := 0; i < n; i++ {
		bf.Release(buf[i])
	}
}

// Add - encode string into data
func (bf *BloomFiltersController) Add(data []uint64, s []byte) {
	for _, mapper := range bf.mappers {
		bf.setBit(data, mapper(s))
	}
}

// MayContain - check that data [possibly] contains s
func (bf *BloomFiltersController) MayContain(data []uint64, s []byte) bool {
	for _, mapper := range bf.mappers {
		pos := mapper(s)
		if isSet := bf.isBitSet(data, pos); !isSet {
			return false
		}
	}

	return true
}

// Debug - print current data state in binary form and highlight bits, that represent value v.
func (bf *BloomFiltersController) Debug(data []uint64, v []byte) string {
	bitsStrParts := make([]string, 0, len(data))
	for i := 0; i < len(data); i++ {
		bitsStrParts = append(bitsStrParts, fmt.Sprintf("%064b", data[i]))
	}
	lastPart := bitsStrParts[len(bitsStrParts)-1]
	lastPart = lastPart[:bf.size%64]
	bitsStrParts[len(bitsStrParts)-1] = lastPart
	bitsStr := strings.Join(bitsStrParts, "")

	highlights := make([]rune, len(bitsStr))
	for i := range highlights {
		highlights[i] = ' '
	}
	for _, mapper := range bf.mappers {
		pos := mapper(v)
		highlights[pos] = '^'
	}

	return bitsStr + "\n" + string(highlights)
}

func (*BloomFiltersController) setBit(data []uint64, i int) {
	blockPos, innerPos := transformPosition(i)
	block := data[blockPos]
	block |= 1 << uint64(innerPos)
	data[blockPos] = block
}

func (*BloomFiltersController) isBitSet(data []uint64, i int) bool {
	blockPos, innerPos := transformPosition(i)
	block := data[blockPos]

	return block&(1<<uint64(innerPos)) != 0
}

func transformPosition(i int) (blockPos, innerPos int) {
	blockPos = i / 64
	innerPos = 63 - i%64 // go left to right

	return
}

func createMappers(size, n int, createMapperFunc func(size, seed int) func([]byte) int) []func([]byte) int {
	mappers := make([]func([]byte) int, n)
	for i := 0; i < n; i++ {
		mappers[i] = createMapperFunc(size, i)
	}

	return mappers
}
