package bff_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tomr-ninja/bff-search"
)

type TestStruct struct {
	Field1 uint64
	Field2 string
}

type TestQuery struct {
	Field1 uint64
	Field2 string
}

type testExtractor struct{}

func (testExtractor) ExtractValueTerms(input TestStruct) [][]byte {
	res := make([][]byte, 2)
	res[0] = make([]byte, 8)
	binary.LittleEndian.PutUint64(res[0], input.Field1)
	res[1] = []byte(input.Field2)

	return res
}

func (testExtractor) ExtractQueryTerms(input TestQuery) [][]byte {
	res := make([][]byte, 2)
	res[0] = make([]byte, 8)
	binary.LittleEndian.PutUint64(res[0], input.Field1)
	res[1] = []byte(input.Field2)

	return res
}

type NoopVerifier struct{}

func (NoopVerifier) Verify(_ uint64, _ [][]byte) (bool, error) {
	return true, nil
}

func TestSearchEngine(t *testing.T) {
	te := testExtractor{}
	// using NoopVerifier actually makes test unstable as details of Bloom filters implementation may change
	s := bff.NewSearchEngine[TestStruct, TestQuery](te, NoopVerifier{}, bff.WithNMappers(1))
	s.Index(42, TestStruct{Field1: 42, Field2: "hello"})
	s.Index(43, TestStruct{Field1: 43, Field2: "world"})

	ids, err := s.Lookup(TestQuery{Field1: 42, Field2: "hello"})
	require.NoError(t, err)
	require.Equal(t, []uint64{42}, ids)
}
