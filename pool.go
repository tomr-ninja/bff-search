package bff

import "sync"

type dataPool struct {
	sync.Pool
}

func newDataPool(n int) *dataPool {
	return &dataPool{
		sync.Pool{
			New: func() any {
				return make([]uint64, n)
			},
		},
	}
}

func (d *dataPool) Get() []uint64 {
	return d.Pool.Get().([]uint64)
}

func (d *dataPool) Put(v []uint64) {
	for i := range v {
		v[i] = 0
	}

	d.Pool.Put(v)
}
