package dhstore

import (
	"sync"
)

const (
	blake3DigestLength         = 32
	pooledKeyMaxCap            = 32
	pooledSectionBufferMaxCap  = 1 << 10 // 1 KiB
	pooledSliceCapGrowthFactor = 2
)

type pool struct {
	simpleKeyer       sync.Pool
	keyPool           sync.Pool
	sectionBufferPool sync.Pool
}

func newPool() *pool {
	var p pool
	p.keyPool.New = func() any {
		return &key{
			buf: make([]byte, 0, pooledKeyMaxCap),
			p:   &p,
		}
	}
	p.simpleKeyer.New = func() any {
		return newBlake3Keyer(blake3DigestLength, &p)
	}
	p.sectionBufferPool.New = func() any {
		return &sectionBuffer{
			buf: make([]byte, 0, pooledSectionBufferMaxCap),
			p:   &p,
		}
	}
	return &p
}

func (p *pool) leaseSimpleKeyer() *blake3Keyer {
	return p.simpleKeyer.Get().(*blake3Keyer)
}

func (p *pool) leaseKey() *key {
	return p.keyPool.Get().(*key)
}

func (p *pool) leaseSectionBuff() *sectionBuffer {
	return p.sectionBufferPool.Get().(*sectionBuffer)
}
