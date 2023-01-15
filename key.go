package dhstore

import (
	"io"

	"github.com/multiformats/go-multihash"
	"lukechampine.com/blake3"
)

var (
	_ keyer     = (*blake3Keyer)(nil)
	_ io.Closer = (*blake3Keyer)(nil)
	_ io.Closer = (*key)(nil)
)

type (
	keyPrefix byte
	key       struct {
		buf []byte
		p   *pool
	}
	keyer interface {
		multihashKey(multihash.Multihash) (*key, error)
		hashedValueKeyKey(HashedValueKey) (*key, error)
	}
	blake3Keyer struct {
		hasher *blake3.Hasher
		p      *pool
	}
)

const (
	// unknownKeyPrefix signals an unknown key prefix.
	unknownKeyPrefix keyPrefix = iota //lint:ignore U1000 - iota
	// multihashKeyPrefix represents the prefix of a key that represent a multihash.
	multihashKeyPrefix
	// hashedValueKeyKeyPrefix represents the prefix of a key that is associated to hashed value-key
	// key.
	hashedValueKeyKeyPrefix
)

func (k *key) append(b ...byte) {
	k.buf = append(k.buf, b...)
}

func (k *key) maybeGrow(n int) {
	l := len(k.buf)
	switch {
	case n <= cap(k.buf)-l:
	case l == 0:
		k.buf = make([]byte, 0, n*pooledSliceCapGrowthFactor)
	default:
		k.buf = append(make([]byte, 0, (l+n)*pooledSliceCapGrowthFactor), k.buf...)
	}
}

func (k *key) Close() error {
	if cap(k.buf) <= pooledKeyMaxCap {
		k.buf = k.buf[:0]
		k.p.keyPool.Put(k)
	}
	return nil
}

func newBlake3Keyer(l int, p *pool) *blake3Keyer {
	return &blake3Keyer{
		hasher: blake3.New(l, nil),
		p:      p,
	}
}

// multihashKey returns the key by which a multihash is identified
func (b *blake3Keyer) multihashKey(mh multihash.Multihash) (*key, error) {
	mhk := b.p.leaseKey()
	mhk.maybeGrow(1 + len(mh))
	mhk.append(byte(multihashKeyPrefix))
	mhk.append(mh...)
	return mhk, nil
}

// hashedValueKeyKey returns the key by which metadata is identified.
func (b *blake3Keyer) hashedValueKeyKey(hvk HashedValueKey) (*key, error) {
	b.hasher.Reset()
	if _, err := b.hasher.Write(hvk); err != nil {
		return nil, err
	}
	sum := b.hasher.Sum([]byte{byte(hashedValueKeyKeyPrefix)})
	hvkk := b.p.leaseKey()
	hvkk.maybeGrow(len(sum))
	hvkk.append(sum...)
	return hvkk, nil
}

func (b *blake3Keyer) Close() error {
	return nil
}
