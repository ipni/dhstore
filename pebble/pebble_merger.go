package pebble

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipni/dhstore"
)

const valueKeysMergerName = "dhstore.v1.valueKeysMerger"

var (
	_ pebble.ValueMerger          = (*valueKeysValueMerger)(nil)
	_ pebble.DeletableValueMerger = (*valueKeysValueMerger)(nil)
)

type valueKeysValueMerger struct {
	merges             []dhstore.EncryptedValueKey
	reverse            bool
	marshalledSizeHint int // Used as a hint to grow the buffer size during marshalling.
	s                  *PebbleDHStore
}

func (s *PebbleDHStore) newValueKeysMerger() *pebble.Merger {
	return &pebble.Merger{
		Merge: func(k, value []byte) (pebble.ValueMerger, error) {
			// Use specialized merger for multihash keys.
			if keyPrefix(k[0]) == multihashKeyPrefix {
				v := &valueKeysValueMerger{s: s}
				return v, v.MergeNewer(value)
			}
			// Use default merger for non-multihash type keys, i.e. the
			// only key type that corresponds to value-keys.
			return pebble.DefaultMerger.Merge(k, value)
		},
		Name: valueKeysMergerName,
	}
}

func (v *valueKeysValueMerger) MergeNewer(value []byte) error {
	if len(value) == 0 {
		return nil
	}

	evks, err := v.s.unmarshalEncryptedIndexKeys(value)
	if err != nil {
		return err
	}

	v.merges = maybeGrow(v.merges, len(evks))

	if len(v.merges) == 0 {
		// Optimise for the case where there are no merges.
		v.merges = append(v.merges, evks...)
		v.marshalledSizeHint += len(value)
	} else {
		for _, evk := range evks {
			if !v.exists(evk) {
				v.merges = append(v.merges, evk)
				v.marshalledSizeHint += len(evk)
			}
		}
	}

	return nil
}

func (v *valueKeysValueMerger) MergeOlder(value []byte) error {
	v.reverse = true
	return v.MergeNewer(value)
}

func (v *valueKeysValueMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	if len(v.merges) == 0 {
		return nil, nil, nil
	}
	if v.reverse {
		for one, other := 0, len(v.merges)-1; one < other; one, other = one+1, other-1 {
			v.merges[one], v.merges[other] = v.merges[other], v.merges[one]
		}
	}
	return v.marshalMerges()
}

func (v *valueKeysValueMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	b, c, err := v.Finish(includesBase)
	return b, len(b) == 0, c, err
}

// exists checks whether the given value is already present, either pending merge or deletion.
func (v *valueKeysValueMerger) exists(value []byte) bool {
	for _, x := range v.merges {
		if bytes.Equal(x, value) {
			return true
		}
	}
	return false
}

func (v *valueKeysValueMerger) marshalMerges() ([]byte, io.Closer, error) {
	buf := v.s.p.leaseSectionBuff()
	// Encrypted value keys are marshalled as varint + their byte value.
	// Optimistically, add the length of merges to the size hint to compensate for the additional
	// varints that'd be added to the beginning.
	buf.maybeGrow(v.marshalledSizeHint + len(v.merges))
	for _, merge := range v.merges {
		buf.writeSection(merge)
	}
	return buf.buf, buf, nil
}

// maybeGrow grows the capacity of the given slice if necessary, such that it can fit n more
// elements and returns the resulting slice.
func maybeGrow(s []dhstore.EncryptedValueKey, n int) []dhstore.EncryptedValueKey {
	const growthFactor = 2
	l := len(s)
	switch {
	case n <= cap(s)-l:
		return s
	case l == 0:
		return make([]dhstore.EncryptedValueKey, 0, n*growthFactor)
	default:
		return append(make([]dhstore.EncryptedValueKey, 0, (l+n)*growthFactor), s...)
	}
}
