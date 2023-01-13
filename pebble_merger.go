package dhstore

import (
	"io"

	"github.com/cockroachdb/pebble"
)

const deletableDefaultMergerName = "dhstore.v1.deletable_default_merger"

var (
	_ pebble.ValueMerger          = (*deletableDefaultMerger)(nil)
	_ pebble.DeletableValueMerger = (*deletableDefaultMerger)(nil)
)

type deletableDefaultMerger struct {
	def pebble.ValueMerger
}

func newDeletableDefaultMerger() *pebble.Merger {
	return &pebble.Merger{
		Merge: func(k, value []byte) (pebble.ValueMerger, error) {
			// Fall back on default merger if the key is not of type multihash, i.e. the only key
			// type that corresponds to value-keys.
			switch keyPrefix(k[0]) {
			case multihashKeyPrefix:
				merge, err := pebble.DefaultMerger.Merge(k, value)
				if err != nil {
					return nil, err
				}
				v := &deletableDefaultMerger{
					def: merge,
				}
				return v, v.MergeNewer(value)
			default:
				return pebble.DefaultMerger.Merge(k, value)
			}
		},
		Name: deletableDefaultMergerName,
	}
}

func (v *deletableDefaultMerger) MergeNewer(value []byte) error {
	return v.def.MergeNewer(value)
}

func (v *deletableDefaultMerger) MergeOlder(value []byte) error {
	return v.def.MergeOlder(value)
}

func (v *deletableDefaultMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return v.def.Finish(includesBase)
}

func (v *deletableDefaultMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	b, c, err := v.Finish(includesBase)
	return b, len(b) == 0, c, err
}
