package pebble

import (
	"bytes"
	"errors"
	"io"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/ipni/dhstore"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

var _ dhstore.DHStore = (*PebbleDHStore)(nil)

const (
	encValueKeysCap          = 5
	encValueKeysGrowthFactor = 2
)

type PebbleDHStore struct {
	db     *pebble.DB
	p      *pool
	closed bool
}

// NewPebbleDHStore instantiates a new instance of a store backed by Pebble.
// Note that any Merger value specified in the given options will be overridden.
func NewPebbleDHStore(path string, opts *pebble.Options) (*PebbleDHStore, error) {
	dhs := &PebbleDHStore{
		p: newPool(),
	}

	if opts == nil {
		opts = &pebble.Options{}
	}
	opts.EnsureDefaults()
	// Override Merger since the store relies on a specific implementation of it
	// to handle read-free writing of value-keys; see: valueKeysValueMerger.
	opts.Merger = dhs.newValueKeysMerger()
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	dhs.db = db

	return dhs, nil
}

func (s *PebbleDHStore) MergeIndexes(indexes []dhstore.Index) error {
	// Sort indexes to reduce cursor churn.
	slices.SortFunc(indexes, func(a, b dhstore.Index) int {
		return bytes.Compare(a.Key, b.Key)
	})

	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	batch := s.db.NewBatch()

	for _, index := range indexes {
		dmh, err := multihash.Decode(index.Key)
		if err != nil {
			return dhstore.ErrMultihashDecode{Err: err, Mh: index.Key}
		}
		if multicodec.Code(dmh.Code) != multicodec.DblSha2_256 {
			return dhstore.ErrUnsupportedMulticodecCode{Code: multicodec.Code(dmh.Code)}
		}
		mhk, err := keygen.multihashKey(index.Key)
		if err != nil {
			return err
		}
		mevk, closer, err := s.marshalEncryptedIndexKey(index.Value)
		if err != nil {
			_ = mhk.Close()
			return err
		}
		if err := batch.Merge(mhk.buf, mevk, pebble.NoSync); err != nil {
			_ = mhk.Close()
			_ = closer.Close()
			return err
		}
		_ = mhk.Close()
		_ = closer.Close()
	}
	return batch.Commit(pebble.NoSync)
}

// DeleteIndexes removes dh-multihash to encrypted-valueKey mappings. This is
// the inverse of MergeIndexes.
func (s *PebbleDHStore) DeleteIndexes(indexes []dhstore.Index) error {
	// Sort indexes to reduce cursor churn.
	slices.SortFunc(indexes, func(a, b dhstore.Index) int {
		return bytes.Compare(a.Key, b.Key)
	})

	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	batch := s.db.NewBatch()

	for _, index := range indexes {
		dmh, err := multihash.Decode(index.Key)
		if err != nil {
			return dhstore.ErrMultihashDecode{Err: err, Mh: index.Key}
		}
		if multicodec.Code(dmh.Code) != multicodec.DblSha2_256 {
			return dhstore.ErrUnsupportedMulticodecCode{Code: multicodec.Code(dmh.Code)}
		}

		// Lookup the encrypted multihash keys for this dh-multihash.
		mhk, err := keygen.multihashKey(index.Key)
		if err != nil {
			return err
		}
		vkb, vkbClose, err := s.db.Get(mhk.buf)
		if err != nil {
			_ = mhk.Close()
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		encValueKeys, err := s.unmarshalEncryptedIndexKeys(vkb)
		vkbClose.Close()
		if err != nil {
			_ = mhk.Close()
			return err
		}

		// Remove the encrypted value key from the returned set of values.
		var removed bool
		for i, evk := range encValueKeys {
			if bytes.Equal(evk, index.Value) {
				if len(encValueKeys) == 1 {
					encValueKeys = nil
				} else {
					// Preserve order when removing value key.
					encValueKeys = append(encValueKeys[:i], encValueKeys[i+1:]...)
				}
				removed = true
				break
			}
		}
		if len(encValueKeys) == 0 {
			// Multihash does not map to any remaining values, so delete it.
			err = batch.Delete(mhk.buf, pebble.NoSync)
			_ = mhk.Close()
			if err != nil {
				return err
			}
			continue
		}
		if !removed {
			// No changes, continue to next multihash.
			_ = mhk.Close()
			continue
		}

		// Update the set of value keys the multihash maps to.
		mevks, mevksCloser, err := s.marshalEncryptedIndexKeys(encValueKeys)
		if err != nil {
			_ = mhk.Close()
			return err
		}
		err = batch.Set(mhk.buf, mevks, pebble.NoSync)
		_ = mevksCloser.Close()
		_ = mhk.Close()
		if err != nil {
			return err
		}
	}
	return batch.Commit(pebble.NoSync)
}

func (s *PebbleDHStore) PutMetadata(hvk dhstore.HashedValueKey, em dhstore.EncryptedMetadata) error {
	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	hvkk, err := keygen.hashedValueKeyKey(hvk)
	if err != nil {
		return err
	}
	defer hvkk.Close()
	return s.db.Set(hvkk.buf, em, pebble.NoSync)
}

func (s *PebbleDHStore) Lookup(mh multihash.Multihash) ([]dhstore.EncryptedValueKey, error) {
	dmh, err := multihash.Decode(mh)
	if err != nil {
		return nil, dhstore.ErrMultihashDecode{Err: err, Mh: mh}
	}
	if dmh.Code != multihash.DBL_SHA2_256 {
		return nil, dhstore.ErrUnsupportedMulticodecCode{Code: multicodec.Code(dmh.Code)}
	}
	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	mhk, err := keygen.multihashKey(mh)
	if err != nil {
		return nil, err
	}

	vkb, vkbClose, err := s.db.Get(mhk.buf)
	_ = mhk.Close()
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer vkbClose.Close()
	return s.unmarshalEncryptedIndexKeys(vkb)
}

func (s *PebbleDHStore) GetMetadata(hvk dhstore.HashedValueKey) (dhstore.EncryptedMetadata, error) {
	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	hvkk, err := keygen.hashedValueKeyKey(hvk)
	if err != nil {
		return nil, err
	}

	emb, emClose, err := s.db.Get(hvkk.buf)
	_ = hvkk.Close()
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	em := make([]byte, len(emb))
	copy(em, emb)
	_ = emClose.Close()
	return em, nil
}

func (s *PebbleDHStore) DeleteMetadata(hvk dhstore.HashedValueKey) error {
	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	hvkk, err := keygen.hashedValueKeyKey(hvk)
	if err != nil {
		return err
	}

	return s.db.Delete(hvkk.buf, pebble.NoSync)
}

func (s *PebbleDHStore) Size() (int64, error) {
	sizeEstimate, err := s.db.EstimateDiskUsage([]byte{0}, []byte{0xff})
	return int64(sizeEstimate), err
}

func (s *PebbleDHStore) Flush() error {
	return s.db.Flush()
}

func (s *PebbleDHStore) Close() error {
	if s.closed {
		return nil
	}
	ferr := s.db.Flush()
	cerr := s.db.Close()
	s.closed = true
	// Prioritise on returning close errors over flush errors, since it is more likely to contain
	// useful information about the failure root cause.
	if cerr != nil {
		return cerr
	}
	return ferr
}

func (s *PebbleDHStore) marshalEncryptedIndexKey(evk dhstore.EncryptedValueKey) ([]byte, io.Closer, error) {
	buf := s.p.leaseSectionBuff()
	buf.writeSection(evk)
	return buf.buf, buf, nil
}

func (s *PebbleDHStore) marshalEncryptedIndexKeys(evks []dhstore.EncryptedValueKey) ([]byte, io.Closer, error) {
	buf := s.p.leaseSectionBuff()
	for _, evk := range evks {
		buf.writeSection(evk)
	}
	return buf.buf, buf, nil
}

func (s *PebbleDHStore) unmarshalEncryptedIndexKeys(b []byte) ([]dhstore.EncryptedValueKey, error) {
	if len(b) == 0 {
		return nil, nil
	}
	buf := s.p.leaseSectionBuff()
	defer buf.Close()
	buf.wrap(b)
	evks := make([]dhstore.EncryptedValueKey, 0, encValueKeysCap)
	var l int
	for buf.remaining() != 0 {
		next, err := buf.copyNextSection()
		if err != nil {
			return nil, err
		}
		evks = append(evks, next)
		l++
		if cap(evks)-l <= 0 {
			evks = append(make([]dhstore.EncryptedValueKey, 0, (l+encValueKeysCap)*encValueKeysGrowthFactor), evks...)
		}
	}
	return evks, nil
}

// Metrics returns underlying pebble DB metrics
func (s *PebbleDHStore) Metrics() *pebble.Metrics {
	return s.db.Metrics()
}
