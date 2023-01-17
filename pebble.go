package dhstore

import (
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

var (
	log = logging.Logger("store/pebble")

	_ DHStore = (*PebbleDHStore)(nil)
)

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

func (s *PebbleDHStore) MergeIndex(mh multihash.Multihash, evk EncryptedValueKey) error {
	dmh, err := multihash.Decode(mh)
	if err != nil {
		return ErrMultihashDecode{err: err, mh: mh}
	}
	if multicodec.Code(dmh.Code) != multicodec.DblSha2_256 {
		return ErrUnsupportedMulticodecCode{code: multicodec.Code(dmh.Code)}
	}
	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	mhk, err := keygen.multihashKey(mh)
	if err != nil {
		return err
	}
	defer mhk.Close()
	mevk, closer, err := s.marshalEncryptedIndexKey(evk)
	if err != nil {
		return err
	}
	defer closer.Close()
	return s.db.Merge(mhk.buf, mevk, pebble.NoSync)
}

func (s *PebbleDHStore) PutMetadata(hvk HashedValueKey, em EncryptedMetadata) error {

	keygen := s.p.leaseSimpleKeyer()
	defer keygen.Close()
	hvkk, err := keygen.hashedValueKeyKey(hvk)
	if err != nil {
		return err
	}
	defer hvkk.Close()
	return s.db.Set(hvkk.buf, em, pebble.NoSync)
}

func (s *PebbleDHStore) Lookup(mh multihash.Multihash) ([]EncryptedValueKey, error) {
	dmh, err := multihash.Decode(mh)
	if err != nil {
		return nil, ErrMultihashDecode{err: err, mh: mh}
	}
	if dmh.Code != multihash.DBL_SHA2_256 {
		return nil, ErrUnsupportedMulticodecCode{code: multicodec.Code(dmh.Code)}
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
		log.Debugw("failed to find multihash", "key", mh.B58String(), "err", err)
		return nil, err
	}
	defer vkbClose.Close()
	return s.unmarshalEncryptedIndexKeys(vkb)
}

func (s *PebbleDHStore) GetMetadata(hvk HashedValueKey) (EncryptedMetadata, error) {
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

func (s *PebbleDHStore) marshalEncryptedIndexKey(evk EncryptedValueKey) ([]byte, io.Closer, error) {
	buf := s.p.leaseSectionBuff()
	buf.writeSection(evk)
	return buf.buf, buf, nil
}

func (s *PebbleDHStore) unmarshalEncryptedIndexKeys(b []byte) ([]EncryptedValueKey, error) {
	if len(b) == 0 {
		return nil, nil
	}
	buf := s.p.leaseSectionBuff()
	defer buf.Close()
	buf.wrap(b)
	evks := make([]EncryptedValueKey, 0, encValueKeysCap)
	var l int
	for buf.remaining() != 0 {
		next, err := buf.copyNextSection()
		if err != nil {
			return nil, err
		}
		evks = append(evks, next)
		l++
		if cap(evks)-l <= 0 {
			evks = append(make([]EncryptedValueKey, 0, (l+encValueKeysCap)*encValueKeysGrowthFactor), evks...)
		}
	}
	return evks, nil
}
