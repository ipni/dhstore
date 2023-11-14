//go:build fdb

package fdb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"lukechampine.com/blake3"
)

var (
	_ dhstore.DHStore = (*FDBDHStore)(nil)

	logger                   = logging.Logger("store/fdb")
	fdbHasherPool            sync.Pool
	errMultihashDigestLength = errors.New("multihash digest must be exactly 32 bytes long")
	errMetadataKeyTooLong    = errors.New("key must be at most 32 bytes long")

	multihashDirectoryPath = []string{"mh"}
	metadataDirectoryPath  = []string{"md"}
)

const (
	maxValueBytes    = 100_000 // 100 KB
	blake3HashLength = 32

	// maxKeyPrefixLen is the threshold at which key prefixes are hashed if they are larger. Otherwise,
	// the prefix is used as is. When used in the context of metadata keys, it represents the max accepted
	// length for metadata key.
	maxKeyPrefixLen = 32
)

type FDBDHStore struct {
	db fdb.Database

	// mhdir is the directory subspace used to store all multihash mappings under a dedicated directory for future extensibility.
	mhdir directory.DirectorySubspace
	// mddir is the directory subspace used to store all metadata mappings under a dedicated directory for future extensibility.
	mddir directory.DirectorySubspace
}

func init() {
	fdbHasherPool.New = func() any {
		return blake3.New(blake3HashLength, nil)
	}
}

func NewFDBDHStore(o ...Option) (*FDBDHStore, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	if err := fdb.APIVersion(opts.apiVersion); err != nil {
		return nil, err
	}
	var dhfdb FDBDHStore
	if dhfdb.db, err = fdb.OpenDatabase(opts.clusterFile); err != nil {
		return nil, err
	}
	if dhfdb.mhdir, err = directory.CreateOrOpen(dhfdb.db, multihashDirectoryPath, nil); err != nil {
		return nil, err
	}
	if dhfdb.mddir, err = directory.CreateOrOpen(dhfdb.db, metadataDirectoryPath, nil); err != nil {
		return nil, err
	}
	return &dhfdb, nil
}

func (f *FDBDHStore) MergeIndexes(indexes []dhstore.Index) error {
	_, err := f.db.Transact(func(transaction fdb.Transaction) (any, error) {
		for _, index := range indexes {
			mh := index.Key
			vk := index.Value

			// Fail fast on invalid multihashes.
			// TODO: make fail-fast optional.
			dmh, err := multihash.Decode(mh)
			if err != nil {
				return nil, dhstore.ErrMultihashDecode{Err: err, Mh: mh}
			}
			if multicodec.Code(dmh.Code) != multicodec.DblSha2_256 {
				return nil, dhstore.ErrUnsupportedMulticodecCode{Code: multicodec.Code(dmh.Code)}
			}
			if dmh.Length != 32 {
				return nil, dhstore.ErrMultihashDecode{Err: errMultihashDigestLength, Mh: mh}
			}
			if len(vk) > maxValueBytes {
				return nil, fmt.Errorf("value key cannot be larger than 100 KB, got: %d", len(vk))
			}
			// Check if vk is longer than the allowed max key prefix. If it is, then
			// hash it and use the original as the value associated to the key.
			// If not, then use vk as is as the prefix and leave value empty.
			// This strategy will result in:
			//  1. lower disk utilisation and
			//  2. lower CPU consumption
			// by opportunistically avoiding:
			// - the re-hash of vk just to get a short key prefix, and
			// - the double storage of vk when it is the same as prefix.
			// On lookup, we then check if the value is empty and if it is we return the prefix.
			var prefix, value []byte
			if len(vk) > maxKeyPrefixLen {
				var err error
				prefix, err = f.hash(vk)
				if err != nil {
					return nil, err
				}
				value = vk
			} else {
				prefix = vk
			}
			key := f.mhdir.Pack(tuple.Tuple{dmh.Digest, prefix})
			transaction.Set(key, value)
		}
		return nil, nil
	})
	return err
}

func (f *FDBDHStore) hash(vk []byte) ([]byte, error) {
	hasher, ok := fdbHasherPool.Get().(*blake3.Hasher)
	if !ok {
		return nil, errors.New("potential bug: unexpected hasher kind")
	}
	hasher.Reset()
	defer fdbHasherPool.Put(hasher)
	if _, err := hasher.Write(vk); err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func (f *FDBDHStore) PutMetadata(vk dhstore.HashedValueKey, md dhstore.EncryptedMetadata) error {
	if len(vk) > maxKeyPrefixLen {
		return dhstore.ErrInvalidHashedValueKey{Key: vk, Err: errMetadataKeyTooLong}
	}
	if len(md) > maxValueBytes {
		return fmt.Errorf("value key cannot be larger than 100 KB, got: %d", len(vk))
	}
	_, err := f.db.Transact(func(transaction fdb.Transaction) (any, error) {
		key := f.mddir.Pack(tuple.Tuple{[]byte(vk)})
		transaction.Set(key, md)
		return nil, nil
	})
	return err
}

func (f *FDBDHStore) Lookup(mh multihash.Multihash) ([]dhstore.EncryptedValueKey, error) {
	dmh, err := multihash.Decode(mh)
	if err != nil {
		return nil, dhstore.ErrMultihashDecode{Err: err, Mh: mh}
	}
	if dmh.Code != multihash.DBL_SHA2_256 {
		return nil, dhstore.ErrUnsupportedMulticodecCode{Code: multicodec.Code(dmh.Code)}
	}
	if dmh.Length != 32 {
		return nil, dhstore.ErrMultihashDecode{Err: errMultihashDigestLength, Mh: mh}
	}
	v, err := f.db.ReadTransact(func(transaction fdb.ReadTransaction) (any, error) {
		vks := transaction.GetRange(f.mhdir.Sub(dmh.Digest), fdb.RangeOptions{})
		// TODO: implement streaming variation since we are dealing with a streaming iterator anyway.
		iterator := vks.Iterator()
		var evks []dhstore.EncryptedValueKey
		var latestErr error
		for iterator.Advance() {
			kv, err := iterator.Get()
			if err != nil {
				latestErr = err
				logger.Errorw("failed to list encrypted value keys for multihash", "mh", mh.B58String(), "err", err)
				continue
			}
			// Check if value is empty, and if so then it means the original vk was shorter than the max
			// accepted key prefix and was used as is. Therefore, the key suffix is the value.
			if len(kv.Value) == 0 {
				unpack, err := f.mhdir.Unpack(kv.Key)
				if err != nil {
					latestErr = err
					logger.Errorw("failed to unpack key to extract value for multihash", "mh", mh.B58String(), "err", err)
					continue
				}
				if len(unpack) != 2 {
					logger.Errorw("expected unpacked key of length 2 ", "len", len(unpack), "mh", mh.B58String())
					continue
				}
				v, ok := unpack[1].([]byte)
				if !ok {
					logger.Errorw("expected unpacked key type bytes ", "got", unpack[0], "mh", mh.B58String())
					continue
				}
				evks = append(evks, v)
			} else {
				evks = append(evks, kv.Value)
			}
		}
		return evks, latestErr
	})
	if err != nil {
		// If error has occurred but we found some result, return whatever we found.
		if v == nil {
			return nil, err
		}
		evks, ok := v.([]dhstore.EncryptedValueKey)
		if !ok || len(evks) <= 0 {
			// Return the non-nil error.
			logger.Warnw("unexpected result from lookup transaction", "v", v, "err", err)
			return nil, err
		}
		return evks, nil
	}
	evks, ok := v.([]dhstore.EncryptedValueKey)
	switch {
	case !ok:
		logger.Warnw("unexpected result from lookup transaction", "v", v)
		return nil, fmt.Errorf("unexpected result from lookup")
	case len(evks) <= 0:
		return nil, nil
	default:
		return evks, nil
	}
}

func (f *FDBDHStore) GetMetadata(vk dhstore.HashedValueKey) (dhstore.EncryptedMetadata, error) {
	if len(vk) > maxKeyPrefixLen {
		return nil, dhstore.ErrInvalidHashedValueKey{Key: vk, Err: errMetadataKeyTooLong}
	}
	v, err := f.db.ReadTransact(func(transaction fdb.ReadTransaction) (any, error) {
		get := transaction.Get(f.mddir.Pack(tuple.Tuple{[]byte(vk)}))
		return get.Get()
	})
	switch {
	case err != nil:
		return nil, err
	case v == nil:
		return nil, nil
	default:
		md, ok := v.([]byte)
		if !ok {
			return nil, errors.New("unexpected result type")
		}
		return md, nil
	}
}

func (f *FDBDHStore) DeleteMetadata(vk dhstore.HashedValueKey) error {
	if len(vk) > maxKeyPrefixLen {
		return dhstore.ErrInvalidHashedValueKey{Key: vk, Err: errMetadataKeyTooLong}
	}
	_, err := f.db.Transact(func(transaction fdb.Transaction) (any, error) {
		transaction.Clear(f.mddir.Pack(tuple.Tuple{[]byte(vk)}))
		return nil, nil
	})
	return err
}

func (f *FDBDHStore) Close() error {
	return nil
}
