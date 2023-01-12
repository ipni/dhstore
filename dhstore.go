package dhstore

import (
	"github.com/multiformats/go-multihash"
)

type (
	EncryptedIndexKey []byte
	EncryptedMetadata []byte
	HashedIndexKey    []byte
	DHStore           interface {
		MergeIndex(multihash.Multihash, EncryptedIndexKey) error
		PutMetadata(HashedIndexKey, EncryptedMetadata) error
		Lookup(multihash.Multihash) ([]EncryptedIndexKey, error)
		GetMetadata(HashedIndexKey) (EncryptedMetadata, error)
	}
)
