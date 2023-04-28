package dhstore

import (
	"github.com/multiformats/go-multihash"
)

type (
	EncryptedValueKey []byte
	EncryptedMetadata []byte
	HashedValueKey    []byte
	DHStore           interface {
		MergeIndex(multihash.Multihash, EncryptedValueKey) error
		PutMetadata(HashedValueKey, EncryptedMetadata) error
		Lookup(multihash.Multihash) ([]EncryptedValueKey, error)
		GetMetadata(HashedValueKey) (EncryptedMetadata, error)
		DeleteMetadata(HashedValueKey) error
	}
)
