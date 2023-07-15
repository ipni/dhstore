package dhstore

import (
	"io"

	"github.com/multiformats/go-multihash"
)

type (
	EncryptedValueKey []byte
	EncryptedMetadata []byte
	HashedValueKey    []byte
	Index             struct {
		Key   multihash.Multihash `json:"key"`
		Value EncryptedValueKey   `json:"value"`
	}
	DHStore interface {
		io.Closer
		MergeIndexes([]Index) error
		PutMetadata(HashedValueKey, EncryptedMetadata) error
		Lookup(multihash.Multihash) ([]EncryptedValueKey, error)
		GetMetadata(HashedValueKey) (EncryptedMetadata, error)
		DeleteMetadata(HashedValueKey) error
	}
)

type (
	EncryptedValueKeyResult struct {
		EncryptedValueKey EncryptedValueKey `json:"EncryptedValueKey"`
	}
)
