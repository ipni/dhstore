package dhstore

import "github.com/multiformats/go-multihash"

type (
	MergeIndexRequest struct {
		Key   multihash.Multihash `json:"key"`
		Value EncryptedIndexKey   `json:"value"`
	}

	PutMetadataRequest struct {
		Key   HashedIndexKey    `json:"key"`
		Value EncryptedMetadata `json:"value"`
	}
)
