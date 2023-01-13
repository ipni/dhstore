package dhstore

import "github.com/multiformats/go-multihash"

type (
	IngestRequest struct {
		Merges []struct {
			Key   multihash.Multihash `json:"key"`
			Value EncryptedValueKey   `json:"value"`
		} `json:"merges"`

		Metadata struct {
			Key   HashedValueKey    `json:"key"`
			Value EncryptedMetadata `json:"value"`
		} `json:"metadata"`
	}

	MergeIndexRequest struct {
		Key   multihash.Multihash `json:"key"`
		Value EncryptedValueKey   `json:"value"`
	}

	PutMetadataRequest struct {
		Key   HashedValueKey    `json:"key"`
		Value EncryptedMetadata `json:"value"`
	}
)
