package dhstore

import "github.com/multiformats/go-multihash"

type (
	MergeIndexRequest struct {
		Merges []struct {
			Key   multihash.Multihash `json:"key"`
			Value EncryptedValueKey   `json:"value"`
		} `json:"merges"`
	}
	PutMetadataRequest struct {
		Key   HashedValueKey    `json:"key"`
		Value EncryptedMetadata `json:"value"`
	}
	LookupResponse struct {
		EncryptedMultihashResults []EncryptedMultihashResult `json:"EncryptedMultihashResult"`
	}
	EncryptedMultihashResult struct {
		Multihash                multihash.Multihash `json:"Multihash"`
		EncryptedProviderResults []EncryptedValueKey `json:"EncryptedProviderResults"`
	}
	GetMetadataResponse struct {
		EncryptedMetadata EncryptedMetadata `json:"EncryptedMetadata"`
	}
)
