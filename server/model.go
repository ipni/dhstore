package server

import (
	"github.com/ipni/dhstore"
	"github.com/multiformats/go-multihash"
)

type (
	MergeIndexRequest struct {
		Merges []dhstore.Index `json:"merges"`
	}
	PutMetadataRequest struct {
		Key   dhstore.HashedValueKey    `json:"key"`
		Value dhstore.EncryptedMetadata `json:"value"`
	}
	LookupResponse struct {
		EncryptedMultihashResults []EncryptedMultihashResult `json:"EncryptedMultihashResults"`
	}
	EncryptedMultihashResult struct {
		Multihash          multihash.Multihash         `json:"Multihash"`
		EncryptedValueKeys []dhstore.EncryptedValueKey `json:"EncryptedValueKeys"`
	}
	GetMetadataResponse struct {
		EncryptedMetadata dhstore.EncryptedMetadata `json:"EncryptedMetadata"`
	}
	EncryptedValueKeyResult struct {
		EncryptedValueKey dhstore.EncryptedValueKey `json:"EncryptedValueKey"`
	}
)
