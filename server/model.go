package server

import (
	"github.com/ipni/dhstore"
	"github.com/ipni/go-libipni/find/model"
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
		EncryptedMultihashResults []model.EncryptedMultihashResult `json:"EncryptedMultihashResults"`
	}
	GetMetadataResponse struct {
		EncryptedMetadata dhstore.EncryptedMetadata `json:"EncryptedMetadata"`
	}
	EncryptedValueKeyResult struct {
		EncryptedValueKey dhstore.EncryptedValueKey `json:"EncryptedValueKey"`
	}
)
