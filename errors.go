package dhstore

import (
	"fmt"
	"net/http"

	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

type (
	ErrUnsupportedMulticodecCode struct {
		Code multicodec.Code
	}
	ErrMultihashDecode struct {
		Mh  multihash.Multihash
		Err error
	}
	ErrInvalidHashedValueKey struct {
		Key HashedValueKey
		Err error
	}
	ErrHttpResponse struct {
		Message string
		Status  int
	}
)

func (e ErrUnsupportedMulticodecCode) Error() string {
	return fmt.Sprintf("multihash must be of code dbl-sha2-256, got: %s", e.Code.String())
}

func (e ErrMultihashDecode) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("failed to decode multihash %s: %s", e.Mh.B58String(), e.Err.Error())
	}
	return fmt.Sprintf("failed to decode multihash %s", e.Mh.B58String())
}

func (e ErrMultihashDecode) Unwrap() error {
	return e.Err
}

func (e ErrInvalidHashedValueKey) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("invalid hashed value key %s: %s", base58.Encode(e.Key), e.Err.Error())
	}
	return fmt.Sprintf("invalid hashed value key %s", base58.Encode(e.Key))
}

func (e ErrInvalidHashedValueKey) Unwrap() error {
	return e.Err
}

func (e ErrHttpResponse) Error() string {
	return e.Message
}

func (e ErrHttpResponse) WriteTo(w http.ResponseWriter) {
	http.Error(w, e.Message, e.Status)
}
