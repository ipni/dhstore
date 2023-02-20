package dhstore

import (
	"fmt"
	"net/http"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

type (
	ErrUnsupportedMulticodecCode struct {
		code multicodec.Code
	}
	ErrMultihashDecode struct {
		mh  multihash.Multihash
		err error
	}

	errHttpResponse struct {
		message string
		status  int
	}
)

func (e ErrUnsupportedMulticodecCode) Error() string {
	return fmt.Sprintf("multihash must be of code dbl-sha2-256, got: %s", e.code.String())
}

func (e ErrMultihashDecode) Error() string {
	if e.err != nil {
		return fmt.Sprintf("failed to decode multihash %s: %s", e.mh.B58String(), e.err.Error())
	}
	return fmt.Sprintf("failed to decode multihash %s", e.mh.B58String())
}

func (e ErrMultihashDecode) Unwrap() error {
	return e.err
}

func (e errHttpResponse) Error() string {
	return e.message
}

func (e errHttpResponse) WriteTo(w http.ResponseWriter) {
	http.Error(w, e.message, e.status)
}
