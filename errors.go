package dhstore

import (
	"fmt"

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
)

func (e ErrUnsupportedMulticodecCode) Error() string {
	return fmt.Sprintf("multihash must be of code multihash.DBL_SHA2_256, got: %s", e.code.String())
}

func (e ErrMultihashDecode) Error() string {
	return fmt.Sprintf("failed to decode multihash %s: %s", e.mh.B58String(), e.err.Error())
}

func (e ErrMultihashDecode) Unwrap() error {
	return e.err
}
