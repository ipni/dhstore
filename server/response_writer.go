package server

import (
	"io"
	"net/http"

	"github.com/ipni/dhstore"
	"github.com/multiformats/go-multihash"
)

type (
	selectiveResponseWriter interface {
		http.ResponseWriter
		Accept(r *http.Request) error
	}
	lookupResponseWriter interface {
		io.Closer
		selectiveResponseWriter
		Key() multihash.Multihash
		WriteEncryptedValueKey(dhstore.EncryptedValueKey) error
	}
)
