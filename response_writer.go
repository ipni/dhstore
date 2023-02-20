package dhstore

import (
	"io"
	"net/http"

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
		WriteEncryptedValueKey(EncryptedValueKey) error
	}
)
