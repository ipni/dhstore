package dhfind

import (
	"io"
	"net/http"

	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

type (
	SelectiveResponseWriter interface {
		http.ResponseWriter
		Accept(r *http.Request) error
	}
	LookupResponseWriter interface {
		io.Closer
		SelectiveResponseWriter
		Key() multihash.Multihash
		WriteProviderResult(model.ProviderResult) error
	}
)
