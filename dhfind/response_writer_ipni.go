package dhfind

import (
	"fmt"
	"net/http"

	"github.com/ipni/dhstore"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

var (
	_ LookupResponseWriter = (*ipniLookupResponseWriter)(nil)

	newline = []byte("\n")
)

type ipniLookupResponseWriter struct {
	jsonResponseWriter
	result model.MultihashResult
	count  int
	// isMultihash is true if the request is for a multihash, false if it is for a CID
	isMultihash bool
}

func NewIPNILookupResponseWriter(w http.ResponseWriter, mh multihash.Multihash, preferJson bool) LookupResponseWriter {
	return &ipniLookupResponseWriter{
		jsonResponseWriter: newJsonResponseWriter(w, preferJson),
		result: model.MultihashResult{
			Multihash: mh,
		},
	}
}

func (i *ipniLookupResponseWriter) Accept(r *http.Request) error {
	return i.jsonResponseWriter.Accept(r)
}

func (i *ipniLookupResponseWriter) Key() multihash.Multihash {
	return i.result.Multihash
}

func (i *ipniLookupResponseWriter) WriteProviderResult(pr model.ProviderResult) error {
	if i.nd {
		if err := i.encoder.Encode(pr); err != nil {
			return fmt.Errorf("failed to write encoded ndjson response: %w", err)
		}
		if _, err := i.w.Write(newline); err != nil {
			return fmt.Errorf("failed to write encoded ndjson response: %w", err)
		}
		if i.f != nil {
			i.f.Flush()
		}
	} else {
		i.result.ProviderResults = append(i.result.ProviderResults, pr)
	}
	i.count++
	return nil
}

func (i *ipniLookupResponseWriter) Close() error {
	if i.count == 0 {
		return dhstore.ErrHttpResponse{Status: http.StatusNotFound}
	}
	if i.nd {
		return nil
	}
	return i.encoder.Encode(model.FindResponse{
		MultihashResults: []model.MultihashResult{i.result},
	})
}
