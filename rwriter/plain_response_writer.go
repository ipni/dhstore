package rwriter

import (
	"net/http"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

var _ ResponseWriter = (*PlainResponseWriter)(nil)

type PlainResponseWriter struct {
	jsonResponseWriter
	result   model.MultihashResult
	count    int
	pathType string // "cid" or "multihash"
}

func (w *PlainResponseWriter) Key() multihash.Multihash {
	return w.result.Multihash
}

func (w *PlainResponseWriter) PathType() string {
	return w.pathType
}

func (w *PlainResponseWriter) WriteProviderResult(pr model.ProviderResult) error {
	if w.nd {
		err := w.encoder.Encode(pr)
		if err != nil {
			return err
		}
		if _, err = w.w.Write(newline); err != nil {
			return err
		}
		if w.f != nil {
			w.f.Flush()
		}
	} else {
		w.result.ProviderResults = append(w.result.ProviderResults, pr)
	}
	w.count++
	return nil
}

func (w *PlainResponseWriter) Close() error {
	if w.count == 0 {
		return apierror.New(nil, http.StatusNotFound)
	}
	if w.nd {
		return nil
	}
	return w.encoder.Encode(model.FindResponse{
		MultihashResults: []model.MultihashResult{w.result},
	})
}
