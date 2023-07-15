package rwriter

import (
	"net/http"

	"github.com/ipni/dhstore"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
)

var _ ResponseWriter = (*EncResponseWriter)(nil)

type EncResponseWriter struct {
	jsonResponseWriter
	encResult model.EncryptedMultihashResult
	count     int
	pathType  string // "cid" or "multihash"
}

func (w *EncResponseWriter) Key() multihash.Multihash {
	return w.encResult.Multihash
}

func (w *EncResponseWriter) PathType() string {
	return w.pathType
}

func (w *EncResponseWriter) WriteEncryptedValueKey(evk dhstore.EncryptedValueKey) error {
	if w.nd {
		err := w.encoder.Encode(dhstore.EncryptedValueKeyResult{
			EncryptedValueKey: evk,
		})
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
		w.encResult.EncryptedValueKeys = append(w.encResult.EncryptedValueKeys, evk)
	}
	w.count++
	return nil
}

func (w *EncResponseWriter) Close() error {
	if w.count == 0 {
		return apierror.New(nil, http.StatusNotFound)
	}
	if w.nd {
		return nil
	}
	return w.encoder.Encode(model.FindResponse{
		EncryptedMultihashResults: []model.EncryptedMultihashResult{w.encResult},
	})
}
