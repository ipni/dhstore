package server

import (
	"net/http"

	"github.com/ipni/dhstore"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/rwriter"
)

type encResponseWriter struct {
	rwriter.ResponseWriter
	count     int
	encResult model.EncryptedMultihashResult
}

func newEncResponseWriter(w rwriter.ResponseWriter) *encResponseWriter {
	return &encResponseWriter{
		ResponseWriter: w,
		encResult: model.EncryptedMultihashResult{
			Multihash: w.Multihash(),
		},
	}
}

func (ew *encResponseWriter) writeEncryptedValueKey(evk dhstore.EncryptedValueKey) error {
	if ew.IsND() {
		err := ew.Encoder().Encode(dhstore.EncryptedValueKeyResult{
			EncryptedValueKey: evk,
		})
		if err != nil {
			return err
		}
		ew.Flush()
	} else {
		ew.encResult.EncryptedValueKeys = append(ew.encResult.EncryptedValueKeys, evk)
	}
	ew.count++
	return nil
}

func (ew *encResponseWriter) close() error {
	if ew.count == 0 {
		return apierror.New(nil, http.StatusNotFound)
	}
	if ew.IsND() {
		return nil
	}
	return ew.Encoder().Encode(model.FindResponse{
		EncryptedMultihashResults: []model.EncryptedMultihashResult{ew.encResult},
	})
}
