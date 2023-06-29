package server

import (
	"net/http"
	"path"
	"strings"

	"github.com/ipni/dhstore"
	"github.com/multiformats/go-multihash"
)

var (
	_ lookupResponseWriter = (*ipniLookupResponseWriter)(nil)

	newline = []byte("\n")
)

type ipniLookupResponseWriter struct {
	jsonResponseWriter
	result EncryptedMultihashResult
	count  int
}

func newIPNILookupResponseWriter(w http.ResponseWriter, preferJson bool) lookupResponseWriter {
	return &ipniLookupResponseWriter{
		jsonResponseWriter: newJsonResponseWriter(w, preferJson),
	}
}

func (i *ipniLookupResponseWriter) Accept(r *http.Request) error {
	err := i.jsonResponseWriter.Accept(r)
	if err != nil {
		return err
	}
	smh := strings.TrimPrefix(path.Base(r.URL.Path), "multihash/")
	i.result.Multihash, err = multihash.FromB58String(smh)
	if err != nil {
		return errHttpResponse{message: err.Error(), status: http.StatusBadRequest}
	}
	return nil
}

func (i *ipniLookupResponseWriter) Key() multihash.Multihash {
	return i.result.Multihash
}

func (i *ipniLookupResponseWriter) WriteEncryptedValueKey(evk dhstore.EncryptedValueKey) error {
	if i.nd {
		err := i.encoder.Encode(EncryptedValueKeyResult{
			EncryptedValueKey: evk,
		})
		if err != nil {
			log.Errorw("Failed to encode ndjson response", "err", err)
			return err
		}
		if _, err = i.w.Write(newline); err != nil {
			log.Errorw("Failed to encode ndjson response", "err", err)
			return err
		}
		if i.f != nil {
			i.f.Flush()
		}
	} else {
		i.result.EncryptedValueKeys = append(i.result.EncryptedValueKeys, evk)
	}
	i.count++
	return nil
}

func (i *ipniLookupResponseWriter) Close() error {
	if i.count == 0 {
		return errHttpResponse{status: http.StatusNotFound}
	}
	log.Debugw("Finished writing ipni results", "count", i.count)
	if i.nd {
		return nil
	}
	return i.encoder.Encode(LookupResponse{
		EncryptedMultihashResults: []EncryptedMultihashResult{i.result},
	})
}
