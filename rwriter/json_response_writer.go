package rwriter

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"

	"github.com/ipni/go-libipni/apierror"
)

const (
	mediaTypeNDJson = "application/x-ndjson"
	mediaTypeJson   = "application/json"
	mediaTypeAny    = "*/*"
)

var _ http.ResponseWriter = (*jsonResponseWriter)(nil)

type jsonResponseWriter struct {
	w          http.ResponseWriter
	f          http.Flusher
	encoder    *json.Encoder
	nd         bool
	preferJson bool
	status     int
}

func newJsonResponseWriter(w http.ResponseWriter, r *http.Request, preferJson bool) (jsonResponseWriter, error) {
	accepts := r.Header.Values("Accept")
	var nd, okJson bool
	for _, accept := range accepts {
		amts := strings.Split(accept, ",")
		for _, amt := range amts {
			mt, _, err := mime.ParseMediaType(amt)
			if err != nil {
				return jsonResponseWriter{}, apierror.New(errors.New("invalid Accept header"), http.StatusBadRequest)
			}
			switch mt {
			case mediaTypeNDJson:
				nd = true
			case mediaTypeJson:
				okJson = true
			case mediaTypeAny:
				nd = !preferJson
				okJson = true
			}
			if nd && okJson {
				break
			}
		}
	}

	if len(accepts) == 0 {
		if !preferJson {
			// If there is no `Accept` header and JSON is preferred then be forgiving and fall back
			// onto JSON media type. Otherwise, strictly require `Accept` header.
			return jsonResponseWriter{}, apierror.New(errors.New("accept header must be specified"), http.StatusBadRequest)
		}
	} else if !okJson && !nd {
		return jsonResponseWriter{}, apierror.New(fmt.Errorf("media type not supported: %s", accepts), http.StatusBadRequest)
	}

	flusher, _ := w.(http.Flusher)

	if nd {
		w.Header().Set("Content-Type", mediaTypeNDJson)
		w.Header().Set("Connection", "Keep-Alive")
		w.Header().Set("X-Content-Type-Options", "nosniff")
	} else {
		w.Header().Set("Content-Type", mediaTypeJson)
	}

	return jsonResponseWriter{
		w:          w,
		f:          flusher,
		encoder:    json.NewEncoder(w),
		nd:         nd,
		preferJson: preferJson,
		status:     http.StatusOK,
	}, nil
}

func (w *jsonResponseWriter) Header() http.Header {
	return w.w.Header()
}

func (w *jsonResponseWriter) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func (w *jsonResponseWriter) WriteHeader(code int) {
	w.status = code
	w.w.WriteHeader(code)
}

func (w *jsonResponseWriter) Status() int {
	return w.status
}
