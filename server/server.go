package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/metrics"
	"github.com/mr-tron/base58"
)

var logger = logging.Logger("server/http")

// preferJSON specifies weather to prefer JSON over NDJSON response when request accepts */*, i.e.
// any response format, has no `Accept` header at all.
const preferJSON = true

type Server struct {
	s   *http.Server
	m   *metrics.Metrics
	dhs dhstore.DHStore
}

// responseWriterWithStatus is required to capture status code from ResponseWriter so that it can be reported
// to metrics in a unified way
type responseWriterWithStatus struct {
	http.ResponseWriter
	status int
}

func newResponseWriterWithStatus(w http.ResponseWriter) *responseWriterWithStatus {
	return &responseWriterWithStatus{
		ResponseWriter: w,
		// 200 status should be assumed by default if WriteHeader hasn't been called explicitly https://pkg.go.dev/net/http#ResponseWriter
		status: 200,
	}
}

func (rec *responseWriterWithStatus) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

func New(dhs dhstore.DHStore, m *metrics.Metrics, addr string) *Server {
	mux := http.NewServeMux()
	s := &Server{
		dhs: dhs,
		m:   m,
		s: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	mux.HandleFunc("/multihash", s.handleMh)
	mux.HandleFunc("/multihash/", s.handleMhSubtree)
	mux.HandleFunc("/metadata", s.handleMetadata)
	mux.HandleFunc("/metadata/", s.handleMetadataSubtree)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/", s.handleCatchAll)

	return s
}

func (s *Server) Handler() http.Handler {
	return s.s.Handler
}

func (s *Server) Start(_ context.Context) error {
	ln, err := net.Listen("tcp", s.s.Addr)
	if err != nil {
		return err
	}
	go func() { _ = s.s.Serve(ln) }()

	logger.Infow("Server started", "addr", ln.Addr())
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.s.Shutdown(ctx)
}

func methodOK(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

func methodsOK(w http.ResponseWriter, r *http.Request, methods ...string) bool {
	for _, method := range methods {
		if r.Method == method {
			return true
		}
	}
	for _, method := range methods {
		w.Header().Add("Allow", method)
	}
	http.Error(w, "", http.StatusMethodNotAllowed)
	return false
}

func (s *Server) handleMh(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer s.reportLatency(start, ws.status, r.Method, "multihash")

	switch r.Method {
	case http.MethodPut:
		s.handlePutMhs(ws, r)
	default:
		w.Header().Set("Allow", http.MethodPut)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleMhSubtree(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer s.reportLatency(start, ws.status, r.Method, "multihash")

	switch r.Method {
	case http.MethodGet:
		s.handleGetMh(newIPNILookupResponseWriter(ws, preferJSON), r)
	default:
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePutMhs(w http.ResponseWriter, r *http.Request) {
	var mir MergeIndexRequest
	err := json.NewDecoder(r.Body).Decode(&mir)
	if err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if len(mir.Merges) == 0 {
		http.Error(w, "at least one merge must be specified", http.StatusBadRequest)
		return
	}
	if err = s.dhs.MergeIndexes(mir.Merges); err != nil {
		s.handleError(w, err)
		return
	}
	logger.Infow("Finished putting multihashes", "count", len(mir.Merges), "sample", mir.Merges[0].Key.B58String())

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleGetMh(w lookupResponseWriter, r *http.Request) {
	if err := w.Accept(r); err != nil {
		switch e := err.(type) {
		case errHttpResponse:
			e.WriteTo(w)
		default:
			logger.Errorw("Failed to accept lookup request", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
		return
	}
	evks, err := s.dhs.Lookup(w.Key())
	if err != nil {
		s.handleError(w, err)
		return
	}
	for _, evk := range evks {
		if err := w.WriteEncryptedValueKey(evk); err != nil {
			logger.Errorw("Failed to encode encrypted value key", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
	}
	if err := w.Close(); err != nil {
		switch e := err.(type) {
		case errHttpResponse:
			e.WriteTo(w)
		default:
			logger.Errorw("Failed to finalize lookup results", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
	}
}

func (s *Server) handleError(w http.ResponseWriter, err error) {
	var status int
	switch err.(type) {
	case dhstore.ErrUnsupportedMulticodecCode, dhstore.ErrMultihashDecode, dhstore.ErrInvalidHashedValueKey:
		status = http.StatusBadRequest
	default:
		status = http.StatusInternalServerError
	}
	http.Error(w, err.Error(), status)
}

func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer s.reportLatency(start, ws.status, r.Method, "metadata")

	switch r.Method {
	case http.MethodPut:
		s.handlePutMetadata(ws, r)
	default:
		w.Header().Set("Allow", http.MethodPut)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
	var pmr PutMetadataRequest
	err := json.NewDecoder(r.Body).Decode(&pmr)
	if err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if err = s.dhs.PutMetadata(pmr.Key, pmr.Value); err != nil {
		s.handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	logger.Infow("Finished putting metadata", "keyLen", len(pmr.Key), "valueLen", len(pmr.Value))
}

func (s *Server) handleMetadataSubtree(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer func() {
		s.m.RecordHttpLatency(context.Background(), time.Since(start), r.Method, "metadata", ws.status)
	}()

	switch r.Method {
	case http.MethodGet:
		s.handleGetMetadata(ws, r)
	case http.MethodDelete:
		s.handleDeleteMetadata(ws, r)
	default:
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	sk := strings.TrimPrefix(path.Base(r.URL.Path), "metadata/")
	b, err := base58.Decode(sk)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot decode key %s as bas58: %s", sk, err.Error()), http.StatusBadRequest)
		return
	}
	hvk := dhstore.HashedValueKey(b)
	emd, err := s.dhs.GetMetadata(hvk)
	if err != nil {
		s.handleError(w, err)
		return
	}
	if len(emd) == 0 {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	gmr := GetMetadataResponse{
		EncryptedMetadata: emd,
	}
	if err = json.NewEncoder(w).Encode(gmr); err != nil {
		logger.Errorw("Failed to write get metadata response", "err", err, "key", sk)
	}
}

func (s *Server) handleDeleteMetadata(w http.ResponseWriter, r *http.Request) {
	sk := strings.TrimPrefix(path.Base(r.URL.Path), "metadata/")
	b, err := base58.Decode(sk)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot decode key %s as bas58: %s", sk, err.Error()), http.StatusBadRequest)
		return
	}
	hvk := dhstore.HashedValueKey(b)
	if err = s.dhs.DeleteMetadata(hvk); err != nil {
		s.handleError(w, err)
		return
	}
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer s.reportLatency(start, ws.status, r.Method, "ready")

	switch r.Method {
	case http.MethodGet:
		ws.WriteHeader(http.StatusOK)
	default:
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "", http.StatusNotFound)
}

func (s *Server) reportLatency(start time.Time, status int, method, path string) {
	s.m.RecordHttpLatency(context.Background(), time.Since(start), method, path, status)
}
