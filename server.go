package dhstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/ipni/dhstore/metrics"
	"github.com/mr-tron/base58"
)

// preferJSON specifies weather to prefer JSON over NDJSON response when request accepts */*, i.e.
// any response format, has no `Accept` header at all.
const preferJSON = true

type Server struct {
	s   *http.Server
	m   *metrics.Metrics
	dhs DHStore
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

func NewHttpServer(dhs DHStore, m *metrics.Metrics, addr string) (*Server, error) {
	var dhss Server
	dhss.s = &http.Server{
		Addr:    addr,
		Handler: dhss.serveMux(),
	}

	dhss.dhs = dhs
	dhss.m = m
	return &dhss, nil
}

func NewHttpServeMux(dhs DHStore, m *metrics.Metrics) *http.ServeMux {
	s := &Server{
		dhs: dhs,
		m:   m,
	}
	return s.serveMux()
}

func (s *Server) serveMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/multihash", s.handleMh)
	mux.HandleFunc("/multihash/", s.handleMhSubtree)
	mux.HandleFunc("/metadata", s.handleMetadata)
	mux.HandleFunc("/metadata/", s.handleMetadataSubtree)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/", s.handleCatchAll)
	return mux
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

func (s *Server) handleMh(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer s.reportLatency(start, ws.status, r.Method, "multihash")
	switch r.Method {
	case http.MethodPut:
		s.handlePutMhs(ws, r)
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
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
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handlePutMhs(w http.ResponseWriter, r *http.Request) {
	var mir MergeIndexRequest
	err := json.NewDecoder(r.Body).Decode(&mir)
	discardBody(r)
	if err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if len(mir.Merges) == 0 {
		http.Error(w, "at least one merge must be specified", http.StatusBadRequest)
	}

	// TODO: Use pebble batch which will require interface changes.
	//       But no big deal for now because every write to pebble is by NoSync.
	for _, merge := range mir.Merges {
		if err := s.dhs.MergeIndex(merge.Key, merge.Value); err != nil {
			s.handleError(w, err)
			return
		}
	}
	logger.Infow("Finished putting multihashes", "count", len(mir.Merges))
	if len(mir.Merges) != 0 {
		logger.Infow("Multihash to try out", "mh", mir.Merges[0].Key.B58String())
	}
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
	case ErrUnsupportedMulticodecCode, ErrMultihashDecode:
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
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var pmr PutMetadataRequest
	err := json.NewDecoder(r.Body).Decode(&pmr)
	discardBody(r)
	if err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if err := s.dhs.PutMetadata(pmr.Key, pmr.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	logger.Infow("Finished putting metadata", "keyLen", len(pmr.Key), "valueLen", len(pmr.Value), "time", time.Since(start))
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
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	discardBody(r)
	sk := strings.TrimPrefix(path.Base(r.URL.Path), "metadata/")
	b, err := base58.Decode(sk)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot decode key %s as bas58: %s", sk, err.Error()), http.StatusBadRequest)
		return
	}
	hvk := HashedValueKey(b)
	emd, err := s.dhs.GetMetadata(hvk)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(emd) == 0 {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	gmr := GetMetadataResponse{
		EncryptedMetadata: emd,
	}
	if err := json.NewEncoder(w).Encode(gmr); err != nil {
		logger.Errorw("Failed to write get metadata response", "err", err, "key", sk)
	}
}

func (s *Server) handleDeleteMetadata(w http.ResponseWriter, r *http.Request) {
	discardBody(r)
	sk := strings.TrimPrefix(path.Base(r.URL.Path), "metadata/")
	b, err := base58.Decode(sk)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot decode key %s as bas58: %s", sk, err.Error()), http.StatusBadRequest)
		return
	}
	hvk := HashedValueKey(b)
	err = s.dhs.DeleteMetadata(hvk)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ws := newResponseWriterWithStatus(w)
	defer s.reportLatency(start, ws.status, r.Method, "ready")
	discardBody(r)
	switch r.Method {
	case http.MethodGet:
		ws.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	discardBody(r)
	http.Error(w, "", http.StatusNotFound)
}

func (s *Server) reportLatency(start time.Time, status int, method, path string) {
	s.m.RecordHttpLatency(context.Background(), time.Since(start), method, path, status)
}

func discardBody(r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()
}
