package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/metrics"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/rwriter"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("server/http")

type Server struct {
	s          *http.Server
	metrics    *metrics.Metrics
	dhs        dhstore.DHStore
	preferJSON bool

	// dhfind is a dh client that is optionally enabled to allow non-dh
	// lookups. If is enabled by providing a valid providersURL.
	dhfind *client.DHashClient
}

// responseWriterWithStatus is required to capture status code from
// ResponseWriter so that it can be reported to metrics in a unified way.
type responseWriterWithStatus struct {
	http.ResponseWriter
	status int
}

func newResponseWriterWithStatus(w http.ResponseWriter) *responseWriterWithStatus {
	return &responseWriterWithStatus{
		ResponseWriter: w,
		// 200 status should be assumed by default if WriteHeader hasn't been
		// called explicitly.
		status: 200,
	}
}

func (rec *responseWriterWithStatus) WriteHeader(code int) {
	rec.status = code
	if code != http.StatusOK {
		rec.ResponseWriter.WriteHeader(code)
	}
}

func New(dhs dhstore.DHStore, addr string, options ...Option) (*Server, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	s := &Server{
		dhs:        dhs,
		metrics:    opts.metrics,
		preferJSON: opts.preferJSON,
		s: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	mux.HandleFunc("/cid/", s.handleNoEncMhOrCidSubtree)
	mux.HandleFunc("/encrypted/cid/", s.handleEncMhOrCidSubtree)
	mux.HandleFunc("/multihash", s.handleMh)
	mux.HandleFunc("/encrypted/multihash", s.handleMh)
	mux.HandleFunc("/multihash/", s.handleNoEncMhOrCidSubtree)
	mux.HandleFunc("/encrypted/multihash/", s.handleEncMhOrCidSubtree)
	mux.HandleFunc("/metadata", s.handleMetadata)
	mux.HandleFunc("/metadata/", s.handleMetadataSubtree)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/", s.handleCatchAll)

	if len(opts.providersURLs) != 0 {
		s.dhfind, err = client.NewDHashClient(client.WithProvidersURL(opts.providersURLs...), client.WithDHStoreAPI(s))
		if err != nil {
			return nil, err
		}
		log.Infow("dhfind enabled", "providersURLs", opts.providersURLs)
	}

	return s, nil
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

	log.Infow("Server started", "addr", ln.Addr())
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.s.Shutdown(ctx)
}

func (s *Server) handleMh(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		ws := newResponseWriterWithStatus(w)
		w = ws
		start := time.Now()
		defer func() {
			s.metrics.RecordHttpLatency(context.Background(), time.Since(start), r.Method, "multihash", ws.status)
		}()
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePutMhs(w, r)
	case http.MethodDelete:
		s.handleDeleteMhs(w, r)
	default:
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleEncMhOrCidSubtree(w http.ResponseWriter, r *http.Request) {
	s.handleMhOrCidSubtree(w, r, true)
}

func (s *Server) handleNoEncMhOrCidSubtree(w http.ResponseWriter, r *http.Request) {
	s.handleMhOrCidSubtree(w, r, false)
}

func (s *Server) handleMhOrCidSubtree(w http.ResponseWriter, r *http.Request, encrypted bool) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	rspWriter, err := rwriter.New(w, r, rwriter.WithPreferJson(s.preferJSON))
	if err != nil {
		log.Errorw("Failed to accept lookup request", "err", err)
		writeError(w, err)
		return
	}

	if encrypted {
		s.lookupMh(newEncResponseWriter(rspWriter), r, true)
		return
	}
	// If multihash is DBL_SHA2_256, then this is probably an encrypted lookup,
	// so try that first. If no results found, then do a non-encrypted lookup.
	// It is possible for a non-encrypted multihash to be DBL_SHA2_256.
	if rspWriter.MultihashCode() == multihash.DBL_SHA2_256 && s.lookupMh(newEncResponseWriter(rspWriter), r, s.dhfind == nil) {
		return
	}
	// Do non-encrypted lookup. All encrypted multihashes are DBL_SHA2_256, so
	// there is no need to do an encrypted lookup for a non-DBL_SHA2_256
	// multihash.
	s.dhfindMh(rwriter.NewProviderResponseWriter(rspWriter), r)
}

func (s *Server) lookupMh(w *encResponseWriter, r *http.Request, writeIfNotFound bool) bool {
	var start time.Time
	if s.metrics != nil {
		start = time.Now()
		defer func() {
			if start.IsZero() {
				return // metrics skipped
			}
			s.metrics.RecordHttpLatency(context.Background(), time.Since(start), r.Method, w.PathType(), w.StatusCode())
		}()
	}

	evks, err := s.dhs.Lookup(w.Multihash())
	if err != nil {
		s.handleError(w, err)
		return true
	}
	if evks == nil && !writeIfNotFound {
		start = time.Time{} // skip mettics
		return false
	}
	for _, evk := range evks {
		if err = w.writeEncryptedValueKey(evk); err != nil {
			log.Errorw("Failed to encode encrypted value key", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return true
		}
	}
	if err = w.close(); err != nil {
		log.Errorw("Failed to finalize lookup results", "err", err)
		writeError(w, err)
	}
	return true
}

func (s *Server) dhfindMh(w *rwriter.ProviderResponseWriter, r *http.Request) {
	if s.dhfind == nil {
		http.Error(w, "unencrypted lookup not available when dhfind not enabled", http.StatusBadRequest)
		return
	}

	var start time.Time
	if s.metrics != nil {
		start = time.Now()
		defer func() {
			s.metrics.RecordDHFindLatency(context.Background(), time.Since(start), r.Method, w.PathType(), w.StatusCode(), false)
		}()
	}

	// create result and error channels
	resChan := make(chan model.ProviderResult)
	errChan := make(chan error, 1)

	// launch the find in a separate go routine
	go func() {
		// FindAsync returns results on resChan until there are no more results
		// or error. When finished, returns the error or nil.
		errChan <- s.dhfind.FindAsync(r.Context(), w.Multihash(), resChan)
	}()

	var haveResults bool
	var err error
	for pr := range resChan {
		if !haveResults {
			haveResults = true
			if s.metrics != nil {
				s.metrics.RecordDHFindLatency(context.Background(), time.Since(start), r.Method, w.PathType(), http.StatusOK, true)
			}
		}
		if err = w.WriteProviderResult(pr); err != nil {
			log.Errorw("Failed to encode provider result", "err", err)
			// This error is due to the client disconnecting. Continue reading
			// from resChan until it is done due to the client context being
			// canceled. The canceled context prevents this error from
			// repeating.
			continue
		}
	}

	// FindAsync finished, check for error.
	err = <-errChan
	if err != nil {
		log.Errorw("Failed dhfind multihash lookup", "err", err)
		s.handleError(w, err)
		return
	}

	// If there were no results - return 404, otherwise finalize the response
	// and return 200.
	if !haveResults {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	if err = w.Close(); err != nil {
		log.Errorw("Failed to finalize lookup results", "err", err)
		writeError(w, err)
		return
	}
}

func writeError(w http.ResponseWriter, err error) {
	var apiErr *apierror.Error
	if errors.As(err, &apiErr) {
		http.Error(w, apiErr.Error(), apiErr.Status())
	} else {
		http.Error(w, "", http.StatusInternalServerError)
	}
}

// FindMultihash implements client.DHStoreAPI interface.
func (s *Server) FindMultihash(ctx context.Context, dhmh multihash.Multihash) ([]model.EncryptedMultihashResult, error) {
	evks, err := s.dhs.Lookup(dhmh)
	if err != nil {
		return nil, err
	}

	result := model.EncryptedMultihashResult{
		Multihash: dhmh,
	}
	for _, evk := range evks {
		result.EncryptedValueKeys = append(result.EncryptedValueKeys, evk)
	}

	return []model.EncryptedMultihashResult{result}, nil
}

// FindMetadata implements the client.DHSToreAPI interface, to lookup encrypted
// metadata using a hash of the value key.
//
// If metadata not found then no data and no error, (nil, nil), returned.
func (s *Server) FindMetadata(ctx context.Context, hvk []byte) ([]byte, error) {
	return s.dhs.GetMetadata(dhstore.HashedValueKey(hvk))
}

func (s *Server) handlePutMhs(w http.ResponseWriter, r *http.Request) {
	var mir MergeIndexRequest
	err := json.NewDecoder(r.Body).Decode(&mir)
	if err != nil {
		log.Errorw("Cannot decode merge index request", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if len(mir.Merges) == 0 {
		log.Error("Cannot put multihashes with no merges specified")
		http.Error(w, "at least one merge must be specified", http.StatusBadRequest)
		return
	}
	if err = s.dhs.MergeIndexes(mir.Merges); err != nil {
		log.Errorw("Failed to merge indexes", "err", err)
		s.handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleDeleteMhs(w http.ResponseWriter, r *http.Request) {
	var mir MergeIndexRequest
	err := json.NewDecoder(r.Body).Decode(&mir)
	if err != nil {
		log.Errorw("Cannot decode delete index request", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if len(mir.Merges) == 0 {
		log.Error("Cannot delete multihashes with no merges specified")
		http.Error(w, "at least one merge must be specified", http.StatusBadRequest)
		return
	}
	if err = s.dhs.DeleteIndexes(mir.Merges); err != nil {
		log.Errorw("Failed to delete indexes", "err", err)
		s.handleError(w, err)
		return
	}
	log.Infow("Deleted indexes", "count", len(mir.Merges))
	w.WriteHeader(http.StatusAccepted)
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
	if s.metrics != nil {
		ws := newResponseWriterWithStatus(w)
		w = ws
		start := time.Now()
		defer func() {
			s.metrics.RecordHttpLatency(context.Background(), time.Since(start), r.Method, "metadata", ws.status)
		}()
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePutMetadata(w, r)
	default:
		w.Header().Set("Allow", http.MethodPut)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
	var pmr PutMetadataRequest
	err := json.NewDecoder(r.Body).Decode(&pmr)
	if err != nil {
		log.Errorw("Cannot decode put metadata request", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if err = s.dhs.PutMetadata(pmr.Key, pmr.Value); err != nil {
		log.Errorw("Failed to put metadata", "err", err)
		s.handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleMetadataSubtree(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		ws := newResponseWriterWithStatus(w)
		w = ws
		start := time.Now()
		defer func() {
			s.metrics.RecordHttpLatency(context.Background(), time.Since(start), r.Method, "metadata", ws.status)
		}()
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGetMetadata(w, r)
	case http.MethodDelete:
		s.handleDeleteMetadata(w, r)
	default:
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	sk := path.Base(r.URL.Path)
	hvk, err := base58.Decode(sk)
	if err != nil {
		log.Errorw("Cannot decode metadata key as base58", "err", err, "key", sk)
		http.Error(w, fmt.Sprintf("cannot decode key %s as base58: %s", sk, err.Error()), http.StatusBadRequest)
		return
	}
	emd, err := s.FindMetadata(r.Context(), hvk)
	if err != nil {
		log.Errorw("Failed to find metadata", "err", err)
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
		log.Errorw("Failed to write get metadata response", "err", err, "key", sk)
	}
}

func (s *Server) handleDeleteMetadata(w http.ResponseWriter, r *http.Request) {
	sk := path.Base(r.URL.Path)
	b, err := base58.Decode(sk)
	if err != nil {
		log.Errorw("Cannot decode metadata key as base58", "err", err, "key", sk)
		http.Error(w, fmt.Sprintf("cannot decode key %s as base58: %s", sk, err.Error()), http.StatusBadRequest)
		return
	}
	hvk := dhstore.HashedValueKey(b)
	if err = s.dhs.DeleteMetadata(hvk); err != nil {
		log.Errorw("Failed to delete metadata", "err", err)
		s.handleError(w, err)
		return
	}
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Cache-Control", "no-cache")
	http.Error(w, dhstore.Version, http.StatusOK)
}

func (s *Server) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "", http.StatusNotFound)
}
