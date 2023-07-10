package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/dhfind"
	"github.com/ipni/dhstore/metrics"
	"github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
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
	rec.ResponseWriter.WriteHeader(code)
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

	mux.HandleFunc("/cid/", s.handleCidSubtree)
	mux.HandleFunc("/multihash", s.handleMh)
	mux.HandleFunc("/multihash/", s.handleMhSubtree)
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
	default:
		w.Header().Set("Allow", http.MethodPut)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleCidSubtree(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	default:
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
	}

	scid := strings.TrimPrefix(path.Base(r.URL.Path), "cid/")
	c, err := cid.Decode(scid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b := c.Hash()
	dm, err := multihash.Decode(b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mh := multihash.Multihash(b)

	if dm.Code == multihash.DBL_SHA2_256 {
		s.lookupMh(w, r, mh)
	} else {
		s.dhfindMh(w, r, mh, "cid")
	}
}

func (s *Server) handleMhSubtree(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	default:
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	smh := strings.TrimPrefix(path.Base(r.URL.Path), "multihash/")

	b, err := base58.Decode(smh)
	if err != nil {
		http.Error(w, multihash.ErrInvalidMultihash.Error(), http.StatusBadRequest)
		return
	}
	dm, err := multihash.Decode(b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mh := multihash.Multihash(b)

	if dm.Code == multihash.DBL_SHA2_256 {
		s.lookupMh(w, r, mh)
	} else {
		s.dhfindMh(w, r, mh, "multihash")
	}
}

func (s *Server) dhfindMh(w http.ResponseWriter, r *http.Request, mh multihash.Multihash, httpPath string) {
	if s.dhfind == nil {
		http.Error(w, "multihash must be of code dbl-sha2-256 when dhfind not enabled", http.StatusBadRequest)
		return
	}

	var start time.Time
	if s.metrics != nil {
		ws := newResponseWriterWithStatus(w)
		w = ws
		start = time.Now()
		defer func() {
			s.metrics.RecordDHFindLatency(context.Background(), time.Since(start), r.Method, httpPath, ws.status, false)
		}()
	}

	rspw := dhfind.NewIPNILookupResponseWriter(w, mh, s.preferJSON)

	err := rspw.Accept(r)
	if err != nil {
		var httpErr *dhstore.ErrHttpResponse
		if errors.As(err, &httpErr) {
			httpErr.WriteTo(w)
		} else {
			log.Errorw("Failed to accept lookup request", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
		return
	}

	// create result and error channels
	resChan := make(chan model.ProviderResult)
	errChan := make(chan error, 1)

	// launch the find in a separate go routine
	go func() {
		// FindAsync returns results on resChan until there are no more results
		// or error. When finished, returns the error or nil.
		errChan <- s.dhfind.FindAsync(r.Context(), mh, resChan)
	}()

	var haveResults bool

	for pr := range resChan {
		if !haveResults {
			haveResults = true
			if s.metrics != nil {
				s.metrics.RecordDHFindLatency(context.Background(), time.Since(start), r.Method, httpPath, http.StatusOK, true)
			}
		}
		if err = rspw.WriteProviderResult(pr); err != nil {
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
		s.handleError(w, err)
		return
	}

	// If there were no results - return 404, otherwise finalize the response
	// and return 200.
	if !haveResults {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	if err = rspw.Close(); err != nil {
		var httpErr *dhstore.ErrHttpResponse
		if errors.As(err, &httpErr) {
			httpErr.WriteTo(w)
		} else {
			log.Errorw("Failed to finalize lookup results", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
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
func (s *Server) FindMetadata(ctx context.Context, hvk []byte) ([]byte, error) {
	encMeta, err := s.dhs.GetMetadata(dhstore.HashedValueKey(hvk))
	return encMeta, err
}

func (s *Server) lookupMh(w http.ResponseWriter, r *http.Request, mh multihash.Multihash) {
	if s.metrics != nil {
		ws := newResponseWriterWithStatus(w)
		w = ws
		start := time.Now()
		defer func() {
			s.metrics.RecordHttpLatency(context.Background(), time.Since(start), r.Method, "multihash", ws.status)
		}()
	}

	s.handleGetMh(newIPNILookupResponseWriter(w, mh, s.preferJSON), r)
}

func (s *Server) handleGetMh(w lookupResponseWriter, r *http.Request) {
	if err := w.Accept(r); err != nil {
		switch e := err.(type) {
		case errHttpResponse:
			e.WriteTo(w)
		default:
			log.Errorw("Failed to accept lookup request", "err", err)
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
			log.Errorw("Failed to encode encrypted value key", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
	}
	if err = w.Close(); err != nil {
		switch e := err.(type) {
		case errHttpResponse:
			e.WriteTo(w)
		default:
			log.Errorw("Failed to finalize lookup results", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
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
	log.Infow("Finished putting multihashes", "count", len(mir.Merges), "sample", mir.Merges[0].Key.B58String())

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
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if err = s.dhs.PutMetadata(pmr.Key, pmr.Value); err != nil {
		s.handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	log.Infow("Finished putting metadata", "keyLen", len(pmr.Key), "valueLen", len(pmr.Value))
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
	sk := strings.TrimPrefix(path.Base(r.URL.Path), "metadata/")
	hvk, err := base58.Decode(sk)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot decode key %s as bas58: %s", sk, err.Error()), http.StatusBadRequest)
		return
	}
	emd, err := s.FindMetadata(r.Context(), hvk)
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
		log.Errorw("Failed to write get metadata response", "err", err, "key", sk)
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
