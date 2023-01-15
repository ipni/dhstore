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

	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multihash"
)

type Server struct {
	s   *http.Server
	dhs DHStore
}

func NewHttpServer(dhs DHStore, addr string) (*Server, error) {
	var dhss Server
	dhss.s = &http.Server{
		Addr:    addr,
		Handler: dhss.serveMux(),
	}
	dhss.dhs = dhs
	return &dhss, nil
}

func NewHttpServeMux(dhs DHStore) *http.ServeMux {
	s := &Server{
		dhs: dhs,
	}
	return s.serveMux()
}

func (s *Server) serveMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/multihash", s.handleMh)
	mux.HandleFunc("/multihash/", s.handleMhSubtree)
	mux.HandleFunc("/metadata/", s.handleMetadata)
	mux.HandleFunc("/", s.handleCatchAll)
	return mux
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
	switch r.Method {
	case http.MethodPut:
		s.handlePutMhs(w, r)
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handleMhSubtree(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetMh(w, r)
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
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleGetMh(w http.ResponseWriter, r *http.Request) {
	discardBody(r)

	smh := strings.TrimPrefix(path.Base(r.URL.Path), "multihash/")
	mh, err := multihash.FromB58String(smh)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	evks, err := s.dhs.Lookup(mh)
	if err != nil {
		s.handleError(w, err)
		return
	}
	if len(evks) == 0 {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	lr := LookupResponse{
		EncryptedMultihashResults: []EncryptedMultihashResult{
			{
				Multihash:                mh,
				EncryptedProviderResults: evks,
			},
		},
	}
	if err := json.NewEncoder(w).Encode(lr); err != nil {
		log.Errorw("Failed to write lookup response", "err", err, "mh", smh, "resultsCount", len(evks))
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
	switch r.Method {
	case http.MethodPut:
		s.handlePutMetadata(w, r)
	case http.MethodGet:
		s.handleGetMetadata(w, r)
	default:
		discardBody(r)
		http.Error(w, "", http.StatusNotFound)
	}
}

func (s *Server) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
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
		log.Errorw("Failed to write get metadata response", "err", err, "key", sk)
	}
}

func (s *Server) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	discardBody(r)
	http.Error(w, "", http.StatusNotFound)
}

func discardBody(r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()
}
