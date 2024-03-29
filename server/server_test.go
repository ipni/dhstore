package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/metrics"
	"github.com/ipni/dhstore/pebble"
	"github.com/ipni/dhstore/server"
	"github.com/ipni/go-libipni/dhash"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestNewServeMux(t *testing.T) {
	tests := []struct {
		name           string
		onStore        func(*testing.T, dhstore.DHStore)
		onAcceptHeader string
		onMethod       string
		onTarget       string
		onBody         string
		expectStatus   int
		expectBody     string
		expectJSON     bool
		dhfind         bool
	}{
		{
			name:         "GET /multihash is 405",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /encrypted/multihash is 405",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "PUT /multihash with no body is 400",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "PUT /multihash with invalid body is 400",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			onBody:       "{]",
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "PUT /multihash with no merges is 400",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			onBody:       "{}",
			expectStatus: http.StatusBadRequest,
			expectBody:   "at least one merge must be specified",
		},
		{
			name:         "PUT /multihash with no merges is 400",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			onBody:       "{}",
			expectStatus: http.StatusBadRequest,
			expectBody:   "at least one merge must be specified",
		},
		{
			name:         "PUT /multihash with invalid multihash is 400",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			onBody:       `{ "merges": [{ "key": "fish", "value": "lobster" }] }`,
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "PUT /multihash with valid non-dbl-sha2-256 multihash is 400",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			onBody:       `{ "merges": [{ "key": "EiC0dKmaJwXiPPkFpITsbRTvWLVrvmLpKSeDRm7DY7UHLQ==", "value": "ZmlzaA==" }] }`,
			expectStatus: http.StatusBadRequest,
			expectBody:   "multihash must be of code dbl-sha2-256, got: sha2-256",
		},
		{
			name:         "DELETE /multihash with valid non-dbl-sha2-256 multihash is 400",
			onMethod:     http.MethodDelete,
			onTarget:     "/multihash",
			onBody:       `{ "merges": [{ "key": "EiC0dKmaJwXiPPkFpITsbRTvWLVrvmLpKSeDRm7DY7UHLQ==", "value": "ZmlzaA==" }] }`,
			expectStatus: http.StatusBadRequest,
			expectBody:   "multihash must be of code dbl-sha2-256, got: sha2-256",
		},
		{
			name:         "PUT /multihash with invalid value is 400",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			onBody:       `{ "merges": [{ "key": "ViAJKqT0hRtxENbtjWwvnRogQknxUnhswNrose3ZjEP8Iw==", "value": "fish is not base64" }] }`,
			expectStatus: http.StatusBadRequest,
		},
		{
			name:         "PUT /multihash with valid dbl-sha2-256 multihash and base64 value is 202",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash",
			onBody:       `{ "merges": [{ "key": "ViAJKqT0hRtxENbtjWwvnRogQknxUnhswNrose3ZjEP8Iw==", "value": "ZmlzaA==" }] }`,
			expectStatus: http.StatusAccepted,
		},
		{
			name:         "DELETE /multihash with valid dbl-sha2-256 multihash and base64 value is 202",
			onMethod:     http.MethodDelete,
			onTarget:     "/multihash",
			onBody:       `{ "merges": [{ "key": "ViAJKqT0hRtxENbtjWwvnRogQknxUnhswNrose3ZjEP8Iw==", "value": "ZmlzaA==" }] }`,
			expectStatus: http.StatusAccepted,
		},
		{
			name:         "PUT /multihash/subtree is 405",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash/fish",
			expectStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "GET /multihash/subtree with bad length is 400",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/asda",
			expectStatus: http.StatusBadRequest,
			expectBody:   "length greater than remaining number of bytes in buffer",
		},
		{
			name:         "GET /multihash/subtree with invalid varint is 400",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/Quickfish",
			expectStatus: http.StatusBadRequest,
			expectBody:   "varint not minimally encoded",
		},
		{
			name:         "GET /multihash/subtree with invalid multihash is 400",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/Qmackerel",
			expectStatus: http.StatusBadRequest,
			expectBody:   "input isn't valid multihash",
		},
		{
			name:         "GET /multihash/subtree with valid non-dbl-sha2-256 multihash an no dhfind is 400",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH",
			expectStatus: http.StatusBadRequest,
			expectBody:   "unencrypted lookup not available when dhfind not enabled",
		},
		{
			name:         "GET /multihash/subtree with valid non-dbl-sha2-256 multihash and dhfind is 404",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH",
			expectStatus: http.StatusNotFound,
			dhfind:       true,
		},
		{
			name:         "GET /encrypted/multihash/subtree with valid absent dbl-sha2-256 multihash is 404",
			onMethod:     http.MethodGet,
			onTarget:     "/encrypted/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus: http.StatusNotFound,
		},
		{
			name: "GET /encrypted/multihash/subtree with valid present dbl-sha2-256 multihash is 200",
			onStore: func(t *testing.T, store dhstore.DHStore) {
				mh, err := multihash.FromB58String("2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82")
				require.NoError(t, err)
				require.NoError(t, store.MergeIndexes([]dhstore.Index{{Key: mh, Value: []byte("fish")}}))
			},
			onMethod:     http.MethodGet,
			onTarget:     "/encrypted/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus: http.StatusOK,
			expectBody:   `{"EncryptedMultihashResults": [{ "Multihash": "ViAJKqT0hRtxENbtjWwvnRogQknxUnhswNrose3ZjEP8Iw==", "EncryptedValueKeys": ["ZmlzaA=="] }]}`,
			expectJSON:   true,
		},
		{
			name: "GET /multihash/subtree with valid present dbl-sha2-256 multihash encrypted lookup is 200",
			onStore: func(t *testing.T, store dhstore.DHStore) {
				mh, err := multihash.FromB58String("2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82")
				require.NoError(t, err)
				require.NoError(t, store.MergeIndexes([]dhstore.Index{{Key: mh, Value: []byte("fish")}}))
			},
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus: http.StatusOK,
			expectBody:   `{"EncryptedMultihashResults": [{ "Multihash": "ViAJKqT0hRtxENbtjWwvnRogQknxUnhswNrose3ZjEP8Iw==", "EncryptedValueKeys": ["ZmlzaA=="] }]}`,
			expectJSON:   true,
		},
		{
			name:         "GET /multihash/subtree with valid absent dbl-sha2-256 multihash unencrypted lookup is 404",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus: http.StatusNotFound,
			dhfind:       true,
		},
		{
			name:           "streaming GET /multihash/subtree with bad length is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/asda",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "length greater than remaining number of bytes in buffer",
		},
		{
			name:           "streaming GET /multihash/subtree with invalid varint is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/Quickfish",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "varint not minimally encoded",
		},
		{
			name:           "streaming GET /encrypted/multihash/subtree with bad length is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/encrypted/multihash/asda",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "length greater than remaining number of bytes in buffer",
		},
		{
			name:           "streaming GET /encrypted/multihash/subtree with invalid varint is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/encrypted/multihash/Quickfish",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "varint not minimally encoded",
		},
		{
			name:           "streaming GET /multihash/subtree with invalid multihash is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/Qmackerel",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "input isn't valid multihash",
		},
		{
			name:           "streaming GET /encrypted/multihash/subtree with invalid multihash is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/encrypted/multihash/Qmackerel",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "input isn't valid multihash",
		},
		{
			name:           "streaming GET /multihash/subtree with valid non-dbl-sha2-256 multihash is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "unencrypted lookup not available when dhfind not enabled",
		},
		{
			name:           "streaming GET /multihash/subtree with valid non-dbl-sha2-256 multihash and dhfind is 404",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH",
			expectStatus:   http.StatusNotFound,
			dhfind:         true,
		},
		{
			name:           "streaming GET /encrypted/multihash/subtree with valid absent dbl-sha2-256 multihash is 404",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/encrypted/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus:   http.StatusNotFound,
		},
		{
			name:           "streaming GET /encrypted/multihash/subtree with valid present dbl-sha2-256 multihash is 200",
			onAcceptHeader: "application/x-ndjson",
			onStore: func(t *testing.T, store dhstore.DHStore) {
				mh, err := multihash.FromB58String("2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82")
				require.NoError(t, err)
				require.NoError(t, store.MergeIndexes([]dhstore.Index{
					{Key: mh, Value: []byte("fish")},
					{Key: mh, Value: []byte("lobster")},
					{Key: mh, Value: []byte("undadasea")},
				}))
			},
			onMethod:     http.MethodGet,
			onTarget:     "/encrypted/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus: http.StatusOK,
			expectBody: `{"EncryptedValueKey":"ZmlzaA=="}
{"EncryptedValueKey":"bG9ic3Rlcg=="}
{"EncryptedValueKey":"dW5kYWRhc2Vh"}`,
		},
		{
			name:         "PUT /metadata with valid key value is 202",
			onMethod:     http.MethodPut,
			onBody:       `{"key": "ZmlzaA==", "value": "ZmlzaA==" }`,
			onTarget:     "/metadata",
			expectStatus: http.StatusAccepted,
		},
		{
			name: "GET /metadata with existing key is 200",
			onStore: func(t *testing.T, store dhstore.DHStore) {
				key := []byte("fish")
				err := store.PutMetadata(key, []byte("lobster"))
				require.NoError(t, err)
				t.Logf("metadata with key %s stored", base58.Encode(key))
			},
			onMethod:     http.MethodGet,
			onBody:       `{"key": "ZmlzaA==", "value": "ZmlzaA==" }`,
			onTarget:     "/metadata/3cqA6K",
			expectStatus: http.StatusOK,
			expectBody:   `{"EncryptedMetadata":"bG9ic3Rlcg=="}`,
			expectJSON:   true,
		},
	}

	provServ := httptest.NewServer(http.HandlerFunc(providersHandler))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := pebble.NewPebbleDHStore(t.TempDir(), nil)
			require.NoError(t, err)
			defer store.Close()
			if test.onStore != nil {
				test.onStore(t, store)
			}
			m, err := metrics.New("0.0.0.0:40081", nil)
			require.NoError(t, err)

			var s *server.Server
			if test.dhfind {
				s, err = server.New(store, "", server.WithMetrics(m), server.WithDHFind(provServ.URL))
			} else {
				s, err = server.New(store, "", server.WithMetrics(m))
			}
			require.NoError(t, err)
			subject := s.Handler()

			given := httptest.NewRequest(test.onMethod, test.onTarget, bytes.NewBufferString(test.onBody))
			if test.onAcceptHeader != "" {
				given.Header.Set("Accept", test.onAcceptHeader)
			}
			got := httptest.NewRecorder()
			subject.ServeHTTP(got, given)
			require.Equal(t, test.expectStatus, got.Code)

			gotBody, err := io.ReadAll(got.Body)
			require.NoError(t, err)
			if test.expectBody != "" {
				if test.expectJSON {
					require.JSONEq(t, test.expectBody, strings.TrimSpace(string(gotBody)))
				} else {
					require.Equal(t, test.expectBody, strings.TrimSpace(string(gotBody)))
				}
			}
		})
	}
}

func TestDHFind(t *testing.T) {
	provServ := httptest.NewServer(http.HandlerFunc(providersHandler))

	store, err := pebble.NewPebbleDHStore(t.TempDir(), nil)
	require.NoError(t, err)
	defer store.Close()

	origMh, err := multihash.FromB58String("QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH")
	require.NoError(t, err)

	const providerID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
	pid, err := peer.Decode(providerID)
	require.NoError(t, err)
	ctxID := []byte("fish")
	metadata := []byte("lobster")

	loadStore(t, origMh, ctxID, metadata, pid, store)

	s, err := server.New(store, "", server.WithDHFind(provServ.URL))
	require.NoError(t, err)
	subject := s.Handler()

	given := httptest.NewRequest(http.MethodGet, "/multihash/"+origMh.B58String(), nil)
	got := httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusOK, got.Code)
	gotBody, err := io.ReadAll(got.Body)
	require.NoError(t, err)

	t.Log("Got find response:", string(gotBody))
	findRsp, err := model.UnmarshalFindResponse(gotBody)
	require.NoError(t, err)

	require.Len(t, findRsp.EncryptedMultihashResults, 0)
	require.Len(t, findRsp.MultihashResults, 1)
	mhr := findRsp.MultihashResults[0]
	require.Equal(t, origMh, mhr.Multihash)
	require.Len(t, mhr.ProviderResults, 1)
	pr := mhr.ProviderResults[0]
	require.Equal(t, ctxID, pr.ContextID)
	require.Equal(t, metadata, pr.Metadata)
	require.Equal(t, pid, pr.Provider.ID)

	given = httptest.NewRequest(http.MethodGet, "/multihash/"+origMh.B58String(), nil)
	given.Header.Set("Accept", "application/x-ndjson")
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusOK, got.Code)
	gotBody, err = io.ReadAll(got.Body)
	require.NoError(t, err)

	t.Log("Got provider result:", string(gotBody))
	err = json.Unmarshal(gotBody, &pr)
	require.NoError(t, err)
	require.Equal(t, ctxID, pr.ContextID)
	require.Equal(t, metadata, pr.Metadata)
	require.Equal(t, pid, pr.Provider.ID)

	given = httptest.NewRequest(http.MethodGet, "/cid/bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy", nil)
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusOK, got.Code)
	gotBody, err = io.ReadAll(got.Body)
	require.NoError(t, err)
	findRsp, err = model.UnmarshalFindResponse(gotBody)
	require.NoError(t, err)
	require.Len(t, findRsp.MultihashResults, 1)

	// Test that results not found when only metadata is deleted.
	deleteMetadata(t, ctxID, pid, store)

	given = httptest.NewRequest(http.MethodGet, "/cid/bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy", nil)
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusNotFound, got.Code)

	given = httptest.NewRequest(http.MethodGet, "/multihash/"+origMh.B58String(), nil)
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusNotFound, got.Code)
}

func TestGetDeleteIndexes(t *testing.T) {
	provServ := httptest.NewServer(http.HandlerFunc(providersHandler))

	store, err := pebble.NewPebbleDHStore(t.TempDir(), nil)
	require.NoError(t, err)
	defer store.Close()

	origMh, err := multihash.FromB58String("QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH")
	require.NoError(t, err)

	const providerID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
	pid, err := peer.Decode(providerID)
	require.NoError(t, err)
	const providerID2 = "12D3KooWQk7r5WKUfTn9dVntWnmvfHfVBaghWtDdZNkRExQ7NwK1"
	pid2, err := peer.Decode(providerID2)
	require.NoError(t, err)

	ctxID := []byte("fish")
	metadata := []byte("lobster")
	ctxID2 := []byte("rodent")
	metadata2 := []byte("squirrel")

	dhMh := loadStore(t, origMh, ctxID, metadata, pid, store)
	dhMh2 := loadStore(t, origMh, ctxID2, metadata2, pid2, store)

	s, err := server.New(store, "", server.WithDHFind(provServ.URL))
	require.NoError(t, err)
	subject := s.Handler()

	// Get encrypted multihash find response.
	given := httptest.NewRequest(http.MethodGet, "/encrypted/multihash/"+dhMh.B58String(), nil)
	got := httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusOK, got.Code)
	gotBody, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	t.Log("Got encrypted find response using double-hashed multihash:", string(gotBody))
	findRsp, err := model.UnmarshalFindResponse(gotBody)
	require.NoError(t, err)

	// Check that encrypted find response is correct.
	require.Len(t, findRsp.MultihashResults, 0)
	require.Len(t, findRsp.EncryptedMultihashResults, 1)
	encMhResult := findRsp.EncryptedMultihashResults[0]
	require.Len(t, encMhResult.EncryptedValueKeys, 2)
	evk := encMhResult.EncryptedValueKeys[0]
	vk, err := dhash.DecryptValueKey(evk, origMh)
	require.NoError(t, err)
	respPid, respCtxID, err := dhash.SplitValueKey(vk)
	require.NoError(t, err)
	require.Equal(t, pid, respPid)
	require.Equal(t, ctxID, respCtxID)

	evk2 := encMhResult.EncryptedValueKeys[1]
	vk2, err := dhash.DecryptValueKey(evk2, origMh)
	require.NoError(t, err)
	respPid2, respCtxID2, err := dhash.SplitValueKey(vk2)
	require.NoError(t, err)
	require.Equal(t, pid2, respPid2)
	require.Equal(t, ctxID2, respCtxID2)

	// Get metadata for find response.
	hvk := dhash.SHA256(vk, nil)
	given = httptest.NewRequest(http.MethodGet, "/metadata/"+base58.Encode(hvk), nil)
	given.Header.Add("Accept", "application/json")
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusOK, got.Code)
	gotBody, err = io.ReadAll(got.Body)
	require.NoError(t, err)
	require.NotZero(t, len(gotBody))

	type GetMetadataResponse struct {
		EncryptedMetadata []byte `json:"EncryptedMetadata"`
	}

	// Check that metadata descrypts and is correct.
	findResponse := &GetMetadataResponse{}
	err = json.Unmarshal(gotBody, findResponse)
	require.NoError(t, err)
	encMetadata := findResponse.EncryptedMetadata
	require.NotZero(t, len(encMetadata))
	respMeta, err := dhash.DecryptMetadata(encMetadata, vk)
	require.NoError(t, err)
	require.Equal(t, metadata, respMeta)

	// Delete first provider from from index.
	mergeReq := makeMergeReq(dhMh, evk)
	reqData, err := json.Marshal(mergeReq)
	require.NoError(t, err)
	given = httptest.NewRequest(http.MethodDelete, "/multihash", bytes.NewBuffer(reqData))
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusAccepted, got.Code)

	// Check that first provider is deleted
	given = httptest.NewRequest(http.MethodGet, "/encrypted/multihash/"+dhMh.B58String(), nil)
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusOK, got.Code)
	gotBody, err = io.ReadAll(got.Body)
	require.NoError(t, err)
	t.Log("Got encrypted find response using double-hashed multihash:", string(gotBody))
	findRsp, err = model.UnmarshalFindResponse(gotBody)
	require.NoError(t, err)
	require.Len(t, findRsp.EncryptedMultihashResults, 1)
	encMhResult = findRsp.EncryptedMultihashResults[0]
	require.Len(t, encMhResult.EncryptedValueKeys, 1)
	require.Equal(t, evk2, encMhResult.EncryptedValueKeys[0])

	// Delete 2nd provider from index.
	mergeReq = makeMergeReq(dhMh2, evk2)
	reqData, err = json.Marshal(mergeReq)
	require.NoError(t, err)
	given = httptest.NewRequest(http.MethodDelete, "/multihash", bytes.NewBuffer(reqData))
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusAccepted, got.Code)

	// Check that index is now completely deleted.
	given = httptest.NewRequest(http.MethodGet, "/encrypted/multihash/"+dhMh.B58String(), nil)
	got = httptest.NewRecorder()
	subject.ServeHTTP(got, given)
	require.Equal(t, http.StatusNotFound, got.Code)
}

func makeMergeReq(dhMh multihash.Multihash, evk dhstore.EncryptedValueKey) server.MergeIndexRequest {
	idx := dhstore.Index{
		Key:   dhMh,
		Value: evk,
	}
	return server.MergeIndexRequest{
		Merges: []dhstore.Index{idx},
	}
}

func loadStore(t *testing.T, origMh multihash.Multihash, ctxID, metadata []byte, providerID peer.ID, store *pebble.PebbleDHStore) multihash.Multihash {
	vk := dhash.CreateValueKey(providerID, ctxID)

	encMeta, err := dhash.EncryptMetadata(metadata, vk)
	require.NoError(t, err)

	err = store.PutMetadata(dhash.SHA256(vk, nil), encMeta)
	require.NoError(t, err)

	// Encrypt value key with original multihash.
	encValueKey, err := dhash.EncryptValueKey(vk, origMh)
	require.NoError(t, err)

	mh2 := dhash.SecondMultihash(origMh)
	err = store.MergeIndexes([]dhstore.Index{
		{
			Key:   mh2,
			Value: []byte(encValueKey),
		},
	})
	require.NoError(t, err)

	return mh2
}

func deleteMetadata(t *testing.T, ctxID []byte, providerID peer.ID, store *pebble.PebbleDHStore) {
	vk := dhash.CreateValueKey(providerID, ctxID)

	err := store.DeleteMetadata(dhash.SHA256(vk, nil))
	require.NoError(t, err)
}

func providersHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	providerID, err := peer.Decode(path.Base(req.URL.Path))
	if err != nil {
		fmt.Println("Cannot get provider ID:", err)
	}

	maddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9876")

	ai := peer.AddrInfo{
		ID:    providerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	pinfo := model.ProviderInfo{
		AddrInfo:  ai,
		Publisher: &ai,
	}
	data, err := json.Marshal(pinfo)
	if err != nil {
		panic(err.Error())
	}

	if req.URL.Path == "/providers" {
		var buf bytes.Buffer
		buf.Grow(len(data) + 2)
		buf.Write([]byte("["))
		buf.Write(data)
		buf.Write([]byte("]"))
		writeJsonResponse(w, http.StatusOK, buf.Bytes())
		return
	}
	writeJsonResponse(w, http.StatusOK, data)
}

func writeJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		http.Error(w, "", http.StatusInternalServerError)
	}
}
