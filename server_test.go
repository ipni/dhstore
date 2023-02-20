package dhstore_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/metrics"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestNewHttpServeMux(t *testing.T) {
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
	}{
		{
			name:         "GET /multihash is 404",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash",
			expectStatus: http.StatusNotFound,
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
			name:         "PUT /multihash/subtree is 404",
			onMethod:     http.MethodPut,
			onTarget:     "/multihash/fish",
			expectStatus: http.StatusNotFound,
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
			name:         "GET /multihash/subtree with valid non-dbl-sha2-256 multihash is 400",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH",
			expectStatus: http.StatusBadRequest,
			expectBody:   "multihash must be of code dbl-sha2-256, got: sha2-256",
		},
		{
			name:         "GET /multihash/subtree with valid absent dbl-sha2-256 multihash is 404",
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus: http.StatusNotFound,
		},
		{
			name: "GET /multihash/subtree with valid present dbl-sha2-256 multihash is 200",
			onStore: func(t *testing.T, store dhstore.DHStore) {
				mh, err := multihash.FromB58String("2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82")
				require.NoError(t, err)
				require.NoError(t, store.MergeIndex(mh, []byte("fish")))
			},
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus: http.StatusOK,
			expectBody:   `{"EncryptedMultihashResults": [{ "Multihash": "ViAJKqT0hRtxENbtjWwvnRogQknxUnhswNrose3ZjEP8Iw==", "EncryptedValueKeys": ["ZmlzaA=="] }]}`,
			expectJSON:   true,
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
			name:           "streaming GET /multihash/subtree with invalid multihash is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/Qmackerel",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "input isn't valid multihash",
		},
		{
			name:           "streaming GET /multihash/subtree with valid non-dbl-sha2-256 multihash is 400",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/QmcgwdNjFQVhKt6aWWtSPgdLbNvULRoFMU6CCYwHsN3EEH",
			expectStatus:   http.StatusBadRequest,
			expectBody:     "multihash must be of code dbl-sha2-256, got: sha2-256",
		},
		{
			name:           "streaming GET /multihash/subtree with valid absent dbl-sha2-256 multihash is 404",
			onAcceptHeader: "application/x-ndjson",
			onMethod:       http.MethodGet,
			onTarget:       "/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
			expectStatus:   http.StatusNotFound,
		},
		{
			name:           "streaming GET /multihash/subtree with valid present dbl-sha2-256 multihash is 200",
			onAcceptHeader: "application/x-ndjson",
			onStore: func(t *testing.T, store dhstore.DHStore) {
				mh, err := multihash.FromB58String("2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82")
				require.NoError(t, err)
				require.NoError(t, store.MergeIndex(mh, []byte("fish")))
				require.NoError(t, store.MergeIndex(mh, []byte("lobster")))
				require.NoError(t, store.MergeIndex(mh, []byte("undadasea")))
			},
			onMethod:     http.MethodGet,
			onTarget:     "/multihash/2wvdp9y1J63yDvaPawP4kUjXezRLcu9x9u2DAB154dwai82",
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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := dhstore.NewPebbleDHStore(t.TempDir(), nil)
			require.NoError(t, err)
			defer store.Close()
			if test.onStore != nil {
				test.onStore(t, store)
			}
			m, err := metrics.New("0.0.0.0:40081")
			require.NoError(t, err)

			subject := dhstore.NewHttpServeMux(store, m)

			given := httptest.NewRequest(test.onMethod, test.onTarget, bytes.NewBufferString(test.onBody))
			if test.onAcceptHeader != "" {
				given.Header.Set("Accept", test.onAcceptHeader)
			}
			got := httptest.NewRecorder()
			subject.ServeHTTP(got, given)
			require.Equal(t, test.expectStatus, got.Code)

			gotBody, err := io.ReadAll(got.Body)
			require.NoError(t, err)
			if test.expectJSON {
				require.JSONEq(t, test.expectBody, strings.TrimSpace(string(gotBody)))
			} else {
				require.Equal(t, test.expectBody, strings.TrimSpace(string(gotBody)))
			}
		})
	}
}
