package dhstore_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ipni/dhstore"
	"github.com/stretchr/testify/require"
)

func TestNewHttpServeMux(t *testing.T) {
	tests := []struct {
		name         string
		onMethod     string
		onTarget     string
		onBody       string
		expectStatus int
		expectBody   string
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := dhstore.NewPebbleDHStore(t.TempDir(), nil)
			require.NoError(t, err)
			defer store.Close()
			subject := dhstore.NewHttpServeMux(store)

			given := httptest.NewRequest(test.onMethod, test.onTarget, bytes.NewBufferString(test.onBody))
			got := httptest.NewRecorder()
			subject.ServeHTTP(got, given)
			require.Equal(t, test.expectStatus, got.Code)

			gotBody, err := io.ReadAll(got.Body)
			require.NoError(t, err)
			require.Equal(t, test.expectBody, strings.TrimSpace(string(gotBody)))
		})
	}
}
