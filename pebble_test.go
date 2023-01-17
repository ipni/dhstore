package dhstore_test

import (
	"testing"

	"github.com/ipni/dhstore"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestPebbleDHStore_MultihashCheck(t *testing.T) {
	someValue := dhstore.EncryptedValueKey("fish")
	notDblMh, err := multihash.Sum([]byte("fish"), multihash.SHA2_256, -1)
	require.NoError(t, err)

	tests := []struct {
		name        string
		givenMh     multihash.Multihash
		wantErrType error
	}{
		{
			name:        "invalid",
			givenMh:     multihash.Multihash("lobster"),
			wantErrType: dhstore.ErrMultihashDecode{},
		},
		{
			name:        "not dbl_sha2_256",
			givenMh:     notDblMh,
			wantErrType: dhstore.ErrUnsupportedMulticodecCode{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subject, err := dhstore.NewPebbleDHStore(t.TempDir(), nil)
			require.NoError(t, err)
			defer subject.Close()

			err = subject.MergeIndex(test.givenMh, someValue)
			require.Error(t, err)
			require.IsType(t, test.wantErrType, err)

			gotV, err := subject.Lookup(test.givenMh)
			require.Error(t, err)
			require.IsType(t, test.wantErrType, err)
			require.Nil(t, gotV)
		})
	}
}
