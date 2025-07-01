package pebble_test

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipni/dhstore"
	dhpebble "github.com/ipni/dhstore/pebble"
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
			subject, err := dhpebble.NewPebbleDHStore(t.TempDir(), nil)
			require.NoError(t, err)
			defer subject.Close()

			err = subject.MergeIndexes([]dhstore.Index{{Key: test.givenMh, Value: someValue}})
			require.Error(t, err)
			require.IsType(t, test.wantErrType, err)

			gotV, err := subject.Lookup(test.givenMh)
			require.Error(t, err)
			require.IsType(t, test.wantErrType, err)
			require.Nil(t, gotV)
		})
	}
}

func TestPebbleDHStore_UpdateFormat(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := dhpebble.NewPebbleDHStore(tmpDir, nil)
	require.NoError(t, err)
	defer store.Close()

	value1 := dhstore.EncryptedValueKey("lobster")
	value2 := dhstore.EncryptedValueKey("eel")
	value3 := dhstore.EncryptedValueKey("grouper")
	mh, err := multihash.Sum([]byte("fish"), multihash.DBL_SHA2_256, -1)
	require.NoError(t, err)

	err = store.MergeIndexes([]dhstore.Index{
		{Key: mh, Value: value1},
		{Key: mh, Value: value2},
		{Key: mh, Value: value3},
	})
	require.NoError(t, err)

	err = store.DeleteIndexes([]dhstore.Index{
		{Key: mh, Value: value3},
	})
	require.NoError(t, err)

	gotV, err := store.Lookup(mh)
	require.NoError(t, err)
	require.NotNil(t, gotV)
	require.Len(t, gotV, 2)

	store.Close()

	opts := &pebble.Options{
		FormatMajorVersion: pebble.FormatNewest,
	}

	store, err = dhpebble.NewPebbleDHStore(tmpDir, opts)
	require.NoError(t, err)
	defer store.Close()

	gotV, err = store.Lookup(mh)
	require.NoError(t, err)
	require.NotNil(t, gotV)
	require.Len(t, gotV, 2)
}
