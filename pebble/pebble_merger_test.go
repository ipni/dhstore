package pebble

import (
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestValueKeysMerger_IsAssociative(t *testing.T) {
	store, err := NewPebbleDHStore(t.TempDir(), nil)
	require.NoError(t, err)
	defer store.Close()

	subject := store.newValueKeysMerger()

	bk := store.p.leaseSimpleKeyer()
	k, err := bk.multihashKey(multihash.Multihash("fish"))
	require.NoError(t, err)

	a := []byte{0x1, 0x65}
	b := []byte{0x1, 0x66}
	c := []byte{0x1, 0x67}

	oneMerge, err := subject.Merge(k.buf, a)
	require.NoError(t, err)
	require.NoError(t, oneMerge.MergeOlder(b))
	require.NoError(t, oneMerge.MergeOlder(c))
	gotOne, _, err := oneMerge.Finish(false)
	require.NoError(t, err)

	anotherMerge, err := subject.Merge(k.buf, c)
	require.NoError(t, err)

	require.NoError(t, anotherMerge.MergeNewer(b))
	require.NoError(t, anotherMerge.MergeNewer(a))
	gotAnother, _, err := anotherMerge.Finish(false)
	require.NoError(t, err)
	require.Equal(t, gotOne, gotAnother, "merge is not associative. %v != %v", gotOne, gotAnother)
}

func TestValueKeysMerger_RemovesDuplicateValues(t *testing.T) {
	store, err := NewPebbleDHStore(t.TempDir(), nil)
	require.NoError(t, err)
	defer store.Close()

	subject := store.newValueKeysMerger()

	bk := store.p.leaseSimpleKeyer()
	k, err := bk.multihashKey(multihash.Multihash("fish"))
	require.NoError(t, err)

	a := []byte{0x1, 0x65}
	b := []byte{0x1, 0x66}
	c := []byte{0x1, 0x67}
	wantMerge := append(a, append(b, c...)...)

	merger, err := subject.Merge(k.buf, a)
	require.NoError(t, err)
	require.NoError(t, merger.MergeNewer(b))
	require.NoError(t, merger.MergeNewer(c))
	require.NoError(t, merger.MergeNewer(c))
	require.NoError(t, merger.MergeNewer(a))
	require.NoError(t, merger.MergeNewer(c))
	require.NoError(t, merger.MergeNewer(b))
	require.NoError(t, merger.MergeNewer(b))
	require.NoError(t, merger.MergeNewer(a))
	require.NoError(t, merger.MergeNewer(a))
	gotMerged, _, err := merger.Finish(false)
	require.NoError(t, err)
	require.Equal(t, wantMerge, gotMerged)

}
