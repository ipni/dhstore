package dhstore_test

import (
	"math/rand"
	"testing"

	"github.com/ipni/dhstore"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func BenchmarkDHStore_PutMultihashes(b *testing.B) {
	// 200 is chosen as approximation for enc(peerID + contextID) length
	benchmarkPutMultihashes(b, 500_000, 200)
}

func BenchmarkDHStore_GetMultihashes(b *testing.B) {
	benchmarkGetMultihashes(b, 500_000, 200)
}

func BenchmarkDHStore_PutMetadata(b *testing.B) {
	// 34 is chosen as a length of multihash, 500 as an approximation for enc(metadata) length
	// when it's not bitswap
	benchmarkPutMetadata(b, 500_000, 34, 500)
}

func BenchmarkDHStore_GetMetadata(b *testing.B) {
	benchmarkGetMetadatas(b, 500_000, 34, 500)
}

func benchmarkPutMultihashes(b *testing.B, n, vkLen int) {
	rng := rand.New(rand.NewSource(1413))

	store, err := dhstore.NewPebbleDHStore(b.TempDir(), nil)
	require.NoError(b, err)

	defer store.Close()

	mhs := randomMultihashes(b, rng, n)
	vks := randomBytes(b, rng, n, vkLen)

	// all multihashes are the same in size
	b.SetBytes(int64(n * (len(mhs[0]) + vkLen)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			putMultihashes(b, mhs, vks, store)
		}
	})
	b.StopTimer()
}

func benchmarkGetMultihashes(b *testing.B, n, vkLen int) {
	rng := rand.New(rand.NewSource(1413))

	store, err := dhstore.NewPebbleDHStore(b.TempDir(), nil)
	require.NoError(b, err)

	defer store.Close()

	mhs := randomMultihashes(b, rng, n)
	vks := randomBytes(b, rng, n, vkLen)

	putMultihashes(b, mhs, vks, store)
	rng.Shuffle(len(mhs), func(i, j int) {
		mhs[i], mhs[j] = mhs[j], mhs[i]
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			getMultihashes(b, mhs, store)
		}
	})
	b.StopTimer()
}

func benchmarkPutMetadata(b *testing.B, n, hvkLen, mdLen int) {
	rng := rand.New(rand.NewSource(1413))

	store, err := dhstore.NewPebbleDHStore(b.TempDir(), nil)
	require.NoError(b, err)

	defer store.Close()

	hvks := randomBytes(b, rng, n, hvkLen)
	metadatas := randomBytes(b, rng, n, mdLen)

	b.SetBytes(int64(n * (hvkLen + mdLen)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			putMetadatas(b, hvks, metadatas, store)
		}
	})
	b.StopTimer()
}

func benchmarkGetMetadatas(b *testing.B, n, hvkLen, mdLen int) {
	rng := rand.New(rand.NewSource(1413))

	store, err := dhstore.NewPebbleDHStore(b.TempDir(), nil)
	require.NoError(b, err)

	defer store.Close()

	hvks := randomBytes(b, rng, n, hvkLen)
	metadatas := randomBytes(b, rng, n, mdLen)
	putMetadatas(b, hvks, metadatas, store)

	rng.Shuffle(len(hvks), func(i, j int) {
		hvks[i], hvks[j] = hvks[j], hvks[i]
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			getMetadatas(b, hvks, store)
		}
	})
	b.StopTimer()
}

func getMultihashes(b *testing.B, mhs []multihash.Multihash, store *dhstore.PebbleDHStore) {
	for _, mh := range mhs {
		evks, err := store.Lookup(mh)
		require.NoError(b, err)
		require.NotNil(b, evks)
	}
}

func putMultihashes(b *testing.B, mhs []multihash.Multihash, vks [][]byte, store *dhstore.PebbleDHStore) {
	for i, mh := range mhs {
		err := store.MergeIndex(mh, vks[i])
		require.NoError(b, err)
	}
}

func putMetadatas(b *testing.B, hvks, metadatas [][]byte, store *dhstore.PebbleDHStore) {
	for i, hvk := range hvks {
		err := store.PutMetadata(hvk, metadatas[i])
		require.NoError(b, err)
	}
}

func getMetadatas(b *testing.B, hvks [][]byte, store *dhstore.PebbleDHStore) {
	for _, hvk := range hvks {
		md, err := store.GetMetadata(hvk)
		require.NoError(b, err)
		require.NotNil(b, md)
	}
}

func randomMultihashes(b *testing.B, rng *rand.Rand, n int) []multihash.Multihash {
	mhs := make([]multihash.Multihash, n)

	var buf [100]byte
	for i := 0; i < n; i++ {
		_, err := rng.Read(buf[:])
		require.NoError(b, err)
		mh, err := multihash.Sum(buf[:], multihash.DBL_SHA2_256, -1)
		require.NoError(b, err)
		mhs[i] = mh
	}
	return mhs
}

func randomBytes(b *testing.B, rng *rand.Rand, n, l int) [][]byte {
	vks := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		vk := make([]byte, l)
		_, err := rng.Read(vk)
		require.NoError(b, err)
		vks = append(vks, vk)
	}
	return vks
}
