package bench_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/ipni/dhstore"
	dhpebble "github.com/ipni/dhstore/pebble"
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
	// 32 is chosen as a length of a hash, 113 as an approximation for enc(metadata) length
	// when it's not bitswap
	benchmarkPutMetadata(b, 500_000, 32, 113)
}

func BenchmarkDHStore_GetMetadata(b *testing.B) {
	benchmarkGetMetadatas(b, 500_000, 32, 113)
}

func newDHStore(b *testing.B) dhstore.DHStore {
	opts := &pebble.Options{
		BytesPerSync:                10 << 20, // 10 MiB
		WALBytesPerSync:             10 << 20, // 10 MiB
		MaxConcurrentCompactions:    func() int { return 10 },
		MemTableSize:                64 << 20, // 64 MiB
		MemTableStopWritesThreshold: 4,
		LBaseMaxBytes:               64 << 20, // 64 MiB
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		DisableWAL:                  true,
		WALMinSyncInterval:          func() time.Duration { return 30 * time.Second },
	}

	opts.Experimental.ReadCompactionRate = 10 << 20 // 20 MiB

	const numLevels = 7
	opts.Levels = make([]pebble.LevelOptions, numLevels)
	for i := 0; i < numLevels; i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KiB
		l.IndexBlockSize = 256 << 10 // 256 KiB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	opts.Levels[numLevels-1].FilterPolicy = nil
	opts.Cache = pebble.NewCache(1 << 30) // 1 GiB
	d, err := dhpebble.NewPebbleDHStore(b.TempDir(), opts)
	require.NoError(b, err)
	return d
}

func benchmarkPutMultihashes(b *testing.B, n, vkLen int) {
	rng := rand.New(rand.NewSource(1413))

	store := newDHStore(b)

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

	store := newDHStore(b)

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

	store := newDHStore(b)

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

	store := newDHStore(b)

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

func getMultihashes(b *testing.B, mhs []multihash.Multihash, store dhstore.DHStore) {
	for _, mh := range mhs {
		evks, err := store.Lookup(mh)
		require.NoError(b, err)
		require.NotNil(b, evks)
	}
}

func putMultihashes(b *testing.B, mhs []multihash.Multihash, vks [][]byte, store dhstore.DHStore) {
	indexes := make([]dhstore.Index, 0, len(mhs))
	for i, mh := range mhs {
		indexes = append(indexes, dhstore.Index{Key: mh, Value: vks[i]})
	}
	err := store.MergeIndexes(indexes)
	require.NoError(b, err)
}

func putMetadatas(b *testing.B, hvks, metadatas [][]byte, store dhstore.DHStore) {
	for i, hvk := range hvks {
		err := store.PutMetadata(hvk, metadatas[i])
		require.NoError(b, err)
	}
}

func getMetadatas(b *testing.B, hvks [][]byte, store dhstore.DHStore) {
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
