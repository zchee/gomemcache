// Copyright 2011 Google Inc.
// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

package memcache

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func benchmarkSet(ctx context.Context, b *testing.B, item *Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(ctx, item); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func benchmarkSetGet(ctx context.Context, b *testing.B, item *Item) {
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	key := item.Key
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(ctx, item); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func largeItem() *Item {
	key := strings.Repeat("f", 240)
	value := make([]byte, 1024)
	return &Item{Key: key, Value: value}
}

func smallItem() *Item {
	return &Item{Key: "foo", Value: []byte("bar")}
}

func BenchmarkSet(b *testing.B) {
	ctx := context.Background()
	benchmarkSet(ctx, b, smallItem())
}

func BenchmarkSetLarge(b *testing.B) {
	ctx := context.Background()
	benchmarkSet(ctx, b, largeItem())
}

func BenchmarkSetGet(b *testing.B) {
	ctx := context.Background()
	benchmarkSetGet(ctx, b, smallItem())
}

func BenchmarkSetGetLarge(b *testing.B) {
	ctx := context.Background()
	benchmarkSetGet(ctx, b, largeItem())
}

func benchmarkConcurrentSetGet(ctx context.Context, b *testing.B, item *Item, count int, opcount int) {
	mp := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(mp)
	runtime.GOMAXPROCS(count)
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	// Items are not thread safe
	items := make([]*Item, count)
	for ii := range items {
		items[ii] = &Item{Key: item.Key, Value: item.Value}
	}
	b.SetBytes(int64((len(item.Key) + len(item.Value)) * count * opcount))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(count)
		for j := 0; j < count; j++ {
			it := items[j]
			key := it.Key
			go func() {
				defer wg.Done()
				for k := 0; k < opcount; k++ {
					if err := c.Set(ctx, it); err != nil {
						b.Fatal(err)
					}
					if _, err := c.Get(ctx, key); err != nil {
						b.Fatal(err)
					}
				}
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func BenchmarkGetCacheMiss(b *testing.B) {
	ctx := context.Background()

	key := "not"
	cmd, c := newUnixServer(b)
	c.SetTimeout(time.Duration(-1))
	c.Delete(ctx, key)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Get(ctx, key); err != ErrCacheMiss {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	cmd.Process.Kill()
	cmd.Wait()
}

func BenchmarkConcurrentSetGetSmall10_100(b *testing.B) {
	ctx := context.Background()
	benchmarkConcurrentSetGet(ctx, b, smallItem(), 10, 100)
}

func BenchmarkConcurrentSetGetLarge10_100(b *testing.B) {
	ctx := context.Background()
	benchmarkConcurrentSetGet(ctx, b, largeItem(), 10, 100)
}

func BenchmarkConcurrentSetGetSmall20_100(b *testing.B) {
	ctx := context.Background()
	benchmarkConcurrentSetGet(ctx, b, smallItem(), 20, 100)
}

func BenchmarkConcurrentSetGetLarge20_100(b *testing.B) {
	ctx := context.Background()
	benchmarkConcurrentSetGet(ctx, b, largeItem(), 20, 100)
}
