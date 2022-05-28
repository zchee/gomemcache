// Copyright 2011 Google Inc.
// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

package memcache

import (
	"net"
	"testing"
)

func BenchmarkPickServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickServer_Single(b *testing.B) {
	benchPickServer(b, "127.0.0.1:1234")
}

func benchPickServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss ServerList
	ss.SetServers(servers...)
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickServer("some key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPickAnyServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickAnyServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func TestPickAnyServer(t *testing.T) {
	pickServerTests := []struct {
		serverList            []string
		expectedServersPicked int
	}{
		{[]string{"127.0.0.1:1234"}, 1},
		{[]string{"127.0.0.1:1234", "127.0.0.1:1235", "127.0.0.1:1236"}, 2},
	}
	for _, tt := range pickServerTests {
		var ss ServerList
		ss.SetServers(tt.serverList...)
		serverCounter := make(map[string]int)
		for i := 0; i < 1000; i++ {
			var addr net.Addr
			var err error
			if addr, err = ss.PickAnyServer(); err != nil {
				t.Errorf("pickAnyServer(%v) failed due to %v", tt.serverList, err)
			}
			serverCounter[addr.String()]++
		}
		// Verify that server counter contains at least 2 values.
		if len(serverCounter) < tt.expectedServersPicked {
			t.Errorf("failed to randomize server list (%v), serverCounter (%v). got:%v, want at least:%v", tt.serverList, serverCounter, len(serverCounter), tt.expectedServersPicked)
		}
	}
}

func TestPickAnyServerThrows(t *testing.T) {
	var ss ServerList
	if _, err := ss.PickAnyServer(); err != ErrNoServers {
		t.Errorf("expected error with no servers, got:%v", err)
	}
}

func BenchmarkPickAnyServer_Single(b *testing.B) {
	benchPickAnyServer(b, "127.0.0.1:1234")
}

func benchPickAnyServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	var ss ServerList
	ss.SetServers(servers...)
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickAnyServer(); err != nil {
			b.Fatal(err)
		}
	}
}
