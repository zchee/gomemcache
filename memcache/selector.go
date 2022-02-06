// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

package memcache

import (
	"hash/crc32"
	"math/rand"
	"net"
	"strings"
	"sync"
)

// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw string
	str string
}

// make sure staticAddr implements the net.Addr interface.
var _ net.Addr = (*staticAddr)(nil)

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

// Network name of the network.
func (s *staticAddr) Network() string { return s.ntw }

// String string form of address.
func (s *staticAddr) String() string { return s.str }

// ServerSelector is the interface that selects a memcache server
// as a function of the item's key.
//
// All ServerSelector implementations must be safe for concurrent use
// by multiple goroutines.
type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(key string) (*Addr, error)

	// PickAnyServer returns any active server, preferably not the
	// same one every time in order to distribute the load.
	// This can be used to get information which is server agnostic.
	PickAnyServer() (*Addr, error)

	// Each iterates over each server calling the given function.
	Each(f func(*Addr) error) error

	// Addrs returns the current server addresses.
	Servers() []*Addr
}

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	mu    sync.RWMutex
	addrs []*Addr
}

// make sure ServerList implements the ServerSelector interface.
var _ ServerSelector = (*ServerList)(nil)

// SetServers changes a ServerList's set of servers at runtime and is
// safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes are made to the ServerList.
func (ss *ServerList) SetServers(servers ...string) error {
	addrs := make([]*Addr, len(servers))
	for i, server := range servers {
		var network string
		var fn func(network, address string) (net.Addr, error)
		switch {
		case strings.Contains(server, "/"):
			network = "unix"
			fn = func(network, address string) (net.Addr, error) {
				return net.ResolveUnixAddr(network, address)
			}
		default:
			network = "tcp"
			fn = func(network, address string) (net.Addr, error) {
				return net.ResolveTCPAddr(network, address)
			}
		}

		addr, err := fn(network, server)
		if err != nil {
			return err
		}
		addrs[i] = NewAddr(addr)
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.addrs = addrs

	return nil
}

// keyBufPool returns []byte buffers for use by PickServer's call to
// crc32.ChecksumIEEE to avoid allocations. (but doesn't avoid the
// copies, which at least are bounded in size and small)
var keyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

// PickAnyServer returns any active server, preferably not the
// same one every time in order to distribute the load.
// This can be used to get information which is server agnostic.
//
// PickServer implements ServerSelector.PickServer.
func (ss *ServerList) PickServer(key string) (*Addr, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}

	if len(ss.addrs) == 1 {
		return ss.addrs[0], nil
	}

	bufp := keyBufPool.Get().(*[]byte)
	n := copy(*bufp, key)
	cs := crc32.ChecksumIEEE((*bufp)[:n])
	keyBufPool.Put(bufp)

	return ss.addrs[cs%uint32(len(ss.addrs))], nil
}

// PickAnyServer picks any active server
// This can be used to get information which is not linked to a key or which could be on any server.
//
// PickAnyServer implements ServerSelector.PickAnyServer.
func (ss *ServerList) PickAnyServer() (*Addr, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}

	if len(ss.addrs) == 1 {
		return ss.addrs[0], nil
	}

	return ss.addrs[rand.Intn(len(ss.addrs))], nil

}

// Each iterates over each server calling the given function.
//
// Each implements ServerSelector.Each.
func (ss *ServerList) Each(f func(*Addr) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}

	return nil
}

// Servers returns the current server addresses.
//
// Servers implements ServerSelector.Servers.
func (ss *ServerList) Servers() []*Addr {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	return ss.addrs
}
