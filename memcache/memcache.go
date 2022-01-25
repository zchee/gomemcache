// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

package memcache

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Similar to:
// https://pkg.go.dev/google.golang.org/appengine/memcache

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	// ErrInvalidPollingDuration is returned when discovery polling is invalid
	ErrInvalidPollingDuration = errors.New("Discovery polling duration is invalid")

	// ErrClusterConfigMiss means that GetConfig failed as cluster config was not present
	ErrClusterConfigMiss = errors.New("memcache: cluster config miss")

	// ErrBadMagic is returned when the magic number in a response is not valid.
	ErrBadMagic = errors.New("memcache: bad magic number in response")

	// ErrBadIncrDec is returned when performing a incr/decr on non-numeric values.
	ErrBadIncrDec = errors.New("memcache: incr or decr on non-numeric value")
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 100 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

const buffered = 8 // arbitrary buffered channel size, for readability

var (
	zero8  = []byte{0}
	zero16 = []byte{0, 0}
	zero32 = []byte{0, 0, 0, 0}
	zero64 = []byte{0, 0, 0, 0, 0, 0, 0, 0}
)

const (
	reqMagic  uint8 = 0x80
	respMagic uint8 = 0x81
)

type response uint16

const (
	respOK response = iota
	respKeyNotFound
	respKeyExists
	respValueTooLarge
	respInvalidArgs
	respItemNotStored
	respInvalidIncrDecr
	respWrongVBucket
	respAuthErr      response = 0x20
	respAuthContinue response = 0x21
	respUnknownCmd   response = 0x81
	respOOM          response = 0x82
	respNotSupported response = 0x83
	respInternalErr  response = 0x85
	respBusy         response = 0x85
	respTemporaryErr response = 0x86
)

func (r response) asError() error {
	switch r {
	case respKeyNotFound:
		return ErrCacheMiss
	case respKeyExists:
		return ErrNotStored
	case respInvalidIncrDecr:
		return ErrBadIncrDec
	}

	return r
}

func (r response) Error() string {
	switch r {
	case respOK:
		return "OK"
	case respKeyNotFound:
		return "key not found"
	case respKeyExists:
		return "key already exists"
	case respValueTooLarge:
		return "value too large"
	case respInvalidArgs:
		return "invalid arguments"
	case respItemNotStored:
		return "item not stored"
	case respInvalidIncrDecr:
		return "incr/decr on non-numeric value"
	case respWrongVBucket:
		return "wrong vbucket"
	case respAuthErr:
		return "auth error"
	case respAuthContinue:
		return "auth continue"
	}

	return ""
}

type command uint8

const (
	cmdGet command = iota
	cmdSet
	cmdAdd
	cmdReplace
	cmdDelete
	cmdIncr
	cmdDecr
	cmdQuit
	cmdFlush
	cmdGetQ
	cmdNoop
	cmdVersion
	cmdGetK
	cmdGetKQ
	cmdAppend
	cmdPrepend
	cmdStat
	cmdSetQ
	cmdAddQ
	cmdReplaceQ
	cmdDeleteQ
	cmdIncremenTQ
	cmdDecremenTQ
	cmdQuitQ
	cmdFlushQ
	cmdAppendQ
	cmdPrependQ
	_ // 0x1b
	cmdTouch
	cmdGat
	cmdGatQ
	_ // 0x1f
	cmdSaslListMechs
	cmdSaslAuth
	cmdSaslStep
	cmdGatK
	cmdGatKQ

	// These commands are used for range operations and exist within
	// this header for use in other projects.
	// Range operations are not expected to be implemented in the memcached server itself.
	cmdRGet command = 11 + iota // 0x30
	cmdRSet
	cmdRSetQ
	cmdRAppend
	cmdRAppendQ
	cmdRPrepend
	cmdRPrependQ
	cmdRDelete
	cmdRDeleteQ
	cmdRIncr
	cmdRIncrQ
	cmdRDecr
	cmdRDecrQ

	cmdConfigGet    command = 0x60
	cmdConfigSet    command = 0x64
	cmdConfigDelete command = 0x66
)

// KeyMaxLength maximum length of a key.
const KeyMaxLength = 250

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey, ErrBadIncrDec:
		return true
	}

	return false
}

func legalKey(key string) bool {
	if len(key) > KeyMaxLength {
		return false
	}

	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] > 0x7e {
			return false
		}
	}

	return true
}

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(server ...string) *Client {
	ss := new(ServerList)
	ss.SetServers(server...)

	return NewFromSelector(ss)
}

// NewFromSelector returns a new Client using the provided ServerSelector.
func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

// NewDiscoveryClient returns a discovery config enabled client which polls
// periodically for new information and update server list if new information is found.
// All the servers which are found are used with equal weight.
// discoveryAddress should be in following form "ipv4-address:port"
// Note: pollingDuration should be at least 1 second.
func NewDiscoveryClient(discoveryAddress string, pollingDuration time.Duration) (*Client, error) {
	// validate pollingDuration
	if pollingDuration.Seconds() < 1.0 {
		return nil, ErrInvalidPollingDuration
	}

	return newDiscoveryClient(discoveryAddress, pollingDuration)
}

// for the unit test
func newDiscoveryClient(discoveryAddress string, pollingDuration time.Duration) (*Client, error) {
	// creates a new ServerList object which contains all the server eventually.
	rand.Seed(time.Now().UnixNano())

	ss := new(ServerList)
	mcCfgPollerHelper := New(discoveryAddress)
	cfgPoller := newConfigPoller(pollingDuration, ss, mcCfgPollerHelper)

	// cfgPoller starts polling immediately.
	c := NewFromSelector(ss)
	c.StopPolling = cfgPoller.stopPolling

	return c, nil
}

type stop func()

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	timeout time.Duration

	selector    ServerSelector
	StopPolling stop

	maxIdlePerAddr int

	mu       sync.RWMutex
	freeconn map[string]chan *conn
	bufPool  chan []byte
}

// Timeout returns the socket read/write timeout. By default, it's
// DefaultTimeout.
func (c *Client) Timeout() time.Duration {
	return c.timeout
}

// SetTimeout specifies the socket read/write timeout.
// If zero, DefaultTimeout is used. If < 0, there's
// no timeout. This method must be called before any
// connections to the memcached server are opened.
func (c *Client) SetTimeout(timeout time.Duration) {
	if timeout == time.Duration(0) {
		timeout = DefaultTimeout
	}
	c.timeout = timeout
}

// MaxIdleConnsPerAddr returns the maximum number of idle
// connections kept per server address.
func (c *Client) MaxIdleConnsPerAddr() int {
	return c.maxIdlePerAddr
}

// SetMaxIdleConnsPerAddr changes the maximum number of
// idle connections kept per server. If maxIdle < 0,
// no idle connections are kept. If maxIdle == 0,
// the default number (currently 2) is used.
func (c *Client) SetMaxIdleConnsPerAddr(maxIdle int) {
	if maxIdle == 0 {
		maxIdle = DefaultMaxIdleConns
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxIdlePerAddr = maxIdle
	if maxIdle > 0 {
		freeconn := make(map[string]chan *conn)
		for k, v := range c.freeconn {
			ch := make(chan *conn, maxIdle)
		ChanDone:
			for {
				select {
				case cn := <-v:
					select {
					case ch <- cn:
					default:
						cn.nc.Close()
					}
				default:
					freeconn[k] = ch
					break ChanDone
				}
			}
		}
		c.freeconn = freeconn
	} else {
		c.closeIdleConns()
		c.freeconn = nil
	}
}

// Close closes all currently open connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeIdleConns()
	c.freeconn = nil
	c.maxIdlePerAddr = 0
	return nil
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Object is the Item's value for use with a Codec.
	Object interface{}

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casID uint64
}

var Now = time.Now

type Addr struct {
	net.Addr
	s string
	n string
}

func (a *Addr) String() string {
	return a.s
}

func NewAddr(addr net.Addr) *Addr {
	return &Addr{
		Addr: addr,
		s:    addr.String(),
		n:    addr.Network(),
	}
}

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	addr *Addr
	c    *Client
}

// condRelease releases this connection if the error pointed to by err
// is is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (c *Client) condRelease(cn *conn, err *error) {
	switch *err {
	case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
		c.putFreeConn(cn)
	default:
		cn.nc.Close()
	}
}

func (cn *conn) condClose(err *error) {
	if *err != nil {
		cn.nc.Close()
	}
}

func (c *Client) closeIdleConns() {
	for _, v := range c.freeconn {
	NextIdle:
		for {
			select {
			case cn := <-v:
				cn.nc.Close()
			default:
				break NextIdle
			}
		}
	}
}

func (c *Client) putFreeConn(cn *conn) {
	c.mu.RLock()
	freelist := c.freeconn[cn.addr.s]
	maxIdle := c.maxIdlePerAddr
	c.mu.RUnlock()
	if freelist == nil && maxIdle > 0 {
		freelist = make(chan *conn, maxIdle)
		c.mu.Lock()
		c.freeconn[cn.addr.s] = freelist
		c.mu.Unlock()
	}
	select {
	case freelist <- cn:
		break
	default:
		cn.nc.Close()
	}
}

func (c *Client) getFreeConn(addr *Addr) *conn {
	c.mu.RLock()
	freelist := c.freeconn[addr.s]
	c.mu.RUnlock()
	if freelist == nil {
		return nil
	}
	select {
	case cn := <-freelist:
		return cn
	default:
		return nil
	}
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr *Addr) (net.Conn, error) {
	if c.timeout > 0 {
		conn, err := net.DialTimeout(addr.n, addr.s, c.timeout)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return nil, &ConnectTimeoutError{addr}
			}
			return nil, err
		}
		return conn, nil
	}
	return net.Dial(addr.n, addr.s)
}

func (c *Client) getConn(addr *Addr) (*conn, error) {
	cn := c.getFreeConn(addr)
	if cn == nil {
		nc, err := c.dial(addr)
		if err != nil {
			return nil, err
		}
		cn = &conn{
			nc:   nc,
			addr: addr,
		}
	}
	if c.timeout > 0 {
		cn.nc.SetDeadline(time.Now().Add(c.timeout))
	}
	return cn, nil
}

func (c *Client) each(cmd command, cas bool) error {
	var chs []chan error
	for _, addr := range c.selector.Addrs() {
		ch := make(chan error)
		chs = append(chs, ch)

		go func(addr *Addr, ch chan error) {
			defer close(ch)

			cn, err := c.getConn(addr)
			if err != nil {
				ch <- err
				return
			}
			defer c.condRelease(cn, &err)

			err = c.sendConnCommand(cn, "", cmd, nil, nil)
			if err != nil {
				ch <- err
				return
			}

			err = c.parseErrorResponse(cn)
			if err != nil {
				ch <- err
				return
			}
		}(addr, ch)
	}

	for _, ch := range chs {
		for err := range ch {
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Client) eachItem(keys []string, cmd command, item *Item, extras []byte) (map[string]*Item, error) {
	km := make(map[*Addr][]string)
	for _, key := range keys {
		if !legalKey(key) {
			return nil, ErrMalformedKey
		}

		addr, err := c.selector.PickServer(key)
		if err != nil {
			return nil, err
		}
		km[addr] = append(km[addr], key)
	}

	var chs []chan *Item
	for addr, keys := range km {
		ch := make(chan *Item)
		chs = append(chs, ch)

		go func(addr *Addr, keys []string, ch chan *Item) {
			defer close(ch)

			cn, err := c.getConn(addr)
			if err != nil {
				return
			}
			defer c.condRelease(cn, &err)

			for _, k := range keys {
				if err := c.sendConnCommand(cn, k, cmd, nil, nil); err != nil {
					return
				}
			}
			if err := c.sendConnCommand(cn, "", cmdNoop, nil, nil); err != nil {
				return
			}

			var item *Item
			var loopErr error
			for {
				item, loopErr = c.parseItemResponse("", cn, false)
				if loopErr != nil {
					return
				}
				if item == nil || item.Key == "" {
					// noop response
					break
				}

				ch <- item
			}
		}(addr, keys, ch)
	}

	m := make(map[string]*Item)
	for _, ch := range chs {
		for item := range ch {
			m[item.Key] = item
		}
	}

	return m, nil
}

func (c *Client) FlushAll(key string) error {
	return c.each(cmdFlush, false)
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (*Item, error) {
	cn, err := c.sendCommand(key, cmdGet, nil, nil)
	if err != nil {
		return nil, err
	}

	return c.parseItemResponse(key, cn, true)
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire.
//
// Zero means the item has no expiration time. ErrCacheMiss is returned if the key is not in the cache.
// The key must be at most 250 bytes in length.
func (c *Client) Touch(key string, seconds int32) (err error) {
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras, uint32(0))
	binary.BigEndian.PutUint32(extras[4:8], uint32(seconds))

	cn, err := c.sendCommand(key, cmdTouch, nil, extras)
	if err != nil {
		return err
	}
	_, err = c.parseUintResponse(cn)

	return err
}

func (c *Client) sendCommand(key string, cmd command, item *Item, extras []byte) (*conn, error) {
	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	addr, err := c.selector.PickServer(key)
	if err != nil {
		return nil, err
	}

	cn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	defer cn.condClose(&err)

	return cn, c.sendConnCommand(cn, key, cmd, item, extras)
}

func (c *Client) sendConnCommand(cn *conn, key string, cmd command, item *Item, extras []byte) (err error) {
	if _, err = cn.nc.Write([]byte{reqMagic, byte(cmd)}); err != nil {
		return err
	}

	kl := len(key)
	el := len(extras)

	// key length
	if err = binary.Write(cn.nc, binary.BigEndian, uint16(kl)); err != nil {
		return err
	}

	// extras length and data type
	if _, err = cn.nc.Write([]byte{byte(el), 0}); err != nil {
		return err
	}

	// vBucket
	if _, err = cn.nc.Write(zero16); err != nil {
		return err
	}

	// total body length
	bl := uint32(kl + el)
	if item != nil {
		bl += uint32(len(item.Value))
	}
	if err = binary.Write(cn.nc, binary.BigEndian, bl); err != nil {
		return err
	}

	// opaque
	if _, err = cn.nc.Write(zero32); err != nil {
		return err
	}

	// cas
	if item != nil && item.casID != 0 {
		if err = binary.Write(cn.nc, binary.BigEndian, item.casID); err != nil {
			return err
		}
	} else {
		if _, err = cn.nc.Write(zero64); err != nil {
			return err
		}
	}

	// extras
	if el > 0 {
		if _, err = cn.nc.Write(extras); err != nil {
			return err
		}
	}

	if kl > 0 {
		// Key itself
		if _, err = cn.nc.Write([]byte(key)); err != nil {
			return err
		}
	}

	if item != nil {
		if _, err = cn.nc.Write(item.Value); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) parseResponse(cn *conn) ([]byte, []byte, []byte, []byte, error) {
	hdr := make([]byte, 24)
	if _, err := cn.nc.Read(hdr); err != nil {
		return nil, nil, nil, nil, err
	}

	if hdr[0] != respMagic {
		return nil, nil, nil, nil, ErrBadMagic
	}

	status := response(binary.BigEndian.Uint16(hdr[6:8]))
	total := int(binary.BigEndian.Uint32(hdr[8:12]))
	if status != respOK {
		if _, err := io.CopyN(ioutil.Discard, cn.nc, int64(total)); err != nil {
			return nil, nil, nil, nil, err
		}
		return nil, nil, nil, nil, response(status).asError()
	}

	var extras []byte
	el := int(hdr[4])
	if el > 0 {
		extras = make([]byte, el)
		if _, err := cn.nc.Read(extras); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	var key []byte
	kl := int(binary.BigEndian.Uint16(hdr[2:4]))
	if kl > 0 {
		key = make([]byte, int(kl))
		if _, err := cn.nc.Read(key); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	var body []byte
	bl := total - el - kl
	if bl > 0 {
		body = make([]byte, bl)
		if _, err := cn.nc.Read(body); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	return hdr, key, extras, body, nil
}

func (c *Client) parseErrorResponse(cn *conn) error {
	_, _, _, _, err := c.parseResponse(cn)
	c.condRelease(cn, &err)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) parseUintResponse(cn *conn) (uint64, error) {
	_, _, _, body, err := c.parseResponse(cn)
	c.condRelease(cn, &err)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(body), nil
}

func (c *Client) parseItemResponse(key string, cn *conn, release bool) (*Item, error) {
	hdr, k, extras, body, err := c.parseResponse(cn)
	if release {
		c.condRelease(cn, &err)
	}
	if err != nil {
		return nil, err
	}

	var flags uint32
	if len(extras) > 0 {
		flags = binary.BigEndian.Uint32(extras)
	}

	if key == "" && len(k) > 0 {
		key = string(k)
	}

	return &Item{
		Key:   key,
		Value: body,
		Flags: flags,
		casID: binary.BigEndian.Uint64(hdr[16:24]),
	}, nil
}

// GetMulti is a batch version of Get.
//
// The returned map from keys to items may have fewer elements than the input slice, due to memcache
// cache misses.
//
// Each key must be at most 250 bytes in length. If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	return c.eachItem(keys, cmdGetKQ, nil, nil)
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.populateOne(cmdSet, item, false)
}

// Add writes the given item, if no value already exists for its key.
//
// ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.populateOne(cmdAdd, item, false)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *Client) Replace(item *Item) error {
	return c.populateOne(cmdReplace, item, false)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls.
//
// The item's Key should not change between calls but all other item fields may differ.
//
// ErrCASConflict is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.populateOne(cmdSet, item, true)
}

func (c *Client) populateOne(cmd command, item *Item, cas bool) error {
	if item == nil || !legalKey(item.Key) {
		return ErrMalformedKey
	}

	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras, item.Flags)
	binary.BigEndian.PutUint32(extras[4:8], uint32(item.Expiration))
	if !cas && item.casID != 0 {
		item.casID = 0
	}

	cn, err := c.sendCommand(item.Key, cmd, item, extras)
	if err != nil {
		return err
	}

	hdr, _, _, _, err := c.parseResponse(cn)
	c.condRelease(cn, &err)
	if err != nil {
		return err
	}
	item.casID = binary.BigEndian.Uint64(hdr[16:24])

	return nil
}

// Delete deletes the item with the provided key.
//
// The error ErrCacheMiss is returned if the item didn't already exist in the cache.
func (c *Client) Delete(key string) error {
	cn, err := c.sendCommand(key, cmdDelete, nil, nil)
	if err != nil {
		return err
	}

	_, _, _, _, err = c.parseResponse(cn)
	c.condRelease(cn, &err)

	return err
}

// DeleteAll deletes all items in the cache.
func (c *Client) DeleteAll() error {
	addr, err := c.selector.PickAnyServer()
	if err != nil {
		return err
	}

	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer c.condRelease(cn, &err)

	if err := c.sendConnCommand(cn, "", cmdFlush, nil, nil); err != nil {
		return err
	}

	return c.parseErrorResponse(cn)
}

// Ping checks all instances if they are alive.
//
// Returns error if any of them is down.
func (c *Client) Ping() error {
	return c.each(cmdVersion, false)
}

// Increment atomically increments key by delta.
//
// The return value is the new value after being incremented or an error.
// If the value didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
//
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr(cmdIncr, key, delta)
}

// Decrement atomically decrements key by delta.
//
// The return value is the new value after being decremented or an error.
// If the value didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
//
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr(cmdDecr, key, delta)
}

func (c *Client) incrDecr(cmd command, key string, delta uint64) (uint64, error) {
	extras := make([]byte, 20)
	binary.BigEndian.PutUint64(extras, delta)

	// Set expiration to 0xfffffff, so the command fails if the key
	// does not exist.
	for ii := 16; ii < 20; ii++ {
		extras[ii] = 0xff
	}

	cn, err := c.sendCommand(key, cmd, nil, extras)
	if err != nil {
		return 0, err
	}

	return c.parseUintResponse(cn)
}

// GetConfig gets the config type.
//
// ErrClusterConfigMiss is returned if config for the type cluster is not found. The type must be at most 250 bytes in length.
func (c *Client) GetConfig(key string) (clusterConfig *ClusterConfig, err error) {
	clusterConfig, err = c.getConfig(key)
	if err != nil {
		return nil, err
	}

	if clusterConfig == nil {
		return nil, ErrClusterConfigMiss
	}

	return clusterConfig, nil
}

// getConfig gets the config type.
//
// ErrClusterConfigMiss is returned if config for the type cluster is not found.
// The configType must be at most 250 bytes in length.
//
// TODO(zchee): implement setConfig as well.
func (c *Client) getConfig(key string) (clusterConfig *ClusterConfig, err error) {
	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	addr, err := c.selector.PickAnyServer()
	if err != nil {
		return nil, err
	}

	cn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	defer cn.condClose(&err)

	clusterConfig, err = c.getConfigFromAddr(cn, key)
	if err != nil {
		return nil, err
	}
	if clusterConfig == nil {
		err = ErrClusterConfigMiss
	}

	return clusterConfig, err
}

func (c *Client) getConfigFromAddr(cn *conn, key string) (*ClusterConfig, error) {
	if err := c.sendConnCommand(cn, "", cmdConfigGet, &Item{Value: []byte(key)}, nil); err != nil {
		return nil, err
	}

	cc, err := c.parseConfigGetResponse(cn, true)
	if err != nil {
		return nil, err
	}

	return cc, nil
}

// TODO(zchee): implements correctly.
func (c *Client) parseConfigGetResponse(cn *conn, release bool) (*ClusterConfig, error) {
	hdr, k, extras, body, err := c.parseResponse(cn)
	if release {
		c.condRelease(cn, &err)
	}
	if err != nil {
		return nil, err
	}
	_ = hdr
	_ = k
	_ = body

	var flags uint32
	if len(extras) > 0 {
		flags = binary.BigEndian.Uint32(extras)
	}
	_ = flags

	return &ClusterConfig{
		// ConfigID:      0,
		// NodeAddresses: []ClusterNode{},
		//
		// Value: body,
		// Flags: flags,
		// casID: binary.BigEndian.Uint64(hdr[16:24]),
	}, nil
}
