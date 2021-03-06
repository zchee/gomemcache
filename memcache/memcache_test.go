// Copyright 2011 Google Inc.
// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

package memcache

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

const memcachedHost = "127.0.0.1"

var flagPort string

var memcachedAddr string

func TestMain(m *testing.M) {
	flag.StringVar(&flagPort, "port", "13134", "port of test memcached instance") // m(13) e m(13) d(4)
	flag.Parse()

	memcachedAddr = net.JoinHostPort(memcachedHost, flagPort)

	m.Run()
}

func setup(t *testing.T) bool {
	c, err := net.Dial("tcp4", memcachedAddr)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", memcachedAddr)
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()

	return true
}

func TestLocalhost(t *testing.T) {
	if !setup(t) {
		return
	}

	ctx := context.Background()
	testWithClient(ctx, t, New(memcachedAddr))
}

func newUnixServer(tb testing.TB) (*exec.Cmd, *Client) {
	sock := fmt.Sprintf("/tmp/test-gomemcache-%d.sock", os.Getpid())
	os.Remove(sock)
	cmd := exec.Command("memcached", "-s", sock)
	if err := cmd.Start(); err != nil {
		tb.Skip("skipping test; couldn't find memcached")
		return nil, nil
	}

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	return cmd, New(sock)
}

// Run the memcached binary as a child process and connect to its unix socket.
func TestUnixSocket(t *testing.T) {
	cmd, c := newUnixServer(t)
	defer cmd.Wait()
	defer cmd.Process.Kill()

	ctx := context.Background()
	testWithClient(ctx, t, c)
}

func mustSet(ctx context.Context, t *testing.T, c *Client) func(*Item) {
	return func(it *Item) {
		if err := c.Set(ctx, it); err != nil {
			t.Fatalf("failed to Set %#v: %v", *it, err)
		}
	}
}

func testWithClient(ctx context.Context, t *testing.T, c *Client) {
	mustSet := mustSet(ctx, t, c)

	// Set
	{
		foo := &Item{
			Key:   "foo",
			Value: []byte("fooval"),
			Flags: 123,
		}
		if err := c.Set(ctx, foo); err != nil {
			t.Fatalf("first set(foo): %v", err)
		}

		if err := c.Set(ctx, foo); err != nil {
			t.Fatalf("second set(foo): %v", err)
		}
	}

	// Get
	{
		it, err := c.Get(ctx, "foo")
		if err != nil {
			t.Fatalf("get(foo): %v", err)
		}
		if it.Key != "foo" {
			t.Errorf("get(foo) Key = %q, want foo", it.Key)
		}
		if string(it.Value) != "fooval" {
			t.Errorf("get(foo) Value = %q, want fooval", string(it.Value))
		}
		if it.Flags != 123 {
			t.Errorf("get(foo) Flags = %v, want 123", it.Flags)
		}
	}

	// Get non-existant
	{
		if _, err := c.Get(ctx, "not-exists"); err != ErrCacheMiss {
			t.Errorf("get(not-exists): expecting %v, got %v instead", ErrCacheMiss, err)
		}
	}

	// Get and set a unicode key
	{
		quxKey := "Hello"
		qux := &Item{
			Key:   quxKey,
			Value: []byte("hello world"),
		}
		if err := c.Set(ctx, qux); err != nil {
			t.Fatalf("first set(Hello_??????): %v", err)
		}

		it, err := c.Get(ctx, quxKey)
		if err != nil {
			t.Fatalf("get(Hello): %v", err)
		}
		if it.Key != quxKey {
			t.Errorf("get(Hello) Key = %q, want Hello", it.Key)
		}
		if string(it.Value) != "hello world" {
			t.Errorf("get(Hello) Value = %q, want hello world", string(it.Value))
		}
	}

	// Set malformed keys
	{
		malFormed := &Item{
			Key:   "foo bar",
			Value: []byte("foobarval"),
		}
		if err := c.Set(ctx, malFormed); err != ErrMalformedKey {
			t.Errorf("set(foo bar) should return ErrMalformedKey instead of %v", err)
		}

		malFormed = &Item{
			Key:   "foo" + string(rune(0x7f)),
			Value: []byte("foobarval"),
		}
		if err := c.Set(ctx, malFormed); err != ErrMalformedKey {
			t.Errorf("set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)
		}
	}

	// Add
	{
		bar := &Item{
			Key:   "bar",
			Value: []byte("barval"),
		}
		if err := c.Add(ctx, bar); err != nil {
			t.Fatalf("first add(bar): %v", err)
		}
		if err := c.Add(ctx, bar); err != ErrNotStored {
			t.Fatalf("second add(bar) want ErrNotStored, got %v", err)
		}
	}

	// Replace
	{
		bar := &Item{
			Key:   "bar",
			Value: []byte("barval"),
		}
		baz := &Item{
			Key:   "baz",
			Value: []byte("bazvalue"),
		}
		if err := c.Replace(ctx, baz); err != ErrNotStored {
			t.Fatalf("expected replace(baz) to return ErrNotStored, got %v", err)
		}
		if err := c.Replace(ctx, bar); err != nil {
			t.Fatalf("replaced(bar): %v", err)
		}
	}

	// GetMulti
	{
		m, err := c.GetMulti(ctx, []string{"foo", "bar"})
		if err != nil {
			t.Fatalf("GetMulti: %v", err)
		}
		if g, e := len(m), 2; g != e {
			t.Errorf("GetMulti: got len(map) = %d, want = %d", g, e)
		}
		if _, ok := m["foo"]; !ok {
			t.Fatalf("GetMulti: didn't get key 'foo'")
		}
		if _, ok := m["bar"]; !ok {
			t.Fatalf("GetMulti: didn't get key 'bar'")
		}
		if g, e := string(m["foo"].Value), "fooval"; g != e {
			t.Errorf("GetMulti: foo: got %q, want %q", g, e)
		}
		if g, e := string(m["bar"].Value), "barval"; g != e {
			t.Errorf("GetMulti: bar: got %q, want %q", g, e)
		}
	}

	// Delete
	{
		if err := c.Delete(ctx, "foo"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := c.Get(ctx, "foo")
		if err != ErrCacheMiss {
			t.Errorf("post-Delete want ErrCacheMiss, got %v", err)
		}
	}

	// Incr/Decr
	{
		mustSet(&Item{
			Key:   "num",
			Value: []byte("42"),
		})
		n, err := c.Increment(ctx, "num", 8)
		if err != nil {
			t.Fatalf("Increment num + 8: %v", err)
		}
		if n != 50 {
			t.Fatalf("Increment num + 8: want=50, got=%d", n)
		}
		n, err = c.Decrement(ctx, "num", 49)
		if err != nil {
			t.Fatalf("Decrement: %v", err)
		}
		if n != 1 {
			t.Fatalf("Decrement 49: want=1, got=%d", n)
		}

		if err := c.Delete(ctx, "num"); err != nil {
			t.Fatalf("delete num: %v", err)
		}

		n, err = c.Increment(ctx, "num", 1)
		if err != ErrCacheMiss {
			t.Fatalf("increment post-delete: want ErrCacheMiss, got %v", err)
		}

		mustSet(&Item{
			Key:   "num",
			Value: []byte("not-numeric"),
		})
		n, err = c.Increment(ctx, "num", 1)
		if err == nil {
			t.Fatalf("increment non-number: want client error, got %v", err)
		}
		testTouchWithClient(ctx, t, c)
	}

	// Test Delete All
	{
		if err := c.DeleteAll(ctx, 0); err != nil {
			t.Fatalf("DeleteAll: %v", err)
		}
		_, err := c.Get(ctx, "bar")
		if err != ErrCacheMiss {
			t.Errorf("post-DeleteAll want ErrCacheMiss, got %v", err)
		}
	}

	// Test Ping
	{
		if err := c.Ping(ctx); err != nil {
			t.Fatalf("error ping: %s", err)
		}
	}
}

func testTouchWithClient(ctx context.Context, t *testing.T, c *Client) {
	if testing.Short() {
		t.Log("Skipping testing memcache Touch with testing in Short mode")
		return
	}

	mustSet := mustSet(ctx, t, c)
	const secondsToExpiry = int32(2)

	// We will set foo and bar to expire in 2 seconds, then we'll keep touching
	// foo every second
	// After 3 seconds, we expect foo to be available, and bar to be expired
	foo := &Item{
		Key:        "foo",
		Value:      []byte("fooval"),
		Expiration: secondsToExpiry,
	}
	bar := &Item{
		Key:        "bar",
		Value:      []byte("barval"),
		Expiration: secondsToExpiry / 2,
	}

	setTime := time.Now()
	mustSet(foo)
	mustSet(bar)

	for s := 0; s < 3; s++ {
		time.Sleep(time.Duration(500 * time.Millisecond))
		if err := c.Touch(ctx, foo.Key, secondsToExpiry); nil != err {
			t.Errorf("error touching foo: %v", err.Error())
		}
	}

	if _, err := c.Get(ctx, "foo"); err != nil {
		if errors.Is(err, ErrCacheMiss) {
			t.Fatalf("touching failed to keep item foo alive")
		}

		t.Fatalf("unexpected error retrieving foo after touching: %v", err.Error())
	}

	_, err := c.Get(ctx, "bar")
	if err == nil {
		t.Fatalf("item bar did not expire within %v seconds", time.Now().Sub(setTime).Seconds())
	}
	if !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("unexpected error retrieving bar: %v", err.Error())
	}
}

func BenchmarkOnItem(b *testing.B) {
	fakeServer, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal("Could not open fake server: ", err)
	}
	defer fakeServer.Close()

	go func() {
		for {
			if c, err := fakeServer.Accept(); err == nil {
				go func() { io.Copy(ioutil.Discard, c) }()
			} else {
				return
			}
		}
	}()

	addr := fakeServer.Addr()
	c := New(addr.String())
	ctx := context.Background()
	if _, err := c.getConn(ctx, NewAddr(addr)); err != nil {
		b.Fatal("failed to initialize connection to fake server")
	}

	item := Item{
		Key: "foo",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		c.populateOne(ctx, cmdNoop, &item, 0)
	}
}
