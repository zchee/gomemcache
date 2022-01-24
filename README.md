# gomemcache

## About

This is a memcache client library for the Go programming language.

This package fork of [github.com/bradfitz/gomemcache/memcache](https://github.com/bradfitz/gomemcache/tree/master/memcache) and [github.com/google/gomemcache/memcache](https://github.com/google/gomemcache/tree/master/memcache).

With support for Autodiscovery client based on https://cloud.google.com/memorystore/docs/memcached/auto-discovery-overview.

## Installing

```sh
go get -u -d github.com/zchee/gomemcache@latest
```

## Example

```go
package main

import (
	"log"

	"github.com/zchee/gomemcache/memcache"
)

func main() {
	mc := memcache.New("10.0.0.1:11211",	"10.0.0.2:11211",	"10.0.0.3:11212")
	mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})
	it, err := mc.Get("foo")
	if err != nil {
		log.Fatal(err)
	}
	...
}
```

## Full docs

See https://pkg.go.dev/github.com/zchee/gomemcache/memcache

Or run:

```sh
go doc github.com/zchee/gomemcache/memcache
```

## Acknowledgement

- [bradfitz/gomemcache: Go Memcached client library #golang](https://github.com/bradfitz/gomemcache)
- [google/gomemcache: Go Memcached client library #golang](https://github.com/google/gomemcache)
- [rainycape/memcache: High performance memcache client in Go (golang)](https://github.com/rainycape/memcache)
- [couchbase/gomemcached: A memcached binary protocol toolkit for go.](https://github.com/couchbase/gomemcached)
