// Copyright 2011 Google Inc.
// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

package memcache

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
)

const emptyResponse = "END"

// Encapsulate a tcp server which can fake cluster config response also called discovery response.
type fakeDiscoveryMemcacheServer struct {
	discoveryResponseMutex sync.RWMutex

	// Usage instructions:
	// Either use (discoveryConfigID and discoveryPorts) OR discoveryConfigResponse
	// Using one clears other.
	discoveryConfigID       int
	discoveryPorts          []int
	discoveryConfigResponse string

	// Output only
	currentAddress string

	// internal bookeeping
	listener net.Listener
	ctx      context.Context
}

// starts a tcp server on any free port
func (s *fakeDiscoveryMemcacheServer) start(ctx context.Context) error {
	l, err := net.Listen("tcp", "")
	if err != nil {
		return err
	}
	s.currentAddress = l.Addr().String()
	s.listener = l
	s.ctx = ctx

	go s.listenForClients()

	return nil
}

func (s *fakeDiscoveryMemcacheServer) stop() error {
	return s.listener.Close()
}

func (s *fakeDiscoveryMemcacheServer) listenForClients() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}
		select {
		case <-s.ctx.Done():
			return
		default:
			go s.handleFakeDiscoveryMemcacheRequest(conn)
		}
	}
}

func (s *fakeDiscoveryMemcacheServer) getResponseToSend() string {
	s.discoveryResponseMutex.RLock()
	defer s.discoveryResponseMutex.RUnlock()

	if s.discoveryConfigResponse != "" {
		return s.discoveryConfigResponse
	}

	if s.discoveryConfigID == 0 {
		return emptyResponse
	}

	var result strings.Builder
	result.WriteString("CONFIG cluster 0 80\r\n")
	result.WriteString(fmt.Sprintf("%d", s.discoveryConfigID))
	result.WriteString("\r\n")
	for i, port := range s.discoveryPorts {
		result.WriteString(fmt.Sprintf("localhost|localhost|%d", port))
		if i < len(s.discoveryPorts)-1 {
			result.WriteString(" ")
		}
	}
	result.WriteString("\n\r\n")
	return result.String()
}

func (s *fakeDiscoveryMemcacheServer) handleFakeDiscoveryMemcacheRequest(c net.Conn) {
	_, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return
	}

	c.Write([]byte(s.getResponseToSend()))
	c.Close()
}

func (s *fakeDiscoveryMemcacheServer) updateDiscoveryResponse(response string) error {
	s.discoveryResponseMutex.Lock()
	defer s.discoveryResponseMutex.Unlock()
	s.discoveryConfigResponse = response
	s.discoveryConfigID = 0
	s.discoveryPorts = nil
	return nil
}

func (s *fakeDiscoveryMemcacheServer) updateDiscoveryInformation(id int, ports []int) error {
	s.discoveryResponseMutex.Lock()
	defer s.discoveryResponseMutex.Unlock()
	s.discoveryConfigID = id
	s.discoveryPorts = ports
	s.discoveryConfigResponse = ""
	return nil
}
