// Copyright 2011 Google Inc.
// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

package memcache

import (
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

const clusterConfigName = "cluster"

// configPoller is config service poller.
// It is not safe for use by multiple concurrent goroutines.
type configPoller struct {
	// pollingFrequency specified how often poller polls.
	pollingFrequency time.Duration

	tick *time.Ticker
	done chan bool
	once sync.Once

	// reference to selector which will used to update the servers for the main client
	serverList *ServerList

	mc *Client

	clusterConfigMU   sync.RWMutex
	prevClusterConfig *ClusterConfig
}

// creates a new cluster config poller
func newConfigPoller(frequency time.Duration, servers *ServerList, mc *Client) *configPoller {
	poller := &configPoller{
		pollingFrequency: frequency,
		serverList:       servers,
		mc:               mc,
		tick:             time.NewTicker(frequency),
		done:             make(chan bool),
	}

	// Hold the thread to initialize before returning.
	if err := poller.readConfigAndUpdateServerList(); err != nil {
		// no action required unless stop is explicitly called
		log.Printf("Warning: First poll for discovery service failed due to %v", err)
	}

	go poller.readConfigPeriodically()

	return poller
}

func (c *configPoller) readConfigPeriodically() {
	for {
		select {
		case <-c.tick.C:
			if err := c.readConfigAndUpdateServerList(); err != nil {
				// no action required unless stop is explicitly called
				log.Printf("Warning: Periodic poll for discovery service failed due to %v", err)
			}

		case <-c.done:
			return
		}
	}
}

// Stop the internal polling.
func (c *configPoller) stopPolling() {
	c.once.Do(func() {
		close(c.done)
	})
}

func (c *configPoller) readConfigAndUpdateServerList() error {
	clusterConfig, err := c.mc.GetConfig(clusterConfigName)
	if err != nil {
		// nothing to do in this round.
		return err
	}
	// compare existing config information with new config information
	updateClusterConf := false
	c.clusterConfigMU.RLock()
	if c.prevClusterConfig != nil {
		if clusterConfig.ConfigID > c.prevClusterConfig.ConfigID {
			updateClusterConf = true
		}
	} else {
		updateClusterConf = true
	}
	c.clusterConfigMU.RUnlock()

	if updateClusterConf {
		c.updateServerList(clusterConfig)
	}
	return nil
}

// updateServerList is not thread safe and should not be called without holding lock on clusterConfigMU
func (c *configPoller) updateServerList(cc *ClusterConfig) error {
	s := getServerAddresses(cc)
	c.serverList.SetServers(s...)
	c.prevClusterConfig = cc
	return nil
}

func getServerAddresses(cc *ClusterConfig) []string {
	servers := make([]string, 0, len(cc.NodeAddresses))
	for _, n := range cc.NodeAddresses {
		// Validation happens when main memcache client tries to connect to this address
		servers = append(servers, net.JoinHostPort(n.Host, strconv.FormatInt(n.Port, 10)))
	}
	return servers
}
