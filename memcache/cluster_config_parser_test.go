/*
Copyright 2020 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func prepareConfigResponse(discoveryID int, discoveryAddress [][]string) string {
	var temp strings.Builder
	temp.WriteString("")
	temp.WriteString("CONFIG cluster 0 80\r\n")
	temp.WriteString(fmt.Sprintf("%d", discoveryID))
	temp.WriteString("\r\n")
	for i, address := range discoveryAddress {
		temp.WriteString(fmt.Sprintf("%s|%s|%s", address[0], address[0], address[1]))
		if i < len(discoveryAddress)-1 {
			temp.WriteString(" ")
		}
	}
	temp.WriteString("\n\r\n")
	return temp.String()
}

func buildClusterConfig(discoveryID int, discoveryAddress [][]string) *ClusterConfig {
	cc := &ClusterConfig{ConfigID: int64(discoveryID)}
	cc.NodeAddresses = make([]ClusterNode, len(discoveryAddress))
	for i, address := range discoveryAddress {
		port, _ := strconv.ParseInt(address[1], 10, 64)
		cc.NodeAddresses[i] = ClusterNode{Host: address[0], Port: port}
	}
	return cc
}

func TestGoodClusterConfigs(t *testing.T) {
	configTests := []struct {
		discoveryID        int
		discoveryAddresses [][]string
	}{
		{2, [][]string{[]string{"localhost", "112233"}}},
		{1000, [][]string{[]string{"localhost", "112233"}, []string{"127.0.0.4", "123435"}}},
		{50, [][]string{[]string{"localhost", "112233"}, []string{"127.0.0.4", "123435"}, []string{"127.0", "123"}}},
	}
	for _, tt := range configTests {
		config := prepareConfigResponse(tt.discoveryID, tt.discoveryAddresses)
		want := buildClusterConfig(tt.discoveryID, tt.discoveryAddresses)
		reader := bufio.NewReader(strings.NewReader(config))
		got := &ClusterConfig{}
		f := func(cb *ClusterConfig) {
			got = cb
		}
		if err := parseConfigGetResponse(reader, f); err != nil {
			t.Errorf("parseConfigGetResponse(%q) had parse err:%v", config, err)
		}
		if !reflect.DeepEqual(*got, *want) {
			t.Errorf("configResponse(%q) = %v; want = %v", config, got, want)
		}
	}

}

func TestBrokenClusterConfigs(t *testing.T) {
	emptyConfig := ClusterConfig{}
	configTests := []struct {
		configResponse string
		wantErrorText  string
		wantConfig     ClusterConfig
	}{
		{"", "", emptyConfig},        // empty config returns no error with empty cluster config
		{"END\r\n", "", emptyConfig}, // empty config returns no error with empty cluster config
		{"CONFIG cluster 0 80\r\nbadCfg\r\n123.76|123.76|5432\r\nEND\r\n", "strconv.ParseInt: parsing \"badCfg\"", emptyConfig},                            // error parsing port
		{"CONFIG cluster 0 80\r\n100\r\n123.76|123.76|portBroken\r\nEND\r\n", "strconv.ParseInt: parsing \"portBroken\"", emptyConfig},                     // error parsing port
		{"CONFIG cluster 0 80\r\n100\r\n123.76123.76portBroken\r\nEND\r\n", "host address ([123.76123.76portBroken]) not in expected format", emptyConfig}, // error tokenizing due to no pipes
		{"CONFIG cluster 0 80\r\n100\r\n123.76|123.76|123123.76|123.76|123\r\nEND\r\n", "invalid syntax", emptyConfig},                                     // error tokenizing due to no spaces

	}
	for _, tt := range configTests {
		reader := bufio.NewReader(strings.NewReader(tt.configResponse))
		got := &ClusterConfig{}
		f := func(cb *ClusterConfig) {
			got = cb
		}
		if gotError := parseConfigGetResponse(reader, f); gotError != nil && !strings.Contains(gotError.Error(), tt.wantErrorText) {
			t.Errorf("parseConfigGetResponse(%q) parse error mismatch, got:%v, wantText:%v", tt.configResponse, gotError, tt.wantErrorText)
		}
		if !reflect.DeepEqual(*got, tt.wantConfig) {
			t.Errorf("parseConfigGetResponse(%q), gotConfig:%v was not equal to wantConfig: %v", tt.configResponse, got, tt.wantConfig)
		}
	}
}
