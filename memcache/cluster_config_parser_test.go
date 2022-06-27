// Copyright 2011 Google Inc.
// Copyright 2022 The gomemcache Authors
// SPDX-License-Identifier: Apache-2.0

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
	var buf strings.Builder
	buf.WriteString("")
	buf.WriteString("CONFIG cluster 0 80\r\n")
	buf.WriteString(fmt.Sprintf("%d", discoveryID))
	buf.WriteString("\r\n")

	for i, address := range discoveryAddress {
		buf.WriteString(fmt.Sprintf("%s|%s|%s", address[0], address[0], address[1]))
		if i < len(discoveryAddress)-1 {
			buf.WriteString(" ")
		}
	}

	buf.WriteString("\n\r\n")

	return buf.String()
}

func buildClusterConfig(discoveryID int, discoveryAddress [][]string) *ClusterConfig {
	cc := &ClusterConfig{ConfigID: int64(discoveryID)}
	cc.NodeAddresses = make([]ClusterNode, len(discoveryAddress))

	for i, address := range discoveryAddress {
		port, _ := strconv.ParseInt(address[1], 10, 64)
		cc.NodeAddresses[i] = ClusterNode{
			Host: address[0],
			Port: port,
		}
	}

	return cc
}

func TestGoodClusterConfigs(t *testing.T) {
	configTests := []struct {
		discoveryID        int
		discoveryAddresses [][]string
	}{
		{
			discoveryID:        2,
			discoveryAddresses: [][]string{{"127.0.0.1", "112233"}},
		},
		{
			discoveryID:        1000,
			discoveryAddresses: [][]string{{"127.0.0.1", "112233"}, {"127.0.0.4", "123435"}},
		},
		{
			discoveryID:        50,
			discoveryAddresses: [][]string{{"127.0.0.1", "112233"}, {"127.0.0.4", "123435"}, {"127.0", "123"}},
		},
	}
	for _, tt := range configTests {
		config := prepareConfigResponse(tt.discoveryID, tt.discoveryAddresses)
		want := buildClusterConfig(tt.discoveryID, tt.discoveryAddresses)
		reader := bufio.NewReader(strings.NewReader(config))

		got := &ClusterConfig{}
		fn := func(cb *ClusterConfig) {
			got = cb
		}
		if err := parseConfigGetResponse(reader, fn); err != nil {
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
		// empty config returns no error with empty cluster config
		{
			configResponse: "",
			wantErrorText:  "",
			wantConfig:     emptyConfig,
		},
		// empty config returns no error with empty cluster config
		{
			configResponse: "END\r\n",
			wantErrorText:  "",
			wantConfig:     emptyConfig,
		},
		// error parsing port
		{
			configResponse: "CONFIG cluster 0 80\r\nbadCfg\r\n123.76|123.76|5432\r\nEND\r\n",
			wantErrorText:  "strconv.ParseInt: parsing \"badCfg\"",
			wantConfig:     emptyConfig,
		},
		// error parsing port
		{

			configResponse: "CONFIG cluster 0 80\r\n100\r\n123.76|123.76|portBroken\r\nEND\r\n",
			wantErrorText:  "strconv.ParseInt: parsing \"portBroken\"",
			wantConfig:     emptyConfig,
		},
		// error tokenizing due to no pipes
		{
			configResponse: "CONFIG cluster 0 80\r\n100\r\n123.76123.76portBroken\r\nEND\r\n",
			wantErrorText:  "host address ([123.76123.76portBroken]) not in expected format",
			wantConfig:     emptyConfig,
		},
		// error tokenizing due to no spaces
		{
			configResponse: "CONFIG cluster 0 80\r\n100\r\n123.76|123.76|123123.76|123.76|123\r\nEND\r\n",
			wantErrorText:  "invalid syntax",
			wantConfig:     emptyConfig,
		},
	}
	for _, tt := range configTests {
		reader := bufio.NewReader(strings.NewReader(tt.configResponse))
		got := &ClusterConfig{}
		f := func(cb *ClusterConfig) {
			got = cb
		}

		gotError := parseConfigGetResponse(reader, f)
		if gotError != nil && !strings.Contains(gotError.Error(), tt.wantErrorText) {
			t.Errorf("parseConfigGetResponse(%q) parse error mismatch, got:%v, wantText:%v", tt.configResponse, gotError, tt.wantErrorText)
		}

		if !reflect.DeepEqual(*got, tt.wantConfig) {
			t.Errorf("parseConfigGetResponse(%q), gotConfig:%v was not equal to wantConfig: %v", tt.configResponse, got, tt.wantConfig)
		}
	}
}
