/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package utils

import (
	"net"
	"net/http"
	"reflect"
	"testing"

	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/functionbus/common"
)

func TestGetSLOAgentClient(t *testing.T) {
	tests := []struct {
		name          string
		httpsConfig   *commontls.InternalHTTPSConfig
		requestConfig *common.RequestConfig
		want          *http.Client
	}{
		{
			name: "normal case",
			want: &http.Client{
				Timeout: defaultRequestSLOAgentTimeout,
				Transport: &http.Transport{
					DialContext: (&net.Dialer{
						Timeout:   dialTimeout,
						KeepAlive: connKeepAlive,
					}).DialContext,
					MaxIdleConns:    maxIdleConns,
					IdleConnTimeout: idleConnTimeout,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetSLOAgentClient()
			if got == nil {
				t.Errorf("GetSLOAgentClient() got nil")
			}
			if !reflect.DeepEqual(got.Timeout, tt.want.Timeout) {
				t.Errorf("GetSLOAgentClient() Timeout = %v, want %v", got, tt.want)
			}
		})
	}
}
