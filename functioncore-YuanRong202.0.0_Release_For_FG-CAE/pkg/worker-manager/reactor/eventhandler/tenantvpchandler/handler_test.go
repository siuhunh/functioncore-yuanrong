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

package tenantvpchandler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/eventsource"
)

func TestTenantVPCHandler_HandleEvent(t *testing.T) {
	tenantID := "12345678"
	tests := []struct {
		name  string
		event eventsource.Event

		expectedErr     bool
		expectedInCache bool
	}{
		{
			name: "invalid event key",
			event: eventsource.Event{
				StartTime: time.Now(),
				KeyRef:    "/sn/business/yrk/tenant/",
			},

			expectedErr:     true,
			expectedInCache: false,
		},
		{
			name: "invalid object type",
			event: eventsource.Event{
				KeyRef: "/sn/business/yrk/tenant/12345678/router",
				Object: etcd3.Event{Value: []byte(`{"tenantVpcCidr":"192.168.0.1/24","gateways":["192.168.0.1","192.168.0.2"]}`)},
			},
			expectedErr:     true,
			expectedInCache: false,
		},
		{
			name: "invalid value",
			event: eventsource.Event{
				KeyRef: "/sn/business/yrk/tenant/12345678/router",
				Object: &etcd3.Event{Value: []byte(``)},
			},
			expectedErr:     true,
			expectedInCache: false,
		},
		{
			name: "invalid event type",
			event: eventsource.Event{
				KeyRef: "/sn/business/yrk/tenant/12345678/router",
				Object: &etcd3.Event{Value: []byte(`{"tenantVpcCidr":"192.168.0.1/24","gateways":["192.168.0.1","192.168.0.2"]}`)},
			},
			expectedErr:     false,
			expectedInCache: false,
		},
		{
			name: "success to add",
			event: eventsource.Event{
				KeyRef:    "/sn/business/yrk/tenant/12345678/router",
				Object:    &etcd3.Event{Value: []byte(`{"tenantVpcCidr":"192.168.0.1/24","gateways":["192.168.0.1","192.168.0.2"]}`)},
				EventType: eventsource.Updated,
			},
			expectedErr:     false,
			expectedInCache: true,
		},
		{
			name: "success to delete",
			event: eventsource.Event{
				KeyRef:    "/sn/business/yrk/tenant/12345678/router",
				Object:    &etcd3.Event{PrevValue: []byte(`{"tenantVpcCidr":"192.168.0.1/24","gateways":["192.168.0.1","192.168.0.2"]}`)},
				EventType: eventsource.Deleted,
			},
			expectedErr:     false,
			expectedInCache: false,
		},
	}

	handler := NewEventHandler()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handler.HandleEvent(tt.event)
			if tt.expectedErr {
				assert.NotNil(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
			}

			_, ok := cache.GetTenantVPCCache().Get(tenantID)
			if tt.expectedInCache {
				assert.True(t, ok, tt.name)
			} else {
				assert.False(t, ok, tt.name)
			}
		})
	}
}
