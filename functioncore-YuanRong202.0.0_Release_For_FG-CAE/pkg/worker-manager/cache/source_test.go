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

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

func TestFuncSource_CheckEventSource(t *testing.T) {
	funcA := "funcA"
	fs := NewFuncSource()

	tests := []struct {
		name      string
		funcSvcID string
		event     eventsource.Event

		expect bool
	}{
		{
			name:      "cae add funcA meta",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: true,
		},
		{
			name:      "meta add funcA meta",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.Meta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: true,
		},
		{
			name:      "cae add funcA meta, skip",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: false,
		},
		{
			name:      "cae add funcA instance, skip",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.InstanceMetadataEventKind,
			},

			expect: false,
		},
		{
			name:      "cae delete funcA meta, skip",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Deleted,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: false,
		},
		{
			name:      "cae delete funcA instance, skip",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Deleted,
				Kind:       utils.InstanceMetadataEventKind,
			},

			expect: false,
		},
		{
			name:      "meta add funcA instance",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.Meta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.InstanceMetadataEventKind,
			},

			expect: true,
		},
		{
			name:      "meta delete funcA meta",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.Meta.String(),
				EventType:  eventsource.Deleted,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: true,
		},
		{
			name:      "cae add funcA meta, skip",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: false,
		},
		{
			name:      "cae add funcA instance, skip",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.InstanceMetadataEventKind,
			},

			expect: false,
		},
		{
			name:      "meta delete funcA instance",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.Meta.String(),
				EventType:  eventsource.Deleted,
				Kind:       utils.InstanceMetadataEventKind,
			},

			expect: true,
		},
		{
			name:      "cae add funcA meta",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: true,
		},
		{
			name:      "cae add funcA instance",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Added,
				Kind:       utils.InstanceMetadataEventKind,
			},

			expect: true,
		},
		{
			name:      "cae delete funcA meta",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Deleted,
				Kind:       utils.FunctionRepoEventKind,
			},

			expect: true,
		},
		{
			name:      "cae delete funcA instance",
			funcSvcID: funcA,
			event: eventsource.Event{
				SourceType: etcd3.CAEMeta.String(),
				EventType:  eventsource.Deleted,
				Kind:       utils.InstanceMetadataEventKind,
			},

			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, fs.CheckEventSource(tt.event, tt.funcSvcID), tt.name)
		})
	}
}
