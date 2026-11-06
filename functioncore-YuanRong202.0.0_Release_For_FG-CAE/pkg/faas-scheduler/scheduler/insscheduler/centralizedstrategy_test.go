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

package insscheduler

import (
	"testing"

	"functioncore/pkg/faas-scheduler/scheduler/common"
)

func Test_centralizedStrategy_addInstance(t *testing.T) {
	type fields struct {
		concurrency    int
		funcKeyWithRes string
	}
	type args struct {
		instance *common.Instance
		t        instanceType
	}
	type want struct {
		length      int
		existingKey string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      want
		instances []*common.Instance
	}{
		{
			name: "add reserved instance to cache",
			fields: fields{
				concurrency:    5,
				funcKeyWithRes: "",
			},
			args: args{
				instance: &common.Instance{
					InstanceKey: "pool22-testMockKey",
					Applier:     "worker-manager",
					PodIP:       "127.0.0.1",
					PodPort:     "8800",
					NodeIP:      "127.0.0.1",
					NodePort:    "8000",
				},
				t: 0,
			},
			want: want{
				length:      1,
				existingKey: "pool22-testMockKey",
			},
		},
		{
			name: "add websocket instance to cache",
			fields: fields{
				concurrency:    5,
				funcKeyWithRes: "",
			},
			args: args{
				instance: &common.Instance{
					InstanceKey: "pool22-websocketMockKey",
					Applier:     "worker-manager",
					PodIP:       "127.0.0.1",
					PodPort:     "8800",
					NodeIP:      "127.0.0.1",
					NodePort:    "8000",
				},
				t: 2,
			},
			want: want{
				length:      1,
				existingKey: "pool22-websocketMockKey",
			},
		},
		{
			name: "add placeholder instance to cache",
			fields: fields{
				concurrency:    5,
				funcKeyWithRes: "",
			},
			args: args{
				instance: &common.Instance{
					InstanceKey: common.InstancePlaceHolderPrefix,
				},
				t: 2,
			},
			want: want{
				length:      1,
				existingKey: common.InstancePlaceHolderPrefix + "0",
			},
		},
		{
			name: "add invalid instance to cache",
			fields: fields{
				concurrency:    5,
				funcKeyWithRes: "",
			},
			args: args{
				instance: &common.Instance{
					InstanceKey: "pool22-websocketMockKey",
				},
				t: 2,
			},
			want: want{
				length: 0,
			},
		},
		{
			name: "unknown type",
			fields: fields{
				concurrency:    5,
				funcKeyWithRes: "",
			},
			args: args{
				instance: &common.Instance{
					InstanceKey: "pool22-websocketMockKey",
					Applier:     "bus",
					PodIP:       "127.0.0.1",
					PodPort:     "8800",
					NodeIP:      "127.0.0.1",
					NodePort:    "8000",
				},
				t: -1,
			},
			want: want{
				length: 0,
			},
		},
		{
			name: "add same function instance",
			fields: fields{
				concurrency:    5,
				funcKeyWithRes: "",
			},
			args: args{
				instance: &common.Instance{
					InstanceKey: "pool23-websocketMockKey",
					Applier:     "bus",
					PodIP:       "127.0.0.1",
					PodPort:     "8800",
					NodeIP:      "127.0.0.1",
					NodePort:    "8000",
				},
				t: 2,
			},
			want: want{
				length:      1,
				existingKey: "pool23-websocketMockKey",
			},
			instances: []*common.Instance{
				{
					InstanceKey: "pool23-websocketMockKey",
					Applier:     "bus",
					PodIP:       "127.0.0.1",
					PodPort:     "8800",
					NodeIP:      "127.0.0.1",
					NodePort:    "8000",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cStrategy := &centralizedStrategy{
				instances:      map[string]*instance{},
				instanceOrders: []*instance{},
				concurrency:    tt.fields.concurrency,
				scalingOutNum:  0,
				funcKeyWithRes: tt.fields.funcKeyWithRes,
			}
			if tt.instances != nil {
				for _, v := range tt.instances {
					cStrategy.addInstance(v, 2)
				}
			}
			cStrategy.addInstance(tt.args.instance, tt.args.t)
			if len(cStrategy.instances) != tt.want.length || len(cStrategy.instanceOrders) != tt.want.length {
				t.Errorf("addInstance() len(cStrategy.instances)= %d, len(cStrategy.instanceOrders)=%d, want %d",
					len(cStrategy.instances), len(cStrategy.instanceOrders), tt.want.length)
			}
			if _, ok := cStrategy.instances[tt.want.existingKey]; tt.want.length != 0 && !ok {
				t.Errorf("instance key %s is not existing in instance order", tt.want.existingKey)
			}
		})
	}
}
