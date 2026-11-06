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

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/faas-scheduler/scheduler/common"
)

func Test_instance_updateCentralizedConcurrency(t *testing.T) {
	type args struct {
		oldConcurrency int
		leaseArr       []bool
		newConcurrency int
	}
	tests := []struct {
		name                string
		args                args
		expectedLeases      []bool
		expectedExtraMapLen int
	}{
		{
			name: "update concurrency when the old concurrency is less than new",
			args: args{
				oldConcurrency: 5,
				leaseArr:       []bool{true, true, false, false, true},
				newConcurrency: 8,
			},
			expectedLeases:      []bool{true, true, false, false, true, false, false, false},
			expectedExtraMapLen: 0,
		},
		{
			name: "update concurrency when the old concurrency is large than new and extraMap should be updated",
			args: args{
				oldConcurrency: 5,
				leaseArr:       []bool{true, true, false, false, true},
				newConcurrency: 3,
			},
			expectedLeases:      []bool{true, true, false},
			expectedExtraMapLen: 1,
		},
		{
			name: "update concurrency when the old concurrency is large than new and extraMap should not be updated",
			args: args{
				oldConcurrency: 5,
				leaseArr:       []bool{true, true, false, false, false},
				newConcurrency: 3,
			},
			expectedLeases:      []bool{true, true, false},
			expectedExtraMapLen: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := createInstance(&common.Instance{}, tt.args.oldConcurrency, true)
			for i, p := range instance.processors {
				p.leased = tt.args.leaseArr[i]
			}
			instance.updateCentralizedConcurrency(tt.args.newConcurrency)
			// checkpoint 1
			if len(instance.processors) != tt.args.newConcurrency {
				t.Errorf("case name: %s actual concurrency %d, expected concurrency %d",
					tt.name, len(instance.processors), tt.args.newConcurrency)
			}
			// checkpoint 2
			for i, p := range instance.processors {
				if p.leased != tt.expectedLeases[i] {
					t.Errorf("case name: %s, index %d, actual leased status %t, expected leased status %t",
						tt.name, i, p.leased, tt.expectedLeases[i])
				}
			}
			// checkpoint 3
			if len(instance.extraProcessorsMap) != tt.expectedExtraMapLen {
				t.Errorf("case name: %s, actual extraMap length: %d, expected extraMap length: %d", tt.name,
					len(instance.extraProcessorsMap), tt.expectedExtraMapLen)
			}
		})
	}
}

func Test_instance_releaseWebSocketProcessor(t *testing.T) {
	type fields struct {
		processors       []*processor
		idleProcessorNum int
		recovered        bool
		concurrency      int
	}
	tests := []struct {
		name        string
		fields      fields
		processorID uint32
		want        int
		wantErr     bool
	}{
		{
			name: "normal case",
			fields: fields{
				processors: []*processor{
					{
						processorID: 0,
						threadNum:   0,
						leased:      true,
						isWebSocket: true,
					},
				},
				idleProcessorNum: 0,
				recovered:        false,
				concurrency:      1,
			},
			want:    1,
			wantErr: false,
		}, {
			name: "valid processor id but has already released before",
			fields: fields{
				processors: []*processor{
					{
						processorID: 0,
						threadNum:   0,
						leased:      false,
						isWebSocket: true,
					},
				},
				idleProcessorNum: 1,
				recovered:        false,
				concurrency:      1,
			},
			want:    -1,
			wantErr: true,
		}, {
			name: "release invalid processor",
			fields: fields{
				processors: []*processor{
					{
						processorID: 0,
						threadNum:   0,
						leased:      false,
						isWebSocket: true,
					},
				},
				idleProcessorNum: 1,
				recovered:        false,
				concurrency:      1,
			},
			processorID: 2,
			want:        -1,
			wantErr:     true,
		}, {
			name: "release recovering processor",
			fields: fields{
				processors: []*processor{
					{
						processorID: 0,
						threadNum:   0,
						leased:      false,
						isWebSocket: true,
					},
				},
				idleProcessorNum: 0,
				recovered:        true,
				concurrency:      1,
			},
			processorID: 0,
			want:        1,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &instance{
				instanceKey:      "pool22",
				processors:       tt.fields.processors,
				idleProcessorNum: tt.fields.idleProcessorNum,
				recovered:        tt.fields.recovered,
				concurrency:      tt.fields.concurrency,
			}
			got, err := instance.releaseWebSocketProcessor(tt.processorID)
			if (err != nil) != tt.wantErr {
				t.Errorf("releaseWebSocketProcessor() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("releaseWebSocketProcessor() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createInstance(t *testing.T) {
	type args struct {
		concurrency          int
		isWebSocketProcessor bool
	}
	tests := []struct {
		name             string
		args             args
		wantProcessorLen int
	}{
		{
			name: "websocket normal concurrency",
			args: args{
				concurrency:          50,
				isWebSocketProcessor: true,
			},
			wantProcessorLen: 50,
		},
		{
			name: "websocket large concurrency",
			args: args{
				concurrency:          260,
				isWebSocketProcessor: true,
			},
			wantProcessorLen: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createInstance(&common.Instance{}, tt.args.concurrency, tt.args.isWebSocketProcessor); len(got.processors) != tt.wantProcessorLen {
				t.Errorf("createInstance() processor len= %d, want %d", len(got.processors), tt.wantProcessorLen)
			}
		})
	}
}

func Test_acquireProcessor(t *testing.T) {
	convey.Convey("Test_acquireProcessor", t, func() {
		instance := createInstance(&common.Instance{}, 2, false)
		convey.So(instance.processors[0].leased, convey.ShouldBeFalse)
		convey.So(instance.processors[1].leased, convey.ShouldBeFalse)
		instance.acquireProcessor()
		convey.So(instance.processors[0].leased, convey.ShouldBeTrue)
		instance.acquireProcessor()
		convey.So(instance.processors[1].leased, convey.ShouldBeTrue)
	})
}
