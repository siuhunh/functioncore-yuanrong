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
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/utils"
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

func Test_acquireWebsocketCae(t *testing.T) {
	convey.Convey("Test_acquireWebsocketCae ok", t, func() {
		cs := &centralizedStrategy{
			instances:         map[string]*instance{},
			instanceOrders:    []*instance{},
			concurrency:       50,
			scalingOutNum:     0,
			nextMockIndex:     0,
			funcKeyWithRes:    "funcKeyWithRes",
			businessType:      "CAE",
			enableRemoteDebug: false,
			enableWebsocket:   true,
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker,
				func(podIP, podPort, traceID string) (uint32, uint32, error) {
					return 10, 99950, nil
				}),
		}
		cs.addInstance(&common.Instance{InstanceKey: "instanceKey1", PodIP: "172.28.0.80", Applier: "AutoScaler"}, caeWebSocketInstance)
		cs.addInstance(&common.Instance{InstanceKey: "instanceKey2", PodIP: "172.28.0.80", Applier: "AutoScaler"}, caeWebSocketInstance)
		time.Sleep(time.Millisecond * 10)
		ins1 := cs.acquireWebsocketCae()
		convey.So(cs.instances[ins1.InstanceKey].idleProcessorNum == 49, convey.ShouldBeTrue)
		for _, ins := range cs.instanceOrders {
			if ins.instanceKey == ins1.InstanceKey {
				convey.So(ins.idleProcessorNum == 49, convey.ShouldBeTrue)
			} else {
				convey.So(ins.idleProcessorNum == 50, convey.ShouldBeTrue)
			}
		}
		ins1 = cs.acquireWebsocketCae()
		convey.So(cs.instances[ins1.InstanceKey].idleProcessorNum == 49, convey.ShouldBeTrue)
		for _, ins := range cs.instanceOrders {
			convey.So(ins.idleProcessorNum == 49, convey.ShouldBeTrue)
		}
		ins1 = cs.acquireWebsocketCae()
		convey.So(cs.instances[ins1.InstanceKey].idleProcessorNum == 48, convey.ShouldBeTrue)
		for _, ins := range cs.instanceOrders {
			if ins.instanceKey == ins1.InstanceKey {
				convey.So(ins.idleProcessorNum == 48, convey.ShouldBeTrue)
			} else {
				convey.So(ins.idleProcessorNum == 49, convey.ShouldBeTrue)
			}
		}
		ins1 = cs.acquireWebsocketCae()
		convey.So(cs.instances[ins1.InstanceKey].idleProcessorNum == 48, convey.ShouldBeTrue)
		for _, ins := range cs.instanceOrders {
			convey.So(ins.idleProcessorNum == 48, convey.ShouldBeTrue)
		}
		convey.So(cs.instances["instanceKey1"].idleProcessorNum == 48, convey.ShouldBeTrue)
		err := cs.releaseInstance(&schedulerapi.ReleaseInstanceReq{InstanceKey: "instanceKey1"})
		convey.So(err, convey.ShouldBeNil)
		convey.So(cs.instances["instanceKey1"].idleProcessorNum == 49, convey.ShouldBeTrue)
		for _, ins := range cs.instanceOrders {
			if ins.instanceKey == "instanceKey1" {
				convey.So(ins.idleProcessorNum == 49, convey.ShouldBeTrue)
			} else {
				convey.So(ins.idleProcessorNum == 48, convey.ShouldBeTrue)
			}
		}
		patches = [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker,
				func(podIP, podPort, traceID string) (uint32, uint32, error) {
					return 10, 20, nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		cs.recoverAllConnStatus("funcKeyWithRes")
		for _, ins := range cs.instanceOrders {
			convey.So(ins.idleProcessorNum == 99980, convey.ShouldBeTrue)
		}
	})
}

func TestSyncWebsocketConn(t *testing.T) {
	convey.Convey("TestSyncWebsocketConn ok", t, func() {
		cs := &centralizedStrategy{
			instances:         map[string]*instance{},
			instanceOrders:    []*instance{},
			concurrency:       50,
			scalingOutNum:     0,
			nextMockIndex:     0,
			funcKeyWithRes:    "funcKeyWithRes",
			businessType:      "CAE",
			enableRemoteDebug: false,
			enableWebsocket:   true,
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker,
				func(podIP, podPort, traceID string) (uint32, uint32, error) {
					return 0, 60, nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ins1 := &common.Instance{InstanceKey: "instanceKey1", PodIP: "172.28.0.80", Applier: "AutoScaler"}
		cs.addInstance(ins1, caeWebSocketInstance)
		ins2 := &common.Instance{InstanceKey: "instanceKey2", PodIP: "172.28.0.80", Applier: "AutoScaler"}
		cs.addInstance(ins2, caeWebSocketInstance)
		time.Sleep(time.Millisecond * 10)
		cs.syncWebsocketConn(&instance{}, "instanceKey1")
		convey.So(cs.instances["instanceKey1"].idleProcessorNum, convey.ShouldEqual, 99940)
	})
	convey.Convey("TestSyncWebsocketConn SendCountWebSocketConRequestToWorker err", t, func() {
		cs := &centralizedStrategy{
			instances:         map[string]*instance{},
			instanceOrders:    []*instance{},
			concurrency:       50,
			scalingOutNum:     0,
			nextMockIndex:     0,
			funcKeyWithRes:    "funcKeyWithRes",
			businessType:      "CAE",
			enableRemoteDebug: false,
			enableWebsocket:   true,
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker,
				func(podIP, podPort, traceID string) (uint32, uint32, error) {
					return 0, 0, errors.New("SendCountWebSocketConRequestToWorker err")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ins1 := &common.Instance{InstanceKey: "instanceKey1", PodIP: "172.28.0.80", Applier: "AutoScaler"}
		cs.addInstance(ins1, caeWebSocketInstance)
		ins2 := &common.Instance{InstanceKey: "instanceKey2", PodIP: "172.28.0.80", Applier: "AutoScaler"}
		cs.addInstance(ins2, caeWebSocketInstance)
		time.Sleep(time.Millisecond * 10)
		cs.syncWebsocketConn(&instance{}, "instanceKey1")
		convey.So(cs.instances["instanceKey1"].idleProcessorNum, convey.ShouldEqual, 50)
	})
}
