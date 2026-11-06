/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

package frontend

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/types"
	"functioncore/pkg/frontend-invoke/functionlb"
)

func TestProcessInstanceEvent_AddEvent(t *testing.T) {
	convey.Convey("test addEvent", t, func() {
		convey.Convey("parse from key err", func() {
			instance := etcdkey.WorkerInstanceKey{}
			defer gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(_ *etcdkey.WorkerInstanceKey,
				key string) error {
				return errors.New("")
			}).Reset()
			event := &etcd3.Event{Type: etcd3.PUT}
			err := processInstanceEvent(event)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("version is nil", func() {
			instance := etcdkey.WorkerInstanceKey{}
			defer gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
				key string) error {
				instance.BusinessID = "aaa"
				instance.TenantID = "bbb"
				instance.Function = "ccc"
				return nil
			}).Reset()
			event := &etcd3.Event{Type: etcd3.PUT}
			err := processInstanceEvent(event)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("event.value is nil", func() {
			instance := etcdkey.WorkerInstanceKey{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "bbb"
					instance.Function = "ccc"
					instance.Version = "ddd"
					return nil
				}),
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					return errors.New("")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			event := &etcd3.Event{Type: etcd3.PUT, Value: make([]byte, 0)}
			err := processInstanceEvent(event)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("unmarshal value err", func() {
			instance := etcdkey.WorkerInstanceKey{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "bbb"
					instance.Function = "ccc"
					instance.Version = "ddd"
					return nil
				}),
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					return errors.New("")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			event := &etcd3.Event{Type: etcd3.PUT, Value: make([]byte, 5)}
			err := processInstanceEvent(event)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("worker test", func() {
			instance := etcdkey.WorkerInstanceKey{}
			lb := &functionlb.Workers{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "bbb"
					instance.Function = "ccc"
					instance.Version = "ddd"
					return nil
				}),
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					ins := v.(*workerInstance)
					ins.CPU = 100
					ins.Memory = 100
					ins.Owner = "10.10.10.10"
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(lb), "DeleteWorkerInstance", func(_ *functionlb.Workers,
					az string, urn string, instance string, insResource types.InstanceResource) error {
					return errors.New("")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			event := &etcd3.Event{Type: etcd3.PUT}
			convey.So(processInstanceEvent(event), convey.ShouldBeError)
		})
		convey.Convey("task test", func() {
			instance := etcdkey.WorkerInstanceKey{}
			lb := &functionlb.Workers{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					ins := v.(*workerInstance)
					ins.CPU = 100
					ins.Memory = 100
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(lb), "DeleteWorkerInstance", func(_ *functionlb.Workers,
					az string, urn string, instance string, insResource types.InstanceResource) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.Convey("tenantID is not zero", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "bbb"
					instance.Function = "ccc"
					instance.Version = "ddd"
					return nil
				}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processInstanceEvent(event), convey.ShouldBeError)
			})
			convey.Convey("tenantID is zero", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "0"
					instance.Function = "ccc"
					instance.Version = "ddd"
					return nil
				}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processInstanceEvent(event), convey.ShouldBeNil)
			})
		})
		convey.Convey("add event", func() {
			instance := etcdkey.WorkerInstanceKey{}
			lb := &functionlb.Workers{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					ins, ok := v.(*workerInstance)
					if !ok {
						return nil
					}
					ins.CPU = 100
					ins.Memory = 100
					ins.Status = workerInstanceStatusReady
					ins.Owner = "10.10.10.10"
					ins.NodePort = "10"
					ins.WillDelete = false
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "bbb"
					instance.Function = "ccc"
					instance.Version = "ddd"
					instance.Instance = "defaultaz-#-pool22"
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.Convey("add err", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(lb),
					"AddWorkerInstance", func(_ *functionlb.Workers,
						instanceInfo functionlb.InstanceInfo) error {
						return errors.New("")
					}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processInstanceEvent(event), convey.ShouldBeError)
			})
			convey.Convey("succeed to add event", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(lb),
					"AddWorkerInstance", func(_ *functionlb.Workers,
						instanceInfo functionlb.InstanceInfo) error {
						return nil
					}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processInstanceEvent(event), convey.ShouldBeNil)
			})
		})
		convey.Convey("worker will be deleted", func() {
			instance := etcdkey.WorkerInstanceKey{}
			lb := &functionlb.Workers{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					ins, ok := v.(*workerInstance)
					if !ok {
						return nil
					}
					ins.CPU = 200
					ins.Memory = 200
					ins.Status = workerInstanceStatusReady
					ins.Owner = "20.20.20.20"
					ins.NodePort = "20"
					ins.WillDelete = true
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "bbb"
					instance.Function = "ccc"
					instance.Version = "ddd"
					instance.Instance = "defaultaz-#-pool22"
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(lb), "DeleteWorkerInstance", func(_ *functionlb.Workers,
					az string, urn string, instance string, insResource types.InstanceResource) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			event := &etcd3.Event{Type: etcd3.PUT}
			convey.So(processInstanceEvent(event), convey.ShouldBeError)
		})
	})
}

func TestProcessInstanceEvent_DeleteEvent(t *testing.T) {
	convey.Convey("test delete event", t, func() {
		convey.Convey("parse from key err", func() {
			instance := etcdkey.WorkerInstanceKey{}
			defer gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(_ *etcdkey.WorkerInstanceKey,
				key string) error {
				return errors.New("")
			}).Reset()
			event := &etcd3.Event{Type: etcd3.DELETE}
			err := processInstanceEvent(event)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("", func() {
			instance := etcdkey.WorkerInstanceKey{}
			lb := &functionlb.Workers{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(instance *etcdkey.WorkerInstanceKey,
					key string) error {
					instance.BusinessID = "aaa"
					instance.TenantID = "0"
					instance.Function = "ccc"
					instance.Version = "ddd"
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.Convey("delete event err", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(lb), "DeleteWorkerInstance", func(_ *functionlb.Workers,
					az string, urn string, instance string, insResource types.InstanceResource) error {
					return errors.New("")
				}).Reset()
				event := &etcd3.Event{Type: etcd3.DELETE}
				convey.So(processInstanceEvent(event), convey.ShouldBeError)
			})
			convey.Convey("succeed to delete event", func() {
				defer gomonkey.ApplyMethod(reflect.TypeOf(lb), "DeleteWorkerInstance", func(_ *functionlb.Workers,
					az string, urn string, instance string, insResource types.InstanceResource) error {
					return nil
				}).Reset()
				event := &etcd3.Event{Type: etcd3.DELETE}
				convey.So(processInstanceEvent(event), convey.ShouldBeError)
			})
		})
	})
}

func TestProcessInstanceEvent_Others(t *testing.T) {
	convey.Convey("test etcd event", t, func() {
		convey.Convey("etcd err event", func() {
			event := &etcd3.Event{Type: etcd3.ERROR}
			convey.So(processInstanceEvent(event), convey.ShouldBeNil)
		})
		convey.Convey("etcd undefined event", func() {
			event := &etcd3.Event{Type: 10}
			convey.So(processInstanceEvent(event), convey.ShouldBeNil)
		})
	})
}

func TestIsWorkerEvent(t *testing.T) {
	convey.Convey("Test Is Worker event", t, func() {
		instance := etcdkey.WorkerInstanceKey{}
		convey.Convey("test event err", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(_ *etcdkey.WorkerInstanceKey,
				key string) error {
				return errors.New("")
			}).Reset()
			convey.So(isWorkerInstance(""), convey.ShouldBeFalse)
		})
		convey.Convey("test event ok", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&instance), "ParseFrom", func(_ *etcdkey.WorkerInstanceKey,
				key string) error {
				return nil
			}).Reset()
			convey.So(isWorkerInstance(""), convey.ShouldBeTrue)
		})
	})
}

func Test_extractURNInstance(t *testing.T) {
	tests := []struct {
		name  string
		event *etcd3.Event

		expected URNInstance
	}{
		{
			name: "functiontask",
			event: &etcd3.Event{
				Key:   `/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/127.0.0.1`,
				Value: []byte(`{"ip":"","port":"8080","status":"ready","p2pPort":"32568","nodeIP":"127.0.0.1","nodePort":"22423"}`),
			},

			expected: URNInstance{
				urn:            "sn:cn:yrk:0:function:function-task:$latest",
				instance:       "127.0.0.1:22423",
				status:         "ready",
				instanceID:     "127.0.0.1",
				tenantID:       "0",
				deployNode:     "127.0.0.1",
				funcKeyWithRes: "0/function-task/$latest/-",
			},
		},
		{
			name: "normal instance",
			event: &etcd3.Event{
				Key:   `/sn/workers/business/yrk/tenant/123456/function/0@default@test/version/latest/defaultaz/defaultaz-#-pool22-300-128-fusion-123`,
				Value: []byte(`{"ip":"10.0.0.1","port":"8080","cluster":"cluster001","status":"ready","p2pPort":"22668","nodeIP":"192.168.1.1","nodePort":"22423","applier":"functiontask-123","ownerIP":"192.168.1.1","cpu":300,"memory":128}`),
			},

			expected: URNInstance{
				urn:            "sn:cn:yrk:123456:function:0@default@test:latest",
				instance:       "192.168.1.1:22423",
				status:         "ready",
				instanceID:     "defaultaz-#-pool22-300-128-fusion-123",
				tenantID:       "123456",
				deployNode:     "192.168.1.1",
				insResource:    types.InstanceResource{CPU: "300", Memory: "128"},
				funcKeyWithRes: "123456/0@default@test/latest/300-128",
			},
		},
		{
			name: "cae instance",
			event: &etcd3.Event{
				Key:   `/sn/workers/business/yrk/tenant/123456/function/0@default@testcae/version/latest/defaultaz/defaultaz-#-custom-600-512-1234`,
				Value: []byte(`{"ip":"10.0.0.1","port":"8080","cluster":"cluster001","status":"ready","p2pPort":"22668","nodeIP":"192.168.1.1","nodePort":"22423","applier":"ASBRes","ownerIP":"192.168.1.1","businessType":"CAE","cpu":0,"memory":0}`),
			},

			expected: URNInstance{
				urn:            "sn:cn:yrk:123456:function:0@default@testcae:latest",
				instance:       "192.168.1.1:22423",
				status:         "ready",
				instanceID:     "defaultaz-#-custom-600-512-1234",
				tenantID:       "123456",
				deployNode:     "192.168.1.1",
				insResource:    types.InstanceResource{CPU: "0", Memory: "0"},
				funcKeyWithRes: "123456/0@default@testcae/latest/0-0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractURNInstance(tt.event), tt.name)
		})
	}
}
