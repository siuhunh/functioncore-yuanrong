/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package cancel

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/test/utils"
)

func TestGetInstance(t *testing.T) {
	managerInstance := GetInstance()
	if managerInstance == nil {
		t.Errorf("get instance error")
	}
}

func TestRegisterCancelHandler(t *testing.T) {
	m := GetInstance()
	m.RegisterCancelHandler(
		func(workerID string, dst *common.CancelProperty, requestID uint64, traceID string,
		) *types.TCPResponse {
			return &types.TCPResponse{}
		})
	if m.handler == nil {
		t.Errorf("RegisterCancelHandler error")
	}
	m.handler = nil
}

func TestHandleCancel(t *testing.T) {
	tests := []struct {
		name        string
		traceID     string
		dst         *common.CancelProperty
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			"test1",
			&common.CancelProperty{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{
							Client: &clientv3.Client{
								Lease: utils.FakeEtcdLease{},
							},
						}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&utils.FakeEtcdLease{}),
						"Grant",
						func(_ *utils.FakeEtcdLease, ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
							return nil, errors.New("grant error")
						}),
				})
				return patches
			},
			false,
		},
		{
			"case2",
			"test2",
			&common.CancelProperty{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{
							Client: &clientv3.Client{
								Lease: utils.FakeEtcdLease{},
							},
						}
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte{}, errors.New("marshal error")
					}),
				})
				return patches
			},
			false,
		},
		{
			"case3",
			"test3",
			&common.CancelProperty{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{
							Client: &clientv3.Client{
								Lease: utils.FakeEtcdLease{},
							},
						}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&utils.FakeEtcdLease{}),
						"Grant",
						func(_ *utils.FakeEtcdLease, ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
							return &clientv3.LeaseGrantResponse{}, nil
						}),
					gomonkey.ApplyFunc(etcd3.CreateEtcdCtxInfo, func(ctx context.Context) etcd3.EtcdCtxInfo {
						return etcd3.EtcdCtxInfo{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			},
			true,
		},
	}

	for _, tt := range tests {
		patches := tt.patchesFunc()
		m := Manager{}
		tcpResponse := m.HandleCancel(context.Background(), tt.traceID, tt.dst)
		if (tcpResponse.ErrorCode == proxyerrors.StateOKCode) != tt.calledWant {
			t.Errorf("handleCancel error = %v, want %v",
				tcpResponse.ErrorCode == proxyerrors.StateOKCode,
				tt.calledWant,
			)
		}
		patches.ResetAll()
	}
}

func TestAddRequest(t *testing.T) {
	tests := []struct {
		name          string
		futureID      string
		dispatcherReq *types.DispatcherRequest
		calledWant    bool
	}{
		{
			"case1",
			"",
			&types.DispatcherRequest{
				Canceled: false,
			},
			false,
		},
		{
			"case2",
			"testObserveValue",
			&types.DispatcherRequest{
				Canceled: false,
			},
			true,
		},
		{
			"case3",
			"testStore",
			&types.DispatcherRequest{
				Canceled: false,
			},
			false,
		},
		{
			"case4",
			"test",
			&types.DispatcherRequest{
				Canceled: false,
			},
			false,
		},
	}

	m := Manager{
		observed: make(map[string]*observeValue),
		store:    make(map[string]*request),
	}
	m.observed["testObserveValue"] = &observeValue{}
	m.store["testStore"] = &request{}
	for _, tt := range tests {
		m.AddRequest(tt.futureID, tt.dispatcherReq)
		if tt.dispatcherReq.Canceled != tt.calledWant {
			t.Errorf("add request  error = %v, want %v",
				tt.dispatcherReq.Canceled,
				tt.calledWant,
			)
		}
	}
}

func TestUpdateRequest(t *testing.T) {
	tests := []struct {
		name       string
		futureID   string
		workerID   string
		calledWant bool
	}{
		{
			"case1",
			"",
			"",
			false,
		},
		{
			"case2",
			"test",
			"",
			false,
		},
		{
			"case3",
			"testStore",
			"test",
			true,
		},
	}

	m := Manager{
		observed: make(map[string]*observeValue),
		store:    make(map[string]*request),
	}
	m.store["testStore"] = &request{}
	for _, tt := range tests {
		m.UpdateRequest(tt.futureID, tt.workerID)
		if item, ok := m.store[tt.futureID]; ok {
			if (item.workerID == tt.workerID) != tt.calledWant {
				t.Errorf("update request test %s error = %v, want %v",
					tt.name,
					item.workerID == tt.workerID,
					tt.calledWant,
				)
			}
		} else {
			if tt.calledWant == true {
				t.Errorf("update request test %s error = %v, want %v",
					tt.name,
					false,
					tt.calledWant,
				)
			}
		}
	}
}

func TestDeleteRequest(t *testing.T) {
	tests := []struct {
		name       string
		futureID   string
		calledWant bool
	}{
		{
			"case1",
			"",
			false,
		},
		{
			"case2",
			"test",
			false,
		},
		{
			"case3",
			"testStore",
			false,
		},
	}

	for _, tt := range tests {
		m := Manager{
			observed: make(map[string]*observeValue),
			store:    make(map[string]*request),
		}
		m.store["testStore"] = &request{}
		m.DeleteRequest(tt.futureID)
		if _, ok := m.store[tt.futureID]; ok != tt.calledWant {
			t.Errorf("delete request test %s error = %v, want %v",
				tt.name,
				ok,
				tt.calledWant,
			)
		}
	}
}

func TestDeleteAbandonedRequest(t *testing.T) {
	tests := []struct {
		name       string
		futureID   string
		calledWant bool
	}{
		{
			"case1",
			"",
			false,
		},
		{
			"case2",
			"test",
			false,
		},
		{
			"case3",
			"testStore",
			false,
		},
	}

	for _, tt := range tests {
		m := Manager{
			observed: make(map[string]*observeValue),
			store:    make(map[string]*request),
		}
		m.store["testStore"] = &request{}
		m.DeleteAbandonedRequest(tt.futureID)
		if _, ok := m.store[tt.futureID]; ok != tt.calledWant {
			t.Errorf("delete request test %s error = %v, want %v",
				tt.name,
				ok,
				tt.calledWant,
			)
		}
	}
}

func TestIsCancel(t *testing.T) {
	m := Manager{}
	if m.IsCancel("test") != true {
		t.Errorf("is cancel test error")
	}
}

func TestUpdateOnObserve(t *testing.T) {
	called := false
	tests := []struct {
		name        string
		event       *etcd3.Event
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			&etcd3.Event{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		{
			"case2",
			&etcd3.Event{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseObservedEvent, func(ev *etcd3.Event) (observeValue, error) {
						called = true
						return observeValue{}, errors.New("parse event error")
					}),
				})
				return patches
			},
			true,
		},
		{
			"case3",
			&etcd3.Event{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseObservedEvent, func(ev *etcd3.Event) (observeValue, error) {
						called = true
						return observeValue{
							CancelProperty: common.CancelProperty{
								FutureID: "t",
							},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
		{
			"case4",
			&etcd3.Event{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseObservedEvent, func(ev *etcd3.Event) (observeValue, error) {
						called = true
						return observeValue{
							CancelProperty: common.CancelProperty{
								FutureID: "test",
							},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
		{
			"case5",
			&etcd3.Event{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseObservedEvent, func(ev *etcd3.Event) (observeValue, error) {
						called = true
						return observeValue{
							CancelProperty: common.CancelProperty{
								FutureID: "test2",
							},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
	}

	for _, tt := range tests {
		called = false
		patches := tt.patchesFunc()
		m := Manager{
			store:    make(map[string]*request),
			observed: make(map[string]*observeValue),
			handler: func(workerID string, dst *common.CancelProperty, requestID uint64, traceID string) *types.TCPResponse {
				return &types.TCPResponse{}
			},
		}
		m.store["test"] = &request{
			workerID: "test",
		}
		m.store["test2"] = &request{
			dispatcherReq: &types.DispatcherRequest{
				Canceled: false,
			},
		}
		m.UpdateOnObserve(tt.event)
		patches.ResetAll()
		if called != tt.calledWant {
			t.Errorf("update on observe test %s error = %v, want %v",
				tt.name,
				called,
				tt.calledWant,
			)
		}
	}
}

func TestDeleteOnObserve(t *testing.T) {
	called := false
	tests := []struct {
		name        string
		event       *etcd3.Event
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			&etcd3.Event{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		{
			"case2",
			&etcd3.Event{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseObservedEvent, func(ev *etcd3.Event) (observeValue, error) {
						called = true
						return observeValue{
							CancelProperty: common.CancelProperty{FutureID: "test"},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
	}

	for _, tt := range tests {
		called = false
		patches := tt.patchesFunc()
		m := Manager{
			observed: make(map[string]*observeValue),
		}
		m.DeleteOnObserve(tt.event)
		patches.ResetAll()
		if called != tt.calledWant {
			t.Errorf("delete on observe test %s error = %v, want %v",
				tt.name,
				called,
				tt.calledWant,
			)
		}
	}
}

func TestRecoverRequests(t *testing.T) {
	tests := []struct {
		name      string
		workerID  string
		futureIDs []string
	}{
		{
			"case1",
			"test",
			[]string{"futureIds"},
		},
	}

	for _, tt := range tests {
		m := Manager{
			store: make(map[string]*request),
		}
		m.RecoverRequests(tt.workerID, tt.futureIDs...)
		for _, val := range tt.futureIDs {
			if _, ok := m.store[val]; !ok {
				t.Errorf("recover request test error")
			}
		}
	}
}

func TestRecordAsyncRequests(t *testing.T) {
	tests := []struct {
		name      string
		workerID  string
		futureIDs []string
	}{
		{
			"case1",
			"test",
			[]string{"futureIds"},
		},
	}

	for _, tt := range tests {
		m := Manager{
			store: make(map[string]*request),
		}
		m.RecordAsyncRequests(tt.workerID, tt.futureIDs...)
		for _, val := range tt.futureIDs {
			if _, ok := m.store[val]; !ok {
				t.Errorf("recover async request test error")
			}
		}
	}
}

func TestRecoverRequestInternal(t *testing.T) {
	tests := []struct {
		name        string
		workerID    string
		futureID    string
		recordAsync bool
		calledWant  bool
	}{
		{
			"case1",
			"",
			"",
			true,
			false,
		},
		{
			"case2",
			"",
			"test",
			true,
			false,
		},
		{
			"case3",
			"",
			"te",
			true,
			true,
		},
		{
			"case4",
			"",
			"te",
			false,
			true,
		},
		{
			"case5",
			"",
			"t",
			false,
			true,
		},
	}

	for _, tt := range tests {
		m := Manager{
			store:    make(map[string]*request),
			observed: make(map[string]*observeValue),
			handler: func(workerID string, dst *common.CancelProperty, requestID uint64, traceID string) *types.TCPResponse {
				return &types.TCPResponse{}
			},
		}
		m.observed["test"] = &observeValue{
			CancelProperty: common.CancelProperty{},
		}
		m.store["te"] = &request{}
		m.recoverRequest(tt.workerID, tt.futureID, tt.recordAsync)
		if _, ok := m.store[tt.futureID]; ok != tt.calledWant {
			t.Errorf("recover request internal test %s error = %v, want %v",
				tt.name,
				ok,
				tt.calledWant,
			)
		}
	}
}

func TestParseObservedEvent(t *testing.T) {
	tests := []struct {
		name       string
		event      *etcd3.Event
		calledWant bool
	}{
		{
			"case1",
			&etcd3.Event{
				Key: "a/a/a",
			},
			true,
		},
		{
			"case2",
			&etcd3.Event{
				Key: "a/a/a/",
			},
			true,
		},
		{
			"case3",
			&etcd3.Event{
				Key:   "a/a/a/a",
				Value: []byte("{tess"),
			},
			true,
		},
		{
			"case4",
			&etcd3.Event{
				Key:   "a/a/a/a",
				Value: []byte("{\"futureID\":\"t\"}"),
			},
			true,
		},
		{
			"case5",
			&etcd3.Event{
				Key:   "a/a/a/a",
				Value: []byte("{\"futureID\":\"\"}"),
			},
			false,
		},
	}

	for _, tt := range tests {
		_, err := parseObservedEvent(tt.event)
		if (err != nil) != tt.calledWant {
			t.Errorf("parseObservedEvent test %s error = %v, want %v",
				tt.name,
				err,
				tt.calledWant,
			)
		}
	}
}
