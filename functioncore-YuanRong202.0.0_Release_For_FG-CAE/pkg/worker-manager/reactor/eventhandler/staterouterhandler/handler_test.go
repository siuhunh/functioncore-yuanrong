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

// Package staterouterhandler defines handler of state router
package staterouterhandler

import (
	"encoding/json"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/eventsource"
	mockUtils "functioncore/test/utils"
)

func TestNewEventHandler(t *testing.T) {
	stateHandler := &StateHandler{}
	tests := []struct {
		name string
		want *StateHandler
	}{
		{"case1", stateHandler},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEventHandler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEventHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateHandler_HandleEvent(t *testing.T) {
	type args struct {
		event eventsource.Event
	}
	var a args
	valueMap := map[string]string{}
	stateID := "aaaa"
	valueMap[constants.FuncNameKey+stateID] = "FuncNameKey"
	valueMap[constants.TenantIDKey+stateID] = "TenantIDKey"
	valueMap[constants.BusinessIDKey+stateID] = "BusinessIDKey"
	valueMap[constants.VersionKey+stateID] = "VersionKey"
	valueMap[constants.ServiceIDKey+stateID] = "ServiceIDKey"
	valueMap[stateID] = "nodeID"
	valueMap[constants.CreatorKey+stateID] = "CreatorKey"
	valueMap[constants.ClusterKey] = os.Getenv("CLUSTER_ID")
	valueMap[constants.ApplierKey] = constants.StateManagerUpdate
	value, _ := json.Marshal(valueMap)
	ev := &etcd3.Event{
		Value: value,
	}
	a.event.EventType = eventsource.Added
	a.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/aaaa"
	a.event.Object = ev

	var a2 args
	a2.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest"

	var a2_2 args
	a2_2.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest"
	ev = &etcd3.Event{
		Value: value,
	}
	a2_2.event.Object = ev

	var a2_3 args
	a2_3.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/aaaa"
	vp := map[int]int64{}
	vp[5] = 5
	value_2, _ := json.Marshal(vp)
	ev_2 := &etcd3.Event{
		Value: value_2,
	}
	a2_3.event.Object = ev_2

	var a2_4 args
	a2_4.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/aaaa"
	valueMaps := map[string]string{}
	valueMaps[constants.ApplierKey] = constants.StateManagerAdd
	value_3, _ := json.Marshal(valueMaps)
	ev_3 := &etcd3.Event{
		Value: value_3,
	}
	a2_4.event.Object = ev_3

	var a3 args
	a3.event.EventType = eventsource.Added
	a3.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/aaaa"
	valueMap[constants.ClusterKey] = "ClusterKey"
	value, _ = json.Marshal(valueMap)
	ev = &etcd3.Event{
		Value: value,
	}
	a3.event.Object = ev

	var a4 args
	a4.event.EventType = eventsource.Deleted
	a4.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/aaaa"
	ev = &etcd3.Event{
		Value: value,
	}
	a4.event.Object = ev

	var a5 args
	a5.event.EventType = "default"
	a5.event.KeyRef = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/aaaa"
	ev = &etcd3.Event{
		Value: value,
	}
	a5.event.Object = ev
	tests := []struct {
		name    string
		args    args
		wantErr bool
		containsStateID bool
	}{
		{"case1", a, true, true},
		{"case2", a2, false, false},
		{"case2_2", a2_2, false, false},
		{"case2_3", a2_3, false, false},
		{"case2_4", a2_4, false, true},
		{"case3", a3, false, false},
		{"case4", a4, false, false},
		{"case5", a5, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache.StateWorkersList = sync.Map{}
			handler := &StateHandler{}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			if _, ok := cache.StateWorkersList.Load(stateID);  ok != tt.containsStateID {
				t.Errorf("HandleEvent() case %s error", tt.name)
			}
		})
	}
}

func Test_handlePodWithPullUp(t *testing.T) {
	workerCache := &cache.WorkerCache
	var actions []cache.Action
	actions = append(actions, cache.Action{Type: cache.ActionStatefulInstanceRePull, NeedPull: 1})
	var stateAction []cache.PodAction
	stateAction = append(stateAction, cache.PodAction{FuncSvc: "", StatefulAction: actions, StatefulMeta: cache.Metadata{}})
	type args struct {
		funcSvcID   string
		stateRouter cache.StateMetadata
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to GetStatePodActionPullUp",
			args{
				funcSvcID:   "",
				stateRouter: cache.StateMetadata{},
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(workerCache), "GetStatePodActionPullUp",
						func(_, funcSvcID, creator, stateID string,
							cpu, memory int64) ([]cache.PodAction, error) {
							return stateAction, nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handlePodWithPullUp(tt.args.funcSvcID, tt.args.stateRouter); (err != nil) != tt.wantErr {
				t.Errorf("handlePodWithPullUp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handlePodWithReduce(t *testing.T) {
	type args struct {
		funcSvcID   string
		stateRouter cache.StateMetadata
	}
	var a args
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handlePodWithReduce(tt.args.funcSvcID, tt.args.stateRouter); (err != nil) != tt.wantErr {
				t.Errorf("handlePodWithReduce() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handlePodWithoutReload(t *testing.T) {
	type args struct {
		funcSvcID   string
		stateRouter cache.StateMetadata
	}
	var a args
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handlePodWithoutReload(tt.args.funcSvcID, tt.args.stateRouter); (err != nil) != tt.wantErr {
				t.Errorf("handlePodWithoutReload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parseStateRouter(t *testing.T) {
	type args struct {
		event eventsource.Event
	}
	var a args
	tests := []struct {
		name  string
		args  args
		want  cache.StateMetadata
		want1 bool
	}{
		{"case1", a, cache.StateMetadata{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := parseStateRouter(tt.args.event)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseStateRouter() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("parseStateRouter() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
