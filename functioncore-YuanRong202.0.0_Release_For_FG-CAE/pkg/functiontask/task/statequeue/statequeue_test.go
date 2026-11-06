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

package statequeue

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	core "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functionbus/proxy"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/statequeue/reqnode"
)

func trigger(funcName string, params common.Params, stateID string, createIfNotExist bool) *common.Response {
	reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
	if stateID == "" {
		return execFunc(funcName, params, reqIDList, stateID, marshalState(new(state)))
	}
	r := common.NewRequest(common.RequestOptions{
		FuncName:  funcName,
		ReqIDList: reqIDList,
		Params:    params,
		Config:    "",
	})
	testStateQueue.Request(&metadata.Metadata{StateID: stateID}, r, createIfNotExist)
	return r.GetResponse()
}

// TestNoSuchFunction checks when there is no such function.
func TestNoSuchFunction(t *testing.T) {
	funcName := "t"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.ENoSuchFunction)
	})
}

// TestPanic checks when there is a panic.
func TestPanic(t *testing.T) {
	funcName := "panicHandler"
	stateID := randomStateID()
	count := 100
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, strconv.Itoa(count), stateID, true)
		So(res.Code, ShouldEqual, common.EPanic)
		rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s := unmarshalState(rawState)
		So(s.Count, ShouldEqual, 0)
	})
}

// TestHello checks a simple hello function.
func TestHello(t *testing.T) {
	funcName := "hello"
	stateID := ""
	name := "koala"
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, name, stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "hello, " + name)
	})
}

// TestAddOne checks a simple add one function.
func TestAddOne(t *testing.T) {
	funcName := "addOne"
	stateID := ""
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldResemble, "1")
	})
}

// TestInitState checks delete state queue map.
func TestInitState(t *testing.T) {
	Convey("initState", t, func() {
		stateID := randomStateID()
		funcName := "initState"
		reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
		r := common.NewRequest(common.RequestOptions{
			FuncName:  funcName,
			ReqIDList: reqIDList,
			Config:    "",
		})
		testStateQueue.Request(&metadata.Metadata{StateID: stateID}, r, true)
		resp := r.GetResponse()
		So(resp.Code, ShouldEqual, common.InitStateErr)
		rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(rawState, ShouldEqual, "")
		So(err, ShouldNotBeNil)
	})
	Convey("initState Concurrency", t, func() {
		stateID := randomStateID()
		funcName := "initState"
		defer gomonkey.ApplyGlobalVar(&core.FunctionCheck, &proxy.Verification{
			GetStatefulFunctionConcurrentNum: func(tenantID, functionName, serviceID string) (int, error) {
				return 2, nil
			},
		}).Reset()
		reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
		r := common.NewRequest(common.RequestOptions{
			FuncName:  funcName,
			ReqIDList: reqIDList,
			Config:    "",
		})
		testStateQueue.Request(&metadata.Metadata{StateID: stateID}, r, true)
		resp := r.GetResponse()
		So(resp.Code, ShouldEqual, common.InitStateErr)
		rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(rawState, ShouldEqual, "")
		So(err, ShouldNotBeNil)
	})
}

// TestAddOneTerminate checks a simple add two function, and terminate it.
func TestAddOneTerminate(t *testing.T) {
	stateID := randomStateID()
	Convey("addOne", t, func() {
		res := trigger("addOne", "", stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "1")
		Convey("terminate", func() {
			reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0), common.NewReqID(nil, "", "", 0)}
			r := common.NewTerminateRequest(reqIDList, "")
			testStateQueue.Request(&metadata.Metadata{StateID: stateID}, r, false)
			r.GetResponse()
			rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
			So(rawState, ShouldEqual, "")
			So(err, ShouldNotBeNil)
			Convey("addOne", func() {
				res := trigger("addOne", "", stateID, false)
				So(res.Code, ShouldEqual, common.EStateIDNotFound)
				Convey("addOne", func() {
					res := trigger("addOne", "", stateID, true)
					So(res.Code, ShouldEqual, common.EOK)
					So(res.Result, ShouldEqual, "1")
				})
			})
		})
	})
}

// TestDeadlock checks a deadlock case.
func TestDeadlock(t *testing.T) {
	funcName := "addTwoDeadlock"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.ETimeout)
	})
}

// TestResolveDeadlock checks a deadlock case and tries to resolve it.
func TestResolveDeadlock(t *testing.T) {
	funcName := "addTwo"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "1")
	})
}

// TestResolveDeadlockSlowly checks a deadlock case and tries to resolve it slowly.
func TestResolveDeadlockSlowly(t *testing.T) {
	funcName := "addTwoAddOneSlowly"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "1")
	})
}

// TestDataRace1 checks a data race case.
func TestDataRace1(t *testing.T) {
	funcName := "addTwoAsyncAddOne1"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "1")
		rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s := unmarshalState(rawState)
		So(s.Count, ShouldEqual, 1)
	})
}

// TestDataRace2 checks a data race case.
func TestDataRace2(t *testing.T) {
	funcName := "addTwoAsyncAddOne2"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "1")
		rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s := unmarshalState(rawState)
		So(s.Count, ShouldEqual, 1)
		time.Sleep(2 * time.Second)
		rawState, err = testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s = unmarshalState(rawState)
		So(s.Count, ShouldEqual, 1)
	})
}

// TestDataRace3 checks a data race case.
func TestDataRace3(t *testing.T) {
	funcName := "addTwoAsyncAddOne3"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, "", stateID, true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "1")
		rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s := unmarshalState(rawState)
		So(s.Count, ShouldEqual, 1)
		time.Sleep(2 * time.Second)
		rawState, err = testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s = unmarshalState(rawState)
		So(s.Count, ShouldEqual, 1)
	})
}

// TestLargeTree checks a large tree case.
func TestLargeTree(t *testing.T) {
	funcName := "largeTree"
	stateID := randomStateID()
	Convey("Trigger function " + funcName + " with state id " + stateID, t, func() {
		res := trigger(funcName, stateID, "", true)
		So(res.Code, ShouldEqual, common.EOK)
		So(res.Result, ShouldEqual, "1,1")
		rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s := unmarshalState(rawState)
		So(s.Count, ShouldEqual, 1)
		time.Sleep(2 * time.Second)
		rawState, err = testStateQueue.GetState(&metadata.Metadata{StateID: stateID})
		So(err, ShouldBeNil)
		s = unmarshalState(rawState)
		So(s.Count, ShouldEqual, 1)
	})
}

func TestStateQueue_GetMetaData(t *testing.T) {
	type fields struct {
		g                 *Map
		mutex             *sync.RWMutex
		root              *reqnode.Node
		head              *reqnode.Node
		curReq            *reqnode.Node
		metadata          *metadata.Metadata
		deadlockResolvers map[string]*deadlockResolver
		state             string
		terminate         bool
		ttl               int
	}
	var f fields
	metadatas := &metadata.Metadata{}
	f.metadata = metadatas
	tests := []struct {
		name   string
		fields fields
		want   *metadata.Metadata
	}{
		{"case1", f, metadatas},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &StateQueue{
				g:                 tt.fields.g,
				mutex:             tt.fields.mutex,
				root:              tt.fields.root,
				head:              tt.fields.head,
				curReq:            tt.fields.curReq,
				metadata:          tt.fields.metadata,
				deadlockResolvers: tt.fields.deadlockResolvers,
				state:             tt.fields.state,
				terminate:         tt.fields.terminate,
				ttl:               tt.fields.ttl,
			}
			if got := q.GetMetaData(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetMetaData() = %v, want %v", got, tt.want)
			}
		})
	}
}
