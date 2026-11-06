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

// package samenode -
package samenode

import (
	"fmt"
	"reflect"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	commonLog "functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/efs"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/statequeue"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/task/util/short"
)

type MockContext struct {
	stateQueueMap     *statequeue.Map
	FormatLogger      commonLog.FormatLogger
	reqIDList         common.ReqIDList
	callerFutureStore *futurestore.FutureStore
	CalleeFutureStore *futurestore.FutureStore
	filter            *taskcontext.Filter
	callerFilter      *taskcontext.Filter
	source            int32
	method            int32
	extFutureStore    efs.ExtFutureStore
	serviceID         string
	creator           string
	createOptions     *types.StateCreateOptions
	stateKey          string
}

func (c MockContext) SetStateKey(key string) {
	c.stateKey = key
}

func (c MockContext) GetStateKey() string {
	return c.stateKey
}

func (c MockContext) IsNewStateKeyScenario() bool {
	return false
}

func (c MockContext) Logger() commonLog.FormatLogger {
	return c.FormatLogger
}

func (c MockContext) WithLogFields(fields ...zap.Field) {
	c.FormatLogger = c.FormatLogger.With(fields...)
}

func (c MockContext) AddLogFieldCategory(category string) {
	c.FormatLogger = c.FormatLogger.With(zap.String("C", category))
}

func (c MockContext) AddLogFieldReqIDList(reqIDList string) {
	c.FormatLogger = c.FormatLogger.With(zap.String("reqIDList", reqIDList))
}

func (c MockContext) SetReqIDList(reqIDList common.ReqIDList) {
	c.reqIDList = reqIDList
}

func (c MockContext) ReqIDList() common.ReqIDList {
	return c.reqIDList
}

func (c MockContext) ShortReqIDList() string {
	return short.List(c.reqIDList)
}

func (c MockContext) ReqID() string {
	return c.reqIDList[len(c.reqIDList)-1]
}

func (c MockContext) TraceID() string {
	if len(c.reqIDList) > 0 {
		return c.reqIDList[0]
	}
	return ""
}

func (c MockContext) SetCreator(creator string) {
	c.creator = creator
}

func (c MockContext) GetCreator() string {
	return c.creator
}

func (c MockContext) SetStateCreateOptions(createOptions *types.StateCreateOptions) {
	c.createOptions = createOptions
}

func (c MockContext) GetStateCreateOptions() *types.StateCreateOptions {
	return c.createOptions
}

func (c MockContext) StateQueueMap() *statequeue.Map {
	return c.stateQueueMap
}

func (c MockContext) Invoker() common.Invoker {
	return c.stateQueueMap.Invoker
}

func (c MockContext) StateClient() stateclient.StateClient {
	return c.stateQueueMap.StateClient
}

func (c MockContext) SetStateQueue(queue *statequeue.Map) {
	c.stateQueueMap = queue
}

func (c MockContext) SetFutureStore(caller *futurestore.FutureStore, callee *futurestore.FutureStore) {
	c.callerFutureStore = caller
	c.CalleeFutureStore = callee
}

func (c MockContext) FutureStore() *futurestore.FutureStore {
	if c.source == common.SourceRuntimeToLocalFunctionTask {
		return c.callerFutureStore
	}
	return c.CalleeFutureStore
}

func (c MockContext) SetFilter(caller *taskcontext.Filter, callee *taskcontext.Filter) {
	c.callerFilter = caller
	c.filter = callee
}

func (c MockContext) Filter() *taskcontext.Filter {
	if c.source == common.SourceRuntimeToLocalFunctionTask {
		return c.callerFilter
	}
	return c.filter
}

func (c MockContext) SetSource(source int32) {
	c.source = source
}

func (c MockContext) SetMethod(method int32) {
	c.method = method
}

func (c MockContext) Source() int32 {
	return c.source
}

func genCategory(source int32, method int32) string {
	if source == common.SourceRuntimeToCentralFunctionTask {
		return common.TypeNames[method]
	}
	if source == common.SourceRuntimeToLocalFunctionTask {
		return common.TypeNames[method] + ".caller"
	}
	return common.TypeNames[method] + ".callee"
}

func (c MockContext) Category() string {
	return genCategory(c.source, c.method)
}

func (c MockContext) SetExtFutureStore(extFutureStore efs.ExtFutureStore) {
	c.extFutureStore = extFutureStore
}

func (c MockContext) EFS() efs.ExtFutureStore {
	return c.extFutureStore
}

func (c MockContext) SetServiceID(serviceID string) {
	c.serviceID = serviceID
}

func (c MockContext) ServiceID() string {
	return c.serviceID
}

func (c MockContext) CallTask(method int32, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
	fmt.Println("")
}

func (c MockContext) CreateState(funcName string, stateID string) (string, string, common.TaskError) {
	return "", "", nil
}

func (c MockContext) InvokeConfig(version string, callKernel bool, futureID string, requestAttributes map[string]int32) interface{} {
	return reflect.Interface
}

func (c MockContext) SetResult(resultStruct taskcontext.Serializable) {
	fmt.Println("")
}

func (c MockContext) SetResultWithLog(resultStruct taskcontext.Serializable, log string) {
	fmt.Println("")
}

func (c MockContext) SetResultWithCode(code int32, resultStruct interface{}, log string) {
	fmt.Println("")
}

func (c MockContext) IsSameNode(stateID string) bool {
	return false
}

func (c MockContext) Clone() taskcontext.Context {
	return c
}

func (c MockContext) GetFuncStateKey(funcName string, clientInstance bgtypes.ClientInstance) (string, common.TaskError) {
	return "", nil
}

func (c MockContext) Release(funcName string, clientInstance bgtypes.ClientInstance) (bool, common.TaskError) {
	return false, nil
}

func (MockContext) TenantID() string {
	return ""
}

func (MockContext) SetIsGetInstanceScenario(flag bool) {
	return
}

func (MockContext) IsGetInstanceScenario() bool {
	return false
}

type MockRequestWithStateID struct {
}

func (m *MockRequestWithStateID) GetStateID() string {
	return ""
}

type MockSerializable struct {
	code    int32
	Message string
	fields  []zap.Field
}

func (m *MockSerializable) Marshal() ([]byte, error) {
	return []byte(""), nil
}

func (m *MockSerializable) Unmarshal(bytes []byte) error {
	return nil
}

func (m *MockSerializable) GetCode() int32 {
	return m.code
}

func (m *MockSerializable) GetMessage() string {
	return m.Message
}

func (m *MockSerializable) SetCode(i int32) {
	m.code = i
}

func (m *MockSerializable) SetMessage(s string) {
	m.Message = s
}

func (m *MockSerializable) Fields() []zap.Field {
	return m.fields
}

func Test_initSpecialReqMap(t *testing.T) {
	Convey("Test_initSpecialReqMap", t, func() {
		initSpecialReqMap()
		So(specialReqMap[reflect.TypeOf((*deadlock.StartRequest)(nil))], ShouldNotEqual, nil)
		So(specialReqMap[reflect.TypeOf((*deadlock.EndRequest)(nil))], ShouldNotEqual, nil)
		So(specialReqMap[reflect.TypeOf((*invoke.TaskRequest)(nil))], ShouldNotEqual, nil)
	})
}

func Test_doCommonRequest(t *testing.T) {
	Convey("Test_doCommonRequest unsupported task request method", t, func() {
		initCommonReqMap()
		c := MockContext{}
		req := MockRequestWithStateID{}
		resp := MockSerializable{}
		doCommonRequest(c, &req, &resp)
		So(resp.GetCode(), ShouldEqual, common.EFunctionTask)
	})

	Convey("Test_doCommonRequest unsupported task request", t, func() {
		initCommonReqMap()
		c := MockContext{}
		req := MockRequestWithStateID{}
		resp := MockSerializable{}
		commonReqMap[reflect.TypeOf((*MockRequestWithStateID)(nil))] = "mockSerializable"
		doCommonRequest(c, &req, &resp)
		So(resp.GetCode(), ShouldEqual, common.EFunctionTask)
	})
}
