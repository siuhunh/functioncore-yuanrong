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

package adaptor

import (
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/efs"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/statequeue"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// ReportFutureStoreDuration defines the time use.
const ReportFutureStoreDuration = 5 * time.Second

type methodInterface func(taskCtx taskcontext.Context, data *FuncArgs)

// Adaptor represents the adaptor that function core requires.
type Adaptor struct {
	FutureStore       *futurestore.FutureStore
	CallerFutureStore *futurestore.FutureStore
	StateQueueMap     *statequeue.Map
	Filter            *taskcontext.Filter
	CallerFilter      *taskcontext.Filter
	ExtFutureStore    efs.ExtFutureStore
	Methods           map[int32]methodInterface
}

// InitMethods initializes the adaptor methods corresponding to the different request type.
func (a *Adaptor) InitMethods() {
	a.Methods = make(map[int32]methodInterface)
	a.Methods[common.MethodInvoke] = a.invoke
	a.Methods[common.MethodTaskInvoke] = a.calleeInvoke
	a.Methods[common.MethodGetFuture] = a.getFuture
	a.Methods[common.MethodTaskGetFuture] = a.calleeGetFuture
	a.Methods[common.MethodTaskStartCheckAndResolveDeadlock] = a.checkerStart
	a.Methods[common.MethodTaskEndCheckAndResolveDeadlock] = a.checkerEnd
	a.Methods[common.MethodOnComplete] = a.onComplete
	a.Methods[common.MethodTerminate] = a.terminate
	a.Methods[common.MethodWait] = a.wait
	a.Methods[common.MethodSetTimeout] = a.setTimeout
	a.Methods[common.MethodReadState] = a.readState
	a.Methods[common.MethodSaveState] = a.saveState
	a.Methods[common.MethodFilter] = a.filter
	a.Methods[common.MethodTaskCreateStateQueue] = a.createStateQueue
	a.Methods[common.MethodGetInstance] = a.getInstance
	a.Filter = taskcontext.NewFilter()
	a.CallerFilter = taskcontext.NewFilter()
}

// CommonAdaptor is the common part of different implementations of adaptor.
func (a *Adaptor) CommonAdaptor(taskCtx taskcontext.Context, funcName string, event []byte, source int32) error {
	args := new(FuncArgs)
	err := proto.Unmarshal(event, args)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal, err: %s", err.Error())
		return err
	}
	// The field `Source` can be sent from runtime or function-task, if function-task, it must be
	// `common.SourceFunctionTaskToFunctionTask`. if runtime, it is also the same value, however, in this case
	// function-task needs to be distinguished from `common.SourceRuntimeToCentralFunctionTask` (used by local task)
	// or `common.SourceRuntimeToLocalFunctionTask`, so we set field `Source` to the correct value here.
	if args.Source != common.SourceFunctionTaskToFunctionTask {
		args.Source = source
	}
	taskCtx.SetSource(args.Source)
	taskCtx.SetReqIDList(args.ReqIDList)
	taskCtx.SetMethod(args.Method)
	if method, ok := a.Methods[args.Method]; ok {
		method(taskCtx, args)
		log.GetLogger().Infof("method: %s, serviceID: %s, tenantID: %s, traceID: %s, funcName: %s",
			common.TypeNames[args.Method], taskCtx.ServiceID(), taskCtx.TenantID(), taskCtx.TraceID(), funcName)
		return nil
	}
	log.GetLogger().Errorf("invalid method: %s, traceID: %s, funcName: %s", args.Method, taskCtx.TraceID(), funcName)
	return errors.New("invalid method")
}
