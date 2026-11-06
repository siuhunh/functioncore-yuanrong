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

// Package service implements service api.
package service

import (
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/terminate"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/task/util"
)

// GetAllStateID returns all state id starts with specific service id.
// notice:
//     1. no need to truncate Scan results anymore. now return stateID rather than metaKey(tenantId-serviceID-stateID)
//     2. no need to rescan stateID again when deleting, as all stateID's Adding is after stateIDHash's Adding
//        and stateID's deleting are after stateIDHash's deleting,
func GetAllStateID(taskCtx taskcontext.Context) (map[string]string, common.TaskError) {
	ptn := metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, "*")
	log.GetLogger().Infof("readyToScan, pattern: %s", ptn)

	nums, err := taskCtx.StateClient().GetNumsByServiceID(ptn)
	if err != nil {
		log.GetLogger().Errorf("get nums by service id error: %s", err.Error())
		return nil, err
	}
	if nums > config.Conf.MaxNumsOfStateID {
		log.GetLogger().Errorf("nums of stateID exceed max limit: %d", config.Conf.MaxNumsOfStateID)
		return nil, common.NewTaskError(common.ETooManyStateInService, util.ErrInfoOfTooManyState(nums))
	}
	idMap, err := taskCtx.StateClient().Scan(ptn)
	if err != nil {
		log.GetLogger().Errorf("scan: %s", err.Error())
		return nil, err
	}
	cnt := len(idMap)
	log.GetLogger().Debugf("scanResult: %d", cnt)

	return idMap, nil
}

// ClearAllState clear all state according to specific service id.
func ClearAllState(taskCtx taskcontext.Context) common.TaskError {
	idMap, err := GetAllStateID(taskCtx)
	if err != nil {
		return err
	}
	log.GetLogger().Infof("readyToClear, count: %d", idMap)

	return Assigner(taskCtx, idMap, clearOneWithRetry)
}

type worker func(taskcontext.Context, string) common.TaskError

// Assigner assigns task to worker.
// notice: even more than one error happens, only return the error first found.
func Assigner(taskCtx taskcontext.Context, idMap map[string]string, wkr worker) common.TaskError {
	errCh := make(chan common.TaskError, 1)
	wg := &sync.WaitGroup{}

	for k := range idMap {
		wg.Add(1)
		go func(ctx taskcontext.Context, id string) {
			err := wkr(ctx, id)

			if err != nil {
				postError(errCh, err)
				log.GetLogger().Errorf("clearState, code: %d, err: %s", err.Code(), err.Error())
			}
			wg.Done()
		}(taskCtx, k)
	}
	go func() {
		wg.Wait()
		postError(errCh, nil)
	}()
	err := <-errCh
	if err != nil {
		return common.NewTaskError(err.Code(), err.Error())
	}
	return nil
}

func postError(ch chan common.TaskError, err common.TaskError) {
	if ch == nil {
		return
	}
	select {
	case err1, ok := <-ch:
		if ok {
			// unless all goroutine works done correctly, err1 must be not nil.
			ch <- err1
		}
	default:
		ch <- err
	}
}

const maxRetryTimes = 3
const intervalRetryTimes = 3 * time.Second

func clearOneWithRetry(taskCtx taskcontext.Context, id string) common.TaskError {
	var err common.TaskError
	for i := 0; i < maxRetryTimes; i++ {
		err = clearOne(taskCtx, id)
		if err == nil {
			break
		}
		log.GetLogger().Errorf("clearOne, retry: %d, id: %s, code: %d, errMsg: %s", i+1, id, err.Code(), err.Error())
		time.Sleep(intervalRetryTimes)
	}
	return err
}

func clearOne(taskCtx taskcontext.Context, id string) common.TaskError {
	taskCtx = taskCtx.Clone()
	taskCtx.SetMethod(common.MethodTerminate)
	taskCtx.SetReqIDList(append(taskCtx.ReqIDList(), common.NewTraceID()))

	terminateReq := new(terminate.Request)
	terminateReq.StateID = id
	rawResp := terminateReq.Caller(taskCtx)
	if rawResp.GetCode() != common.EOK {
		return checkErr(taskCtx, id, rawResp.GetCode(), rawResp.GetMessage())
	}
	resp, ok := rawResp.(*terminate.Response)
	if !ok {
		return checkErr(taskCtx, id, resp.Code, resp.Message)
	}
	f := taskCtx.FutureStore().Get(resp.FutureID)
	if f == nil {
		return common.NewTaskError(common.EUnknownFutureID, "unknown future id: "+resp.FutureID)
	}
	getResp := f.GetResponse()
	if getResp.Code != common.EOK {
		return common.NewTaskError(getResp.Code, getResp.Message)
	}
	return nil
}

func checkErr(taskCtx taskcontext.Context, id string, code int32, message string) common.TaskError {
	if code == common.ScheduleInsErr || code == common.EStateIDNotFound {
		log.GetLogger().Warnf("clearOne, warning: %s", message)
		err := taskCtx.StateClient().DeleteState(
			metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, id), "")
		if err != nil && taskCtx.StateClient().IsNotExist(err) {
			return nil
		}
		return err
	}
	if code != common.EOK {
		return common.NewTaskError(code, message)
	}
	return nil
}
