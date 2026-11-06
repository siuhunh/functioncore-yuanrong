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

// Package waitfuture implements wait api.
package waitfuture

import (
	"fmt"
	"strings"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

const defaultCap = 6

func waitSingleFuture(sncFuture *snfuture.Future, futureListChan chan<- string, waitNum int32, doneCh chan string) {
	sncFuture.GetResponse()
	futureListChan <- sncFuture.ID
	log.GetLogger().Debugf("futureDone, waitNum: %d, received: %d, futureID: %s", waitNum,
		len(futureListChan), sncFuture.ID)
	// futureListChan will be consumed when doneCh is filled or timeout, so there is no concurrency problems.
	if len(futureListChan) >= int(waitNum) && doneCh != nil {
		doneCh <- ""
	}
}

func getKFutureIDs(waitNum int32, futureListChan chan string) []string {
	log.GetLogger().Debugf("get FutureIDs, waitNum: %d, received: %d", waitNum, len(futureListChan))
	futureIDs := make([]string, 0, configureCapacity(waitNum))
	if futureListChan == nil {
		return futureIDs
	}
	for i := int32(0); i < waitNum; i++ {
		select {
		case futureID := <-futureListChan:
			futureIDs = append(futureIDs, futureID)
		}
	}
	return futureIDs
}

func configureCapacity(cnt int32) int32 {
	if cnt <= 0 {
		return defaultCap
	}
	if cnt < config.Conf.MaxSingleWaitFutureCnt {
		return cnt
	}
	return config.Conf.MaxSingleWaitFutureCnt
}

func getResolvedFutureIDs(waitNum int32, futureListChan chan string) []string {
	log.GetLogger().Debugf("get Resolved FutureIDs, waitNum: %d, received: %d", waitNum, len(futureListChan))
	futureIDs := make([]string, 0, configureCapacity(waitNum))
	if futureListChan == nil {
		return futureIDs
	}
	end := false
	for i := int32(0); i < waitNum; i++ {
		select {
		case futureID, ok := <-futureListChan:
			if !ok {
				log.GetLogger().Warn("future list channel is closed")
				break
			}
			futureIDs = append(futureIDs, futureID)
		default:
			end = true
		}
		if end {
			break
		}
	}
	return futureIDs
}

// Wait for the futures.
func Wait(taskCtx taskcontext.Context, req *TransformedRequest, checker deadlock.Checker) *Response {
	cnt := int32(len(req.FutureIDs))
	log.GetLogger().Infof("wait, futureIDs: %s, traceID: %s, cnt: %d, waitNum: %d, timeout: %d",
		strings.Join(req.FutureIDs, ","), taskCtx.TraceID(), cnt, req.WaitNum, req.Timeout)

	if cnt == 0 || req.WaitNum == 0 {
		return &Response{
			Code:      common.EOK,
			FutureIDs: []string{},
			State:     req.State,
		}
	}
	if cnt > config.Conf.MaxSingleWaitFutureCnt {
		log.GetLogger().Warnf("wait for too many futures, waitNum: %d, traceID: %s, countOfFutureIDs: %d",
			req.WaitNum, taskCtx.TraceID(), cnt)
		return &Response{
			Code: common.EInvalidParam,
			Message: fmt.Sprintf("invalid future count, expect <= %d, actual %d",
				config.Conf.MaxSingleWaitFutureCnt, cnt),
			FutureIDs: []string{},
		}
	}
	if req.WaitNum < 0 || req.WaitNum > cnt {
		log.GetLogger().Warnf("invalid waitNum, traceID: %s, waitNum: %d, countOfFutureIDs: %d", taskCtx.TraceID(),
			req.WaitNum, cnt)
		return &Response{
			Code:      common.EInvalidParam,
			Message:   fmt.Sprintf("invalid waitNum, expect 0 <= waitNum <= %d, actual %d", cnt, req.WaitNum),
			FutureIDs: []string{},
		}
	}
	return wait(taskCtx, req, checker, cnt)
}

func wait(taskCtx taskcontext.Context, req *TransformedRequest, checker deadlock.Checker, cnt int32) *Response {
	futureListChan := make(chan string, cnt)
	doneCh := make(chan string, cnt)
	futures := make([]*snfuture.Future, cnt, configureCapacity(cnt))
	futureReqIDLists := make([]common.ReqIDList, cnt, configureCapacity(cnt))
	// check that all future exists.
	for i, futureID := range req.FutureIDs {
		sncFuture := util.GetFuture(taskCtx, futureID)
		if sncFuture == nil {
			return &Response{
				Code:      common.EInvalidParam,
				Message:   "invalid future id: " + futureID,
				FutureIDs: []string{},
			}
		}
		futures[i] = sncFuture
		futureReqIDLists[i] = sncFuture.ReqIDList
	}
	var startResp *deadlock.StartResponse
	if config.Conf.ResolveDeadlock && req.StateID != "" {
		startResp = checker.Start(&deadlock.StartRequest{
			TenantID: taskCtx.TenantID(), ServiceID: taskCtx.ServiceID(),
			StateID: req.StateID, State: req.State,
			FutureReqIDLists: deadlock.ConvertLists2Struct(futureReqIDLists),
		})
		if startResp.Code != common.EOK {
			return &Response{
				Code:      startResp.Code,
				Message:   startResp.Message,
				FutureIDs: []string{},
			}
		}
	}
	for _, sncFuture := range futures {
		go waitSingleFuture(sncFuture, futureListChan, req.WaitNum, doneCh)
	}
	if req.Timeout > 0 {
		log.GetLogger().Debugf("wait, timeout: %d", req.Timeout)
		select {
		case <-time.After(req.Timeout):
			futureIDs := getResolvedFutureIDs(req.WaitNum, futureListChan)
			return end(taskCtx, req.StateID, checker, startResp, futureIDs)
		case _, ok := <-doneCh:
			if !ok {
				log.GetLogger().Debugf("doneCh closed and has no data")
			}
		}
	}
	futureIDs := getKFutureIDs(req.WaitNum, futureListChan)
	return end(taskCtx, req.StateID, checker, startResp, futureIDs)
}

func end(taskCtx taskcontext.Context, stateID string,
	checker deadlock.Checker, resp *deadlock.StartResponse, futureIDs []string) *Response {
	updateState := false
	state := ""
	if config.Conf.ResolveDeadlock && stateID != "" && resp.DeadlockID != "" {
		endResp := checker.End(&deadlock.EndRequest{
			TenantID:   taskCtx.TenantID(),
			ServiceID:  taskCtx.ServiceID(),
			StateID:    stateID,
			DeadlockID: resp.DeadlockID,
		})
		if endResp.Code != common.EOK {
			log.GetLogger().Errorf("getResolvedFutureIDs.EndCheckAndResolveDeadlock, code: %d, msg: %s",
				endResp.GetCode(), endResp.GetMessage())
			return &Response{
				Code:      endResp.Code,
				Message:   endResp.Message,
				FutureIDs: []string{},
			}
		}
		updateState = true
		state = endResp.State
	}
	// return the first waitNum futures even if we received more than waitNum futures in time.
	return &Response{
		Code:        common.EOK,
		FutureIDs:   futureIDs,
		State:       state,
		UpdateState: updateState,
	}
}
