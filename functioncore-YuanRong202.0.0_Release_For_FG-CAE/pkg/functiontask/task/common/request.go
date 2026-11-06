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

package common

import (
	"strconv"
	"sync"

	uuid "github.com/google/uuid"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/util/short"
)

// ReqID represents the request id.
type ReqID = string

// Params represents the invoke params.
type Params = string

// Result represents the invoke result.
type Result = string

// Request type
const (
	NormalRequest = iota
	TerminateRequest
	SetTimeoutRequest
)

// NewTraceID creates a new trace id.
func NewTraceID() ReqID {
	return uuid.New().String()
}

// AppendReqID adds a new request id.
func AppendReqID(reqIDList ReqIDList, funcName, version string, countOfFuncName int32) ReqIDList {
	reqID := NewReqID(reqIDList, funcName, version, countOfFuncName)
	reqIDList = append(reqIDList, reqID)
	return reqIDList
}

// NewReqID creates a new request id.
func NewReqID(reqIDList ReqIDList, funcName, version string, countOfFuncName int32) string {
	data := funcName + version + strconv.Itoa(int(countOfFuncName))
	if len(reqIDList) > 0 {
		data += reqIDList[len(reqIDList)-1]
	} else {
		data += uuid.New().String()
	}
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte(data)).String()
	log.GetLogger().Debugf("new request ID, reqIDList: %s, funcName: %s, version: %s, countOfFuncName: %d, id: %s",
		short.List(reqIDList), funcName, version, countOfFuncName, id)
	return id
}

// ReqIDList represents the request id list.
type ReqIDList = []ReqID

// Request represents the request in state queue.
type Request struct {
	mutex       *sync.Mutex
	response    *Response
	FutureID    string
	RequestType int8
	Config      interface{}
	FuncName    string
	ReqIDList   ReqIDList
	Params      Params
	ch          chan *Response
	TTL         int
}

// RequestOptions represents the options of creating request.
type RequestOptions struct {
	FutureID  string
	FuncName  string
	ReqIDList ReqIDList
	Params    Params
	Config    interface{}
}

// NewRequest returns a new request.
func NewRequest(opts RequestOptions) *Request {
	return &Request{
		FutureID:    opts.FutureID,
		RequestType: NormalRequest,
		Config:      opts.Config,
		FuncName:    opts.FuncName,
		ReqIDList:   opts.ReqIDList,
		Params:      opts.Params,
		ch:          make(chan *Response, 1),
		mutex:       new(sync.Mutex),
	}
}

// TerminateFuncName represents the function name of terminate.
const TerminateFuncName = "$terminate$"

// NewTerminateRequest returns a terminated request.
func NewTerminateRequest(ReqIDList ReqIDList, futureID string) *Request {
	return &Request{
		FutureID:    futureID,
		FuncName:    TerminateFuncName,
		RequestType: TerminateRequest,
		ReqIDList:   ReqIDList,
		ch:          make(chan *Response, 1),
		mutex:       new(sync.Mutex),
	}
}

// SetTimeoutFuncName represents the function name of setTimeout.
const SetTimeoutFuncName = "$setTimeout$"

// NewSetTimeoutRequest returns a setTimeout request.
func NewSetTimeoutRequest(ttl int, futureID string) *Request {
	return &Request{
		FutureID:    futureID,
		FuncName:    SetTimeoutFuncName,
		RequestType: SetTimeoutRequest,
		// create new ReqIDList, make sure SetTimeoutRequest always belong to root children
		ReqIDList: []ReqID{NewTraceID()},
		ch:        make(chan *Response, 1),
		mutex:     new(sync.Mutex),
		TTL:       ttl,
	}
}

// GetResponse blocking get the response of the request.
func (r *Request) GetResponse() *Response {
	r.mutex.Lock()
	if r.response == nil {
		r.response = <-r.ch
	}
	r.mutex.Unlock()
	return r.response
}

// SetResponse set the response of the request.
func (r *Request) SetResponse(result *Response) {
	if cb != nil {
		cb(result)
	}
	r.ch <- result
}

var cb func(result *Response)

// SetRequestMonitor sets monitor for request.
func SetRequestMonitor(callback func(result *Response)) {
	cb = callback
}
