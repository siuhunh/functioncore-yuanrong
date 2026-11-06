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

// Package types -
package types

import (
	"context"
	"time"
)

// TCPRequest contains users request
type TCPRequest struct {
	LogType            string
	RawData            []byte
	RequestID          uint64
	UserData           map[string]string
	Extra              map[string]string
	WorkerID           string
	Priority           int
	DirectRequest      bool
	CanBackpressure    bool
	IsStateFul         bool
	CPU                string
	Memory             string
	Concurrency        uint32
	TraceCtx           context.Context
	RetryFlag          bool
	PersistResponse    bool
	StateID            string
	WorkflowID         string
	WorkflowStateID    string
	WorkflowRunID      string
	BackpressureNodeID string
	BackPressureNums   uint64
	BusinessType       string
	// key: instance name
	UsedInstance        map[string]struct{}
	IsInitialConcurrent bool
	// used for invoke specified instance
	InstanceID          string
	IsInvokeAll         bool
	RemoteScheduleFlag  bool
	ScheduleInstanceKey string
	ExtraMap            map[string]string
	MetricsErrSaved     bool
}

// TCPResponse contains users response
type TCPResponse struct {
	ErrorCode           uint32
	ErrorMessage        string
	DcCallStack         []byte
	RawData             []byte
	Logs                string
	RequestID           uint64
	Summary             string
	FunctionKey         string
	InstanceID          string
	NodeID              string
	Labels              map[string]string
	ActualCallStartTime time.Time
	ActualCallCost      time.Duration
	CallInstanceID      string
	CallNode            string
	MetricsErrSaved     bool
	CffHTTPResponse     bool
}

// Position position where to do requests
type Position struct {
	InstanceID string
	NodeID     string
}

// DispatcherRequest call request struct
type DispatcherRequest struct {
	Request     *TCPRequest
	Response    chan *TCPResponse
	Position    Position
	FunctionKey string
	TraceID     string
	NodeLabel   string
	FutureID    string
	Canceled    bool
	WorkerID    string
}
