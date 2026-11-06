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

// Package model defines models of requests
package model

import (
	"sync/atomic"
	"time"
)

// FunctionRequestInfo function request info
type FunctionRequestInfo struct {
	URN                  string
	BusinessID           string
	TenantID             string
	Name                 string
	Version              string
	TraceID              string
	Alias                string
	AppID                string
	StateKey             string
	StateID              string
	TryCount             int
	HashMove             bool
	StartNode            string
	CurrentNode          string
	TargetNode           string
	TargetInstance       string
	FuseInstance         string
	BackPressure         bool
	AnonymizeURN         string
	InnerCode            int
	RequestCount         int
	StartTime            time.Time
	AllBusCost           time.Duration
	LastBusCost          time.Duration
	Deadline             time.Time
	BackPressureDeadline time.Time
	BusinessType         string
	WSReqInfo            WSRequestInfo
	EnableRemoteDebug    bool
	EnableWebSocket      bool

	RequestStreamName      string
	RequestStreamErrorCode int32
	ResponseStreamName     string
}

// WSRequestInfo websocket request info
type WSRequestInfo struct {
	FuncKeyWithRes string
	InstanceKey    string
	ProcessorID    uint32
	LeaseVersion   uint64
	DemandNodeIP   string
	DemandNodePort string
}

// SetRequestStreamErrorCode -
func (r *FunctionRequestInfo) SetRequestStreamErrorCode(errorCode int32) {
	atomic.StoreInt32(&r.RequestStreamErrorCode, errorCode)
}

// GetRequestStreamErrorCode -
func (r *FunctionRequestInfo) GetRequestStreamErrorCode() int32 {
	return atomic.LoadInt32(&r.RequestStreamErrorCode)
}
