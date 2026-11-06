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

// Package schedulerapi container scheduler api define
package schedulerapi

const (
	// NormalRequest normal request type sent to faas-scheduler
	NormalRequest = 0
	// WebSocketRequest websocket creation request type
	WebSocketRequest = 1
	// WebsocketHTTPRequest websocket function instance acquiring request and using http transport
	WebsocketHTTPRequest = 2
)

// Error scheduler error
type Error struct {
	ErrorCode    uint32 `json:"code" valid:",optional"`
	ErrorMessage string `json:"message" valid:",optional"`
}

// CreateWebSocketConnectionReq send request to worker to build websocket connection with rdispatcher
type CreateWebSocketConnectionReq struct {
	FunctionKey     string `json:"functionKey,omitempty" valid:",optional"`
	WebSocketConfig WebSocketInsAcquireConfig
}

// WebSocketInsAcquireConfig label which rdispatcher are asking for the websocket connection
type WebSocketInsAcquireConfig struct {
	// rdispatcher ip
	TargetIP string `json:"instanceIP,omitempty" valid:",optional"`
	// rdispatcher port
	TargetPort string `json:"instancePort,omitempty" valid:",optional"`
	// websocket request body including path and so on
	RequestBody map[string]any `json:"requestBody,omitempty" valid:",optional"`
	// websocket request path
	Path        string `json:"path,omitempty" valid:",optional"`
	Concurrency uint32 `json:"concurrency,omitempty" valid:",optional"`
}

// WebsocketInsInfo generated websocket function instance info
type WebsocketInsInfo struct {
	PodName     string
	PodIP       string
	PodPort     string
	NodeIP      string
	NodePort    string
	ProcessorID uint32
	Error       Error
}

// AcquireInstanceReq acquire instance request
type AcquireInstanceReq struct {
	TraceID                   string
	FuncKeyWithRes            string
	Timeout                   string
	ReqType                   uint32
	WebSocketInsAcquireConfig WebSocketInsAcquireConfig
}

// AcquireInstanceRsp acquire instance response
// for websocket scenario InstanceIP and InstancePort should be returned as response Body
type AcquireInstanceRsp struct {
	InstanceKey     string                 `json:"instanceKey" valid:",optional"`
	ProcessorID     uint32                 `json:"processorID" valid:",optional"`
	RestThread      uint32                 `json:"restThread" valid:",optional"`
	ThreadNum       uint32                 `json:"-"`
	PlaceHolderChan chan *WebsocketInsInfo `json:"-"`
	LeaseVersion    uint64                 `json:"leaseVersion" valid:",optional"`
	InstanceIP      string                 `json:"instanceIP" valid:",optional"`
	InstancePort    string                 `json:"instancePort" valid:",optional"`
	NodeIP          string                 `json:"nodeIP" valid:",optional"`
	NodePort        string                 `json:"nodePort" valid:",optional"`
	Error           Error                  `json:"error,omitempty" valid:",optional"`
}

// ReleaseInstanceReq release instance request
type ReleaseInstanceReq struct {
	TraceID        string
	FuncKeyWithRes string
	InstanceKey    string
	ProcessorID    uint32
	LeaseVersion   uint64
	IsWebSocketReq bool
}

// ReleaseInstanceReqFromRdispatcher release instance request from caller
type ReleaseInstanceReqFromRdispatcher struct {
	InstanceKey string `json:"instanceKey,omitempty" valid:",optional"`
	ProcessorID uint32 `json:"processorID,omitempty" valid:",optional"`
}

// ReleaseInstanceRsp release instance response
type ReleaseInstanceRsp struct{}

// RetainInstanceReq retain instance response
type RetainInstanceReq struct {
	TraceID        string
	FuncKeyWithRes string
	InstanceKey    string
	ProcessorID    uint32
	LeaseVersion   uint64
}

// RecoverInstancesRes recover instances response
type RecoverInstancesRes struct {
	InstancesRes map[string]*CheckInstanceReq `json:"instancesRes,omitempty" valid:",optional"`
}

// CheckInstanceReq check instance request
type CheckInstanceReq struct {
	FuncKeyWithRes     string                      `json:"funcKeyWithRes,omitempty" valid:",optional"`
	InstancesStatusMap map[string]*InstancesStatus `json:"instancesStatusMap,omitempty" valid:",optional"`
}

// InstancesStatus process status
type InstancesStatus struct {
	InstanceKey        string                    `json:"instanceKey,omitempty" valid:",optional"`
	ProcessesStatusMap map[uint32]*ProcessStatus `json:"processesStatusMap,omitempty" valid:",optional"`
}

// ProcessStatus process status
type ProcessStatus struct {
	ProcessorID  uint32 `json:"processorID,omitempty" valid:",optional"`
	RestThread   uint32 `json:"restThread,omitempty" valid:",optional"`
	LeaseVersion uint64 `json:"leaseVersion,omitempty" valid:",optional"`
}

// RetainInstanceRsp retain instance response
type RetainInstanceRsp struct {
	Error Error
}

// WorkerWssCountResponse response from worker saying how many websocket request is running
type WorkerWssCountResponse struct {
	TotalConnNum uint32 `json:"totalConnNum" valid:",optional"`
	WssConnNum   uint32 `json:"wssConnNum" valid:",optional"`
}

// Scheduler scheduler interface
type Scheduler interface {
	AcquireInstance(req *AcquireInstanceReq) (*AcquireInstanceRsp, error)
	ReleaseInstance(req *ReleaseInstanceReq) error
	RetainInstance(req *RetainInstanceReq) (*RetainInstanceRsp, error)
	CheckInstance(req *CheckInstanceReq) error
}
