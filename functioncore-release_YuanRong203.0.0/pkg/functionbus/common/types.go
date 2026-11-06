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

// Package common types is used for types defined
package common

import (
	"context"
	"time"

	"functioncore/pkg/common/types"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/functionbus/bus/tcpbustool"
)

// ETCDConnectHandler is the handler of etcd client connection failure
type ETCDConnectHandler func(bool)

// BusType is the type of bus: channel bus or tcp bus current
type BusType byte

// ChannelBusType or TCPBusType is the type of bus when creating bus
const (
	ChannelBusType BusType = iota
	TCPBusType
)

// BusOption option for newing TCPBus or ChannelBus
type BusOption struct {
	NewBusOpt interface{}
}

// msgType is the type of message
type messageType byte

// request message type, response message type and register message type
const (
	ReqMsg messageType = iota
	RspMsg
	RegMsg
)

// NotifyType notify remote worker event type
const (
	ChangeState = iota
)

const (
	percent = 100
)

// ChannelBusMessage channel bus communication message struct
type ChannelBusMessage struct {
	Property  *FunctionProperty
	Request   *types.TCPRequest
	Response  *types.TCPResponse
	PacketID  string // uuid 36 byte
	MsgType   messageType
	ErrorCode uint32
	ErrorMsg  string
	TraceID   string
}

// ReqResult return request result struct
type ReqResult struct {
	Response  *types.TCPResponse
	ErrorCode uint32
	ErrorMsg  string
}

// NotifyEvent notify remote worker event
type NotifyEvent struct {
	NotifyType          int
	Enable              bool
	FuncKeyWithResource string
	WorkerAddress       string
	WorkerOwnerIP       string
	IsDirectFunc        bool
	HasInitializer      bool
	BusinessType        string
	PodUID              string
	ContainerIDs        vpa.ContainerInfo
	Resource            vpa.PodResourceInfo
	ViewResource        vpa.ViewResourceInfo
}

// WorkerInfo -
type WorkerInfo struct {
	FuncKeyWithResource string               `json:"funcKeyWithResource"`
	WorkerAddress       string               `json:"workerAddress"`
	OwnerIP             string               `json:"ownerIP"`
	IsDirectFunc        bool                 `json:"isDirectFunc"`
	HasInitializer      bool                 `json:"hasInitializer"`
	BusinessType        string               `json:"businessType,omitempty"`
	PodUID              string               `json:"podUID,omitempty"`
	ContainerIDs        vpa.ContainerInfo    `json:"containerIDs,omitempty"`
	Resource            vpa.PodResourceInfo  `json:"resource,omitempty"`
	ViewResource        vpa.ViewResourceInfo `json:"viewResource,omitempty"`
}

// BusReceiver is implemented by the upper layer, process received request and registry message
type BusReceiver interface {
	HandleRequest(msg *ChannelBusMessage) ReqResult
}

// KeepAliveObserver subject for updating keepalive time after retrieving data from redis
type KeepAliveObserver interface {
	UpdateKeepAliveTime(keepAliveMap map[string]string)
}

// ChannelBusOption option for newing ChannelBus
type ChannelBusOption struct {
	// Receiver process received request message
	Receiver BusReceiver
	// IsProxy is proxy or task, true: proxy, false: task
	IsProxy bool
}

// Call function definition
type Call func(ctx context.Context, dst *FunctionProperty, req *types.TCPRequest) error

// AsyncCall -
type AsyncCall func(ctx context.Context, dst *FunctionProperty, req *types.TCPRequest)

// Cancel function definition
type Cancel func(ctx context.Context, dst *CancelProperty, req *types.TCPRequest) error

// Recover function definition
type Recover func(req *types.TCPRequest, commonBus interface{}) error

// PauseRuntime function definition
type PauseRuntime func() error

// RecoverRuntime function definition
type RecoverRuntime func(restartRuntime bool) error

// CheckRuntime function definition
type CheckRuntime func(traceID string, shouldWait bool) ([]byte, error)

// SendCallReq function definition
var SendCallReq func(nodeID, traceID string, callReq *types.TCPRequest, dst *FunctionProperty) *types.TCPResponse

// SendRecoverReq function definition
var SendRecoverReq func(nodeID, traceID string, req *types.TCPRequest) *types.TCPResponse

// SendCancelReq function definition
var SendCancelReq func(nodeID, traceID string, req *types.TCPRequest, dst *CancelProperty) *types.TCPResponse

// NotifyRemote function definition
var NotifyRemote func(remoteIP string, dst *FunctionProperty, event *NotifyEvent) error

// ReportRuntimeMetrics function definition
type ReportRuntimeMetrics func(runtimeMetrics RuntimeMetricsProperty) error

// StoreResourceUsage function definition
type StoreResourceUsage func(resourceUsage ResourceUsage)

// StoreCaeMetrics store cae metrics
type StoreCaeMetrics func(metrics CaeMetrics)

// ReportBridgeFuncMetrics report bridgeFunc metrics
type ReportBridgeFuncMetrics func(metrics BridgeFuncMetrics)

// FunctionProperty identifies the function instance
type FunctionProperty struct {
	TenantID        string
	FunctionName    string
	FunctionVersion string
	InstanceID      string
	StateID         string
	NodeLabel       string
	FutureID        string
}

// CancelProperty defines the cancel info
type CancelProperty struct {
	FutureID  string `json:"futureID"`
	Force     bool   `json:"force"`
	Recursive bool   `json:"recursive"`
}

// RuntimeMetricsProperty defines the runtime metrics
type RuntimeMetricsProperty struct {
	FunctionURN               string
	PodName                   string
	MetricsName               string
	MetricsValue              string
	GPUMem                    int64
	EphemeralStorage          int64
	IsTriggeredBridgeFunction bool
	IsBridgeFunction          bool
}

// ResourceUsage resource usage
type ResourceUsage struct {
	FunctionKey                   string
	PodName                       string
	CPUUsage                      float64
	MemoryUsage                   float64
	CPULimit                      int64
	MemoryLimit                   int64
	ReportTime                    int64
	StartTime                     int64
	ReceiveBytesPerSecond         float64
	ReceivePackagePerSecond       float64
	TransmitBytesPerSecond        float64
	TransmitPackagePerSecond      float64
	FileSystemReadBytesPerSecond  float64
	FileSystemWriteBytesPerSecond float64
	CustomReportMetrics           tcpbustool.CustomReportMetrics
}

// AvgCaeMetrics avg cae metrics
type AvgCaeMetrics struct {
	ReceiveBytesPerSecond         float64
	ReceivePackagePerSecond       float64
	TransmitBytesPerSecond        float64
	TransmitPackagePerSecond      float64
	FileSystemReadBytesPerSecond  float64
	FileSystemWriteBytesPerSecond float64
}

// CaeMetrics cae metrics
type CaeMetrics struct {
	FunctionKey                   string
	PodName                       string
	CPUUsageRate                  float64
	MemoryUsageRate               float64
	ReceiveBytesPerSecond         float64
	ReceivePackagePerSecond       float64
	TransmitBytesPerSecond        float64
	TransmitPackagePerSecond      float64
	FileSystemReadBytesPerSecond  float64
	FileSystemWriteBytesPerSecond float64
	CustomReportMetrics           tcpbustool.CustomReportMetrics
	ReportTime                    int64
	Properties                    map[string]string
	FileSystemUsageRate           float64
	FileSystemEmptySpace          float64
	FileSystemCapacity            float64
	MemoryUsage                   float64
	MemoryCapacity                float64
	CPUUsage                      float64
	CPUCapacity                   float64
	Status                        int64
}

// BridgeFuncMetrics bridgeFunc metrics
type BridgeFuncMetrics struct {
	TimeStamp           int64
	PodName             string
	MemoryUsage         float64
	MemoryCapacity      float64
	CPUUsage            float64
	CPUCapacity         float64
	TotalReqCount       int64
	TotalFailedCount    int64
	AverageTimeCost     float64
	TenantID            string
	FuncName            string
	Package             string
	Version             string
	ClusterID           string
	BridgeType          string
	CustomReportMetrics tcpbustool.CustomReportMetrics
	ForBridgeManager    bool
}

// InitCaeMetrics -
func InitCaeMetrics() *CaeMetrics {
	return &CaeMetrics{
		ReportTime: time.Now().Unix(),
	}
}

// WithAvgCaeMetrics -
func (caeMetrics *CaeMetrics) WithAvgCaeMetrics(avgCaeMetrics AvgCaeMetrics) *CaeMetrics {
	caeMetrics.ReceiveBytesPerSecond = avgCaeMetrics.ReceiveBytesPerSecond
	caeMetrics.ReceivePackagePerSecond = avgCaeMetrics.ReceivePackagePerSecond
	caeMetrics.TransmitBytesPerSecond = avgCaeMetrics.TransmitBytesPerSecond
	caeMetrics.TransmitPackagePerSecond = avgCaeMetrics.TransmitPackagePerSecond
	caeMetrics.FileSystemReadBytesPerSecond = avgCaeMetrics.FileSystemReadBytesPerSecond
	caeMetrics.FileSystemWriteBytesPerSecond = avgCaeMetrics.FileSystemWriteBytesPerSecond
	return caeMetrics
}

// WithFunctionKey -
func (caeMetrics *CaeMetrics) WithFunctionKey(functionKey string) *CaeMetrics {
	caeMetrics.FunctionKey = functionKey
	return caeMetrics
}

// WithPodName -
func (caeMetrics *CaeMetrics) WithPodName(podName string) *CaeMetrics {
	caeMetrics.PodName = podName
	return caeMetrics
}

// WithCPUMetrics -
func (caeMetrics *CaeMetrics) WithCPUMetrics(cpuUsage, cpuLimit float64) *CaeMetrics {
	caeMetrics.CPUUsage = cpuUsage
	caeMetrics.CPUCapacity = cpuLimit
	if cpuLimit == 0 {
		caeMetrics.CPUUsageRate = 0
	} else {
		caeMetrics.CPUUsageRate = cpuUsage * percent / cpuLimit
	}
	return caeMetrics
}

// WithMemoryMetrics -
func (caeMetrics *CaeMetrics) WithMemoryMetrics(memoryUsage, memoryLimit float64) *CaeMetrics {
	caeMetrics.MemoryUsage = memoryUsage
	caeMetrics.MemoryCapacity = memoryLimit
	if memoryLimit == 0 {
		caeMetrics.MemoryUsageRate = 0
	} else {
		caeMetrics.MemoryUsageRate = memoryUsage * percent / memoryLimit
	}
	return caeMetrics
}

// WithCustomMetrics -
func (caeMetrics *CaeMetrics) WithCustomMetrics(customReportMetrics tcpbustool.CustomReportMetrics) *CaeMetrics {
	caeMetrics.CustomReportMetrics = customReportMetrics
	return caeMetrics
}

// WithProperties -
func (caeMetrics *CaeMetrics) WithProperties(properties map[string]string) *CaeMetrics {
	caeMetrics.Properties = properties
	return caeMetrics
}

// WithFileSystemData -
func (caeMetrics *CaeMetrics) WithFileSystemData(fileSystemUsage, fileSystemCapacity float64) *CaeMetrics {
	if fileSystemUsage > fileSystemCapacity {
		fileSystemUsage = fileSystemCapacity
	}

	caeMetrics.FileSystemCapacity = fileSystemCapacity
	if fileSystemCapacity == 0 {
		caeMetrics.FileSystemUsageRate = 0
	} else {
		caeMetrics.FileSystemUsageRate = fileSystemUsage * percent / fileSystemCapacity
	}
	caeMetrics.FileSystemEmptySpace = fileSystemCapacity - fileSystemUsage
	return caeMetrics
}

// WithStatus -
func (caeMetrics *CaeMetrics) WithStatus(status int64) *CaeMetrics {
	caeMetrics.Status = status
	return caeMetrics
}

// RecoverTraceRecord defines the recover request trace info
type RecoverTraceRecord struct {
	Records []string
}

// RecoverProperty defines the recover request
type RecoverProperty struct {
	Requests map[string]*RecoverTraceRecord
	PodName  string `json:"podName"`
}

// ScaleUpParam scale up param
type ScaleUpParam struct {
	InsResource       *types.InstanceResource
	TraceID           string
	FunctionKey       string
	NodeLabel         string
	StateID           string
	OwnedWorkerNum    int
	CurrentWorkerNum  int
	ScalingWorkersNum int
	Timeout           time.Duration
	TraceParent       string
	ChildTraceParent  string
}

// StatefulFuncMetaData describes metadata for a state.
type StatefulFuncMetaData struct {
	TenantID     string
	ServiceID    string
	FunctionName string
	StateID      string
	StateKey     string
}

// WorkerReportNotify -
type WorkerReportNotify struct {
	ReportTime time.Time
	Ch         chan WorkerReport
}

// WorkerReport -
type WorkerReport struct {
	ErrorCode    uint32
	ErrorMessage string
	Logs         string
	Summary      string
}
