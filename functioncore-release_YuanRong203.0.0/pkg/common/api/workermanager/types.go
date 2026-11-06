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

// Package workermanager message api of worker manager
package workermanager

const (
	// ViewType is view's type in time window
	ViewType = "type"
	// ViewObject is view's object in time window
	ViewObject = "object"
	// WorkerMetric is worker view type in time window
	WorkerMetric = "Worker"
	// PendingRequestMetric is pending request view type in time window
	PendingRequestMetric = "PendingRequest"
	// DiskUsageRequestMetric is disk usage request metric
	DiskUsageRequestMetric = "DiskUsageRequest"
)

// ReportMetrics reported worker metrics
type ReportMetrics struct {
	MaxCurrentRequestNums int `json:"max_current_request_num"`
	// unit: second
	UsedTimeSlice int `json:"used_time_slice"`
	// Represents seconds of UTC time since Unix epoch
	// 1970-01-01T00:00:00Z
	SecondsTimestamp int64 `json:"seconds_timestamp"`
}

// View in time window
type View struct {
	Type   string      `json:"type"`
	Object interface{} `json:"object"`
}

// WorkerView worker view in time window
type WorkerView struct {
	InstanceID  string          `json:"instance_id"`
	FuncName    string          `json:"function_name"`
	FuncVersion string          `json:"function_version"`
	TenantID    string          `json:"tenant_id"`
	BusinessID  string          `json:"business_id"`
	Metrics     []ReportMetrics `json:"metrics"`
}

// PendingRequestView pending requests view in time window
type PendingRequestView struct {
	FuncName        string `json:"function_name"`
	FuncVersion     string `json:"function_version"`
	TenantID        string `json:"tenant_id"`
	BusinessID      string `json:"business_id"`
	PendingRequests int    `json:"pending_requests"`
	ReportTimestamp int64  `json:"report_timestamp"`
}

// MirroredDiskSpaceView disk space view
type MirroredDiskSpaceView struct {
	NodeIP        string `json:"node_ip"`
	FreeDiskSpace string `json:"free_disk_space"`
}

// FuncMetaBaseInfo -
type FuncMetaBaseInfo struct {
	FuncName   string `json:"funcName"`
	TenantID   string `json:"tenantID"`
	BusinessID string `json:"businessID"`
	Version    string `json:"version"`
	ServiceID  string `json:"serviceID"`
}

// FuncMetaBaseInfoResp -
type FuncMetaBaseInfoResp struct {
	FuncMetaInfo FuncMetaInfo `json:"funcMetaInfo"`
	ErrMsg       string       `json:"errMsg"`
}

// FuncMetaInfo -
type FuncMetaInfo struct {
	FuncMetaBaseInfo         FuncMetaBaseInfo `json:"funcMetaBaseInfo"`
	IsStatefulFunction       bool             `json:"isStatefulFunction" valid:"option"`
	BusinessType             string           `json:"businessType"`
	ConcurrentNum            int              `json:"concurrentNum"`
	Timeout                  int64            `json:"timeout" valid:"optional"`
	InitializerTimeout       int64            `json:"initializer_timeout"`
	IdleMode                 bool             `json:"idleMode"`
	EnableRemoteDebug        bool             `json:"enableRemoteDebug"`
	MicCircuitBreakerEnabled bool             `json:"microservice_circuit_breaker_enabled" valid:"optional"`
	IsBridgeFunction         bool             `json:"isBridgeFunction" valid:"optional"`
	EnableWebsocket          bool             `json:"enable_websocket" valid:"optional"`
}
