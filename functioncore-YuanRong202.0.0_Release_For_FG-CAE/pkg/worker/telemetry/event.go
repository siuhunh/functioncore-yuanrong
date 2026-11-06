/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package telemetry -
package telemetry

// ReportMetrics -
type ReportMetrics struct {
	DurationMs        float64 `json:"durationMs,omitempty"`
	BillingDurationMS int     `json:"billingDurationMS,omitempty"`
	MemoryUsed        float64 `json:"memoryUsed,omitempty"`
	MemorySize        int     `json:"memorySize,omitempty"`
	PodName           string  `json:"podName,omitempty"`
}

// Event -
type Event struct {
	Type          string        `json:"type,omitempty"`
	Name          string        `json:"name,omitempty"`
	StartTime     string        `json:"startTime,omitempty"`
	EndTime       string        `json:"endTime,omitempty"`
	TraceID       string        `json:"traceID,omitempty"`
	Status        string        `json:"status,omitempty"`
	ReportMetrics ReportMetrics `json:"reportMetrics,omitempty"`
	DroppedEvent  int           `json:"droppedEvent,omitempty"`
	DroppedBytes  int           `json:"droppedBytes,omitempty"`
	Reason        string        `json:"reason,omitempty"`
	Logs          string        `json:"logs,omitempty"`
}

// StartEvent -
type StartEvent struct {
	Name      string `json:"name,omitempty"`
	StartTime string `json:"startTime,omitempty"`
	TraceID   string `json:"traceID,omitempty"`
}

// RuntimeDoneEvent -
type RuntimeDoneEvent struct {
	Name      string `json:"name,omitempty"`
	StartTime string `json:"startTime,omitempty"`
	TraceID   string `json:"traceID,omitempty"`
	Status    string `json:"status,omitempty"`
}

// ReportEvent -
type ReportEvent struct {
	Name          string        `json:"name,omitempty"`
	StartTime     string        `json:"startTime,omitempty"`
	EndTime       string        `json:"endTime,omitempty"`
	TraceID       string        `json:"traceID,omitempty"`
	Status        string        `json:"status,omitempty"`
	ReportMetrics ReportMetrics `json:"reportMetrics,omitempty"`
}

// LogsDroppedEvent -
type LogsDroppedEvent struct {
	Name         string `json:"name,omitempty"`
	StartTime    string `json:"startTime,omitempty"`
	DroppedEvent int    `json:"droppedEvent,omitempty"`
	DroppedBytes int    `json:"droppedBytes,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

// FunctionEvent -
type FunctionEvent struct {
	Name      string `json:"name,omitempty"`
	StartTime string `json:"startTime,omitempty"`
	Logs      string `json:"logs,omitempty"`
}

// ExtensionEvent -
type ExtensionEvent struct {
	Name      string `json:"name,omitempty"`
	StartTime string `json:"startTime,omitempty"`
	Logs      string `json:"logs,omitempty"`
}
