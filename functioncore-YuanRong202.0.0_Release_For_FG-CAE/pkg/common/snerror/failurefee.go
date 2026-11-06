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

// Package snerror is basic information contained in the SN error.
package snerror

import (
	"encoding/json"
	"strconv"
	"strings"
)

var errorCodeMap = map[int]int{
	RuntimeInitTimeoutCode:        0,
	RuntimeRestoreHookTimeoutCode: 0,
	RuntimeProcessExit:            0,
	DiskUsageExceed:               0,
	RuntimeMemoryExceedLimit:      0,
	RuntimeInvokeTimeoutCode:      0,
	RequestCanceled:               0,
	RuntimeUnHealthy:              0,
	FunctionInitFailCode:          0,
}

const (
	summaryInfoMarshalJSONPart1 = `{"funcDigest":"`
	summaryInfoMarshalJSONPart2 = `","duration":`
	summaryInfoMarshalJSONPart3 = `,"billingDuration":`
	summaryInfoMarshalJSONPart4 = `,"memorySize":`
	summaryInfoMarshalJSONPart5 = `,"memoryUsed":`
	summaryInfoMarshalJSONPart6 = `,"podName":"`
	summaryInfoMarshalJSONPart7 = `","gpuMemorySize":`
	summaryInfoMarshalJSONPart8 = `,"ephemeralStorage":`
	summaryInfoMarshalJSONPart9 = `}`

	precisionControl = 3
	bitSize          = 64
)

var summaryInfoMarshalJSONBaseSize = len(summaryInfoMarshalJSONPart1) +
	len(summaryInfoMarshalJSONPart2) +
	len(summaryInfoMarshalJSONPart3) +
	len(summaryInfoMarshalJSONPart4) +
	len(summaryInfoMarshalJSONPart5) +
	len(summaryInfoMarshalJSONPart6) +
	len(summaryInfoMarshalJSONPart7) +
	len(summaryInfoMarshalJSONPart8) +
	len(summaryInfoMarshalJSONPart9)

// SummaryInfo -
type SummaryInfo struct {
	FuncDigest       string  `json:"funcDigest"`                 // Function Summary
	Duration         float64 `json:"duration"`                   // Running time (unit: ms)
	BillingDuration  int     `json:"billingDuration"`            // Charging time (unit: ms)
	MemorySize       int     `json:"memorySize"`                 // Memory configuration (unit: MB)
	MemoryUsed       float64 `json:"memoryUsed"`                 // Memory usage (unit: MB)
	PodName          string  `json:"podName"`                    // podName
	CPUUsed          float64 `json:"cpuUsed,omitempty"`          // cpu usage (unit u)
	GPUMemorySize    int     `json:"gpuMemorySize"`              // GPU Memory size (unit: MB)
	EphemeralStorage int     `json:"ephemeralStorage,omitempty"` // Ephemeral Storage size (unit: MB)
	StorageUsed      float64 `json:"storage_used"`
}

// MarshalJSONString creates a json struct for SummaryInfo
func (s SummaryInfo) MarshalJSONString() string {
	durationStr := strconv.FormatFloat(s.Duration, 'f', precisionControl, bitSize)
	billingDurationStr := strconv.Itoa(s.BillingDuration)
	memorySizeStr := strconv.Itoa(s.MemorySize)
	memoryUsedStr := strconv.FormatFloat(s.MemoryUsed, 'f', precisionControl, bitSize)
	gpuMemoryStr := strconv.Itoa(s.GPUMemorySize)
	ephemeralStorage := strconv.Itoa(s.EphemeralStorage)

	var b strings.Builder
	b.Grow(summaryInfoMarshalJSONBaseSize +
		len(s.FuncDigest) +
		len(durationStr) +
		len(billingDurationStr) +
		len(memorySizeStr) +
		len(memoryUsedStr) +
		len(s.PodName) +
		len(gpuMemoryStr) +
		len(ephemeralStorage))

	b.WriteString(summaryInfoMarshalJSONPart1)
	b.WriteString(s.FuncDigest)
	b.WriteString(summaryInfoMarshalJSONPart2)
	b.WriteString(durationStr)
	b.WriteString(summaryInfoMarshalJSONPart3)
	b.WriteString(billingDurationStr)
	b.WriteString(summaryInfoMarshalJSONPart4)
	b.WriteString(memorySizeStr)
	b.WriteString(summaryInfoMarshalJSONPart5)
	b.WriteString(memoryUsedStr)
	b.WriteString(summaryInfoMarshalJSONPart6)
	b.WriteString(s.PodName)
	b.WriteString(summaryInfoMarshalJSONPart7)
	b.WriteString(gpuMemoryStr)
	b.WriteString(summaryInfoMarshalJSONPart8)
	b.WriteString(ephemeralStorage)
	b.WriteString(summaryInfoMarshalJSONPart9)

	return b.String()
}

// FailureFee -
type FailureFee struct {
	Log     string      `json:"log"`
	Summary SummaryInfo `json:"summary"`
	Message string      `json:"errorMessage"`
}

// IsStartUpError -
func IsStartUpError(errorCode int) bool {
	if _, ok := errorCodeMap[errorCode]; ok {
		return true
	}
	return false
}

// DecodeFromErrMSG decode error str from startup error message
func DecodeFromErrMSG(errCode uint32, errMsg string) (string, string, string) {
	if !IsStartUpError(int(errCode)) {
		return "", "", errMsg
	}
	startupErrorMessage := FailureFee{}
	if err := json.Unmarshal([]byte(errMsg), &startupErrorMessage); err != nil {
		return "", "", errMsg
	}
	summaryBytes, err := json.Marshal(startupErrorMessage.Summary)
	if err != nil {
		return "", "", errMsg
	}
	return startupErrorMessage.Log, string(summaryBytes), startupErrorMessage.Message
}
