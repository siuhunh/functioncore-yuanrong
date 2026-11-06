/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package functionlog

import (
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker/config"
)

// InfoRecoder -
type InfoRecoder struct {
	IsUsed           bool
	ID               uint64
	CPUUsedNs        uint64
	TotalTimestampNs uint64
}

var runtimeLoggerRecoder = struct {
	functionLogger *FunctionLogger
	m              map[string]*InfoRecoder
	sync.Mutex
}{
	m:     make(map[string]*InfoRecoder),
	Mutex: sync.Mutex{},
}

// InitRuntimeLoggerRecoder -
func InitRuntimeLoggerRecoder(conf *config.Configuration) {
	var err error
	runtimeLoggerRecoder.functionLogger, err = GetFunctionLogger(conf)
	if err != nil {
		log.GetLogger().Errorf("failed to InitRuntimeLoggerRecoder: %s", err.Error())
		return
	}
}

// RecodeIcWithTraceID -
func RecodeIcWithTraceID(traceID string, id uint64, CPUUsedNs, TotalTimestampNs uint64) {
	runtimeLoggerRecoder.Lock()
	info := &InfoRecoder{
		ID:               id,
		CPUUsedNs:        CPUUsedNs,
		TotalTimestampNs: TotalTimestampNs,
	}
	runtimeLoggerRecoder.m[traceID] = info
	runtimeLoggerRecoder.Unlock()
}

// ClearIcWithTraceID -
func ClearIcWithTraceID(traceID string) {
	runtimeLoggerRecoder.Lock()
	delete(runtimeLoggerRecoder.m, traceID)
	runtimeLoggerRecoder.Unlock()
}

// GetRuntimeFunctionLogger -
func GetRuntimeFunctionLogger() *FunctionLogger {
	runtimeLoggerRecoder.Lock()
	defer runtimeLoggerRecoder.Unlock()
	functionLogger := runtimeLoggerRecoder.functionLogger
	return functionLogger
}

// GetRuntimeInfo -
func GetRuntimeInfo(traceID string) *InfoRecoder {
	runtimeLoggerRecoder.Lock()
	defer runtimeLoggerRecoder.Unlock()
	info, ok := runtimeLoggerRecoder.m[traceID]
	if !ok {
		return nil
	}
	return info
}

// UpdateIsUsedFlag -
func UpdateIsUsedFlag(traceID string) (*FunctionLogger, *InfoRecoder, bool) {
	runtimeLoggerRecoder.Lock()
	defer runtimeLoggerRecoder.Unlock()
	functionLogger := runtimeLoggerRecoder.functionLogger
	info, ok := runtimeLoggerRecoder.m[traceID]
	if !ok {
		return functionLogger, nil, true
	}
	curIsUsed := info.IsUsed
	if !info.IsUsed {
		info.IsUsed = true
	}
	return functionLogger, info, curIsUsed
}
