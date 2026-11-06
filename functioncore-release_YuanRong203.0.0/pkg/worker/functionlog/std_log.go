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

// Package functionlog create a log module
package functionlog

import (
	"errors"

	"functioncore/pkg/common/logger/log"
)

const (
	defaultSplitLimit = 90*1024 - 1
)

// STDLogger log reader of stdout and stderr
type STDLogger struct {
	// logCallback if it exists, collect log and send back
	logCallback func([]byte)
	splitLimit  int
}

// CreateSTDLogger new std log reader
func CreateSTDLogger() *STDLogger {
	return CreateSTDLoggerWithSplitLimit(defaultSplitLimit)
}

// CreateSTDLoggerWithSplitLimit -
func CreateSTDLoggerWithSplitLimit(splitLimit int) *STDLogger {
	stdLogger := &STDLogger{
		logCallback: nil,
		splitLimit:  splitLimit,
	}

	return stdLogger
}

// RegisterLogCallback callback function for std log collection
func (l *STDLogger) RegisterLogCallback(cb func([]byte)) {
	l.logCallback = cb
}

// Write covered std log write method
func (l *STDLogger) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}

	if l.logCallback == nil {
		log.GetLogger().Errorf("logCallback is nil")
		return 0, errors.New("logCallback is nil")
	}

	if n < l.splitLimit {
		l.logCallback(p)
	} else {
		start := 0
		end := l.splitLimit
		for end < n {
			l.logCallback(p[start:end])
			start = end
			end += l.splitLimit
		}
		l.logCallback(p[start:])
	}

	log.GetLogger().Debugf("successfully log %d bytes", n)

	return n, nil
}
