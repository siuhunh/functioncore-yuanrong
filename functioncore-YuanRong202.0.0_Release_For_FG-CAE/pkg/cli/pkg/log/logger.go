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

// Package log is logs of cli
package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	uberZap "go.uber.org/zap"
)

var (
	zaplog     *zapLoggerWithFormat
	clientonce sync.Once
)

func initClientLog() {
	clientonce.Do(func() {
		logPath := filepath.Join(os.Getenv("HOME"), "logs")
		rollingLog := NewProductionRollingLog(logPath, "cli")
		zaplog = &zapLoggerWithFormat{
			Logger: rollingLog,
		}
	})
}

// GetLogger 获取Logger
func GetLogger() LoggerWithFormat {
	if zaplog == nil {
		initClientLog()
	}
	return LoggerWithFormat{zaplog}
}

// LoggerWithFormat 日志格式化
type LoggerWithFormat struct {
	*zapLoggerWithFormat
}

type zapLoggerWithFormat struct {
	Logger *uberZap.Logger
}

// Infof INFO 级别日志
func (z *zapLoggerWithFormat) Infof(format string, paras ...interface{}) {
	z.Logger.Info(fmt.Sprintf(format, paras...))
}

// Errorf ERROR 级别日志
func (z *zapLoggerWithFormat) Errorf(format string, paras ...interface{}) {
	z.Logger.Error(fmt.Sprintf(format, paras...))
}

// Warnf WARNING 级别日志
func (z *zapLoggerWithFormat) Warnf(format string, paras ...interface{}) {
	z.Logger.Warn(fmt.Sprintf(format, paras...))
}

// Debugf DEBUG 级别日志
func (z *zapLoggerWithFormat) Debugf(format string, paras ...interface{}) {
	z.Logger.Debug(fmt.Sprintf(format, paras...))
}

// Fatalf FATAL 级别日志
func (z *zapLoggerWithFormat) Fatalf(format string, paras ...interface{}) {
	z.Logger.Fatal(fmt.Sprintf(format, paras...))
}
