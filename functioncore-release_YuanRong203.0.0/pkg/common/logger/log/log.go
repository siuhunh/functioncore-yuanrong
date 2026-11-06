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

// Package log is common logger client
package log

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/asaskevich/govalidator"
	uberZap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/zap"
)

const snuserLogPath = "/home/snuser/log"

var (
	isForDebug       = false
	once             sync.Once
	formatLogger     FormatLogger
	defaultLogger, _ = uberZap.NewProduction()
)

// InitRunLog init run log with log.json file
func InitRunLog(fileName string, isAsync bool) error {
	coreInfo, err := config.GetCoreInfoFromEnv()
	if err != nil {
		return err
	}
	if coreInfo.Disable {
		return nil
	}
	formatLogger, err = newFormatLogger(fileName, isAsync, coreInfo)
	return err
}

// InitRunLogWithConfig init run log with config
func InitRunLogWithConfig(fileName string, isAsync bool, coreInfo config.CoreInfo) (FormatLogger, error) {
	if _, err := govalidator.ValidateStruct(coreInfo); err != nil {
		return nil, err
	}
	return newFormatLogger(fileName, isAsync, coreInfo)
}

// FormatLogger format logger interface
type FormatLogger interface {
	With(fields ...zapcore.Field) FormatLogger

	Infof(format string, paras ...interface{})
	Errorf(format string, paras ...interface{})
	Warnf(format string, paras ...interface{})
	Debugf(format string, paras ...interface{})
	Fatalf(format string, paras ...interface{})
	ForDebugf(format string, paras ...interface{})

	Info(msg string, fields ...uberZap.Field)
	Error(msg string, fields ...uberZap.Field)
	Warn(msg string, fields ...uberZap.Field)
	Debug(msg string, fields ...uberZap.Field)
	Fatal(msg string, fields ...uberZap.Field)

	Sync()
}

// zapLoggerWithFormat define logger
type zapLoggerWithFormat struct {
	Logger  *uberZap.Logger
	SLogger *uberZap.SugaredLogger
}

// newFormatLogger new formatLogger with log config info
func newFormatLogger(fileName string, isAsync bool, coreInfo config.CoreInfo) (FormatLogger, error) {
	if strings.Compare(constants.MonitorFileName, fileName) == 0 {
		coreInfo.FilePath = snuserLogPath
	}
	coreInfo.FilePath = filepath.Join(coreInfo.FilePath, fileName+"-run.log")
	logger, err := zap.NewWithLevel(coreInfo, isAsync)
	if err != nil {
		return nil, err
	}

	return &zapLoggerWithFormat{
		Logger:  logger,
		SLogger: logger.Sugar(),
	}, nil
}

// NewConsoleLogger returns a console logger
func NewConsoleLogger() FormatLogger {
	logger, err := zap.NewConsoleLog()
	if err != nil {
		fmt.Println("new console log error", err)
		logger = defaultLogger
	}
	return &zapLoggerWithFormat{
		Logger:  logger,
		SLogger: logger.Sugar(),
	}
}

// GetLogger get logger directly
func GetLogger() FormatLogger {
	if formatLogger == nil {
		once.Do(func() {
			formatLogger = NewConsoleLogger()
		})
	}
	return formatLogger
}

// With add fields to log header
func (z *zapLoggerWithFormat) With(fields ...zapcore.Field) FormatLogger {
	logger := z.Logger.With(fields...)
	return &zapLoggerWithFormat{
		Logger:  logger,
		SLogger: logger.Sugar(),
	}
}

// Infof stdout format and paras
func (z *zapLoggerWithFormat) Infof(format string, paras ...interface{}) {
	z.SLogger.Infof(format, paras...)
}

// Errorf stdout format and paras
func (z *zapLoggerWithFormat) Errorf(format string, paras ...interface{}) {
	z.SLogger.Errorf(format, paras...)
}

// Warnf stdout format and paras
func (z *zapLoggerWithFormat) Warnf(format string, paras ...interface{}) {
	z.SLogger.Warnf(format, paras...)
}

// Debugf stdout format and paras
func (z *zapLoggerWithFormat) Debugf(format string, paras ...interface{}) {
	if config.LogLevel > zapcore.DebugLevel {
		return
	}
	z.SLogger.Debugf(format, paras...)
}

// Fatalf stdout format and paras
func (z *zapLoggerWithFormat) Fatalf(format string, paras ...interface{}) {
	z.SLogger.Fatalf(format, paras...)
}

// ForDebugf log for debug control by config
func (z *zapLoggerWithFormat) ForDebugf(format string, paras ...interface{}) {
	if isForDebug || config.LogLevel == zapcore.DebugLevel {
		z.SLogger.Infof("======logForDebug====== "+format, paras...)
	}
}

// Info stdout format and paras
func (z *zapLoggerWithFormat) Info(msg string, fields ...uberZap.Field) {
	z.Logger.Info(msg, fields...)
}

// Error stdout format and paras
func (z *zapLoggerWithFormat) Error(msg string, fields ...uberZap.Field) {
	z.Logger.Error(msg, fields...)
}

// Warn stdout format and paras
func (z *zapLoggerWithFormat) Warn(msg string, fields ...uberZap.Field) {
	z.Logger.Warn(msg, fields...)
}

// Debug stdout format and paras
func (z *zapLoggerWithFormat) Debug(msg string, fields ...uberZap.Field) {
	if config.LogLevel > zapcore.DebugLevel {
		return
	}
	z.Logger.Debug(msg, fields...)
}

// Fatal stdout format and paras
func (z *zapLoggerWithFormat) Fatal(msg string, fields ...uberZap.Field) {
	z.Logger.Fatal(msg, fields...)
}

// Sync calls the underlying Core's Sync method, flushing any buffered log
// entries. Applications should take care to call Sync before exiting.
func (z *zapLoggerWithFormat) Sync() {
	z.Logger.Sync()
}

// SetIsForDebug control is open debug log
func SetIsForDebug(forDebug bool) {
	isForDebug = forDebug
}

// GetIsForDebug get is open debug log
func GetIsForDebug() bool {
	return isForDebug
}
