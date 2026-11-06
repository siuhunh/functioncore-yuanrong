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

// Package zap zapper log
package zap

import (
	"fmt"
	"time"

	uberZap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/logger"
	"functioncore/pkg/common/logger/async"
	"functioncore/pkg/common/logger/config"
)

const (
	skipLevel = 1
)

func init() {
	uberZap.RegisterEncoder("custom_console", logger.NewConsoleEncoder)
}

// NewDevelopmentLog returns a development logger based on uber zap and it output entry to stdout and stderr
func NewDevelopmentLog() (*uberZap.Logger, error) {
	cfg := uberZap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return cfg.Build()
}

// NewConsoleLog returns a console logger based on uber zap
func NewConsoleLog() (*uberZap.Logger, error) {
	outputPaths := []string{"stdout"}
	cfg := uberZap.Config{
		Level:             uberZap.NewAtomicLevelAt(uberZap.InfoLevel),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: true,
		Encoding:          "custom_console",
		OutputPaths:       outputPaths,
		ErrorOutputPaths:  outputPaths,
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:      "T",
			LevelKey:     "L",
			NameKey:      "Logger",
			MessageKey:   "M",
			CallerKey:    "C",
			LineEnding:   zapcore.DefaultLineEnding,
			EncodeLevel:  zapcore.CapitalLevelEncoder,
			EncodeTime:   zapcore.ISO8601TimeEncoder,
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}
	consoleLogger, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	return consoleLogger.WithOptions(uberZap.AddCaller(), uberZap.AddCallerSkip(skipLevel)), nil
}

// NewWithLevel returns a log based on zap with Level
func NewWithLevel(coreInfo config.CoreInfo, isAsync bool) (*uberZap.Logger, error) {
	core, err := newCore(coreInfo, isAsync)
	if err != nil {
		return nil, err
	}

	return uberZap.New(core, uberZap.AddCaller(), uberZap.AddCallerSkip(skipLevel)), nil
}

func newCore(coreInfo config.CoreInfo, isAsync bool) (zapcore.Core, error) {
	w, err := logger.CreateSink(coreInfo)
	if err != nil {
		return nil, err
	}

	var syncer zapcore.WriteSyncer
	if isAsync {
		syncer = async.NewAsyncWriteSyncer(w)
	} else {
		syncer = zapcore.AddSync(w)
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:    "T",
		LevelKey:   "L",
		NameKey:    "Logger",
		MessageKey: "M",
		CallerKey:  "C",
		LineEnding: zapcore.DefaultLineEnding,
		EncodeLevel: func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(levelToShort(l))
		},
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	fileEncoder := logger.NewCustomEncoder(&encoderConfig)

	if err := config.LogLevel.UnmarshalText([]byte(coreInfo.Level)); err != nil {
		config.LogLevel = zapcore.InfoLevel
	}

	priority := uberZap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= config.LogLevel
	})

	if coreInfo.Tick == 0 || coreInfo.First == 0 || coreInfo.Thereafter == 0 {
		return zapcore.NewCore(fileEncoder, syncer, priority), nil
	}
	return zapcore.NewSamplerWithOptions(zapcore.NewCore(fileEncoder, syncer, priority),
		time.Duration(coreInfo.Tick)*time.Second, coreInfo.First, coreInfo.Thereafter), nil
}

func levelToShort(level zapcore.Level) string {
	switch level {
	case zapcore.DebugLevel:
		return "D"
	case zapcore.InfoLevel:
		return "I"
	case zapcore.WarnLevel:
		return "W"
	case zapcore.ErrorLevel:
		return "E"
	case zapcore.DPanicLevel:
		return "DP"
	case zapcore.PanicLevel:
		return "P"
	case zapcore.FatalLevel:
		return "F"
	default:
		return ""
	}
}

// LoggerWithFormat zap logger
type LoggerWithFormat struct {
	*uberZap.Logger
}

// Infof stdout format and paras
func (z *LoggerWithFormat) Infof(format string, paras ...interface{}) {
	z.Logger.Info(fmt.Sprintf(format, paras...))
}

// Errorf stdout format and paras
func (z *LoggerWithFormat) Errorf(format string, paras ...interface{}) {
	z.Logger.Error(fmt.Sprintf(format, paras...))
}

// Warnf stdout format and paras
func (z *LoggerWithFormat) Warnf(format string, paras ...interface{}) {
	z.Logger.Warn(fmt.Sprintf(format, paras...))
}

// Debugf stdout format and paras
func (z *LoggerWithFormat) Debugf(format string, paras ...interface{}) {
	if config.LogLevel > zapcore.DebugLevel {
		return
	}
	z.Logger.Debug(fmt.Sprintf(format, paras...))
}

// Fatalf stdout format and paras
func (z *LoggerWithFormat) Fatalf(format string, paras ...interface{}) {
	z.Logger.Fatal(fmt.Sprintf(format, paras...))
}
