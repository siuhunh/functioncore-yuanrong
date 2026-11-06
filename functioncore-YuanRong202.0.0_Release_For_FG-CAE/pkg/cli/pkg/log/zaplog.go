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
	"path/filepath"

	uberZap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// NewProductionRollingLog returns a rolling log based on zap
func NewProductionRollingLog(logPath string, fileName string) *uberZap.Logger {
	filePath := filepath.Join(logPath, fileName+".log")

	// Each file 500 MB, remain 10 backups and 5 days, use gzip compress
	core := newCore(filePath, zapcore.InfoLevel, 80, 10, 5)

	return uberZap.New(core)
}

func newCore(filePath string, level zapcore.Level, maxSize int, maxBackups int,
	maxAge int) zapcore.Core {
	hook := lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
		LocalTime:  true,
		Compress:   true,
	}

	lumberSink := zapcore.AddSync(&hook)
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:      "T",
		LevelKey:     "L",
		NameKey:      "Logger",
		MessageKey:   "M",
		CallerKey:    "C",
		LineEnding:   zapcore.DefaultLineEnding,
		EncodeLevel:  zapcore.CapitalLevelEncoder,
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	fileEncoder := zapcore.NewJSONEncoder(encoderConfig)

	priority := uberZap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= level
	})

	return zapcore.NewCore(fileEncoder, lumberSink, priority)
}
