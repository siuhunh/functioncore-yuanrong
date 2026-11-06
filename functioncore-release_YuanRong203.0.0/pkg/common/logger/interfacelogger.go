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

// Package logger log
package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/logger/config"
)

const defaultPerm = 0666

// NewInterfaceLogger returns a new interface logger
func NewInterfaceLogger(logPath, fileName string, cfg InterfaceEncoderConfig) (*InterfaceLogger, error) {
	coreInfo, err := config.GetCoreInfoFromEnv()
	if err != nil {
		coreInfo = config.GetDefaultCoreInfo()
	}
	filePath := filepath.Join(coreInfo.FilePath, fileName+".log")

	coreInfo.FilePath = filePath
	cfg.EncodeCaller = zapcore.ShortCallerEncoder
	// skip level to print caller line of origin log
	const skipLevel = 5
	core, err := newCore(coreInfo, cfg)
	if err != nil {
		return nil, err
	}
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(skipLevel))

	return &InterfaceLogger{log: logger}, nil
}

// InterfaceLogger interface logger which implements by zap logger
type InterfaceLogger struct {
	log *zap.Logger
}

// Write writes message information
func (logger *InterfaceLogger) Write(msg string) {
	logger.log.Debug(msg)
}

func newCore(coreInfo config.CoreInfo, cfg InterfaceEncoderConfig) (zapcore.Core, error) {
	w, err := CreateSink(coreInfo)
	if err != nil {
		return nil, err
	}
	syncer := zapcore.AddSync(w)

	encoder := NewInterfaceEncoder(cfg, false)

	priority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		var customLevel zapcore.Level
		if err := customLevel.UnmarshalText([]byte(coreInfo.Level)); err != nil {
			customLevel = zapcore.InfoLevel
		}
		return lvl >= customLevel
	})

	return zapcore.NewCore(encoder, syncer, priority), nil
}

// CreateSink creates a new zap log sink
func CreateSink(coreInfo config.CoreInfo) (io.Writer, error) {
	// create directory if not already exist
	dir := filepath.Dir(coreInfo.FilePath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		fmt.Printf("failed to mkdir: %s", dir)
		return nil, err
	}
	w, err := initRollingLog(coreInfo, os.O_WRONLY|os.O_APPEND|os.O_CREATE, defaultPerm)
	if err != nil {
		fmt.Printf("failed to open log file: %s, err: %s\n", coreInfo.FilePath, err.Error())
		return nil, err
	}
	return w, nil
}
