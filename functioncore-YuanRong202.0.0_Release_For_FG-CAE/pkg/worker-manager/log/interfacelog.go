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

// Package log is the log module of worker-manager
package log

import (
	"strings"
	"sync"

	"functioncore/pkg/common/logger"
	"functioncore/pkg/common/logger/log"
)

// SplitStr define log split symbol
const SplitStr = "|"

// InterfaceLog define interface log info
type InterfaceLog struct {
	BusinessID   string
	SrcAppID     string
	FunctionName string
	Alias        string
	Version      string
	TraceID      string
	Code         string
	InnerCode    string
	Desc         string
}

var (
	interfaceLog *logger.InterfaceLogger
	once         sync.Once
	logPath      = "/home/sn/log"
)

// CreateInterLogger Create Inter Logger
func CreateInterLogger(logPath string) error {
	cfg := logger.InterfaceEncoderConfig{ModuleName: "WorkerManager"}

	var err error
	if interfaceLog, err = logger.NewInterfaceLogger(logPath, "worker-manager-interface", cfg); err != nil {
		return err
	}
	return nil
}

// Write Write log String
func Write(i InterfaceLog) {
	logStr := []string{
		i.BusinessID, i.SrcAppID, i.FunctionName, i.Alias, i.Version,
		i.TraceID, i.Code, i.InnerCode, i.Desc}
	var err error
	once.Do(func() {
		if interfaceLog == nil {
			if err = CreateInterLogger(logPath); err != nil {
				log.GetLogger().Errorf("failed to create interface logger with error %s", err.Error())
			}
		}
	})
	if err != nil {
		return
	}

	if interfaceLog == nil {
		return
	}

	interfaceLog.Write(strings.Join(logStr, SplitStr))
}
