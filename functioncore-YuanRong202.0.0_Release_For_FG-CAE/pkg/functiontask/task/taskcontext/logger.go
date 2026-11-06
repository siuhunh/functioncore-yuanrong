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

package taskcontext

import (
	"go.uber.org/zap"

	logc "functioncore/pkg/common/logger/config"
	commonLog "functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

// LoggerContext represents context for logger.
type LoggerContext interface {
	Logger() commonLog.FormatLogger
	WithLogFields(fields ...zap.Field)
	AddLogFieldCategory(category string)
	AddLogFieldReqIDList(reqIDList string)
}

// LoggerImpl implements context for logger.
type LoggerImpl struct {
	logger commonLog.FormatLogger
}

// Init the logger.
func (c *LoggerImpl) Init() {
	if coreconfig.Logger == nil {
		coreInfo := logc.CoreInfo{
			FilePath: "/home/sn/log",
			Level:    "INFO",
		}
		runLog, err := commonLog.InitRunLogWithConfig("functiontask", true, coreInfo)
		if err != nil {
			return
		}
		c.logger = runLog
		return
	}
	c.logger = coreconfig.Logger
}

// Logger returns context's logger.
func (c *LoggerImpl) Logger() commonLog.FormatLogger {
	return c.logger
}

// WithLogFields adds fields for logger.
func (c *LoggerImpl) WithLogFields(fields ...zap.Field) {
	c.logger = c.logger.With(fields...)
}

// AddLogFieldCategory set category for logger.
func (c *LoggerImpl) AddLogFieldCategory(category string) {
	c.logger = c.logger.With(zap.String("C", category))
}

// AddLogFieldReqIDList set reqIDList for logger.
func (c *LoggerImpl) AddLogFieldReqIDList(reqIDList string) {
	c.logger = c.logger.With(zap.String("reqIDList", reqIDList))
}
