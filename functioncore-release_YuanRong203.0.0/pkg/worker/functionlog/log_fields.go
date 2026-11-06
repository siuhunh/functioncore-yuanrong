/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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
	"strconv"

	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/logger/log"
)

const (
	traceIDPos = iota
	timePos
	errorTypePos
	stagePos
	statusPos
	finishPos
	livedataTraceIDPos
	endFields
)

type logFileds []zapcore.Field

func newLogFields() logFileds {
	return []zapcore.Field{
		{
			Key:  "requestId",
			Type: zapcore.StringType,
		},
		{
			Key:  "time",
			Type: zapcore.StringType,
		},
		{
			Key:  "errorType",
			Type: zapcore.StringType,
		},
		{
			Key:  "stage",
			Type: zapcore.StringType,
		},
		{
			Key:  "status",
			Type: zapcore.StringType,
		},
		{
			Key:  "finishLog",
			Type: zapcore.StringType,
		},
		{
			Key:  "livedataTraceId",
			Type: zapcore.StringType,
		},
	}
}

func (f logFileds) set(funcLog *internalFunctionLog) {
	if len(f) < endFields {
		log.GetLogger().Warnf("invalid logFields")
		return
	}

	f[traceIDPos].String = funcLog.TraceID
	f[timePos].String = funcLog.NanoTime
	f[errorTypePos].String = funcLog.ErrorType
	f[stagePos].String = funcLog.Stage

	statusStr := "success"
	if funcLog.ErrorType != "" {
		statusStr = "fail"
	}
	f[statusPos].String = statusStr
	f[finishPos].String = strconv.FormatBool(funcLog.IsFinishedLog)
	f[livedataTraceIDPos].String = funcLog.LivedataID
}
