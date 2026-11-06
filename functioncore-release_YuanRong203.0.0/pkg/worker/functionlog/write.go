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
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker/config"
)

const (
	precision = 3
	bitSize   = 64
)

// Sync ensure all log is written in file before worker exit
func Sync(conf *config.Configuration) {
	f, err := GetFunctionLogger(conf)
	if err != nil {
		log.GetLogger().Warnf("failed to get functionLogger: %s", err.Error())
		return
	}

	f.logger.Sync()
}

func setPrecision(f float64, p int) float64 {
	res, err := strconv.ParseFloat(strconv.FormatFloat(f, 'f', p, bitSize), bitSize)
	if err != nil {
		log.GetLogger().Errorf("failed to set float64 precision: %f %d", f, p)
		return 0
	}
	return res
}

func (f *FunctionLogger) write(funcLog *internalFunctionLog) {
	if f == nil || f.logger == nil {
		log.GetLogger().Errorf("invalid logger")
		return
	}
	defer func() {
		if f != nil && funcLog != nil {
			f.ReleaseLog(funcLog.FunctionLog)
		}
	}()

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(funcLog.Level)); err != nil {
		level = zapcore.InfoLevel
	}
	message := getLTSLoggerMessage(funcLog)
	ent := zapcore.Entry{
		LoggerName: "",
		Time:       time.Now(),
		Level:      level,
		Message:    message,
	}

	logFieldsIf := f.fieldsPool.Get()
	defer f.fieldsPool.Put(logFieldsIf)

	logFileds, ok := logFieldsIf.(logFileds)
	if !ok {
		log.GetLogger().Errorf("failed to assert logFields")
		return
	}
	logFileds.set(funcLog)

	var err error
	if funcLog.ReportSummary {
		err = f.logger.Core().With([]zapcore.Field{
			zap.Float64("cpu", setPrecision(funcLog.CPUUsed, precision)),
			zap.Float64("memory", setPrecision(funcLog.MemoryUsed, precision)),
			zap.Float64("storage", setPrecision(funcLog.StorageUsed, precision)),
			zap.Float64("duration", setPrecision(funcLog.Duration, precision)),
		}).Write(ent, logFileds)
	} else {
		err = f.logger.Core().Write(ent, logFileds)
	}

	if err != nil {
		log.GetLogger().Errorf("failed to write, %s", err.Error())
		return
	}
}

func getLTSLoggerMessage(funcLog *internalFunctionLog) string {
	if !funcLog.IsStdLog {
		if funcLog.Index > 0 {
			var b strings.Builder
			b.WriteString(funcLog.Time)
			b.WriteString(" ")
			b.WriteString(funcLog.TraceID)
			b.WriteString(" ")
			b.WriteString(funcLog.Level)
			b.WriteString(" ")
			b.Write(funcLog.Message.Bytes())
			return b.String()
		}
		if funcLog.Index == constants.DefaultFuncLogIndex {
			var b strings.Builder
			b.WriteString(funcLog.Time)
			b.WriteString(" ")
			b.Write(funcLog.Message.Bytes())
			return b.String()
		}
	}
	return funcLog.Message.String()
}
