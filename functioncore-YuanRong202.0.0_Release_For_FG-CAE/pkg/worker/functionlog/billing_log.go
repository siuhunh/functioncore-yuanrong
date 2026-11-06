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

package functionlog

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logConfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
)

// BillingLogger the logger print billing info
type BillingLogger struct {
	writer *zap.Logger
}

// NewBillingLogger -
func NewBillingLogger(cfg *config.Configuration) (*BillingLogger, error) {
	coreInfo, err := logConfig.GetCoreInfoFromEnv()
	if err != nil {
		return nil, err
	}

	fullPath, err := getAbsFilePath(coreInfo.FilePath, "billing")
	if err != nil {
		return nil, err
	}

	rollingLogger, err := newZapLogger(fullPath, "Message")
	if err != nil {
		return nil, err
	}

	functionURN := urnutils.GetBillingFunctionURN(*utils.LocalFuncURN,
		fmt.Sprintf("%d", cfg.ResourceMetaData.Mem))
	rollingLogger = rollingLogger.With(zapcore.Field{
		Key:    "FunctionURN",
		Type:   zapcore.StringType,
		String: functionURN,
	}, zapcore.Field{
		Key:    "PodName",
		Type:   zapcore.StringType,
		String: os.Getenv("PodName"),
	})

	return &BillingLogger{
		writer: rollingLogger,
	}, nil
}

// WriteLog -
func (b *BillingLogger) WriteLog(billingType, billingMessage string) {
	if b.writer == nil {
		log.GetLogger().Warnf("BillingLogger writer is nil")
		return
	}
	timestampFiled := zapcore.Field{
		Key:    "Time",
		Type:   zapcore.StringType,
		String: time.Now().Format("2006-01-02 15:04:05.999-07:00"),
	}
	billingTypeField := zapcore.Field{
		Key:    "BillingType",
		Type:   zapcore.StringType,
		String: billingType,
	}
	b.writer.Info(billingMessage, timestampFiled, billingTypeField)
}
