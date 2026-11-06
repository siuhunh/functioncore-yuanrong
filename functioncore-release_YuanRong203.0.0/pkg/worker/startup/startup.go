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

// Package startup -
package startup

import (
	"encoding/json"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/runtime"
)

// GenerateErrorString -
func GenerateErrorString(logs string, summary snerror.SummaryInfo, error *runtime.Error) string {
	if !snerror.IsStartUpError(int(error.Code)) {
		return error.Message
	}
	startupError := snerror.FailureFee{
		Log:     logs,
		Summary: summary,
		Message: error.Message,
	}
	data, err := json.Marshal(startupError)
	if err != nil {
		log.GetLogger().Warnf("failed to Marshal startupErrorMessage, %s",
			err.Error())
		return ""
	}
	return string(data)
}

// DecodeErrorString -
func DecodeErrorString(s string) snerror.FailureFee {
	startupError := snerror.FailureFee{}
	if err := json.Unmarshal([]byte(s), &startupError); err != nil {
		return snerror.FailureFee{
			Message: s,
		}
	}
	return startupError
}
