/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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
	"testing"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/worker/config"
)

func TestLogRecorderMap(m *testing.T) {
	conf := &config.Configuration{}
	InitRuntimeLoggerRecoder(conf)
	RecodeIcWithTraceID("test1", 1, 1, 1)
	RecodeIcWithTraceID("test2", 2, 2, 2)
	err := GetRuntimeFunctionLogger()
	assert.NotNil(m, err)
	ClearIcWithTraceID("test2")
	ClearIcWithTraceID("test1")
	info := GetRuntimeInfo("test2")
	assert.Nil(m, info)
}
