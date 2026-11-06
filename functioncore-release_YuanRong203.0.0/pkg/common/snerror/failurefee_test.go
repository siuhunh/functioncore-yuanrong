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

// Package snerror is basic information contained in the SN error.
package snerror

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
)

func TestIsStartUpError(t *testing.T) {
	convey.Convey("Test is Start Up Error", t, func() {
		convey.Convey("Test start", func() {
			testCases := []struct {
				name         string
				errorCode    int
				expectResult bool
			}{
				{
					name:         "runtime init timeout",
					errorCode:    RuntimeInitTimeoutCode,
					expectResult: true,
				},
				{
					name:         "runtime process exit",
					errorCode:    RuntimeProcessExit,
					expectResult: true,
				},
				{
					name:         "runtime bootstrap not found",
					errorCode:    RuntimeBootstrapNotFound,
					expectResult: false,
				},
				{
					name:         "init function exception",
					errorCode:    FunctionInitFailCode,
					expectResult: true,
				},
			}

			for _, tt := range testCases {
				t.Run(tt.name, func(t *testing.T) {
					actualResult := IsStartUpError(tt.errorCode)
					if actualResult != tt.expectResult {
						t.Errorf("IsStartUpError result actual: %v, expect: %v",
							actualResult, tt.expectResult)
					}
				})
			}
		})
	})
}

func TestSummaryInfoMarshalJSONString(t *testing.T) {
	convey.Convey("test MarshalJSONString", t, func() {
		convey.Convey("case1: without extra 4 duration", func() {
			summaryInfo := SummaryInfo{
				FuncDigest:      "func-digest",
				Duration:        123.213,
				BillingDuration: 456,
				MemorySize:      34,
				MemoryUsed:      3.123,
				PodName:         "test",
				NodeIP:          "127.0.0.1",
				ClusterID:       "cluster001",
			}
			summaryInfoStr := summaryInfo.MarshalJSONString()
			expected := "{\"funcDigest\":\"func-digest\",\"duration\":123.213,\"billingDuration\":456,\"memorySize\":34,\"memoryUsed\":3.123,\"podName\":\"test\",\"gpuMemorySize\":0,\"ephemeralStorage\":0,\"nodeIP\":\"127.0.0.1\",\"clusterID\":\"cluster001\"}"
			convey.So(summaryInfoStr, convey.ShouldResemble, expected)
		})
		convey.Convey("case2: with extra 4 duration", func() {
			summaryInfo := SummaryInfo{
				FuncDigest:                   "func-digest",
				Duration:                     123.213,
				BillingDuration:              456,
				SpecifyPodDuration:           111111 * time.Microsecond,
				InitFuncDuration:             22222 * time.Microsecond,
				LoadFuncDuration:             3333 * time.Microsecond,
				GetCodeAndDecompressDuration: 444 * time.Microsecond,
				MemorySize:                   34,
				MemoryUsed:                   3.123,
				PodName:                      "test",
				NodeIP:                       "127.0.0.1",
				ClusterID:                    "cluster001",
			}
			summaryInfoStr := summaryInfo.MarshalJSONString()
			expected := "{\"funcDigest\":\"func-digest\",\"duration\":123.213,\"billingDuration\":456,\"specifyPodDuration\":111.111,\"initFuncDuration\":22.222,\"loadFuncDuration\":3.333,\"getCodeAndDecompressDuration\":0.444,\"memorySize\":34,\"memoryUsed\":3.123,\"podName\":\"test\",\"gpuMemorySize\":0,\"ephemeralStorage\":0,\"nodeIP\":\"127.0.0.1\",\"clusterID\":\"cluster001\"}"
			convey.So(summaryInfoStr, convey.ShouldResemble, expected)
		})
	})
}

func TestDecodeFromErrMSG(t *testing.T) {
	convey.Convey("TestDecodeFromErrMSG", t, func() {
		convey.Convey("Test start", func() {
			failureFee := &FailureFee{
				Log:     "111",
				Summary: SummaryInfo{},
				Message: "222",
			}
			inputMsg, _ := json.Marshal(failureFee)
			testCases := []struct {
				name      string
				errorCode uint32
				message   string
				msgResult string
			}{
				{
					name:      "runtime bootstrap not found",
					errorCode: RuntimeBootstrapNotFound,
					message:   "",
					msgResult: "",
				},
				{
					name:      "runtime init timeout message none",
					errorCode: RuntimeInitTimeoutCode,
					message:   "",
					msgResult: "",
				},
				{
					name:      "runtime init timeout with message",
					errorCode: RuntimeInitTimeoutCode,
					message:   string(inputMsg),
					msgResult: "222",
				},
			}

			for _, tt := range testCases {
				t.Run(tt.name, func(t *testing.T) {
					_, _, actualResult := DecodeFromErrMSG(tt.errorCode, tt.message)
					if actualResult != tt.msgResult {
						t.Errorf("DecodeFromErrMSG result actual: %v, expect: %v",
							actualResult, tt.msgResult)
					}
				})
			}
		})
	})
}
