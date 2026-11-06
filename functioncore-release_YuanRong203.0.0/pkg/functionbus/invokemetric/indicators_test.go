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

package invokemetric

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/redisclient"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
)

func buildIndicatorRequest() *commontypes.TCPRequest {
	urn := &urnutils.BaseURN{
		TenantID: "tenantID",
		Name:     "0@default@callerFunc",
		Version:  "callerVersion",
	}
	return &commontypes.TCPRequest{
		CPU:    "300",
		Memory: "128",
		UserData: map[string]string{
			constants.SrcFuncUrnFlag: urn.String(),
			constants.SrcPodNameFlag: "callerPod",
		},
	}
}

func buildIndicatorResponse() *commontypes.TCPResponse {
	return &commontypes.TCPResponse{
		ErrorCode:    100500,
		ErrorMessage: "failure",
		Summary:      "{\"memoryUsed\":31.14,\"podName\":\"calleePod\",\"nodeIP\":\"calleeNodeIP\",\"clusterID\":\"calleeClusterID\"}",
	}
}

func buildFunctionProperty() *common.FunctionProperty {
	return &common.FunctionProperty{
		InstanceID:      "localHostName",
		TenantID:        "tenantID",
		FunctionVersion: "calleeVersion",
		FunctionName:    "0@default@calleeFunc",
	}
}

func TestIndicators(t *testing.T) {
	convey.Convey("TestIndicators", t, func() {
		convey.Convey("test recordIndicators", func() {
			indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
			defer func() {
				indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
			}()
			req := buildIndicatorRequest()
			dst := buildFunctionProperty()
			resp := buildIndicatorResponse()
			RecordIndicators("traceID", req, dst, resp)
			convey.So(len(indicatorMap), convey.ShouldEqual, 1)

			for _, v := range indicatorMap {
				convey.So(v.IndicatorKey.ErrorCode, convey.ShouldEqual, 100500)
				convey.So(v.IndicatorKey.CallerPodName, convey.ShouldEqual, "callerPod")
				convey.So(v.IndicatorKey.CalleePodName, convey.ShouldEqual, "calleePod")

				convey.So(v.ErrType, convey.ShouldEqual, "sysErr")
				convey.So(v.CallerFuncName, convey.ShouldEqual, "tenantID#default#callerFunc#callerVersion")
				convey.So(v.CalleeFuncName, convey.ShouldEqual, "tenantID#default#calleeFunc#calleeVersion")
				convey.So(v.CalleeNodeIP, convey.ShouldEqual, "calleeNodeIP")
				convey.So(v.CalleeClusterID, convey.ShouldEqual, "calleeClusterID")
				convey.So(v.Count, convey.ShouldEqual, 1)
			}

			RecordIndicators("traceID", req, dst, resp)
			convey.So(len(indicatorMap), convey.ShouldEqual, 1)
			for _, v := range indicatorMap {
				convey.So(v.Count, convey.ShouldEqual, 2)
			}
		})
		convey.Convey("test report redis when reach max num", func() {
			flag := false
			indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
			for i := 0; i < maxCachedRecordNum-1; i++ {
				key := IndicatorKey{ErrorCode: uint32(i)}
				indicatorMap[key] = InnerInvokeIndicators{}
			}
			defer func() {
				indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
			}()
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(reportToRedisWithRetry, func(_ map[IndicatorKey]InnerInvokeIndicators) {
					flag = true
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := buildIndicatorRequest()
			dst := buildFunctionProperty()
			resp := buildIndicatorResponse()
			RecordIndicators("traceID", req, dst, resp)
			convey.So(flag, convey.ShouldBeTrue)
		})
		convey.Convey("test periodic report redis", func() {
			stopChan := make(chan struct{})
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(reportToRedisWithRetry, func(_ map[IndicatorKey]InnerInvokeIndicators) {
					flag = true
					close(stopChan)
				}),
				gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
					ch := make(chan time.Time, 1)
					ch <- time.Time{}
					return &time.Ticker{C: ch}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			go reportInvokeErrToRedisPeriodically(&redisclient.Client{}, stopChan)
			time.Sleep(10 * time.Millisecond)
			convey.So(flag, convey.ShouldBeTrue)
		})
		convey.Convey("test report to redis", func() {
			redisClient = &redisclient.Client{}
			indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
			mockKey := ""
			var indicatorSlice []InnerInvokeIndicators
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*redisclient.Client).ZAdd,
					func(_ *redisclient.Client, _ context.Context, key string, values ...redis.Z) *redis.IntCmd {
						mockKey = key
						fmt.Printf("values len: %d.\n", len(values))
						for _, value := range values {
							indicatorSlice = append(indicatorSlice, value.Member.(InnerInvokeIndicators))
						}
						intCmd := &redis.IntCmd{}
						intCmd.SetErr(nil)
						return intCmd
					},
				),
				gomonkey.ApplyFunc((*redisclient.Client).Expire,
					func(_ *redisclient.Client, _ context.Context, key string, _ time.Duration) *redis.IntCmd {
						intCmd := &redis.IntCmd{}
						intCmd.SetErr(nil)
						return intCmd
					},
				),
			}
			defer func() {
				indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := buildIndicatorRequest()
			dst := buildFunctionProperty()
			resp := buildIndicatorResponse()
			RecordIndicators("traceID", req, dst, resp)
			reportToRedisWithRetry(indicatorMap)
			convey.So(mockKey, convey.ShouldEqual, fssInnerInvokeIndicators)
			convey.So(len(indicatorSlice), convey.ShouldEqual, 1)
		})
	})
}

func TestGetErrType(t *testing.T) {
	convey.Convey("TestGetErrType", t, func() {
		convey.So(getErrType(0), convey.ShouldEqual, "sysOK")
		convey.So(getErrType(4020), convey.ShouldEqual, "usrErr")
		convey.So(getErrType(100500), convey.ShouldEqual, "sysErr")
	})
}
