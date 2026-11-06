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

package metrics

import (
    "sync"
    "testing"

    . "github.com/agiledragon/gomonkey/v2"
    "github.com/smartystreets/goconvey/convey"
)

func TestLoadFuncInfo(t *testing.T) {
    convey.Convey("LoadFuncInfo", t, func() {
        convey.Convey("key not exist", func() {
            tmpMap := sync.Map{}
            patch := ApplyGlobalVar(&functionDataMap, tmpMap)
            defer patch.Reset()
            _, flag := LoadFuncInfo("functionKey")
            convey.So(flag, convey.ShouldBeFalse)
        })

        convey.Convey("value fmt err", func() {
            tmpMap := sync.Map{}
            tmpMap.Store("functionKey", "test")
            patch := ApplyGlobalVar(&functionDataMap, tmpMap)
            defer patch.Reset()
            _, flag := LoadFuncInfo("functionKey")
            convey.So(flag, convey.ShouldBeFalse)
        })

        convey.Convey("success", func() {
            tmpMap := sync.Map{}
            funcInfo := FunctionInfo{
                FunctionName: "test",
                TenantID:     "123",
            }
            tmpMap.Store("functionKey", &funcInfo)
            patch := ApplyGlobalVar(&functionDataMap, tmpMap)
            defer patch.Reset()
            _, flag := LoadFuncInfo("functionKey")
            convey.So(flag, convey.ShouldBeTrue)
        })
    })
}

func TestFunctionInfoProcess(t *testing.T) {
    convey.Convey("FunctionInfoProcess", t, func() {
        convey.Convey("key not exist", func() {
           tmpMap := sync.Map{}
           patch := ApplyGlobalVar(&functionDataMap, tmpMap)
           defer patch.Reset()
           funcInfo := FunctionInfo{
               FunctionName: "test",
               TenantID:     "123",
           }
           info := FunctionInfoProcess(&funcInfo)
           convey.So(info.FunctionName, convey.ShouldEqual, "test")
           convey.So(info.TenantID, convey.ShouldEqual, "123")
        })

        convey.Convey("value fmt err", func() {
           tmpMap := sync.Map{}
           tmpMap.Store("functionKey", "test")
           funcInfo := FunctionInfo{
               FunctionName: "test",
               TenantID:     "123",
           }
           patch := ApplyGlobalVar(&functionDataMap, tmpMap)
           defer patch.Reset()
           info := FunctionInfoProcess(&funcInfo)
           convey.So(info.FunctionName, convey.ShouldEqual, "test")
           convey.So(info.TenantID, convey.ShouldEqual, "123")
        })

        convey.Convey("key exist", func() {
           tmpMap := sync.Map{}
           funcInfoValue := FunctionInfo{
               FunctionName: "test-2",
               TenantID:     "123-2",
           }
           funcInfo := FunctionInfo{
              FunctionName: "0-serviceid-caller",
              TenantID:     "123",
              Version:      "latest",
           }
           tmpMap.Store("0-serviceid-caller-123-latest", &funcInfoValue)
           patch := ApplyGlobalVar(&functionDataMap, tmpMap)
           defer patch.Reset()
           info := FunctionInfoProcess(&funcInfo)
           convey.So(info.FunctionName, convey.ShouldEqual, "test-2")
           convey.So(info.TenantID, convey.ShouldEqual, "123-2")
        })
    })
}

func TestGetServiceIDFromFuncName(t *testing.T) {
    convey.Convey("GetServiceIDFromFuncName", t, func() {
        convey.So(GetServiceID("caller"), convey.ShouldEqual, "")
        convey.So(GetServiceID("0-caller"), convey.ShouldEqual, "")
        convey.So(GetServiceID("0-serviceid-caller"), convey.ShouldEqual, "serviceid")
    })
}