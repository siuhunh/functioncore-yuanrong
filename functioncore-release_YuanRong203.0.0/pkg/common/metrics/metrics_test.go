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

func TestCreateFunctionMetrics(t *testing.T) {
	convey.Convey("CreateFunctionMetricsTest", t, func() {
		convey.Convey("abnormal eventKey", func() {
			eventKey := "/sn/functions/business/yrk/tenant/0123456/function/0@stable@hello/version"
			err := CreateFunctionMetrics(eventKey, nil)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "incorrect etcdKey length")
		})

		convey.Convey("abnormal eventValue 1", func() {
			eventKey := "/sn/functions/business/yrk/tenant/0123456/function/0@stable@hello/version/latest"
			eventValue := `{
                "funcMetaData2" : {
                    "revisionId": "revisionId-test"
                }
            }`
			err := CreateFunctionMetrics(eventKey, []byte(eventValue))
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "cannot change the type of funcMetaData")
		})

        convey.Convey("abnormal eventValue 2", func() {
            eventKey := "/sn/functions/business/yrk/tenant/0123456/function/0@stable@hello/version/latest"
            eventValue := `{
                "funcMetaData" : {
                    "revisionId": "revisionId-test"
                }
            }`
            err := CreateFunctionMetrics(eventKey, []byte(eventValue))
			convey.So(err, convey.ShouldNotBeNil)
            convey.So(err.Error(), convey.ShouldEqual, "cannot change the type of revisionID or createTime")
        })

		convey.Convey("normal event Key and Value", func() {
			eventKey := "/sn/functions/business/yrk/tenant/0123456/function/0@stable@hello/version/latest"
			eventValue := `{
                "funcMetaData" : {
                    "revisionId": "revisionId-test",
                    "created" : "created-test"
                }
            }`
			err := CreateFunctionMetrics(eventKey, []byte(eventValue))
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestGetServiceID(t *testing.T) {
	convey.Convey("GetServiceID", t, func() {
		convey.So(GetServiceID("caller"), convey.ShouldEqual, "")
		convey.So(GetServiceID("0-caller"), convey.ShouldEqual, "")
		convey.So(GetServiceID("0-serviceid-caller"), convey.ShouldEqual, "serviceid")
	})
}

func TestFetchFunctionInfo(t *testing.T) {
	convey.Convey("FetchFunctionInfoTest", t, func() {
		convey.Convey("abnormal eventKey", func() {
			eventKey := "/sn/functions/business/yrk/tenant/0123456/function/0@stable@hello/version"
			_, _, err := FetchFunctionInfo(eventKey)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "incorrect etcdKey length")
		})

		convey.Convey("success", func() {
			tmpMap := sync.Map{}
			funcInfo := FunctionInfo{
				FunctionName: "test",
				TenantID:     "yrk",
			}
			tmpMap.Store("0@stable@hello-0123456-latest", &funcInfo)
			patch := ApplyGlobalVar(&functionDataMap, tmpMap)
			defer patch.Reset()
			eventKey := "/sn/functions/business/yrk/tenant/0123456/function/0@stable@hello/version/latest"
			tmpFuncInfo, funcKey, err := FetchFunctionInfo(eventKey)
			convey.So(err, convey.ShouldBeNil)
			convey.So(tmpFuncInfo.FunctionName, convey.ShouldEqual, "test")
			convey.So(tmpFuncInfo.TenantID, convey.ShouldEqual, "yrk")
			convey.So(funcKey, convey.ShouldEqual, "0@stable@hello-0123456-latest")
		})
	})
}