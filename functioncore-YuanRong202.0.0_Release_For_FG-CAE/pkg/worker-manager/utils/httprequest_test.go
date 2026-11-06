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

package utils

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/worker-manager/storage/meta"
)

func TestTenant2Namespace(t *testing.T) {
	convey.Convey("TestTenant2Namespace", t, func() {
		convey.Convey("Tenant2Namespace when empty", func() {
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request, _ = http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			res := Tenant2Namespace(ctx)
			convey.So(res, convey.ShouldEqual, DefaultNamespace)
		})

		convey.Convey("Tenant2Namespace success", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&gin.Context{}), "Param",
				func(c *gin.Context, string2 string) string {
					return "test"
				})
			defer patch.Reset()
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request, _ = http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker}", nil)
			res := Tenant2Namespace(ctx)
			convey.So(res, convey.ShouldEqual, "test")
		})
	})
}

func TestGetMetaName(t *testing.T) {
	convey.Convey("TestGetMetaName", t, func() {
		convey.Convey("GetMetaName failed", func() {
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request, _ = http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			res, err := GetMetaName(ctx)
			convey.So(res, convey.ShouldBeEmpty)
			convey.So(err, convey.ShouldEqual, ErrFunctionNameNotFound)
		})
		convey.Convey("GetMetaName success", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&gin.Context{}), "Param",
				func(c *gin.Context, string2 string) string {
					return "test"
				})
			defer patch.Reset()
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request, _ = http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker}", nil)
			res, err := GetMetaName(ctx)
			convey.So(res, convey.ShouldEqual, "test")
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestGetType(t *testing.T) {
	convey.Convey("TestGetType", t, func() {
		convey.Convey("GetType when empty", func() {
			req, _ := http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			res := GetType(req)
			convey.So(res, convey.ShouldEqual, "get")
		})
		convey.Convey("GetType success", func() {
			req, _ := http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			req.Header.Add("X-Call-Type", "post")
			res := GetType(req)
			convey.So(res, convey.ShouldEqual, "post")
		})
	})
}

func TestGetTraceID(t *testing.T) {
	convey.Convey("TestGetTraceID", t, func() {
		convey.Convey("TraceID success", func() {
			req, _ := http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			req.Header.Add("X-Trace-ID", "333")
			res := GetTraceID(req)
			convey.So(res, convey.ShouldEqual, "333")
		})
	})
}

func TestGetTriggerFlag(t *testing.T) {
	convey.Convey("TestGetTriggerFlag", t, func() {
		convey.Convey("GetTriggerFlag success", func() {
			req, _ := http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			req.Header.Add("X-Trigger-Flag", "true")
			res := GetTriggerFlag(req)
			convey.So(res, convey.ShouldEqual, "true")
		})
	})
}

func TestGetForceDeploy(t *testing.T) {
	convey.Convey("TestGetForceDeploy", t, func() {
		convey.Convey("GetForceDeploy success", func() {
			req, _ := http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			req.Header.Add("X-Force-Deploy", "true")
			res := GetForceDeploy(req)
			convey.So(res, convey.ShouldEqual, "true")
		})
	})
}

func TestGetNodeLabel(t *testing.T) {
	convey.Convey("TestGetNodeLabel", t, func() {
		convey.Convey("GetNodeLabel success", func() {
			req, _ := http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			req.Header.Add("X-Node-Label", "testNodeLabel")
			res := GetNodeLabel(req)
			convey.So(res, convey.ShouldEqual, "testNodeLabel")
		})
	})
}

func TestGetOwnerIP(t *testing.T) {
	convey.Convey("GetOwnerIP", t, func() {
		convey.Convey("test ownerIP", func() {
			req, _ := http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker?ownerIp=192.168.6.6", nil)
			res := GetOwnerIP(req)
			convey.So(res, convey.ShouldEqual, "192.168.6.6")
		})
		convey.Convey("test empty", func() {
			req, _ := http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			res := GetOwnerIP(req)
			convey.So(res, convey.ShouldEqual, "")
		})
	})
}

func TestIsAlias(t *testing.T) {
	convey.Convey("IsAlias", t, func() {
		convey.Convey("test isAlias", func() {
			req, _ := http.NewRequest("GET",
				"worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker?isAlias=true", nil)
			res := IsAlias(req)
			convey.So(res, convey.ShouldEqual, true)
		})
		convey.Convey("test empty", func() {
			req, _ := http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			res := IsAlias(req)
			convey.So(res, convey.ShouldEqual, false)
		})
	})
}

func TestGetApplier(t *testing.T) {
	convey.Convey("GetApplier", t, func() {
		convey.Convey("test applier", func() {
			req, _ := http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker?applier=1234", nil)
			res := GetApplier(req)
			convey.So(res, convey.ShouldEqual, "1234")
		})
		convey.Convey("test empty", func() {
			req, _ := http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			res := GetApplier(req)
			convey.So(res, convey.ShouldEqual, "")
		})
	})
}

func TestGetDeployedNode(t *testing.T) {
	convey.Convey("GetDeployedNode", t, func() {
		convey.Convey("test deployedNode", func() {
			req, _ := http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker?deployedNode=4321", nil)
			res := GetDeployedNode(req)
			convey.So(res, convey.ShouldEqual, "4321")
		})
		convey.Convey("test empty", func() {
			req, _ := http.NewRequest("GET", "worker-manager/v1/functions/sn:cn:yrk:0:function:test:$latest/worker", nil)
			res := GetDeployedNode(req)
			convey.So(res, convey.ShouldEqual, "")
		})
	})
}

func TestNewFilter(t *testing.T) {
	testFilter := NewFilter()
	if testFilter == nil {
		t.Errorf("get filter is nil")
	}

	if testFilter.Filters == nil {
		t.Errorf("get filter's Filters is nil")
	}

	filtersLen := len(testFilter.Filters)
	if filtersLen != 1 {
		t.Errorf("get filter's Filters len is %d, want is 1", filtersLen)
	}

	if testFilter.Filters[meta.FilterKeyMetaType] != "functionservice" {
		t.Errorf("get the filter's meta type is %s, want functionservice", testFilter.Filters[meta.FilterKeyMetaType])
	}
}
