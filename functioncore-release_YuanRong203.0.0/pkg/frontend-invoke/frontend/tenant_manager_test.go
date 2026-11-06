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

package frontend

import (
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/frontend-invoke/trafficlimit"
)

func TestProcessTenantEvent(t *testing.T) {
	convey.Convey("Test Process Tenant Event", t, func() {
		convey.Convey("test put event", func() {
			convey.Convey("test put err", func() {
				tenant := &trafficlimit.TenantContainer{}
				defer gomonkey.ApplyMethod(reflect.TypeOf(tenant), "ProcessUpdate", func(_ *trafficlimit.TenantContainer,
					event *etcd3.Event) error {
					return errors.New("")
				}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processTenantEvent(event), convey.ShouldBeError)
			})
			convey.Convey("test put ok", func() {
				tenant := &trafficlimit.TenantContainer{}
				defer gomonkey.ApplyMethod(reflect.TypeOf(tenant), "ProcessUpdate", func(_ *trafficlimit.TenantContainer,
					event *etcd3.Event) error {
					return nil
				}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processTenantEvent(event), convey.ShouldBeNil)
			})
		})
		convey.Convey("test delete event", func() {
			tenant := &trafficlimit.TenantContainer{}
			defer gomonkey.ApplyMethod(reflect.TypeOf(tenant), "ProcessDelete", func(_ *trafficlimit.TenantContainer,
				event *etcd3.Event) {
			}).Reset()
			event := &etcd3.Event{Type: etcd3.DELETE}
			convey.So(processTenantEvent(event), convey.ShouldBeNil)
		})
		convey.Convey("test other event", func() {
			event := &etcd3.Event{Type: etcd3.ERROR}
			convey.So(processTenantEvent(event), convey.ShouldBeNil)
			event = &etcd3.Event{Type: -1}
			convey.So(processTenantEvent(event), convey.ShouldBeNil)
		})
	})
}

func TestIsTenantQOS(t *testing.T) {
	convey.Convey("Test Is Tenant QOS", t, func() {
		convey.So(IsTenantQOS("/sn/qos/business/yrk/tenant/tenant1/111"), convey.ShouldBeFalse)
		convey.So(IsTenantQOS("/sn/qos/business/yrk/tenant111/tenant1"), convey.ShouldBeFalse)
		convey.So(IsTenantQOS("/sn/qos/business/yrk/tenant/tenant1"), convey.ShouldBeTrue)
	})
}
