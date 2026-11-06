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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/aliasroute"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/frontend-invoke/metadata"
)

func TestProcessFunctionEvent(t *testing.T) {
	convey.Convey("test process func event", t, func() {
		convey.Convey("process update event", func() {
			convey.Convey("process err", func() {
				defer gomonkey.ApplyFunc(metadata.ProcessUpdate, func(key string, value []byte, source etcd3.EtcdType) error {
					return errors.New("")
				}).Reset()
				convey.So(processFunctionEvent(&etcd3.Event{Type: etcd3.PUT}), convey.ShouldBeError)
			})
			convey.Convey("process ok", func() {
				defer gomonkey.ApplyFunc(metadata.ProcessUpdate, func(key string, value []byte, source etcd3.EtcdType) error {
					return nil
				}).Reset()
				convey.So(processFunctionEvent(&etcd3.Event{Type: etcd3.PUT}), convey.ShouldBeNil)
			})
		})
		convey.Convey("process delete event", func() {
			convey.Convey("process err", func() {
				defer gomonkey.ApplyFunc(metadata.ProcessDelete, func(key string, source etcd3.EtcdType) error {
					return errors.New("")
				}).Reset()
				convey.So(processFunctionEvent(&etcd3.Event{Type: etcd3.DELETE}), convey.ShouldBeError)
			})
			convey.Convey("process ok", func() {
				defer gomonkey.ApplyFunc(metadata.ProcessDelete, func(key string, source etcd3.EtcdType) error {
					return nil
				}).Reset()
				convey.So(processFunctionEvent(&etcd3.Event{Type: etcd3.DELETE}), convey.ShouldBeNil)
			})
		})
		convey.Convey("process other event", func() {
			convey.So(processFunctionEvent(&etcd3.Event{Type: etcd3.ERROR}), convey.ShouldBeError)
			convey.So(processFunctionEvent(&etcd3.Event{Type: -1}), convey.ShouldBeError)

		})
	})
}

func TestIsFunction(t *testing.T) {
	convey.Convey("Test Is Function", t, func() {
		convey.So(isFunction(""), convey.ShouldBeFalse)
		convey.So(isFunction("/sn/functions/business/business/tenant/tenantid/11funasdfction1/funcname/version/version"), convey.ShouldBeFalse)
		convey.So(isFunction("/sn/functions/business/business/tenant/tenantid/function/funcname/version/version"), convey.ShouldBeTrue)
	})
}

func TestProcessAliasEvent(t *testing.T) {
	convey.Convey("Test Process Alias Event", t, func() {
		defer gomonkey.ApplyFunc(aliasroute.ProcessAliasEvent, func(event *etcd3.Event) error {
			return nil
		}).Reset()
		convey.So(processAliasEvent(&etcd3.Event{}), convey.ShouldBeNil)
	})
}
