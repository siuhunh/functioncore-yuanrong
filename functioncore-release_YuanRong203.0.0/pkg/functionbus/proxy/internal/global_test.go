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

package internal

import (
	"net"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestGetEnvs(t *testing.T) {
	SelfNodeIP = "a"
	SelfPodIP = "b"
	HostName = "c"

	os.Setenv("REQUESTS_LIMIT", "100")
	os.Setenv("MAX_STORE_RESPONSE_SIZE", "100")

	patches := gomonkey.NewPatches()
	patches.ApplyFunc(net.ParseIP, func(s string) net.IP {
		return net.IP{}
	})
	defer patches.Reset()

	err := GetEnvs()
	assert.NoError(t, err)
}

func TestGetEnvsError(t *testing.T) {
	convey.Convey("TestGetEnvsError", t, func() {
		convey.Convey("errNodeIP", func() {
			SelfNodeIP = ""
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(net.ParseIP, func(s string) net.IP {
				return net.IP{}
			})
			defer patches.Reset()
			err := GetEnvs()
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("errPodIP", func() {
			SelfNodeIP = "a"
			SelfPodIP = ""
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(net.ParseIP, func(s string) net.IP {
				return net.IP{}
			})
			defer patches.Reset()
			err := GetEnvs()
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("errNodeName", func() {
			SelfNodeIP = "a"
			SelfPodIP = "b"
			HostName = ""
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(net.ParseIP, func(s string) net.IP {
				return net.IP{}
			})
			defer patches.Reset()
			err := GetEnvs()
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("DISABLE_BACK_PRESSURE", func() {
			SelfNodeIP = "a"
			SelfPodIP = "b"
			HostName = "c"
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(net.ParseIP, func(s string) net.IP {
				return net.IP{}
			})
			defer patches.Reset()
			os.Setenv("REQUESTS_LIMIT", "100")
			os.Setenv("MAX_STORE_RESPONSE_SIZE", "100")
			os.Setenv("DISABLE_BACK_PRESSURE", "true")
			err := GetEnvs()
			convey.So(err, convey.ShouldBeNil)
			convey.So(DisableBackPressure, convey.ShouldEqual, true)
		})
	})
}

func TestChangeOwnedInstances(t *testing.T) {
	convey.Convey("TestChangeOwnedInstances", t, func() {
		convey.Convey("update OwnedInstances map", func() {
			SaveOwnedInstances("instance1", "test1", true)
			UpdateOwnedInstances("instance1", true)
			convey.So(OwnedInstances.InstanceMap["instance1"].IsBrokenConnection, convey.ShouldBeTrue)
			SaveOwnedInstances("instance1", "test1", false)
			UpdateOwnedInstances("instance1", true)
			convey.So(OwnedInstances.InstanceMap["instance1"].IsBrokenConnection, convey.ShouldBeFalse)
			DeleteOwnedInstances("instance1")
			UpdateOwnedInstances("instance1", true)
			convey.So(len(OwnedInstances.InstanceMap), convey.ShouldEqual, 0)
			DeleteOwnedInstances("instance1")
			UpdateOwnedInstances("instance1", false)
			convey.So(len(OwnedInstances.InstanceMap), convey.ShouldEqual, 0)
		})
	})
}
