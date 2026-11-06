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

package network

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/worker/utils"
)

func TestPingPingRetry(t *testing.T) {
	pinger := NewPinger("127.0.0.1", "123")

	Convey("TestPingPingRetry", t, func() {
		Convey("test change vpc success", func() {
			patches := NewPatches()
			patches.ApplyFunc((*exec.Cmd).Run, func(c *exec.Cmd) error {
				return nil
			})
			patches.ApplyFunc(utils.GetHash, func(path string) (hashValue string) {
				return "123"
			})
			patches.ApplyFunc(time.Sleep, func(d time.Duration) {
			})
			defer patches.Reset()
			err := pinger.PingRetry(1)
			So(err, ShouldEqual, nil)
		})

		Convey("failed to check ping bin", func() {
			patches := NewPatches()
			patches.ApplyFunc((*exec.Cmd).Run, func(c *exec.Cmd) error {
				return nil
			})
			patches.ApplyFunc(utils.GetHash, func(path string) (hashValue string) {
				return "456"
			})
			patches.ApplyFunc(time.Sleep, func(d time.Duration) {
			})
			defer patches.Reset()
			err := pinger.PingRetry(1)
			So(err, ShouldNotBeNil)
		})

		Convey("test change vpc failed", func() {
			retryNum := 0
			patches := NewPatches()
			patches.ApplyFunc((*exec.Cmd).Run, func(c *exec.Cmd) error {
				retryNum++
				return errors.New("test")
			})
			patches.ApplyFunc(utils.GetHash, func(path string) (hashValue string) {
				return "123"
			})
			patches.ApplyFunc(time.Sleep, func(d time.Duration) {
			})
			defer patches.Reset()
			err := pinger.PingRetry(defaultHealthCheckRetry)
			So(err, ShouldNotEqual, nil)
			So(retryNum, ShouldEqual, defaultHealthCheckRetry)
		})
	})
}

func TestPing6Retry(t *testing.T) {
	pinger := NewPinger("2c0f:9981:0:0:0:0:0:0", "123")

	Convey("TestPing6Retry", t, func() {
		Convey("test change vpc success", func() {
			patches := NewPatches()
			patches.ApplyFunc((*exec.Cmd).Run, func(c *exec.Cmd) error {
				return nil
			})
			patches.ApplyFunc(utils.GetHash, func(path string) (hashValue string) {
				return "123"
			})
			patches.ApplyFunc(time.Sleep, func(d time.Duration) {
			})
			defer patches.Reset()
			err := pinger.Ping6Retry(1)
			So(err, ShouldEqual, nil)
		})

		Convey("failed to check ping bin", func() {
			patches := NewPatches()
			patches.ApplyFunc((*exec.Cmd).Run, func(c *exec.Cmd) error {
				return nil
			})
			patches.ApplyFunc(utils.GetHash, func(path string) (hashValue string) {
				return "456"
			})
			patches.ApplyFunc(time.Sleep, func(d time.Duration) {
			})
			defer patches.Reset()
			err := pinger.Ping6Retry(1)
			So(err, ShouldNotBeNil)
		})

		Convey("test change vpc failed", func() {
			retryNum := 0
			patches := NewPatches()
			patches.ApplyFunc((*exec.Cmd).Run, func(c *exec.Cmd) error {
				retryNum++
				return errors.New("test")
			})
			patches.ApplyFunc(utils.GetHash, func(path string) (hashValue string) {
				return "123"
			})
			patches.ApplyFunc(time.Sleep, func(d time.Duration) {
			})
			defer patches.Reset()
			err := pinger.Ping6Retry(defaultHealthCheckRetry)
			So(err, ShouldNotEqual, nil)
			So(retryNum, ShouldEqual, defaultHealthCheckRetry)
		})
	})
}

func TestKeepPing(t *testing.T) {
	pinger := NewPinger("127.0.0.1", "123")

	Convey("TestKeepPing", t, func() {
		Convey("test change vpc fail", func() {
			patch := ApplyFunc((*Pinger).PingRetry, func(p *Pinger, retryTimes int) error {
				return errors.New("failed to connect")
			})
			defer patch.Reset()
			err := pinger.DoHealthCheck(context.Background())
			So(err, ShouldNotEqual, nil)
		})
	})
}
