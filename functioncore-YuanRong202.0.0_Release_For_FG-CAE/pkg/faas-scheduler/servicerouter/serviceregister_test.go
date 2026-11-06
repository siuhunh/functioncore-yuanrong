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

package servicerouter

import (
	"functioncore/pkg/faas-scheduler/config"
	"github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

type mockServiceRegister struct {
	config Config
}

func (register *mockServiceRegister) Register(c Config) error {
	register.config = c
	return nil
}

func (register *mockServiceRegister) UpdateConfig(c Config) {
	register.config = c
}

func setEnv() {
	os.Setenv("CLUSTER_ID", "cluster001")
	os.Setenv("NODE_IP", "127.0.0.1")
	os.Setenv("POD_NAME", "faas-scheduler-5b5886db99-66gn8")
	os.Setenv("POD_IP", "127.0.0.1")
}

func TestRegisterService(t *testing.T) {
	convey.Convey("TestRegisterService", t, func() {
		config.InitDefaultConfig()
		setEnv()
		register := &mockServiceRegister{}
		RegisterAdapter = register
		stopCh := make(chan struct{})
		err := RegisterService(stopCh)
		convey.So(err, convey.ShouldBeNil)
		convey.So(register.config.Key, convey.ShouldEqual,
			"/sn/faas-scheduler/instances/cluster001/127.0.0.1/faas-scheduler-5b5886db99-66gn8")
	})
}

func TestValidateEnvs(t *testing.T) {
	convey.Convey("test validateEnvs", t, func() {
		convey.Convey("case1: success", func() {
			err := validateEnvs("clusterID", "nodeIP", "podName", "podIP")
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case2: error", func() {
			err := validateEnvs("", "", "", "")
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}
