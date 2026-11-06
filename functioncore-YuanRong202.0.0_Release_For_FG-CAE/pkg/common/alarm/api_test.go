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

package alarm

import (
	"fmt"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snapshotmanager"
)

func TestClient_RegisterAlarmType(t *testing.T) {
	convey.Convey("SnapshotManagerMode true", t, func() {
		snapshotmanager.SetSnapshotManagerMode(true)
		defer snapshotmanager.SetSnapshotManagerMode(false)
		c := Client{
			alarmMetas: make(map[Type]*alarmMeta, 0),
		}
		c.RegisterAlarmType(RouterEtcdConnection)
		convey.So(c.alarmMetas[RouterEtcdConnection], convey.ShouldBeNil)
	})
}

func TestClient_ClearAlarm(t *testing.T) {
	convey.Convey("SnapshotManagerMode true", t, func() {
		stopCh := make(chan struct{})
		err := NewClient("test-url", constants.FunctionTask, stopCh, WithAuth("", ""),
			WithTLSEnable(nil))
		defer func() {
			close(stopCh)
			close(GetClient().sendCh)
		}()
		convey.So(err, convey.ShouldBeNil)

		// report alarm after registration
		GetClient().RegisterAlarmType(RouterEtcdRegistry)
		assertRegistered(t, RouterEtcdRegistry)
		GetClient().ReportAlarm(RouterEtcdRegistry, fmt.Sprintf("error msg=etcd lease failed for %v,", time.Duration(1)*time.Second))
		convey.So(client.alarmMetas[RouterEtcdRegistry].isNormal, convey.ShouldBeFalse)
		convey.So(client.alarmMetas[RouterEtcdRegistry].num, convey.ShouldEqual, 1)
		snapshotmanager.SetSnapshotManagerMode(true)
		defer snapshotmanager.SetSnapshotManagerMode(false)
		GetClient().ClearAlarm(RouterEtcdRegistry)
		convey.So(client.alarmMetas[RouterEtcdRegistry].isNormal, convey.ShouldBeFalse)
		convey.So(client.alarmMetas[RouterEtcdRegistry].num, convey.ShouldEqual, 1)
	})
}
