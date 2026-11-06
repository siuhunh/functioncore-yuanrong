/* Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package workermanagerinstancehandler is handler of worker-manager
package workermanagerinstancehandler

import (
	"errors"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/nodemgr"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	statemanagerutils "functioncore/pkg/state-manager/utils"
)

var (
	handler = NewEventHandler(&etcd3.EtcdClient{})
)

func TestHandler_HandleEvent(t *testing.T) {
	convey.Convey("Test handle event", t, func() {
		convey.Convey("Case1: no match event kind", func() {
			event := evs.Event{
				Kind:      "Not exist",
				EventType: "",
				KeyRef:    "",
			}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldNotBeNil)
			convey.ShouldBeTrue(strings.Contains(err.Error(), "event type not match"))
		})
		convey.Convey("Case2: failed to parse worker-manager cluster info", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.ParseWorkerManagerClusterInfoFromKey, func(key string) (string, string, error) {
					return "", "", errors.New("error")
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			event := evs.Event{
				Kind:      "WorkerManagerInstance",
				EventType: "Added",
				KeyRef:    "error",
			}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldNotBeNil)
			convey.ShouldBeTrue(strings.Contains(err.Error(), "error"))
		})
		convey.Convey("Case3: add event, succeed", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.ParseWorkerManagerClusterInfoFromKey, func(key string) (string, string, error) {
					return "cluster", "node", nil
				}),
				gomonkey.ApplyFunc(statemanagerutils.GetClusterID, func() string {
					return "cluster"
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			event := evs.Event{
				Kind:      "WorkerManagerInstance",
				EventType: "Added",
				KeyRef:    "key",
			}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldBeNil)
			convey.So(nodemgr.Instance().GetNode(), convey.ShouldEqual, "node")
			nodemgr.Instance().RemoveNode()
		})
		convey.Convey("Case4: delete event, succeed", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.ParseWorkerManagerClusterInfoFromKey, func(key string) (string, string, error) {
					return "cluster", "node", nil
				}),
				gomonkey.ApplyFunc(statemanagerutils.GetClusterID, func() string {
					return "cluster"
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			event := evs.Event{
				Kind:      "WorkerManagerInstance",
				EventType: "Deleted",
				KeyRef:    "key",
			}
			nodemgr.Instance().AddNode("node")
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldBeNil)
			convey.So(nodemgr.Instance().GetNode(), convey.ShouldEqual, "")
		})
		convey.Convey("Case6: add event, clusterID not match", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.ParseWorkerManagerClusterInfoFromKey, func(key string) (string, string, error) {
					return "cluster-other", "node", nil
				}),
				gomonkey.ApplyFunc(statemanagerutils.GetClusterID, func() string {
					return "cluster"
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			event := evs.Event{
				Kind:      "WorkerManagerInstance",
				EventType: "Added",
				KeyRef:    "key",
			}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldBeNil)
			convey.So(nodemgr.Instance().GetNode(), convey.ShouldEqual, "")
		})
	})
}
