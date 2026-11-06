/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package stateworkershandler -
package stateworkershandler

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

func TestNewEventHandler(t *testing.T) {
	convey.Convey("Test NewEventHandler", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		convey.So(handler, convey.ShouldNotBeNil)
	})
}

func TestHandleEvent(t *testing.T) {
	convey.Convey("Test Handle delete Event ok", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371",
			EventType: eventsource.Deleted,
			Kind:      utils.StateWorkersEventKind,
		})
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test Handle update Event ok", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", stateInfo)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyMethod(bridgeCache, "StoreSpecificStateIDInfoToEtcd",
				func(b *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		jsonStr, _ := json.Marshal(map[string]string{"cluster": "cluster002"})
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371",
			Kind: utils.StateWorkersEventKind,
			Object: &etcd3.Event{
				Value: jsonStr,
			},
		})
		convey.So(err, convey.ShouldBeNil)
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster002")
	})
	convey.Convey("Test Handle update Event nok", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", stateInfo)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyMethod(bridgeCache, "StoreSpecificStateIDInfoToEtcd",
				func(b *cache.BridgeCache, stateID string, traceID string) error {
					return errors.New("StoreSpecificStateIDInfoToEtcd failed")
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		jsonStr, _ := json.Marshal(map[string]string{"cluster": "cluster002"})
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371",
			Kind: utils.StateWorkersEventKind,
			Object: &etcd3.Event{
				Value: jsonStr,
			},
		})
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
	})
	convey.Convey("Test HandleEvent type nok", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371",
			EventType: eventsource.Deleted,
		})
		convey.So(strings.Contains(err.Error(), "event type not match, can matched type"), convey.ShouldBeTrue)
	})
	convey.Convey("Test Handle delete Event ok", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371",
			EventType: eventsource.Deleted,
			Kind:      utils.StateWorkersEventKind,
		})
		convey.So(strings.Contains(err.Error(), "is illegal"), convey.ShouldBeTrue)
	})
	convey.Convey("Test Handle update Event bridgeid not found", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", stateInfo)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyMethod(bridgeCache, "StoreSpecificStateIDInfoToEtcd",
				func(b *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		jsonStr, _ := json.Marshal(map[string]string{"cluster": "cluster002"})
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9372",
			Kind: utils.StateWorkersEventKind,
			Object: &etcd3.Event{
				Value: jsonStr,
			},
		})
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test HandleEvent loaded from stateInfoCache assert err", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", "stateInfo")
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyMethod(bridgeCache, "StoreSpecificStateIDInfoToEtcd",
				func(b *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		jsonStr, _ := json.Marshal(map[string]string{"cluster": "cluster002"})
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371",
			Kind: utils.StateWorkersEventKind,
			Object: &etcd3.Event{
				Value: jsonStr,
			},
		})
		convey.So(strings.Contains(err.Error(), "loaded from stateInfoCache assert err"), convey.ShouldBeTrue)
	})
	convey.Convey("Test HandleEvent event object assert etcd3.Event failed", t, func() {
		handler := NewEventHandler(&etcd3.EtcdClient{})
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", stateInfo)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyMethod(bridgeCache, "StoreSpecificStateIDInfoToEtcd",
				func(b *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
		err := handler.HandleEvent(eventsource.Event{
			KeyRef: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version" +
				"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371",
			Kind:   utils.StateWorkersEventKind,
			Object: "",
		})
		convey.So(strings.Contains(err.Error(), "event object assert etcd3.Event failed"), convey.ShouldBeTrue)
	})
}
