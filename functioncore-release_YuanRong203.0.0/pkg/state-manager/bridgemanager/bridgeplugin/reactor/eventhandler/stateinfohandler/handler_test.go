package stateinfohandler

import (
	"encoding/json"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	bgutils "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

var (
	stateInfoHandler = NewEventHandler(&etcd3.EtcdClient{})
)

func TestStateInfoHandler_HandleEvent(t *testing.T) {
	Convey("Test stateInfoHandler handle event", t, func() {
		Convey("Case1 no match event type", func() {
			event := evs.Event{
				Kind:      "Not exist",
				EventType: "",
				KeyRef:    "",
			}
			err := stateInfoHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] event: Not exist failed to match event kind: StateInfo")
		})
		Convey("Case2 parseStateIDInfo error", func() {
			event := evs.Event{
				Kind:      "StateInfo",
				EventType: "Added",
				KeyRef:    "",
			}
			patch := gomonkey.ApplyFunc((*StateInfoHandler).parseStateIDInfo, func(_ *StateInfoHandler, event evs.Event) (string, *cache.StateIDInfo, error) {
				return "", nil, errors.New("parse stateID info error")
			})
			err := stateInfoHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "parse stateID info error")
			patch.Reset()
		})
		Convey("Case4 event type is Deleted", func() {
			event := evs.Event{
				Kind:      "StateInfo",
				EventType: "Deleted",
				KeyRef:    "",
				Object: &etcd3.Event{
					Key:       "/sn/bridgemanager/business/yrk/tenant/ID1/function/0@base@func/version/latest/defaultaz/stateID1",
					PrevValue: []byte(""),
				},
			}
			patch1 := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return nil
			})
			patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).ClearStateInfoCache, func(_ *cache.BridgeCache, stateID string) error {
				return nil
			})
			err := stateInfoHandler.HandleEvent(event)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch2.Reset()
		})
		Convey("Case4 event type is not Deleted", func() {
			event := evs.Event{
				Kind:      "StateInfo",
				EventType: "Added",
				KeyRef:    "",
				Object: &etcd3.Event{
					Key:   "/sn/bridgemanager/business/yrk/tenant/ID1/function/0@base@func/version/latest/defaultaz/stateID1",
					Value: []byte(""),
				},
			}
			patch1 := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return nil
			})
			patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).RefreshStateInfoCache, func(_ *cache.BridgeCache, stateID string, stateIDInfo *cache.StateIDInfo) error {
				return nil
			})
			err := stateInfoHandler.HandleEvent(event)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch2.Reset()
		})
		Convey("Case5 event type is not Deleted and belong the current cluster", func() {
			event := evs.Event{
				Kind:      "StateInfo",
				EventType: "Added",
				KeyRef:    "",
			}
			patch1 := gomonkey.ApplyFunc((*StateInfoHandler).parseStateIDInfo, func(_ *StateInfoHandler, event evs.Event) (string, *cache.StateIDInfo, error) {
				return "stateID1", &cache.StateIDInfo{LastModifyCluster: "cluster1"}, nil
			})
			patch2 := gomonkey.ApplyGlobalVar(&bgutils.ClusterID, "cluster1")
			err := stateInfoHandler.HandleEvent(event)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch2.Reset()
		})
	})
}

func TestStateInfoHandler_parseStateIDInfo(t *testing.T) {
	Convey("Test parse state info invalid etcd event", t, func() {
		event := evs.Event{
			Object: "invalid",
		}
		_, _, err := stateInfoHandler.parseStateIDInfo(event)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] StateInfoHandler invalid etcd event")
	})
	Convey("Test other event unmarshal error", t, func() {
		event := evs.Event{
			EventType: "Added",
			Object: &etcd3.Event{
				Value: []byte(""),
			},
		}
		stateID, stateIDInfo, err := stateInfoHandler.parseStateIDInfo(event)
		So(stateID, ShouldEqual, "")
		So(stateIDInfo, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})
	Convey("Test event unmarshal successfully", t, func() {
		event := evs.Event{
			EventType: "Added",
			Object: &etcd3.Event{
				Key:   "/sn/bridgemanager/business/yrk/tenant/ID1/function/0@base@func/version/latest/defaultaz/stateID1",
				Value: []byte(""),
			},
		}
		patch := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
			return nil
		})
		stateID, _, err := stateInfoHandler.parseStateIDInfo(event)
		So(stateID, ShouldEqual, "stateID1")
		So(err, ShouldBeNil)
		patch.Reset()
	})
}
