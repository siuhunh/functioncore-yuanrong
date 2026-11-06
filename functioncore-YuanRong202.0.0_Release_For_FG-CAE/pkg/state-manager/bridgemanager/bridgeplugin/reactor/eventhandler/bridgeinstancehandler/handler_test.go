package bridgeinstancehandler

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	mockUtils "functioncore/test/utils"
)

var (
	bridgeInstHandler = NewEventHandler(&etcd3.EtcdClient{})
)

func TestBridgeInstanceHandler_HandleEvent(t *testing.T) {
	Convey("Test bridgeInstanceHandler handle event", t, func() {
		Convey("Case1 no match event type", func() {
			event := evs.Event{
				Kind:      "Not exist",
				EventType: "",
				KeyRef:    "",
			}
			err := bridgeInstHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] event: Not exist failed to match event kind: BridgeInstance")
		})
		Convey("Case2 no available bridgeCache when bridgeInstance delete crash handle event", func() {
			event := evs.Event{
				Kind:      "BridgeInstance",
				EventType: "Deleted",
				KeyRef:    "",
				Object: &etcd3.Event{
					Key: "/sn/workers/business/yrk/tenant/ID1/function/0@base@rds/version/latest/defaultaz/defaultaz-#-pool0-xxxx",
					PrevValue: []byte(`{"ip":"xxxx","port":"xxxx","status":"ready","p2pPort":"xxxx","nodeIP":"xxxx","nodePort":"xxxx",
					"stateID":"IDTest","applier":"xxxx","ownerIP":"xxxx","cpu":800,"memory":768}`),
					Value: nil,
				},
			}
			cache.Instance().StateIDInfoCache.Store("IDTest", &bgtypes.ClientInstance{})
			defer cache.Instance().StateIDInfoCache.Delete("IDTest")
			patch := gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide, func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
				return nil
			})
			defer patch.Reset()
			err := bridgeInstHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] failed to get type StateIDInfo. key: IDTest")
		})
		Convey("Case3 bridgeInstance delete crash handle event", func() {
			event := evs.Event{
				Kind:      "BridgeInstance",
				EventType: "Deleted",
				KeyRef:    "",
				Object: &etcd3.Event{
					Key: "/sn/workers/business/yrk/tenant/ID1/function/0@base@rds/version/latest/defaultaz/defaultaz-#-pool0-xxxx",
					PrevValue: []byte(`{"ip":"xxxx","port":"xxxx","status":"ready","p2pPort":"xxxx","nodeIP":"xxxx","nodePort":"xxxx",
					"stateID":"IDTest","applier":"xxxx","ownerIP":"xxxx","cpu":800,"memory":768}`),
					Value: nil,
				},
			}
			cache.Instance().StateIDInfoCache.Store("IDTest", &cache.StateIDInfo{})
			defer cache.Instance().StateIDInfoCache.Delete("IDTest")
			patch := gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide, func(bgInst *bgtypes.BridgeInstance,
				stateID string, traceID string) error {
				return nil
			})
			defer patch.Reset()
			err := bridgeInstHandler.HandleEvent(event)
			So(err, ShouldBeNil)
		})
		Convey("Test event type and kind matched", func() {
			event := evs.Event{
				Kind:      "BridgeInstance",
				EventType: "Added",
				KeyRef:    "",
			}
			Convey("Case3 parseBridgeInstMetaInfo error", func() {
				patch := gomonkey.ApplyFunc((*BridgeInstanceHandler).parseBridgeInstMetaInfo, func(_ *BridgeInstanceHandler, event evs.Event) (*config.WorkerInstance,
					*bgtypes.BridgeInstance, error) {
					return nil, nil, errors.New("parse bridge instance meta info error")
				})
				err := bridgeInstHandler.HandleEvent(event)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "parse bridge instance meta info error")
				patch.Reset()
			})
			Convey("Case4 parseBridgeInstMetaInfo success and NewBridge", func() {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*BridgeInstanceHandler).parseBridgeInstMetaInfo, func(_ *BridgeInstanceHandler, event evs.Event) (*config.WorkerInstance,
						*bgtypes.BridgeInstance, error) {
						bridgeInstanceMeta := &config.WorkerInstance{
							StateID: "testID",
						}
						bridgeInstance := &bgtypes.BridgeInstance{}
						return bridgeInstanceMeta, bridgeInstance, nil
					}),
					gomonkey.ApplyFunc(action.NewBridge, func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
						return nil
					}),
				})
				cache.Instance().StateIDInfoCache.Store("testID", &cache.StateIDInfo{})
				err := bridgeInstHandler.HandleEvent(event)
				So(err, ShouldBeNil)
				patches.ResetAll()
			})
		})
	})
}

func TestBridgeInstanceHandler_parseBridgeInstMetaInfo(t *testing.T) {
	Convey("Test parse bridge invalid etcd event", t, func() {
		event := evs.Event{
			Object: "invalid",
		}
		_, _, err := bridgeInstHandler.parseBridgeInstMetaInfo(event)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] parse BridgeInstanceHandler invalid etcd event")
	})
	Convey("Test unmarshal failed", t, func() {
		event := evs.Event{
			Object: &etcd3.Event{
				Key:   "/sn/workers/business/yrk/tenant/ID1/function/0@base@rds/version/latest/defaultaz/defaultaz-#-pool0-xxxx",
				Value: []byte(`aaa`),
			},
		}
		_, _, err := bridgeInstHandler.parseBridgeInstMetaInfo(event)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "invalid character 'a' looking for beginning of value")
	})
	Convey("Test parse bridge valid etcd event", t, func() {
		event := evs.Event{
			Object: &etcd3.Event{
				Key: "/sn/workers/business/yrk/tenant/ID1/function/0@base@rds/version/latest/defaultaz/defaultaz-#-pool0-xxxx",
				Value: []byte(`{"ip":"xxxx","port":"xxxx","status":"ready","p2pPort":"xxxx","nodeIP":"xxxx","nodePort":"xxxx",
"stateID":"IDtest","applier":"xxxx","ownerIP":"xxxx","cpu":800,"memory":768}`),
			},
		}
		workerInstance, bridgeInstance, err := bridgeInstHandler.parseBridgeInstMetaInfo(event)
		So(err, ShouldBeNil)
		So(workerInstance.StateID, ShouldEqual, "IDtest")
		So(bridgeInstance.PodName, ShouldEqual, "pool0-xxxx")
	})
}
