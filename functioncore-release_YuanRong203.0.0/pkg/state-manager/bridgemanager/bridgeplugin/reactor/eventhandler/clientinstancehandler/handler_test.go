package clientinstancehandler

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	mockUtils "functioncore/test/utils"
)

var (
	clientInstHandler = NewEventHandler(&etcd3.EtcdClient{})
)

func TestClientInstanceHandler_HandleEvent(t *testing.T) {
	Convey("Test clientInstanceHandler handle event", t, func() {
		Convey("Case1 no match event type", func() {
			event := evs.Event{
				Kind:      "Not exist",
				EventType: "",
				KeyRef:    "",
			}
			err := clientInstHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] event: Not exist failed to match event kind: ClientInstance")
		})
		Convey("Case2 not delete type", func() {
			event := evs.Event{
				Kind:      "ClientInstance",
				EventType: "Added",
				KeyRef:    "",
			}
			err := clientInstHandler.HandleEvent(event)
			So(err, ShouldBeNil)
		})
		Convey("Test event type and kind matched", func() {
			event := evs.Event{
				Kind:      "ClientInstance",
				EventType: "Deleted",
				KeyRef:    "",
			}
			Convey("Case3 parseInstanceMetaInfo error", func() {
				patch := gomonkey.ApplyFunc((*ClientInstanceHandler).parseInstanceMetaInfo, func(_ *ClientInstanceHandler, event evs.Event) (bgtypes.PodInfo, error) {
					return bgtypes.PodInfo{}, errors.New("parse client instance meta info error")
				})
				err := clientInstHandler.HandleEvent(event)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "parse client instance meta info error")
				patch.Reset()
			})
			Convey("Case4 parseInstanceMetaInfo success and release", func() {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*ClientInstanceHandler).parseInstanceMetaInfo,
						func(_ *ClientInstanceHandler, event evs.Event) (bgtypes.PodInfo, error) {
							return bgtypes.PodInfo{}, nil
						}),
					gomonkey.ApplyFunc(action.ReleaseBridge,
						func(bgInst *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
							return false, nil
						}),
				})
				cache.Instance().ClientPodClientInstCache.Store("clientPod", &bgtypes.ClientInstance{})
				err := clientInstHandler.HandleEvent(event)
				So(err, ShouldBeNil)
				patches.ResetAll()
			})
		})
	})
}

func TestClientInstanceHandler_parseInstanceMetaInfo(t *testing.T) {
	Convey("Test parse bridge invalid etcd event", t, func() {
		event := evs.Event{
			Object: "invalid",
		}
		_, err := clientInstHandler.parseInstanceMetaInfo(event)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] ClientInstanceHandler invalid etcd event")
	})
	Convey("Test parseInstanceMetaInfo successfully", t, func() {
		event := evs.Event{
			Object: &etcd3.Event{
				Key: "/sn/workers/business/yrk/tenant/ID1/function/0@base@client/version/latest/defaultaz/defaultaz-#-pool0-xxxx",
			},
		}
		podInfo, err := clientInstHandler.parseInstanceMetaInfo(event)
		So(err, ShouldBeNil)
		So(podInfo.ClientPodName, ShouldEqual, "pool0-xxxx")
		So(podInfo.BridgeFuncName, ShouldEqual, "client")
		So(podInfo.TenantID, ShouldEqual, "ID1")
	})
}

func TestClientInstanceHandler_getClientInstance(t *testing.T) {
	Convey("Test get client instance not exist", t, func() {
		clientInstance, err := clientInstHandler.getClientInstance("invalid", "invalid", "invalid")
		So(err, ShouldBeNil)
		So(clientInstance, ShouldBeNil)
	})
	Convey("Test get client instance exist but invalid", t, func() {
		cache.Instance().ClientPodClientInstCache.Store("pod_tenantId_rds", "invalid")
		clientInstance, err := clientInstHandler.getClientInstance("pod", "rds", "tenantId")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] failed to assert type *ClientInstance in getClientInstance")
		So(clientInstance, ShouldBeNil)
	})
	Convey("Test get client instance successfully", t, func() {
		cache.Instance().ClientPodClientInstCache.Store("pod_tenantId_rds", &bgtypes.ClientInstance{
			PodName: "pod",
		})
		clientInstance, err := clientInstHandler.getClientInstance("pod", "rds", "tenantId")
		So(err, ShouldBeNil)
		So(clientInstance.PodName, ShouldEqual, "pod")
	})
}

func TestClientInstanceHandler_processReleaseInfoByClientCited(t *testing.T) {
	Convey("Test processReleaseInfoByClientCited", t, func() {
		podInfo := bgtypes.PodInfo{
			ClientPodName:  "ClientPodName",
			BridgeFuncName: "BridgeFuncName",
			TenantID:       "TenantID",
		}
		event := evs.Event{
			Object: "invalid",
		}
		citeBridgeInfo := make([]string, 0)
		citeBridgeInfo = append(citeBridgeInfo, "123")
		citeBridgeInfo = append(citeBridgeInfo, "456")
		cache.Instance().ClientCiteBridgeFuncMap.Store(podInfo.ClientPodName, citeBridgeInfo)
		err := clientInstHandler.processReleaseInfoByClientCited(podInfo, event)
		So(err, ShouldBeNil)
	})
}
