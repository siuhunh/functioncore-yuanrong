package clientinfohandler

import (
	"encoding/json"
	bgutils "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
)

var (
	clientInfoHandler = NewEventHandler(&etcd3.EtcdClient{})
)

func TestClientInfoHandler_HandleEvent(t *testing.T) {
	Convey("Test clientInfoHandler handle event", t, func() {
		Convey("Case1 no match event type", func() {
			event := evs.Event{
				Kind:      "Not exist",
				EventType: "",
				KeyRef:    "",
			}
			err := clientInfoHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] event: Not exist failed to match event kind: ClientInfo")
		})
		Convey("Case2 parseClientInfo error", func() {
			event := evs.Event{
				Kind:      "ClientInfo",
				EventType: "Added",
				KeyRef:    "",
			}
			patch := gomonkey.ApplyFunc((*ClientInfoHandler).parseClientInfo, func(_ *ClientInfoHandler, event evs.Event) (*bridgetypes.ClientInstance, error) {
				return nil, errors.New("parse client info error")
			})
			err := clientInfoHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] ClientInfoHandler parsePodInfoByEvent invalid etcd event")
			patch.Reset()
		})
		Convey("Case4 event type is Deleted", func() {
			event := evs.Event{
				Kind:      "ClientInfo",
				EventType: "Deleted",
				KeyRef:    "",
				Object: &etcd3.Event{
					Key:       "/sn/bridgeclient/business/yrk/tenant/ID1/function/0@base@func/version/latest/defaultaz/defaultaz-#-poolXXXX",
					PrevValue: []byte(""),
				},
			}
			patch1 := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return nil
			})
			patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).ClearClientCache, func(_ *cache.BridgeCache,
				podName string, bridgeFuncName string, tenantId string) error {
				return nil
			})
			err := clientInfoHandler.HandleEvent(event)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch2.Reset()
		})
		Convey("Case4 event type is not Deleted", func() {
			event := evs.Event{
				Kind:      "ClientInfo",
				EventType: "Added",
				KeyRef:    "",
				Object: &etcd3.Event{
					Key:   "/sn/bridgeclient/business/yrk/tenant/ID1/function/0@base@func/version/latest/defaultaz/defaultaz-#-poolXXXX",
					Value: []byte(""),
				},
			}
			patch1 := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return nil
			})
			patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).RefreshClientCache, func(_ *cache.BridgeCache, podName string, clientInstance *bridgetypes.ClientInstance) error {
				return nil
			})
			err := clientInfoHandler.HandleEvent(event)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch2.Reset()
		})
		Convey("Case5 event type is Added and belong the current cluster", func() {
			event := evs.Event{
				Kind:      "ClientInfo",
				EventType: "Added",
				KeyRef:    "",
				Object: &etcd3.Event{
					Key:       "/sn/bridgeclient/business/yrk/tenant/ID1/function/0@base@func/version/latest/defaultaz/defaultaz-#-poolXXXX",
					PrevValue: []byte(""),
				},
			}
			clientInstance := &bridgetypes.ClientInstance{
				LastModifyClusterID: "cluster1",
			}
			patch1 := gomonkey.ApplyGlobalVar(&bgutils.ClusterID, "cluster1")
			patch2 := gomonkey.ApplyFunc((*ClientInfoHandler).parseClientInfo, func(_ *ClientInfoHandler, event evs.Event) (*bridgetypes.ClientInstance, error) {
				return clientInstance, nil
			})
			err := clientInfoHandler.HandleEvent(event)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch2.Reset()
		})
	})
}

func TestClientInfoHandler_parseClientInfo(t *testing.T) {
	Convey("Test parse state info invalid etcd event", t, func() {
		event := evs.Event{
			Object: "invalid",
		}
		_, err := clientInfoHandler.parseClientInfo(event)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] ClientInfoHandler parseClientInfo invalid etcd event")
	})
	Convey("Test other event unmarshal error", t, func() {
		event := evs.Event{
			EventType: "Added",
			Object: &etcd3.Event{
				Value: []byte(""),
			},
		}
		clientInstance, err := clientInfoHandler.parseClientInfo(event)
		So(clientInstance, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})
	Convey("Test event unmarshal successfully", t, func() {
		event := evs.Event{
			EventType: "Added",
			Object: &etcd3.Event{
				Key:   "/sn/bridgeclient/business/yrk/tenant/ID1/function/0@base@func/version/latest/defaultaz/defaultaz-#-poolXXXX",
				Value: []byte(""),
			},
		}
		patch := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
			return nil
		})
		clientInstance, err := clientInfoHandler.parseClientInfo(event)
		So(clientInstance.PodName, ShouldEqual, "poolXXXX")
		So(err, ShouldBeNil)
		patch.Reset()
	})
}
