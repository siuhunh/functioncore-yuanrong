package functionrepohandler

import (
	"encoding/json"
	"strings"
	"sync"
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
)

var (
	functionRepoHandler = NewEventHandler(&etcd3.EtcdClient{})
)

func TestFunctionRepoHandler_HandleEvent(t *testing.T) {
	Convey("Test functionRepoHandler handle event", t, func() {
		Convey("Case1 no match event type", func() {
			event := evs.Event{
				Kind:      "Not exist",
				EventType: "",
				KeyRef:    "",
			}
			err := functionRepoHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(strings.Contains(err.Error(), "[bridgeplugin] event: Not exist failed to match event kind: FunctionRepo"), ShouldBeTrue)
		})
		Convey("Case2 parseFunctionMetaInfo error", func() {
			event := evs.Event{
				Kind:      "FunctionRepo",
				EventType: "Added",
				KeyRef:    "",
			}
			patch := gomonkey.ApplyFunc((*FunctionRepoHandler).parseFunctionMetaInfo, func(_ *FunctionRepoHandler, event evs.Event) (*config.FunctionMetaInfo, error) {
				return nil, errors.New("parse function meta info error")
			})
			err := functionRepoHandler.HandleEvent(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "parse function meta info error")
			patch.Reset()
		})
		Convey("Case3 functionMeta not bridgefunc", func() {
			event := evs.Event{
				Kind:      "FunctionRepo",
				EventType: "Added",
				KeyRef:    "",
			}
			patch := gomonkey.ApplyFunc((*FunctionRepoHandler).parseFunctionMetaInfo, func(_ *FunctionRepoHandler, event evs.Event) (*config.FunctionMetaInfo, error) {
				return &config.FunctionMetaInfo{
					FuncMetaData: config.FuncMetaData{
						IsBridgeFunction: false,
					},
				}, nil
			})
			err := functionRepoHandler.HandleEvent(event)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("Test functionMeta is bridgefunc", func() {
			Convey("Case4 event type is Deleted", func() {
				event := evs.Event{
					Kind:      "FunctionRepo",
					EventType: "Deleted",
					KeyRef:    "",
				}
				patch := gomonkey.ApplyFunc((*FunctionRepoHandler).parseFunctionMetaInfo, func(_ *FunctionRepoHandler, event evs.Event) (*config.FunctionMetaInfo, error) {
					return &config.FunctionMetaInfo{
						FuncMetaData: config.FuncMetaData{
							IsBridgeFunction: true,
							FunctionURN:      "testURN",
						},
					}, nil
				})
				err := functionRepoHandler.HandleEvent(event)
				So(err, ShouldBeNil)
				patch.Reset()
			})
			Convey("Case4 event type is not Deleted", func() {
				event := evs.Event{
					Kind:      "FunctionRepo",
					EventType: "Added",
					KeyRef:    "",
				}
				patch := gomonkey.ApplyFunc((*FunctionRepoHandler).parseFunctionMetaInfo, func(_ *FunctionRepoHandler, event evs.Event) (*config.FunctionMetaInfo, error) {
					return &config.FunctionMetaInfo{
						FuncMetaData: config.FuncMetaData{
							IsBridgeFunction: true,
							FunctionURN:      "testURN",
							FuncName:         "testFuncName",
						},
					}, nil
				})
				err := functionRepoHandler.HandleEvent(event)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "[bridgeplugin] env of bridgefunc [testFuncName] is empty")
				patch.Reset()
			})
		})
	})
}

func TestFunctionRepoHandler_parseFunctionMetaInfo(t *testing.T) {
	Convey("Test parse function meta info invalid etcd event", t, func() {
		event := evs.Event{
			Object: "invalid",
		}
		_, err := functionRepoHandler.parseFunctionMetaInfo(event)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] FunctionRepoHandler invalid etcd event")
	})
	Convey("Test delete event unmarshal error", t, func() {
		event := evs.Event{
			EventType: "Deleted",
			Object: &etcd3.Event{
				PrevValue: []byte(""),
			},
		}
		funcMetaInfo, err := functionRepoHandler.parseFunctionMetaInfo(event)
		So(funcMetaInfo, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})
	Convey("Test other event unmarshal error", t, func() {
		event := evs.Event{
			EventType: "Added",
			Object: &etcd3.Event{
				PrevValue: []byte(""),
			},
		}
		funcMetaInfo, err := functionRepoHandler.parseFunctionMetaInfo(event)
		So(funcMetaInfo, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})
	Convey("Test event unmarshal successfully", t, func() {
		event := evs.Event{
			EventType: "Added",
			Object: &etcd3.Event{
				PrevValue: []byte(""),
			},
		}
		patch := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
			return nil
		})
		funcMetaInfo, err := functionRepoHandler.parseFunctionMetaInfo(event)
		So(funcMetaInfo, ShouldNotBeNil)
		So(err, ShouldBeNil)
		patch.Reset()
	})
}

func TestReleaseFuncRelatedBridgeRef(t *testing.T) {
	Convey("Test releaseFuncRelatedBridgeRef ok", t, func() {
		bridgeCache := cache.Instance()
		bridgeCache.StateIDInfoCache.Store(20, &cache.StateIDInfo{})
		bridgeCache.StateIDInfoCache.Store("stateid1", "xxx")
		bridgeCache.StateIDInfoCache.Store("stateid2", &cache.StateIDInfo{BridgeFuncURN: "funcURN"})
		_, ok := bridgeCache.StateIDInfoCache.Load("stateid2")
		So(ok, ShouldBeTrue)
		funcMeta := &config.FunctionMetaInfo{FuncMetaData: config.FuncMetaData{FunctionURN: "funcURN"}}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					bridgeCache.StateIDInfoCache.Delete("stateid2")
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		releaseFuncRelatedBridgeRef(bridgeCache, funcMeta, "traceid")
		_, ok = bridgeCache.StateIDInfoCache.Load("stateid2")
		So(ok, ShouldBeFalse)
	})
	Convey("Test releaseFuncRelatedBridgeRef concurrently ok", t, func() {
		bridgeCache := cache.Instance()
		bridgeCache.StateIDInfoCache.Store(20, &cache.StateIDInfo{})
		bridgeCache.StateIDInfoCache.Store("stateid1", "xxx")
		bridgeCache.StateIDInfoCache.Store("stateid2", &cache.StateIDInfo{BridgeFuncURN: "funcURN"})
		_, ok := bridgeCache.StateIDInfoCache.Load("stateid2")
		So(ok, ShouldBeTrue)
		funcMeta := &config.FunctionMetaInfo{FuncMetaData: config.FuncMetaData{FunctionURN: "funcURN"}}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					bridgeCache.StateIDInfoCache.Delete("stateid2")
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		wait := sync.WaitGroup{}
		concurrent := 50
		wait.Add(concurrent)
		for i := 0; i < concurrent; i++ {
			go func() {
				releaseFuncRelatedBridgeRef(bridgeCache, funcMeta, "traceid")
				wait.Done()
			}()
		}
		wait.Wait()
		_, ok = bridgeCache.StateIDInfoCache.Load("stateid2")
		So(ok, ShouldBeFalse)
	})
}
