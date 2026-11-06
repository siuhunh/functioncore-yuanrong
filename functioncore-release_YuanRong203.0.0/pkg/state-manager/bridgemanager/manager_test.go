package bridgemanager

import (
	"plugin"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"

	bgtypes "functioncore/pkg/common/types/bridgetypes"
	mockUtils "functioncore/test/utils"
)

func TestInstance(t *testing.T) {
	tests := []struct {
		name    string
		want    *BridgeHandler
		wantErr bool
	}{
		{
			name:    "test case",
			want:    &BridgeHandler{},
			wantErr: false,
		},
	}
	patch := gomonkey.ApplyFunc(initBridgeManager, func() (*BridgeHandler, error) {
		return &BridgeHandler{}, nil
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Instance()
			if (err != nil) != tt.wantErr {
				t.Errorf("Instance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Instance() got = %v, want %v", got, tt.want)
			}
		})
	}
	patch.Reset()
}

func Test_initBridgeManager(t *testing.T) {
	Convey("Test init bridgeManager", t, func() {
		Convey("Case1 plugin exist and init successfully", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(plugin.Open, func(path string) (*plugin.Plugin, error) {
					return &plugin.Plugin{}, nil
				}),
				gomonkey.ApplyFunc(initPlugin, func(pluginFile *plugin.Plugin) error {
					return nil
				}),
			})
			handler, err := initBridgeManager()
			So(handler, ShouldNotBeNil)
			So(err, ShouldBeNil)
			patches.ResetAll()
		})
		Convey("Case2 plugin exist but init failed", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(plugin.Open, func(path string) (*plugin.Plugin, error) {
					return &plugin.Plugin{}, nil
				}),
				gomonkey.ApplyFunc(initPlugin, func(pluginFile *plugin.Plugin) error {
					return errors.New("initPlugin failed")
				}),
			})
			handler, err := initBridgeManager()
			So(handler, ShouldBeNil)
			So(err, ShouldNotBeNil)
			patches.ResetAll()
		})
	})
}

func Test_initPlugin(t *testing.T) {
	Convey("Test init plugin", t, func() {
		pluginFile := &plugin.Plugin{}
		Convey("Case1 test init plugin failed", func() {
			patch := gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
				if s == "InitBridgeManager" {
					return func() error {
						return nil
					}, errors.New("init bridgeManager plugin error")
				}
				return func() {}, nil
			})
			defer patch.Reset()
			err := initPlugin(pluginFile)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "init bridgeManager plugin error")
		})
		Convey("Case2 test init plugin successfully", func() {
			patch := gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
				if s == "InitBridgeManager" {
					return func() error {
						return nil
					}, nil
				}
				return func() {}, nil
			})
			defer patch.Reset()
			err := initPlugin(pluginFile)
			So(err, ShouldBeNil)
		})
	})
}

func TestBridgeHandler_ApplyStateKey(t *testing.T) {
	Convey("Test ApplyStateKey in plugin", t, func() {
		b := &BridgeHandler{
			PluginBridge: &plugin.Plugin{},
		}
		Convey("Case1 ApplyStateKey in plugin failed", func() {
			patch := gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
				if s == "ApplyStateKey" {
					return func(clientInstance *bgtypes.ClientInstance, traceID string) (string, bool, error) {
						return "", false, errors.New("apply failed")
					}, nil
				}
				return func() {}, nil
			})
			defer patch.Reset()
			key, _, err := b.ApplyStateKey(&bgtypes.ClientInstance{}, "")
			So(key, ShouldEqual, "")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "apply failed")
		})
		Convey("Case1 ApplyStateKey in plugin successfully", func() {
			patch := gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
				if s == "ApplyStateKey" {
					return func(clientInstance *bgtypes.ClientInstance, traceID string) (string, bool, error) {
						return "stateKey", false, nil
					}, nil
				}
				return func() {}, nil
			})
			defer patch.Reset()
			key, _, err := b.ApplyStateKey(&bgtypes.ClientInstance{}, "")
			So(key, ShouldEqual, "stateKey")
			So(err, ShouldBeNil)
		})
	})
}

func TestBridgeHandler_ReleaseBridge(t *testing.T) {
	Convey("Test ReleaseBridge in plugin", t, func() {
		b := &BridgeHandler{
			PluginBridge: &plugin.Plugin{},
		}
		Convey("Case1 ReleaseBridge in plugin failed", func() {
			patch := gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
				if s == "ReleaseBridge" {
					return func(clientInstance *bgtypes.ClientInstance, traceID string) (bool, error) {
						return false, errors.New("release failed")
					}, nil
				}
				return func() {}, nil
			})
			defer patch.Reset()
			isNeedRelease, err := b.ReleaseBridge(&bgtypes.ClientInstance{}, "")
			So(isNeedRelease, ShouldBeFalse)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "release failed")
		})
		Convey("Case1 ReleaseBridge in plugin successfully", func() {
			patch := gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
				if s == "ReleaseBridge" {
					return func(clientInstance *bgtypes.ClientInstance, traceID string) (bool, error) {
						return true, nil
					}, nil
				}
				return func() {}, nil
			})
			defer patch.Reset()
			isNeedRelease, err := b.ReleaseBridge(&bgtypes.ClientInstance{}, "")
			So(isNeedRelease, ShouldBeTrue)
			So(err, ShouldBeNil)
		})
	})
}
