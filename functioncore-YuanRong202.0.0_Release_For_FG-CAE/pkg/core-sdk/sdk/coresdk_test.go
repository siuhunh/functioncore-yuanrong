/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

package sdk

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/valyala/fasthttp"
	"gotest.tools/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/core-sdk/common"
	"functioncore/pkg/core-sdk/communication"
	"functioncore/pkg/core-sdk/metrics"
	"functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/channelbus"
	buscommon "functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy"
	"functioncore/pkg/functionbus/proxy/channelbusconn"
	"functioncore/pkg/functionbus/utils"
	sc "functioncore/pkg/functionstate/client"
)

func TestInit(t *testing.T) {
	Convey("TestInit", t, func() {
		Convey("failed: InitRunLog error", func() {
			var stopCh <-chan struct{}
			err := errors.New("init logger error")
			patch := ApplyFunc(log.InitRunLog, func(_ string, _ string) error {
				return err
			})
			defer patch.Reset()
			resErr := Init(stopCh)
			So(resErr, ShouldNotEqual, nil)
		})
		Convey("failed: failed to init bus proxy", func() {
			var stopCh <-chan struct{}
			patch := ApplyFunc(log.InitRunLog, func(_ string, _ string) error {
				return nil
			})
			defer patch.Reset()
			patch = ApplyFunc(proxy.InitBusProxy, func(_ proxy.BusProxyConfig, _ <-chan struct{},
				_ channelbusconn.RegisterInfo, functionCheck *proxy.Verification) error {
				return errors.New("failed to init bus proxy")
			})
			defer patch.Reset()
			resErr := Init(stopCh)
			So(resErr, ShouldNotEqual, nil)
		})
		Convey("failed: channel bus is nil", func() {
			var stopCh <-chan struct{}
			patches := [...]*Patches{
				ApplyFunc(log.InitRunLog, func(_ string, _ string) error {
					return nil
				}),
				ApplyFunc(proxy.InitBusProxy, func(_ proxy.BusProxyConfig, _ <-chan struct{},
					_ channelbusconn.RegisterInfo, functionCheck *proxy.Verification) error {
					return nil
				}),
				ApplyFunc(bus.NewBus, func(_ buscommon.BusType, busOption buscommon.BusOption) (bus.CommunicationBus, error) {
					return nil, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := errors.New("channel bus is nil")
			resErr := Init(stopCh)
			So(resErr.Error(), ShouldEqual, err.Error())
		})
		Convey("success", func() {
			var stopCh <-chan struct{}
			channelBusOpt := &buscommon.ChannelBusOption{IsProxy: false}
			channelBus, _ = bus.NewBus(buscommon.ChannelBusType, buscommon.BusOption{NewBusOpt: channelBusOpt})
			patches := [...]*Patches{
				ApplyFunc(log.InitRunLog, func(_ string, _ string) error {
					return nil
				}),
				ApplyFunc(proxy.InitBusProxy, func(_ proxy.BusProxyConfig, _ <-chan struct{},
					_ channelbusconn.RegisterInfo, functionCheck *proxy.Verification) error {
					return nil
				}),
				ApplyFunc(bus.NewBus, func(_ buscommon.BusType, busOption buscommon.BusOption) (bus.CommunicationBus, error) {
					return channelBus, nil
				}),
				ApplyMethod(reflect.TypeOf(channelBus), "StartRecvMessage", func(_ *channelbus.ChannelBus) {
				}),
				ApplyMethod(reflect.TypeOf(channelBus), "RegisterBus", func(_ *channelbus.ChannelBus, _ buscommon.FunctionProperty) error {
					return nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			//err := errors.New("channel bus is nil")
			res := Init(stopCh)
			So(res, ShouldEqual, nil)
		})
	})
}

func TestSetResult(t *testing.T) {
	Convey("TestSetResult", t, func() {
		Convey("succeed", func() {
			string := strings.Repeat("string", maxRequestLength)
			result := []byte(string)
			channelBusOpt := &buscommon.ChannelBusOption{IsProxy: false}
			channelBus, _ = bus.NewBus(buscommon.ChannelBusType, buscommon.BusOption{NewBusOpt: channelBusOpt})
			patch := ApplyMethod(reflect.TypeOf(channelBus), "SetResult", func(_ *channelbus.ChannelBus, _ *commontypes.TCPResponse) error {
				return nil
			})
			defer patch.Reset()
			_, res := SetResult(1, 1, result, "logs")
			So(res, ShouldEqual, nil)
		})
		Convey("channelBus SetResult failed", func() {
			string := strings.Repeat("string", maxRequestLength)
			result := []byte(string)
			channelBusOpt := &buscommon.ChannelBusOption{IsProxy: false}
			channelBus, _ = bus.NewBus(buscommon.ChannelBusType, buscommon.BusOption{NewBusOpt: channelBusOpt})
			patch := ApplyMethod(reflect.TypeOf(channelBus), "SetResult", func(_ *channelbus.ChannelBus, _ *commontypes.TCPResponse) error {
				return errors.New("set result error")
			})
			defer patch.Reset()
			_, res := SetResult(1, 1, result, "logs")
			So(res, ShouldNotEqual, nil)
		})
	})
}

func TAbsAdaptor(str string, u uint64, args *HookArgs) error {
	return nil
}
func TestRegMsgHandler(t *testing.T) {
	Convey("TestRegMsgHandler", t, func() {
		Convey("failed: adaptor is nil", func() {
			var adaptor AbsAdaptor
			err := errors.New("adaptor is nil")
			result := RegMsgHandler(adaptor)
			So(result.Error(), ShouldEqual, err.Error())
		})
	})

	Convey("failed: channel bus is not nil", t, func() {
		adaptor := func(string, uint64, *HookArgs) error {
			return nil
		}
		result := RegMsgHandler(adaptor)
		So(result, ShouldEqual, nil)
	})

	Convey("failed: channel bus is nil", t, func() {
		adaptor := func(string, uint64, *HookArgs) error {
			return nil
		}
		channelBus = nil
		err := errors.New("please call functioncore RegFuncInfo first")
		result := RegMsgHandler(adaptor)
		So(result.Error(), ShouldEqual, err.Error())
	})
}

func TestDeleteState(t *testing.T) {
	Convey("TestDeleteState", t, func() {
		Convey("failed: stateClient is nil", func() {
			err := errors.New(common.StateClientErrMsg)
			result := DeleteState("stateID")
			So(result.Error(), ShouldEqual, err.Error())
		})
		Convey("succeed", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patch = ApplyMethod(reflect.TypeOf(stateClient), "DeleteState", func(_ *sc.StateClient, _ string) error {
				return nil
			})
			defer patch.Reset()
			result := DeleteState("stateID")
			So(result, ShouldEqual, nil)
		})
	})
}

func TestSaveState(t *testing.T) {
	option := &SaveOption{}
	Convey("TestSaveState", t, func() {
		Convey("failed: state is too large", func() {
			state := strings.Repeat("state", maxStateLength)
			err := errors.New("state is too large")
			result := SaveState("stateID", state, option)
			So(result.Error(), ShouldEqual, err.Error())
		})
		Convey("failed: TTL is invalid (59)", func() {
			option.TTL = 59
			err := errors.New("TTL is invalid, it should be in range [60, MaxInt32]")
			result := SaveState("stateID", "state", option)
			So(result.Error(), ShouldEqual, err.Error())
		})
		Convey("failed: TTL is invalid (MaxInt32+1)", func() {
			option.TTL = math.MaxInt32 + 1
			err := errors.New("TTL is invalid, it should be in range [60, MaxInt32]")
			result := SaveState("stateID", "state", option)
			So(result.Error(), ShouldEqual, err.Error())
		})
		Convey("failed: stateClient is nil", func() {
			option.TTL = 60
			err := errors.New(common.StateClientErrMsg)
			result := SaveState("stateID", "state", option)
			So(result.Error(), ShouldEqual, err.Error())
		})
		Convey("failed: save state error", func() {
			err := errors.New("save state error")
			option.TTL = 60
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patch = ApplyMethod(reflect.TypeOf(stateClient), "SaveState", func(_ *sc.StateClient, _ string, _ string, _ *sc.SaveOption) error {
				return errors.New("save state error")
			})
			defer patch.Reset()
			result := SaveState("stateID", "state", option)
			So(result.Error(), ShouldEqual, err.Error())
		})
		Convey("succeed", func() {
			option.TTL = 60
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patch = ApplyMethod(reflect.TypeOf(stateClient), "SaveState", func(_ *sc.StateClient, _ string, _ string, _ *sc.SaveOption) error {
				return nil
			})
			defer patch.Reset()
			result := SaveState("stateID", "state", option)
			So(result, ShouldEqual, nil)
		})
	})
}

func TestResetExpiredTTL(t *testing.T) {
	Convey("TestResetExpiredTTL", t, func() {
		Convey("test1", func() {
			patches := [...]*Patches{
				ApplyGlobalVar(&stateClient, new(sc.StateClient)),
				ApplyMethod(reflect.TypeOf(stateClient), "ResetExpiredTTL",
					func(_ *sc.StateClient, id string, ttlInt int) error {
						return nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			err := ResetExpiredTTL("stateID-test-001", 60)
			So(err, ShouldEqual, nil)
		})
		Convey("test2", func() {
			patches := [...]*Patches{
				ApplyGlobalVar(&stateClient, new(sc.StateClient)),
				ApplyMethod(reflect.TypeOf(stateClient), "ResetExpiredTTL",
					func(_ *sc.StateClient, id string, ttlInt int) error {
						return nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			err := ResetExpiredTTL("stateID-test-001", 59)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test3", func() {
			err := ResetExpiredTTL("stateID-test-001", 61)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test4", func() {
			patches := [...]*Patches{
				ApplyGlobalVar(&stateClient, new(sc.StateClient)),
				ApplyMethod(reflect.TypeOf(stateClient), "ResetExpiredTTL",
					func(_ *sc.StateClient, id string, ttlInt int) error {
						return errors.New("mock reset expired ttl error")
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			err := ResetExpiredTTL("stateID-test-001", 60)
			So(err, ShouldNotEqual, nil)
		})
	})
}
func TestGetStateBySnID(t *testing.T) {
	option := &SaveOption{}
	Convey("TestGetStateBySnID", t, func() {
		Convey("failed: TTL is invalid (59)", func() {
			//option.TTL = 60
			SaveState("snID:stateID1", "state", option)
			SaveState("snID:stateID2", "state", option)
			SaveState("snID:stateID3", "state", option)
			SaveState("snID:stateID4", "state", option)
			SaveState("snID:stateID5", "state", option)
			length := 0
			v, resErr := ScanState("snID:*")
			length += len(v)
			err := errors.New(common.StateClientErrMsg)
			So(resErr.Error(), ShouldEqual, err.Error())
		})
	})
}
func TestGetState(t *testing.T) {
	Convey("TestGetState", t, func() {
		Convey("failed: stateClient is nil", func() {
			err := errors.New(common.StateClientErrMsg)
			_, resErr := GetState("stateID")
			So(resErr.Error(), ShouldEqual, err.Error())
		})
		Convey("succeed", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patch = ApplyMethod(reflect.TypeOf(stateClient), "RestoreState", func(_ *sc.StateClient, _ string) (string, error) {
				return "state", nil
			})
			defer patch.Reset()
			_, resErr := GetState("stateID")
			So(resErr, ShouldEqual, nil)
		})
	})
}

func TestInitStateClient(t *testing.T) {
	config := StateClientConfig{}
	config.ServerAddr = "0.0.0.0"
	stateClient := &sc.StateClient{}
	stopCh := make(<-chan struct{})
	Convey("TestInitStateClient", t, func() {
		Convey("failed: Init state client error", func() {
			err := errors.New("Init state client error")
			patch := ApplyFunc(sc.CreateStateClient, func(_ sc.StateClientConfig, _ <-chan struct{}) (*sc.StateClient, error) {
				return nil, err
			})
			defer patch.Reset()
			resErr := InitStateClient(config, stopCh)
			So(resErr.Error(), ShouldEqual, err.Error())
		})
		Convey("succeed", func() {
			patch := ApplyFunc(sc.CreateStateClient, func(_ sc.StateClientConfig, _ <-chan struct{}) (*sc.StateClient, error) {
				return stateClient, nil
			})
			defer patch.Reset()
			resErr := InitStateClient(config, stopCh)
			So(resErr, ShouldEqual, nil)
		})
	})
}

func TestSAdd_SRem_SMembers(t *testing.T) {
	Convey("TestSAdd_SRem_SMembers", t, func() {
		Convey("success", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "SAdd",
					func(_ *sc.StateClient, key string, value string) (int, error) {
						return 1, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SRem",
					func(_ *sc.StateClient, key string, value string) (int, error) {
						return 1, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SMembers",
					func(_ *sc.StateClient, key string) ([]string, error) {
						return []string{"value"}, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SCard",
					func(_ *sc.StateClient, key string) (int, error) {
						return 1, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SIsMember",
					func(_ *sc.StateClient, key string, value string) (bool, error) {
						return true, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			{
				n, err := SAdd("key", "value")
				So(n, ShouldEqual, 1)
				So(err, ShouldEqual, nil)
			}
			{
				values, err := SMembers("key")
				So(len(values), ShouldEqual, 1)
				So(err, ShouldEqual, nil)
			}
			{
				nums, err := SCard("key")
				So(nums, ShouldEqual, 1)
				So(err, ShouldEqual, nil)
			}
			{
				n, err := SRem("key", "value")
				So(n, ShouldEqual, 1)
				So(err, ShouldEqual, nil)
			}
			{
				n, err := SIsMember("key", "value")
				So(n, ShouldEqual, true)
				So(err, ShouldEqual, nil)
			}
		})
		Convey("state client is nil", func() {
			stateClient = nil
			var err error
			_, err = SAdd("key", "value")
			So(err, ShouldNotEqual, nil)
			_, err = SRem("key", "value")
			So(err, ShouldNotEqual, nil)
			_, err = SMembers("key")
			So(err, ShouldNotEqual, nil)
			_, err = SCard("key")
			So(err, ShouldNotEqual, nil)
			_, err = SIsMember("key", "value")
			So(err, ShouldNotEqual, nil)
		})
		Convey("error", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "SAdd",
					func(_ *sc.StateClient, key string, value string) (int, error) {
						return 0, errors.New("test sadd error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SRem",
					func(_ *sc.StateClient, key string, value string) (int, error) {
						return 0, errors.New("test srem error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SMembers",
					func(_ *sc.StateClient, key string) ([]string, error) {
						return nil, errors.New("test smembers error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SCard",
					func(_ *sc.StateClient, key string) (int, error) {
						return 0, errors.New("test scard error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "SIsMember",
					func(_ *sc.StateClient, key string, value string) (bool, error) {
						return true, errors.New("test SIsMember error")
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			var err error
			_, err = SAdd("key", "value")
			So(err, ShouldNotEqual, nil)
			_, err = SRem("key", "value")
			So(err, ShouldNotEqual, nil)
			_, err = SMembers("key")
			So(err, ShouldNotEqual, nil)
			_, err = SCard("key")
			So(err, ShouldNotEqual, nil)
			_, err = SIsMember("key", "value")
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestHSetNX_HDel_HGetAll_HExists_HLen(t *testing.T) {
	Convey("TestHSetNX_HDel_HGetAll_HExists_HLen", t, func() {
		Convey("success", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "HSetNX",
					func(_ *sc.StateClient, key string, filed string, value string) (bool, error) {
						return true, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HDel",
					func(_ *sc.StateClient, key string, fields ...string) (int, error) {
						return 1, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HGetAll",
					func(_ *sc.StateClient, key string) (map[string]string, error) {
						var m map[string]string
						return m, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HExists",
					func(_ *sc.StateClient, key string, filed string) (bool, error) {
						return true, nil
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HLen",
					func(_ *sc.StateClient, key string) (int, error) {
						return 1, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			{
				n, err := HSetNX("key", "filed", "value")
				So(n, ShouldEqual, true)
				So(err, ShouldEqual, nil)
			}
			{
				values, err := HDel("key", "fields")
				So(values, ShouldEqual, 1)
				So(err, ShouldEqual, nil)
			}
			{
				_, err := HGetAll("key")
				So(err, ShouldEqual, nil)
			}
			{
				n, err := HExists("key", "filed")
				So(n, ShouldEqual, true)
				So(err, ShouldEqual, nil)
			}
			{
				n, err := HLen("key")
				So(n, ShouldEqual, 1)
				So(err, ShouldEqual, nil)
			}
		})
		Convey("state client is nil", func() {
			stateClient = nil
			error := errors.New(common.StateClientErrMsg)
			_, err := HSetNX("key", "filed", "value")
			So(err.Error(), ShouldEqual, error.Error())
			_, err = HDel("key", "fields")
			So(err.Error(), ShouldEqual, error.Error())
			_, err = HGetAll("key")
			So(err.Error(), ShouldEqual, error.Error())
			_, err = HExists("key", "filed")
			So(err.Error(), ShouldEqual, error.Error())
			_, err = HLen("key")
			So(err.Error(), ShouldEqual, error.Error())
		})
		Convey("error", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "HSetNX",
					func(_ *sc.StateClient, key string, filed string, value string) (bool, error) {
						return false, errors.New("test HSetNX error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HDel",
					func(_ *sc.StateClient, key string, fields ...string) (int, error) {
						return 0, errors.New("test HSetNX error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HGetAll",
					func(_ *sc.StateClient, key string) (map[string]string, error) {
						return nil, errors.New("test HSetNX error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HExists",
					func(_ *sc.StateClient, key string, filed string) (bool, error) {
						return false, errors.New("test HSetNX error")
					}),
				ApplyMethod(reflect.TypeOf(stateClient), "HLen",
					func(_ *sc.StateClient, key string) (int, error) {
						return 0, errors.New("test HSetNX error")
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			_, err := HSetNX("key", "filed", "value")
			So(err, ShouldNotEqual, nil)
			_, err = HDel("key", "fields")
			So(err, ShouldNotEqual, nil)
			_, err = HGetAll("key")
			So(err, ShouldNotEqual, nil)
			_, err = HExists("key", "filed")
			So(err, ShouldNotEqual, nil)
			_, err = HLen("key")
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestSetKeyWithTTL(t *testing.T) {
	Convey("TestSetKeyWithTTL", t, func() {
		Convey("success", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "SetKeyWithTTL",
					func(_ *sc.StateClient, key, value string, ttl int) error {
						return nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			err := SetKeyWithTTL("key", "value", 2)
			So(err, ShouldEqual, nil)
		})
		Convey("error", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "SetKeyWithTTL",
					func(_ *sc.StateClient, key, value string, ttl int) error {
						return errors.New("test SetKeyWithTTL error")
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			err := SetKeyWithTTL("key", "value", 2)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestExists(t *testing.T) {
	Convey("TestExists", t, func() {
		Convey("success", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "Exists",
					func(_ *sc.StateClient, key string) (bool, error) {
						return true, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			{
				ret, err := Exists("key")
				So(ret, ShouldEqual, true)
				So(err, ShouldEqual, nil)
			}
		})
		Convey("state client is nil", func() {
			var err error
			_, err = Exists("key")
			So(err, ShouldNotEqual, nil)
		})
		Convey("error", func() {
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(stateClient), "Exists",
					func(_ *sc.StateClient, key string) (bool, error) {
						return false, errors.New("test exists error")
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			{
				ret, err := Exists("key")
				So(ret, ShouldEqual, false)
				So(err, ShouldNotEqual, nil)
			}
		})
	})
}

func TestCall(t *testing.T) {
	registerInfo = RegisterInfo{
		FunctionName: "functiontask",
	}
	Convey("TestCall", t, func() {
		Convey("failed: destFuncName is invalid", func() {
			config := MsgConfig{}
			config.Timeout = 60 * time.Second
			config.LogType = "Tail"
			result := Call("!destfunc", "stateID", []byte("hi"), config)
			So(result.ErrCode, ShouldEqual, uint32(220001))
		})

		Convey("failed: invalid args length", func() {
			var args []byte
			for i := 0; i < 18*1024*1024+1; i++ {
				args = append(args, '0')
			}
			config := MsgConfig{}
			config.Timeout = 60 * time.Second
			config.LogType = "Tail"
			result := Call("destfunc", "stateID", args, config)
			So(result.ErrCode, ShouldEqual, common.ArgsLengthInvalidErrCode)
		})

		Convey("failed: stateID is empty", func() {
			config := MsgConfig{}
			config.Timeout = 60 * time.Second
			config.LogType = "Tail"
			config.DestFuncTenant = "0"
			result := Call("functiontask", "", []byte("hi"), config)
			So(result.ErrCode, ShouldEqual, uint32(220006))
		})

		Convey("failed: Call failed", func() {
			config := MsgConfig{}
			config.Timeout = 60 * time.Second
			config.LogType = "Tail"
			patch := ApplyFunc(callByBus,
				func(destFuncName string, stateID string, args []byte, config MsgConfig) Result {
					res := Result{
						Reply:   nil,
						ErrCode: 500,
						ErrMsg:  "",
						Log:     "",
					}
					return res
				})
			defer patch.Reset()
			result := Call("destfunc", "stateID", []byte("hi"), config)
			So(result.ErrCode, ShouldEqual, 500)
		})

		Convey("succeed1", func() {
			config := MsgConfig{}
			config.Timeout = 60 * time.Second
			config.LogType = "Tail"
			patch := ApplyFunc(callByBus,
				func(destFuncName string, stateID string, args []byte, config MsgConfig) Result {
					res := Result{
						Reply:   nil,
						ErrCode: 0,
						ErrMsg:  "",
						Log:     "",
					}
					return res
				})
			defer patch.Reset()
			result := Call("destfunc-test01-a", "stateID", []byte("hi"), config)
			So(result.ErrCode, ShouldEqual, uint32(0))
		})

	})
}

func TestRegFuncInfo(t *testing.T) {
	Convey("TestRegFuncInfo", t, func() {
		Convey("failed: funcName is invalid", func() {
			info := RegisterInfo{}
			info.FunctionName = "!func"
			err := errors.New("funcName is invalid, funcName is !func")
			resErr := RegFuncInfo(info)
			So(resErr.Error(), ShouldEqual, err.Error())
		})
		Convey("succeed: FunctionVersion is empty", func() {
			info := RegisterInfo{}
			info.FunctionName = "functiontask"
			info.IP = "0.0.0.0"
			info.Tenant = "0"
			resErr := RegFuncInfo(info)
			So(resErr, ShouldEqual, nil)
		})
		Convey("succeed: FunctionVersion is $latest", func() {
			info := RegisterInfo{}
			info.IP = "0.0.0.0"
			info.FunctionName = "functiontask"
			info.Tenant = "0"
			info.FunctionVersion = "$latest"
			resErr := RegFuncInfo(info)
			So(resErr, ShouldEqual, nil)
		})
	})
}

func TestBusAdaptor(t *testing.T) {
	Convey("TestBusAdaptor", t, func() {
		Convey("failed: Adaptor is empty", func() {
			dst := &buscommon.FunctionProperty{}
			req := &commontypes.TCPRequest{}
			ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, "test")
			err := BusAdaptor(ctx, dst, req)
			So(err, ShouldNotEqual, nil)
		})
		Convey("success", func() {
			dst := &buscommon.FunctionProperty{}
			req := &commontypes.TCPRequest{}
			ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, "test")
			patch := ApplyFunc(utils.GetUserData, func(_ string) (map[string]string, error) {
				m := make(map[string]string)
				m["SNServiceID"] = "snServiceID"
				m["FrontendCallFlag"] = "FrontendCallFlag"
				m["SrcTenantID"] = "SrcTenantID"
				m["StateKey"] = "StateKey"
				return m, nil
			})
			defer patch.Reset()
			realAdaptor = func(string, uint64, *HookArgs) error {
				return nil
			}
			//patch = ApplyFuncVar(&realAdaptor, realAdaptor1)
			res := BusAdaptor(ctx, dst, req)
			So(res, ShouldEqual, nil)
		})
	})
}

func TestCreateState(t *testing.T) {
	Convey("TestCreateState", t, func() {
		Convey("failed: funcName is invalid", func() {
			_, _, resErr := CreateState("!func", "stateID",
				types.CreateStateConfig{TenantID: "tenantid", SnServiceID: "snserviceid", ProxyFunction: "functiontask"})
			So(resErr, ShouldNotBeNil)
		})
		Convey("succeed", func() {
			patch := ApplyFunc(communication.CreateStateByHTTP, func(_ string, _ string, _ types.CreateStateConfig) (string, string, snerror.SNError) {
				return "stateID", "nodeid", nil
			})
			defer patch.Reset()
			patch = ApplyFunc(fasthttp.DoTimeout, func(_ *fasthttp.Request, _ *fasthttp.Response, _ time.Duration) error {
				return nil
			})
			defer patch.Reset()
			_, _, resErr := CreateState("func", "stateID",
				types.CreateStateConfig{TenantID: "tenantid", SnServiceID: "snserviceid", ProxyFunction: "functiontask"})
			So(resErr, ShouldEqual, nil)
		})
		Convey("create state error", func() {
			patch := ApplyFunc(communication.CreateStateByHTTP, func(_ string, _ string, _ types.CreateStateConfig) (string, string, snerror.SNError) {
				return "", "", snerror.New(1, errors.New("test").Error())
			})
			defer patch.Reset()
			patch = ApplyFunc(fasthttp.DoTimeout, func(_ *fasthttp.Request, _ *fasthttp.Response, _ time.Duration) error {
				return nil
			})
			defer patch.Reset()
			_, _, resErr := CreateState("func", "stateID",
				types.CreateStateConfig{TenantID: "tenantid", SnServiceID: "snserviceid", ProxyFunction: "functiontask"})
			So(resErr, ShouldNotEqual, nil)
		})
	})
}

func TestDeleteStateMapping(t *testing.T) {
	Convey("TestDeleteStateMapping", t, func() {
		Convey("succeed", func() {
			patch := ApplyFunc(communication.DeleteStateByHTTP, func(_ string, _ string, _ string) snerror.SNError {
				return nil
			})
			defer patch.Reset()
			patch = ApplyFunc(fasthttp.DoTimeout, func(_ *fasthttp.Request, _ *fasthttp.Response, _ time.Duration) error {
				return nil
			})
			defer patch.Reset()
			resErr := DeleteStateMapping("stateID", "tenantid", "functiontask")
			So(resErr, ShouldEqual, nil)
		})
	})
}

func TestIsNotExist(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test1",
			args: args{err: errors.New("stateID recycled or not exists")},
			want: true,
		},
		{
			name: "test2",
			args: args{err: errors.New("test IsNotExist method return false")},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNotExist(tt.args.err); got != tt.want {
				t.Errorf("IsNotExist() = %v, want %v", got, tt.want)
			}
		})
	}
}

var connect = true

func TestRegEtcdConnStatusHandler(t *testing.T) {
	RegEtcdConnStatusHandler(etcdHandler)
	if etcdHandler != nil {
		etcdHandler(false)
	}
	if !connect {
		t.Errorf("TestRegEtcdConnStatusHandler fail, get connect = %v, want true", connect)
	}

	RegEtcdConnStatusHandler(func(f bool) {
		connect = f
	})
	etcdHandler(false)
	if connect {
		t.Errorf("TestRegEtcdConnStatusHandler fail, get connect = %v, want false", connect)
	}
}

func TestSaveStateAndResult(t *testing.T) {
	Convey("TestSaveStateAndResult", t, func() {
		Convey("failed: stateClient is nil", func() {
			stateCmd := SetCmdAndArg{}
			resultCmd := SetCmdAndArg{}
			err := errors.New(common.StateClientErrMsg)
			result := SaveStateAndResult(stateCmd, resultCmd)
			So(result.Error(), ShouldEqual, err.Error())
		})
		Convey("succeed", func() {
			stateCmd := SetCmdAndArg{}
			resultCmd := SetCmdAndArg{}
			patch := ApplyGlobalVar(&stateClient, new(sc.StateClient))
			defer patch.Reset()
			patch = ApplyMethod(reflect.TypeOf(stateClient), "Transaction", func(_ *sc.StateClient, _ ...sc.DoArg) error {
				return nil
			})
			defer patch.Reset()
			result := SaveStateAndResult(stateCmd, resultCmd)
			So(result, ShouldEqual, nil)
		})
	})
}

func TestReportStateLoad(t *testing.T) {
	Convey("TestReportStateLoad", t, func() {
		Convey("failed1: load value illegal", func() {
			stateLoad := -1
			err := errors.New("load value illegal")
			res := ReportStateLoad(stateLoad)
			So(res.Error(), ShouldEqual, err.Error())
		})
		Convey("failed2: load value illegal", func() {
			stateLoad := 110
			err := errors.New("load value illegal")
			res := ReportStateLoad(stateLoad)
			So(res.Error(), ShouldEqual, err.Error())
		})
		Convey("success1", func() {
			stateLoad := 50
			res := ReportStateLoad(stateLoad)
			So(res, ShouldEqual, nil)
		})
		Convey("success2", func() {
			stateLoad := 50
			patch := ApplyGlobalVar(&metricServer, new(metrics.MetricServer))
			defer patch.Reset()
			patch = ApplyMethod(reflect.TypeOf(metricServer), "SetLoad", func(_ *metrics.MetricServer, _ string, _ int32) {
			})
			defer patch.Reset()
			res := ReportStateLoad(stateLoad)
			So(res, ShouldEqual, nil)
		})
	})
}

func TestConstructCallResponse(t *testing.T) {
	requestID := uint64(1)
	errCode := int32(0)
	message := []byte("test")
	logs := "test log"
	response := constructCallResponse(requestID, errCode, message, logs)
	assert.Equal(t, uint32(0), response.ErrorCode)

	requestID = uint64(1)
	errCode = int32(1)
	message = []byte("test")
	logs = "test log"
	response = constructCallResponse(requestID, errCode, message, logs)
	assert.Equal(t, uint32(1), response.ErrorCode)
}

func TestGetPriority(t *testing.T) {
	requestAttributes := make(map[string]int32)
	requestAttributes[requestLayer] = 1
	result := getPriority(requestAttributes, common.RequestAttributesLayer)
	assert.Equal(t, result, 1)

	requestAttributes = nil
	result = getPriority(requestAttributes, common.RequestAttributesLayer)
	assert.Equal(t, result, 0)
}

func TestCallOnDemand(t *testing.T) {
	destFuncName := "test1"
	nodeID := "node1"
	stateID := "stateID1"
	args := []byte("test1")
	config := MsgConfig{Timeout: 60}
	result := CallOnDemand(destFuncName, nodeID, stateID, args, config)
	assert.Equal(t, uint32(220003), result.ErrCode)

	config = MsgConfig{Timeout: 60 * time.Second}
	result = CallOnDemand(destFuncName, nodeID, stateID, args, config)
	assert.Equal(t, uint32(220500), result.ErrCode)
}

func TestCheckStateful(t *testing.T) {
	config := MsgConfig{}
	var destFuncName, stateID string
	res := checkStateful(destFuncName, stateID, config)
	if res {
		t.Errorf("this should be false")
	}
	res = checkStateful(taskFuncName, stateID, config)
	if !res {
		t.Errorf("this should be false")
	}
}
