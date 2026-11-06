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

package corestateclient

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/efs/kvstorage"
)

var metaKey = metadata.Gen("0", "0", metadata.StateIDPrefix, "xx")

func TestStateClient_Get(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.GetState, func(stateID string) (string, snerror.SNError) {
			return "xxx", nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestStateClient_Get", t, func() {
		client := New()
		Convey("test: get failed", func() {
			patch := ApplyFunc(functioncore.GetState, func(stateID string) (string, snerror.SNError) {
				return "xxx", snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			_, err := client.Get(metaKey)
			So(err, ShouldNotBeNil)
		})
		Convey("test: get success", func() {
			stateID, err := client.Get(metaKey)
			So(err, ShouldBeNil)
			So(stateID, ShouldEqual, "xxx")
		})
	})
}
func TestStateClient_Set(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
			return true, nil
		}),
		ApplyFunc(functioncore.SaveState,
			func(stateID string, state string, option *functioncore.SaveOption) snerror.SNError {
				return nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestStateClient_Set", t, func() {
		client := New()
		config.Conf.PersistentState = true
		Convey("test: sadd failed", func() {
			patch := ApplyFunc(functioncore.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
				return false, snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			err := client.Set(metaKey, "xx", 0)
			So(err, ShouldBeNil)
		})
		Convey("test: saveState failed", func() {
			patch := ApplyFunc(functioncore.SaveState,
				func(stateID string, state string, option *functioncore.SaveOption) snerror.SNError {
					return snerror.New(common.EFunctionCore, "xx")
				})
			defer patch.Reset()
			err := client.Set(metaKey, "xx", 0)
			So(err, ShouldNotBeNil)
		})
		Convey("test: set success", func() {
			err := client.Set(metaKey, "xx", 0)
			So(err, ShouldBeNil)
		})
		Convey("test: set success, do not persistent", func() {
			config.Conf.PersistentState = false
			err := client.Set(metaKey, "xx", 0)
			So(err, ShouldBeNil)
		})
	})
}
func TestStateClient_DeleteState(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.HDel, func(key string, filed ...string) (int, snerror.SNError) {
			return 0, nil
		}),
		ApplyFunc(functioncore.DeleteState, func(stateID string) snerror.SNError {
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestStateClient_DeleteState", t, func() {
		client := New()
		config.Conf.PersistentState = true
		Convey("test: deleteState success", func() {
			err := client.DeleteState(metaKey, "")
			So(err, ShouldBeNil)
		})
		Convey("test: deleteState success, do not persistent", func() {
			config.Conf.PersistentState = false
			err := client.DeleteState(metaKey, "")
			So(err, ShouldBeNil)
		})
		Convey("test: set failed, reason: srem failed", func() {
			patch := ApplyFunc(functioncore.HDel, func(key string, filed ...string) (int, snerror.SNError) {
				return 0, snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			err := client.DeleteState(metaKey, "")
			So(err, ShouldNotBeNil)
		})
		Convey("test: set failed, reason: DeleteState failed", func() {
			patch := ApplyFunc(functioncore.DeleteState, func(stateID string) snerror.SNError {
				return snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			err := client.DeleteState(metaKey, "")
			So(err, ShouldNotBeNil)
		})
	})
}
func TestStateClient_DeleteStateValueAndStateMapping(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.HDel, func(key string, filed ...string) (int, snerror.SNError) {
			return 0, nil
		}),
		ApplyFunc(functioncore.DeleteState, func(stateID string) snerror.SNError {
			return nil
		}),
		ApplyFunc(functioncore.DeleteStateMapping, func(stateID string, tenantID string, proxyFunction string) snerror.SNError {
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestStateClient_DeleteStateValueAndStateMapping", t, func() {
		client := New()
		config.Conf.PersistentState = true
		Convey("test: DeleteStateValueAndStateMapping success", func() {
			err := client.DeleteStateValueAndStateMapping(metaKey)
			So(err, ShouldBeNil)
		})
		Convey("test: deleteState success, do not persistent", func() {
			config.Conf.PersistentState = false
			err := client.DeleteStateValueAndStateMapping(metaKey)
			So(err, ShouldBeNil)
		})
		Convey("test: set failed, reason: srem failed", func() {
			patch := ApplyFunc(functioncore.HDel, func(key string, filed ...string) (int, snerror.SNError) {
				return 0, snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			err := client.DeleteStateValueAndStateMapping(metaKey)
			So(err, ShouldBeNil)
		})
		Convey("test: set failed, reason: DeleteState failed", func() {
			patch := ApplyFunc(functioncore.DeleteState, func(stateID string) snerror.SNError {
				return snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			err := client.DeleteStateValueAndStateMapping(metaKey)
			So(err, ShouldBeNil)
		})
		Convey("test: set failed, reason: DeleteStateMapping failed", func() {
			patch := ApplyFunc(functioncore.DeleteStateMapping, func(stateID string, tenantID string, proxyFunction string) snerror.SNError {
				return snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			err := client.DeleteStateValueAndStateMapping(metaKey)
			So(err, ShouldNotBeNil)
		})
	})
}
func TestStateClient_IsNotExist(t *testing.T) {
	Convey("TestStateClient_IsNotExist", t, func() {
		patch := ApplyFunc(functioncore.IsNotExist, func(err error) bool {
			return strings.Contains(err.Error(), "stateID recycled or not exists")
		})
		defer patch.Reset()
		client := New()
		So(client.IsNotExist(errors.New("stateID recycled or not exists")), ShouldBeTrue)
		So(client.IsNotExist(errors.New("not stateID error")), ShouldBeFalse)
	})
}
func TestStateClient_Scan(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.HGetAll, func(key string) (map[string]string, snerror.SNError) {
			return map[string]string{"state1": "1", "state2": "2"}, nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	client := New()
	Convey("TestStateClient_Scan", t, func() {
		Convey("test: HGetAll failed", func() {
			patch := ApplyFunc(functioncore.HGetAll, func(key string) (map[string]string, snerror.SNError) {
				return nil, snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			_, err := client.Scan(metaKey)
			So(err, ShouldNotBeNil)
		})
		Convey("test: success", func() {
			_, err := client.Scan(metaKey)
			So(err, ShouldBeNil)
		})
	})
}

func TestStateClient_SetStateAndResult(t *testing.T) {
	client := New()
	patches := []*Patches{
		ApplyFunc(functioncore.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
			return true, nil
		}),
		ApplyFunc(functioncore.SaveState,
			func(stateID string, state string, option *functioncore.SaveOption) snerror.SNError {
				return nil
			}),
		ApplyFunc(functioncore.SaveStateAndResult,
			func(stateCmd, resultCmd functioncore.SetCmdAndArg) snerror.SNError {
				return nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestStateClient_SetStateAndResult", t, func() {
		config.Conf.PersistentState = true
		Convey("test: success", func() {
			err := client.SetStateAndResult(&metadata.Metadata{}, "xx", 0,
				&common.Request{}, &common.Response{Code: common.EStateIDNotFound})
			So(err, ShouldBeNil)
		})

		Convey("test: success, result.code is not zero", func() {
			err := client.SetStateAndResult(&metadata.Metadata{}, "xx", 0,
				&common.Request{}, &common.Response{Code: common.EStateIDNotFound})
			So(err, ShouldBeNil)
		})

		Convey("test: sadd failed", func() {
			patch := ApplyFunc(functioncore.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
				return false, snerror.New(common.EFunctionCore, "xx")
			})
			defer patch.Reset()
			err := client.SetStateAndResult(&metadata.Metadata{}, "xx", 0,
				&common.Request{}, &common.Response{})
			So(err, ShouldNotBeNil)
		})
		Convey("test: SaveStateAndResult failed", func() {
			patch := ApplyFunc(functioncore.SaveStateAndResult,
				func(stateCmd, resultCmd functioncore.SetCmdAndArg) snerror.SNError {
					return snerror.New(common.EFunctionCore, "xx")
				})
			defer patch.Reset()
			err := client.SetStateAndResult(&metadata.Metadata{}, "xx", 0,
				&common.Request{}, &common.Response{})
			So(err, ShouldNotBeNil)
			config.Conf.PersistentState = false
			err2 := client.SetStateAndResult(&metadata.Metadata{}, "xx", 0,
				&common.Request{}, &common.Response{})
			So(err2, ShouldBeNil)
		})
	})
}
func TestStateClient_SetTTL(t *testing.T) {
	client := New()
	Convey("TestStateClient_SetTTL", t, func() {
		patch := ApplyFunc(functioncore.ResetExpiredTTL, func(id string, ttlInt int) snerror.SNError {
			return snerror.New(common.EFunctionCore, "xx")
		})
		defer patch.Reset()
		err := client.SetTTL(metaKey, 60)
		So(err, ShouldNotBeNil)
	})
}
func TestStateClient_StateExists(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.Exists, func(key string) (bool, snerror.SNError) {
			switch key {
			case "existState":
				return true, nil
			default:
				return false, nil
			}
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	client := New()
	Convey("TestStateClient_StateExists", t, func() {
		Convey("test: call exists failed", func() {
			patch := ApplyFunc(functioncore.Exists, func(key string) (bool, snerror.SNError) {
				return true, snerror.New(common.EFunctionCore, "")
			})
			defer patch.Reset()
			_, err := client.StateExists("s:1")
			So(err, ShouldNotBeNil)
			So(err.Code(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: call success", func() {
			exists, err := client.StateExists("existState")
			So(err, ShouldBeNil)
			So(exists, ShouldBeTrue)
			exists, err = client.StateExists("NotExistState")
			So(err, ShouldBeNil)
			So(exists, ShouldBeFalse)
		})
	})
}

// Test_stateClient_IsStateIDInHash include exist and not exist condition
func Test_stateClient_IsStateIDInHash(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.HExists, func(key string, field string) (bool, snerror.SNError) {
			switch key {
			case "existState":
				return true, nil
			default:
				return false, snerror.New(common.EInvalidParam, "not exist")
			}
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	type fields struct {
		storage kvstorage.Storage
	}
	type args struct {
		stateHashKey string
		stateID      string
	}
	var a args
	var f fields
	a.stateHashKey = "stateHashKey"
	a.stateID = "stateID"
	result := common.NewTaskError(int32(common.EInvalidParam), "not exist")

	var b args
	b.stateHashKey = "existState"
	b.stateID = "existState"
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
		want1  common.TaskError
	}{
		{"case1", f, a, false, result},
		{"case2", f, b, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &stateClient{
				storage: tt.fields.storage,
			}
			got, got1 := s.IsStateIDInHash(tt.args.stateHashKey, tt.args.stateID)
			if got != tt.want {
				t.Errorf("IsStateIDInHash() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("IsStateIDInHash() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

// Test_stateClient_GetNumsByServiceID include success get nums and failed to get nums
func Test_stateClient_GetNumsByServiceID(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(functioncore.HLen, func(key string) (int, snerror.SNError) {
			switch key {
			case "success":
				return 0, nil
			default:
				return 0, snerror.New(common.EInvalidParam, "not exist")
			}
		}),
		ApplyFunc(metadata.GeneStateHashKey, func(tenantID, serviceID, funcName string) string {
			switch tenantID {
			case "success":
				return "success"
			default:
				return "fail"
			}
		}),
		ApplyFunc(metadata.ExtractMetaKey, func(metaKey string) (string, string, string) {
			switch metaKey {
			case "success":
				return "success", "", ""
			default:
				return "fail", "", ""
			}
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	type fields struct {
		storage kvstorage.Storage
	}
	type args struct {
		pattern string
	}
	var f fields
	var a args
	a.pattern = "success"
	var b args
	b.pattern = "fail"
	result := common.NewTaskError(int32(common.EInvalidParam), "not exist")
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
		want1  common.TaskError
	}{
		{"case1", f, a, 0, nil},
		{"case2", f, b, 0, result},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &stateClient{
				storage: tt.fields.storage,
			}
			got, got1 := s.GetNumsByServiceID(tt.args.pattern)
			if got != tt.want {
				t.Errorf("GetNumsByServiceID() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("GetNumsByServiceID() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
