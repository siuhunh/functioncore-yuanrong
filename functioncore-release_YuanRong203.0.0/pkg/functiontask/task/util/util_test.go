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

package util

import (
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	common2 "functioncore/pkg/core-sdk/common"
	"functioncore/pkg/functionbus/servicediscovery"
	busUtils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/test/utils"
)

type MockStateStore struct{}

// Set the state by state id.
func (s MockStateStore) Set(key string, value string, expireTime int) common.TaskError {
	return nil
}

// Get the state by state id.
func (s MockStateStore) Get(key string) (string, common.TaskError) {
	return "", nil
}

// DeleteStateValueAndStateMapping delete the state by state id.
func (s MockStateStore) DeleteStateValueAndStateMapping(key string) common.TaskError {
	return s.DeleteState(key, "")
}

// SetStateAndResult Sets the state and result.
func (s MockStateStore) SetStateAndResult(key *metadata.Metadata, state string, TTL int,
	req *common.Request, result *common.Response) common.TaskError {
	return nil
}

// DeleteState the state.
func (s MockStateStore) DeleteState(key, funcName string) common.TaskError {
	return nil
}

// SetTTL sets the ttl for the state.
func (s MockStateStore) SetTTL(key string, ttl int) common.TaskError {
	return nil
}

// Scan returns state ids with specific service id.
func (s MockStateStore) Scan(pattern string) (map[string]string, common.TaskError) {
	return nil, nil
}

// GetNumsByServiceID get nums of stateID by serviceID
func (s MockStateStore) GetNumsByServiceID(pattern string) (int, common.TaskError) {
	return 0, nil
}

// IsNotExist check whether it is a state not found error.
func (s MockStateStore) IsNotExist(err error) bool {
	return err != nil && err.Error() == common.StateIDNotFoundMsg
}

// StateExists get stateID status directly
func (s MockStateStore) StateExists(key string) (bool, common.TaskError) {
	return true, nil
}

// IsStateIDInHash check stateID is in hash
func (s MockStateStore) IsStateIDInHash(stateHashKey string, stateID string) (bool, common.TaskError) {
	return true, nil
}

func TestDelayExit(t *testing.T) {
	type args struct {
		module string
		err    error
	}
	var a args
	errMessage := errors.New("aaa")
	a.err = errMessage
	a.module = "aaa"
	tests := []struct {
		name string
		args args
	}{
		{"delay exit", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DelayExit(tt.args.module, tt.args.err)
		})
	}
}

func TestErrInfoOfTooManyState(t *testing.T) {
	type args struct {
		nums int
	}
	var a args
	a.nums = 10001
	tests := []struct {
		name string
		args args
		want string
	}{
		{"too many state", a, "number of stateID (10001) reaches or exceeds maximum limit (10000)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ErrInfoOfTooManyState(tt.args.nums); got != tt.want {
				t.Errorf("ErrInfoOfTooManyState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasExceedNumsError(t *testing.T) {
	var client MockStateStore
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(IsExceedMaxNums, func(int) bool {
			return true
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		client    stateclient.StateClient
		tenantID  string
		serviceID string
	}
	var a args
	a.client = client
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := HasExceedNumsError(tt.args.client, tt.args.tenantID, tt.args.serviceID); (err != nil) != tt.wantErr {
				t.Errorf("HasExceedNumsError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHasExceedNumsError_2(t *testing.T) {
	var client MockStateStore
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(IsExceedMaxNums, func(int) bool {
			return false
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		client    stateclient.StateClient
		tenantID  string
		serviceID string
	}
	var a args
	a.client = client
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := HasExceedNumsError(tt.args.client, tt.args.tenantID, tt.args.serviceID); (err != nil) != tt.wantErr {
				t.Errorf("HasExceedNumsError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetStateETCDPath(t *testing.T) {
	type args struct {
		stateID string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"case1",
			args{
				stateID: "test",
			},
			"/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStateETCDPath(tt.args.stateID); got != tt.want {
				t.Errorf("GetStateETCDPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeleteStateInRedis(t *testing.T) {
	type args struct {
		tenantID     string
		serviceID    string
		functionName string
		stateID      string
	}
	tests := []struct {
		name string
		args args
		utils.PatchesFunc
		wantErr bool
	}{
		// test success delete
		{
			"case1",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateclient.HDel, func(key string, fields ...string) (int, snerror.SNError) {
						return 1, nil
					}),
				})
				return patches
			},
			false,
		},
		// test retry util err
		{
			"case2",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateclient.HDel, func(key string, fields ...string) (int, snerror.SNError) {
						return 0, snerror.New(common2.StateClientErrCode, "hdel error")
					}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
						return
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.PatchesFunc()
			if err := DeleteStateInRedis(tt.args.tenantID, tt.args.serviceID, tt.args.functionName, tt.args.stateID); (err != nil) != tt.wantErr {
				t.Errorf("DeleteStateInRedis() = %v, want %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestSaveStateInRedis(t *testing.T) {
	type args struct {
		tenantID     string
		serviceID    string
		functionName string
		stateID      string
		stateKey     string
	}
	tests := []struct {
		name string
		args args
		utils.PatchesFunc
		wantErr bool
	}{
		// test retry util err
		{
			"case1",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateclient.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
						return true, snerror.New(common2.StateClientErrCode, "HSetNX error")
					}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
						return
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case2",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateclient.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
						return true, nil
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.PatchesFunc()
			if err := SaveStateInRedis(tt.args.tenantID, tt.args.serviceID, tt.args.functionName,
				tt.args.stateID, tt.args.stateKey); (err != nil) != tt.wantErr {
				t.Errorf("SaveStateInRedis() = %v, want %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestParseFuncName(t *testing.T) {
	type args struct {
		serviceID string
		funcName  string
	}
	var a args
	a.serviceID = "serviceID"
	a.funcName = "func:latest"
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{"case1", a, "serviceID@func", "latest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := ParseFuncName(tt.args.serviceID, tt.args.funcName)
			if got != tt.want {
				t.Errorf("ParseFuncName() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ParseFuncName() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestSplitFuncNameAndVersion(t *testing.T) {
	type args struct {
		funcName string
	}
	tests := []struct {
		name         string
		args         args
		wantFuncName string
		wantVersion  string
	}{
		{"case1 no config version",
			args{
				funcName: "testFuncNameWithoutVersion",
			},
			"testFuncNameWithoutVersion", "latest"},
		{"case2 config version latest",
			args{
				funcName: "testFuncNameWithoutVersion:",
			},
			"testFuncNameWithoutVersion", "latest"},
		{"case3 config version test",
			args{
				funcName: "testFuncNameWithoutVersion:test",
			},
			"testFuncNameWithoutVersion", "test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, version := SplitFuncNameAndVersion(tt.args.funcName)
			if name != tt.wantFuncName {
				t.Errorf("SplitFuncNameAndVersion() name = %v, wantFuncName %v", name, tt.wantFuncName)
			}
			if version != tt.wantVersion {
				t.Errorf("SplitFuncNameAndVersion() version = %v, wantVersion %v", version, tt.wantVersion)
			}
		})
	}
}

func TestGetBusinessID(t *testing.T) {
	registry := servicediscovery.GetRegistry()
	registry.FuncMeta = make(map[string]*servicediscovery.FunctionMetaInfo)

	// no businessID
	registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
		&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
			FunctionVersionURN: "sn:cn:580943580943580943:function:0@debugservice@hello-world:latest",
		}}
	// empty businessID
	registry.FuncMeta["580943580943580943/0@debugservice@hello-world2/latest"] =
		&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
			FunctionVersionURN: "sn:cn::580943580943580943:function:0@debugservice@hello-world2:latest",
		}}
	// config businessID
	registry.FuncMeta["580943580943580943/0@debugservice@hello-world3/latest"] =
		&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
			FunctionVersionURN: "sn:cn:test:580943580943580943:function:0@debugservice@hello-world3:latest",
		}}

	type args struct {
		functionName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1 defaultBusinessID",
			args{
				functionName: "0@debugservice@hello-world",
			},
			"yrk",
		},
		{"case2 defaultBusinessID",
			args{
				functionName: "0@debugservice@hello-world2",
			},
			"yrk",
		},
		{"case3 config BusinessID",
			args{
				functionName: "0@debugservice@hello-world3",
			},
			"test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			businessID := GetBusinessID("580943580943580943", tt.args.functionName, "latest")

			if businessID != tt.want {
				t.Errorf("GetBusinessID() businessID = %v, want %v", businessID, tt.want)
			}
		})
	}
}

func Test_GetRealFuncName(t *testing.T) {
	convey.Convey("test GetRealFuncName", t, func() {
		convey.Convey("case1: funcName is funcA, version is empty", func() {
			realFuncName, ok := GetRealFuncName("funcA", "", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realFuncName, convey.ShouldEqual, "funcA")
		})
		convey.Convey("case2: funcName is funcA, version is latest", func() {
			realFuncName, ok := GetRealFuncName("funcA", "latest", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realFuncName, convey.ShouldEqual, "funcA")
		})
		convey.Convey("case3: funcName is funcA, version is v1", func() {
			realFuncName, ok := GetRealFuncName("funcA", "v1", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realFuncName, convey.ShouldEqual, "funcA:v1")
		})
		convey.Convey("case4: funcName is funcA, version is !alias", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(busUtils.GetRouteFuncVersion, func(_, _, _ string) string {
					return "v2"
				}),
			})
			defer patches.ResetAll()
			realFuncName, ok := GetRealFuncName("funcA", "!alise", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realFuncName, convey.ShouldEqual, "funcA:v2")
		})
		convey.Convey("case5: routeVersion is empty, funcName is funcA, version is !alias", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(busUtils.GetRouteFuncVersion, func(_, _, _ string) string {
					return ""
				}),
			})
			defer patches.ResetAll()
			_, ok := GetRealFuncName("funcA", "!alise", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case6: routeVersion is empty, get metadata from etcd failed", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(busUtils.GetRouteFuncVersion, func(_, _, _ string) string {
					return ""
				}),
				gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionMetadata, func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.FunctionMetaInfo, error) {
					if functionKey == "tenantId/0@serviceId@bridgeFunc/latest" {
						return nil, errors.New("test error")
					}
					return &servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{ConcurrentNum: 1}}, nil
				}),
			})
			defer patches.ResetAll()
			_, ok := GetRealFuncName("bridgeFunc", "!alise", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case7: routeVersion is empty, get metadata from etcd ok, is bridgeFunc", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(busUtils.GetRouteFuncVersion, func(_, _, _ string) string {
					return ""
				}),
				gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionMetadata, func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.FunctionMetaInfo, error) {
					return &servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
						IsBridgeFunction: true,
					}}, nil
				}),
			})
			defer patches.ResetAll()
			realFuncName, ok := GetRealFuncName("bridgeFunc", "!alise", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realFuncName, convey.ShouldEqual, "bridgeFunc")
		})
		convey.Convey("case8: routeVersion is empty, get metadata from etcd ok, not bridgeFunc", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(busUtils.GetRouteFuncVersion, func(_, _, _ string) string {
					return ""
				}),
				gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionMetadata, func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.FunctionMetaInfo, error) {
					return &servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
						IsBridgeFunction: false,
					}}, nil
				}),
			})
			defer patches.ResetAll()
			_, ok := GetRealFuncName("bridgeFunc", "!alise", "serviceId", "tenantId")
			convey.So(ok, convey.ShouldBeFalse)
		})
	})
}
