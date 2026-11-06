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

package utils

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/types"

	mockutils "functioncore/test/utils"
)

func Test_GetFuncVersion(t *testing.T) {
	key := "sn:cn:yrk:i1fe5394:function:functionB:$latest"
	assert.Equal(t, "$latest", GetFuncVersion(key))
	key = "sn:cn:yrk:i1fe5394:function:functionB:$latest:XXX"
	assert.Equal(t, "", GetFuncVersion(key))
}

func Test_GetFuncVersionURN(t *testing.T) {
	URN := "sn:cn:yrk:i1fe5394:function:functionB:$latest"
	actualURN := GetFuncVersionURN("i1fe5394", "functionB", "$latest")
	assert.Equal(t, URN, actualURN)
}

func Test_GetFuncNameAndVersion(t *testing.T) {
	key := "functionB:aliastest"
	name, alias := GetFuncNameAndVersion(key)
	assert.Equal(t, "functionB", name)
	assert.Equal(t, "aliastest", alias)
	key = "functionB"
	name, alias = GetFuncNameAndVersion(key)
	assert.Equal(t, "functionB", name)
	assert.Equal(t, "", alias)
}

func Test_GetAliasURN(t *testing.T) {
	key := "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:test007:alias007"
	actualKey := GetAliasURN("i1fe539427b24702acc11fbb4e134e17", "test007", "alias007")
	assert.Equal(t, key, actualKey)
}

func Test_IsAliasForFuncBranch(t *testing.T) {
	assert.Equal(t, false, IsAliasForFuncBranch("$latest"))
	assert.Equal(t, true, IsAliasForFuncBranch("!latest"))
}

func Test_ContextValueString(t *testing.T) {
	ctx := ContextWithValueString(context.Background(), constants.HeaderTraceID, "test")
	assert.Equal(t, "test", ContextValueString(ctx, constants.HeaderTraceID))
}

func Test_ContextKeyString(t *testing.T) {
	assert.Equal(t, "test", contextKey("test").String())
}

func Test_GenerateWorkerInfoStr(t *testing.T) {
	workerInfo := GenerateWorkerInfoStr("testFunc", "127.0.0.1", "127.0.0.1", false, false)
	assert.Equal(t, "{\"funcKeyWithResource\":\"testFunc\",\"workerAddress\":\"127.0.0.1\",\"ownerIP\":\"127.0.0.1\",\"isDirectFunc\":false,\"hasInitializer\":false}", workerInfo)
}

func Test_ParseFromWorkerInfoStr(t *testing.T) {
	workerInfo := ParseFromWorkerInfoStr("{\"funcKeyWithResource\":\"testFunc\",\"workerAddress\":\"127.0.0.1\",\"ownerIP\":\"127.0.0.1\"}")
	assert.Equal(t, "testFunc", workerInfo.FuncKeyWithResource)
	assert.Equal(t, "127.0.0.1", workerInfo.WorkerAddress)
	assert.Equal(t, "127.0.0.1", workerInfo.OwnerIP)
}

func Test_AddGetDeleteUserData(t *testing.T) {
	m := map[string]string{
		"flow_id":       "example_flow_id_001",
		"sn_service_id": "example_sn_service_id_001",
	}
	data, err := json.Marshal(&m)
	assert.Nil(t, err)
	oldUserData := string(data)
	oldSNID, err := GetFromUserData(oldUserData, "sn_service_id")
	assert.Nil(t, err)
	assert.Equal(t, oldSNID, "example_sn_service_id_001")
	_, err = GetFromUserData("error id", "sn_service_id")
	assert.NotNil(t, err)
	newKey := "new_key"
	newValue := "new_value"
	newUserData, err := AddToUserData(oldUserData, newKey, newValue)
	assert.Nil(t, err)
	_, err = AddToUserData("error user data", newKey, newValue)
	assert.NotNil(t, err)
	actualNewValue, err := GetFromUserData(newUserData, newKey)
	assert.Nil(t, err)
	assert.Equal(t, newValue, actualNewValue)
	newUserData = DelFromUserData(newUserData, newKey)
	notExistValue, err := GetFromUserData(newUserData, newKey)
	assert.EqualError(t, err, "key:new_key is not exists")
	assert.Equal(t, "", notExistValue)

	// userData is empty
	_, err = GetFromUserData("", "")
	assert.NotNil(t, err)
	result := DelFromUserData("", "")
	assert.Equal(t, "", result)
	_, err = GetUserData("")
	assert.NotNil(t, err)
	_, err = GetUserData("err user data")
	assert.NotNil(t, err)
	_, err = AddToUserData("", "", "")
	assert.Nil(t, err)
}

func Test_GetAndParseFuncKey(t *testing.T) {
	functionKey := GetFuncKey("tenantID", "funcName", "latest")
	assert.Equal(t, "tenantID/funcName/latest", functionKey)

	tenantID, funcName, funcVersion := ParseFuncKey(functionKey)
	assert.Equal(t, "tenantID", tenantID)
	assert.Equal(t, "funcName", funcName)
	assert.Equal(t, "latest", funcVersion)
}

func TestGetFuncKeyWithResource(t *testing.T) {
	insResource := types.InstanceResource{
		CPU:    "400",
		Memory: "256",
	}
	functionKeyWithResource := "functionKey/400-256"
	assert.Equal(t, functionKeyWithResource, GetFuncKeyWithResource("functionKey", insResource))
}

func TestGetFuncKeyFromFuncKeyWithResource(t *testing.T) {
	functionKeyWithResource := "tenantID/hello/latest/400-256"
	funcKey := GetFuncKeyFromFuncKeyWithResource(functionKeyWithResource)
	assert.Equal(t, "tenantID/hello/latest", funcKey)
}

func Test_CheckIP(t *testing.T) {
	convey.Convey("TestCheckIP", t, func() {
		convey.Convey("success", func() {
			tests := []struct {
				name string
				want bool
				ip   string
			}{
				{"nil ip", false, ""},
				{"invalid ip", false, "127.0.0.1:8080"},
				{"normal ip", true, "127.0.0.1"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					portFlag := CheckIP(tt.ip)
					assert.Equal(t, tt.want, portFlag)
				})
			}
		})
	})
}

func Test_CheckPort(t *testing.T) {
	convey.Convey("TestCheckPort", t, func() {
		convey.Convey("success", func() {
			tests := []struct {
				name string
				want bool
				port string
			}{
				{"regexp fail", false, "port111"},
				{"min port", false, "-1"},
				{"max port", false, "65536"},
				{"normal port", true, "1024"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					portFlag := CheckPort(tt.port)
					assert.Equal(t, tt.want, portFlag)
				})
			}
		})
	})
}

func Test_GetPriority(t *testing.T) {
	assert.Equal(t, LowPriority, GetPriority("l"))
	assert.Equal(t, MediumPriority, GetPriority("m"))
	assert.Equal(t, HighPriority, GetPriority("h"))
	assert.Equal(t, 0, GetPriority("k"))
}

func TestGetResourceFromFuncKeyWithResource(t *testing.T) {
	tasks := []struct {
		functionKeyWithResource string
		want                    *types.InstanceResource
	}{
		{
			functionKeyWithResource: "tenantID/funcName/version/100-100",
			want: &types.InstanceResource{
				CPU:    "100",
				Memory: "100",
			},
		},
		{
			functionKeyWithResource: "tenantID/funcName/version",
			want:                    nil,
		},
		{
			functionKeyWithResource: "tenantID/funcName/version/100",
			want:                    nil,
		},
	}
	for _, task := range tasks {
		assert.Equal(t, task.want, GetResourceFromFuncKeyWithResource(task.functionKeyWithResource))
	}
}

func TestGetCPUMemoryFromInstanceID(t *testing.T) {
	convey.Convey("TestGetCPUMemoryFromInstanceID", t, func() {
		convey.Convey("success", func() {
			tests := []struct {
				name       string
				cpu        string
				memory     string
				instanceID string
			}{
				{"invalid instanceID", "", "",
					"pool23-400-256-fusion-7ddf8c485d-mcmpr"},
				{"success instanceID", "400", "256",
					"defaultaz-#-pool23-400-256-fusion-7ddf8c485d-mcmpr"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					cpu, memory := GetCPUMemoryFromInstanceID(tt.instanceID)
					assert.Equal(t, tt.cpu, cpu)
					assert.Equal(t, tt.memory, memory)
				})
			}
		})
	})
}

func TestBuildFunctionPropertyWithValidUrn(t *testing.T) {
	convey.Convey("TestBuildFunctionProperty", t, func() {
		urn := "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:Function:1"
		dst, err := BuildFunctionProperty(urn, "", "123")
		convey.So(err, convey.ShouldBeNil)
		convey.So(dst.TenantID, convey.ShouldEqual, "i1fe539427b24702acc11fbb4e134e17")
		convey.So(dst.FunctionName, convey.ShouldEqual, "Function")
		convey.So(dst.FunctionVersion, convey.ShouldEqual, "1")
		convey.So(dst.FutureID, convey.ShouldEqual, "123")
	})
}

func TestBuildFunctionPropertyWithInvalidUrn(t *testing.T) {
	convey.Convey("TestBuildFunctionProperty", t, func() {
		urn := "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:Function"
		_, err := BuildFunctionProperty(urn, "", "")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestBuildFunctionPropertyWithDemandInstance(t *testing.T) {
	convey.Convey("TestBuildFunctionProperty", t, func() {
		urn := "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:Function:1"
		dst, err := BuildFunctionProperty(urn, "instanceA", "")
		convey.So(err, convey.ShouldBeNil)
		convey.So(dst.InstanceID, convey.ShouldEqual, "instanceA")
	})
}

func TestGetMemoryFromFuncKeyResource(t *testing.T) {
	convey.Convey("TestGetMemoryFromFuncKeyResourceError", t, func() {
		memory := GetMemoryFromFuncKeyResource(nil)
		convey.ShouldEqual(memory, 0)
	})
	convey.Convey("TestGetMemoryFromFuncKeyResourceError", t, func() {
		res := &types.InstanceResource{
			CPU:    "400",
			Memory: "256a",
		}
		memory := GetMemoryFromFuncKeyResource(res)
		convey.ShouldEqual(memory, 0)
	})
}

func TestGetCPUFlagList(t *testing.T) {
	convey.Convey("Test Get CPU Flags List", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte("pu vme de pse tsc"), nil
			})})
		defer patches.ResetAll()
		res := GetCPUFlagList()
		expectedList := []string{"de", "pse", "pu", "tsc", "vme"}
		expectedListString := strings.Join(expectedList, ",")
		convey.So(strings.Join(res, ","), convey.ShouldEqual, expectedListString)
	})
	convey.Convey("Test Get CPU Flags List Error", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return nil, errors.New("test error")
			})})
		defer patches.ResetAll()
		res := GetCPUFlagList()
		convey.So(res, convey.ShouldBeNil)
	})
}

func Test_CheckHasSnError(t *testing.T) {
	assert.Equal(t, false, CheckHasSnError(nil))
}

func TestGetMirrorDiskSpace(t *testing.T) {
	convey.Convey("Test Get mirror docker disk space output failed", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(engine.IsContainerd, func() bool {
			return false
		})})
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return nil, errors.New("test error")
			})})
		defer patches.ResetAll()
		_, err := GetMirrorDiskSpace()
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Get mirror containerd disk space output failed", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(engine.IsContainerd, func() bool {
			return true
		})})
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return nil, errors.New("test error")
			})})
		defer patches.ResetAll()
		_, err := GetMirrorDiskSpace()
		convey.So(err, convey.ShouldNotBeNil)
	})
	mockOutputStr := "Filesystem      Size  Used Avail Use% Mounted on"
	failOutputStr := "Filesystem      Size  Used Avail Use% Mounted on\n/dev/vdb         98G"
	outputStr := "Filesystem      Size  Used Avail Use% Mounted on\n/dev/vdb         98G   82G   12G  88% /home/disk"
	convey.Convey("Test Get mirror containerd disk space output failed", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(engine.IsContainerd, func() bool {
			return true
		})})
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte(mockOutputStr), nil
			})})
		defer patches.ResetAll()
		_, err := GetMirrorDiskSpace()
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Get mirror containerd disk space output failed", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(engine.IsContainerd, func() bool {
			return true
		})})
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte(failOutputStr), nil
			})})
		defer patches.ResetAll()
		_, err := GetMirrorDiskSpace()
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Get mirror containerd disk space output failed", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(engine.IsContainerd, func() bool {
			return true
		})})
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte(outputStr), nil
			})})
		defer patches.ResetAll()
		_, err := GetMirrorDiskSpace()
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestGetSessionConcurrency(t *testing.T) {
	assert.Equal(t, defaultSessionConcurrency, GetSessionConcurrency(-1))
	assert.Equal(t, defaultSessionConcurrency, GetSessionConcurrency(0))

	assert.Equal(t, defaultSessionConcurrency, GetSessionConcurrency(9))
	assert.Equal(t, defaultSessionConcurrency, GetSessionConcurrency(10))
	assert.Equal(t, defaultSessionConcurrency, GetSessionConcurrency(19))
	assert.Equal(t, 2, GetSessionConcurrency(20))
	assert.Equal(t, 2, GetSessionConcurrency(27))
	assert.Equal(t, 10, GetSessionConcurrency(100))

	sessionConcurrencyFactor = 5

	assert.Equal(t, defaultSessionConcurrency, GetSessionConcurrency(9))
	assert.Equal(t, 2, GetSessionConcurrency(10))
	assert.Equal(t, 3, GetSessionConcurrency(19))
	assert.Equal(t, 4, GetSessionConcurrency(20))
	assert.Equal(t, 5, GetSessionConcurrency(27))
	assert.Equal(t, 20, GetSessionConcurrency(100))
}
