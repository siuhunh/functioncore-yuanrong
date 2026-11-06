/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// package adaptor -
package adaptor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gotest.tools/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/core-sdk/sdk"
	busUtils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/functiontask/task/api/filter"
	"functioncore/pkg/functiontask/task/api/getinstance"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	taskConf "functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/taskcontext"
	taskutil "functioncore/pkg/functiontask/task/util"
	"functioncore/test/functiontask/test/testcontext"
)

func Test_isExistRealInETCD(t *testing.T) {
	patches := [...]*Patches{
		ApplyFunc((*etcd3.EtcdClient).GetValues,
			func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return nil, errors.New("error")
			}),
		ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	type args struct {
		stateID string
	}
	var a args
	a.stateID = "Test_isExistRealInETCD"
	tests := []struct {
		name    string
		args    args
		want    bool
		want1   string
		wantErr bool
	}{
		{"case1", a, false, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := isExistRealInETCD(tt.args.stateID)
			if (err != nil) != tt.wantErr {
				t.Errorf("isExistRealInETCD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isExistRealInETCD() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("isExistRealInETCD() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_isExistRealInETCD_2(t *testing.T) {
	patches := [...]*Patches{
		ApplyFunc((*etcd3.EtcdClient).GetValues,
			func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{"aaa", "bbb"}, nil
			}),
		ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	type args struct {
		stateID string
	}
	var a args
	a.stateID = "Test_isExistRealInETCD_2"
	tests := []struct {
		name    string
		args    args
		want    bool
		want1   string
		wantErr bool
	}{
		{"case1", a, false, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := isExistRealInETCD(tt.args.stateID)
			if (err != nil) != tt.wantErr {
				t.Errorf("isExistRealInETCD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isExistRealInETCD() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("isExistRealInETCD() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_isExistRealInETCD3(t *testing.T) {
	convey.Convey("success", t, func() {
		etcdCli := &etcd3.EtcdClient{
			Client: &clientv3.Client{},
		}
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return etcdCli
		}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(etcdCli), "GetValues",
			func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{"{ \"key\": \"val\" }"}, nil
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		existed, _, _ := isExistRealInETCD("key")
		convey.So(existed, convey.ShouldBeTrue)
	})
	convey.Convey("success", t, func() {
		etcdCli := &etcd3.EtcdClient{
			Client: &clientv3.Client{},
		}
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return etcdCli
		}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(etcdCli), "GetValues",
			func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{"{ \"key\": \"val\" }"}, nil
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		existed, _, _ := isExistRealInETCD("key1")
		convey.So(existed, convey.ShouldBeFalse)
	})
}

func TestAdaptorFilter(t *testing.T) {
	convey.Convey("success", t, func() {
		adaptor := &Adaptor{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(isStatefulFunctionCheck,
			func(taskCtx taskcontext.Context, args *FuncArgs) bool {
				return true
			}))
		patches = append(patches, gomonkey.ApplyFunc(isReqPodNameAndStateKeyEmpty,
			func(taskCtx taskcontext.Context, req *filter.RequestPb) bool {
				return false
			}))
		patches = append(patches, gomonkey.ApplyFunc(isReqStateIDExists,
			func(taskCtx taskcontext.Context, stateID string, metaData *metadata.Metadata, req *filter.RequestPb) bool {
				return false
			}))
		patches = append(patches, gomonkey.ApplyFunc(checkStateIsNotExistDuringCreatingState,
			func(taskCtx taskcontext.Context, stateID string, req *filter.RequestPb) bool {
				return true
			}))
		patches = append(patches, gomonkey.ApplyFunc(scaleUp,
			func(ctx context.Context, taskCtx taskcontext.Context, scaleUpParam *metadata.Metadata) {
			}))
		patches = append(patches, gomonkey.ApplyFunc(createStateQueue,
			func(taskCtx taskcontext.Context, metaData *metadata.Metadata, req *filter.RequestPb) {
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetFilterReq",
			func(x *CallRequestPb) *filter.RequestPb {
				return &filter.RequestPb{}
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		adaptor.filter(taskCtx, funcArgs)
	})
	convey.Convey("failed: getRealFuncName failed", t, func() {
		adaptor := &Adaptor{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		var patches []*Patches
		patches = append(patches, ApplyMethod(reflect.TypeOf(funcArgs), "GetFilterReq",
			func(x *CallRequestPb) *filter.RequestPb {
				return &filter.RequestPb{
					FuncName: "hello-world",
				}
			}))
		patches = append(patches, ApplyFunc(taskutil.GetRealFuncName,
			func(funcName, version, serviceId, tenantId string) (string, bool) {
				return "", false
			}))
		isFuncCalled := false
		patches = append(patches, ApplyFunc(processGetFuncNameErrorOfFilter, func(taskCtx taskcontext.Context) {
			isFuncCalled = true
			return
		}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		adaptor.filter(taskCtx, funcArgs)
		convey.So(isFuncCalled, convey.ShouldBeTrue)
	})
}

func TestAdaptorGetInstance(t *testing.T) {
	convey.Convey("success", t, func() {
		adaptor := &Adaptor{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(isStateExist,
			func(taskCtx taskcontext.Context, funcName, stateID string) (bool, snerror.SNError) {
				return true, nil
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetGetInstanceReq",
			func(x *CallRequestPb) *getinstance.RequestPb {
				return &getinstance.RequestPb{
					FuncName: "hello-world",
					Key:      "test-key",
				}
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		adaptor.getInstance(taskCtx, funcArgs)
	})
	convey.Convey("failed", t, func() {
		adaptor := &Adaptor{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(isStateExist,
			func(taskCtx taskcontext.Context, funcName, stateID string) (bool, snerror.SNError) {
				return false, nil
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetGetInstanceReq",
			func(x *CallRequestPb) *getinstance.RequestPb {
				return &getinstance.RequestPb{
					FuncName: "hello-world",
					Key:      "test-key",
				}
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		adaptor.getInstance(taskCtx, funcArgs)
	})
	convey.Convey("failed: getRealFuncName failed", t, func() {
		adaptor := &Adaptor{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		var patches []*Patches
		patches = append(patches, ApplyMethod(reflect.TypeOf(funcArgs), "GetGetInstanceReq",
			func(x *CallRequestPb) *getinstance.RequestPb {
				return &getinstance.RequestPb{
					FuncName: "hello-world",
					Key:      "test-key",
				}
			}))
		patches = append(patches, ApplyFunc(taskutil.GetRealFuncName,
			func(funcName, version, serviceId, tenantId string) (string, bool) {
				return "", false
			}))
		isFuncCalled := false
		patches = append(patches, ApplyFunc(processGetFuncNameErrorOfGetInstance, func(taskCtx taskcontext.Context) {
			isFuncCalled = true
			return
		}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		adaptor.getInstance(taskCtx, funcArgs)
		convey.So(isFuncCalled, convey.ShouldBeTrue)
	})
}

func Test_IsReqStateIDExists(t *testing.T) {
	taskCtx := testcontext.New(common.MethodReadState)
	metadata := &metadata.Metadata{}
	req := &filter.RequestPb{}
	value := isReqStateIDExists(taskCtx, "test1", metadata, req)
	assert.Equal(t, true, value)
}

func Test_createStateQueue(t *testing.T) {
	taskCtx := testcontext.New(common.MethodReadState)
	metadata := &metadata.Metadata{}
	req := &filter.RequestPb{}
	createStateQueue(taskCtx, metadata, req)
}

func Test_checkStateIsNotExistDuringCreatingState(t *testing.T) {
	convey.Convey("Test_CheckStateIsNotExistDuringCreatingState", t, func() {
		convey.Convey("checkCacheConsistencyAndRepair is true", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			req := &filter.RequestPb{}
			patch := [...]*Patches{
				ApplyFunc(CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
					funcName, stateID string) bool {
					return true
				}),
				ApplyFunc(recoverStateHashKey, func(stateHashKey, stateID string, taskCtx taskcontext.Context) {}),
				ApplyFunc(rollbackStateInfoInRedis, func(taskCtx taskcontext.Context, funcName, stateID string) {}),
				ApplyFunc(taskutil.DeleteStateInRedis, func(tenantID, serviceID, funcName, stateID string) snerror.SNError {
					return nil
				}),
				ApplyFunc(stateclient.HExists, func(stateHashKey, stateID string) (bool, snerror.SNError) {
					return false, nil
				}),
			}
			defer func() {
				for _, p := range patch {
					p.Reset()
				}
			}()
			result := checkStateIsNotExistDuringCreatingState(taskCtx, stateID, req)
			assert.Equal(t, false, result)
		})

		convey.Convey("isStateExist is false", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			req := &filter.RequestPb{}
			patches := [...]*Patches{
				ApplyFunc(isStateExist, func(taskCtx taskcontext.Context, funcName, stateID string) (bool, snerror.SNError) {
					return false, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			result := checkStateIsNotExistDuringCreatingState(taskCtx, stateID, req)
			assert.Equal(t, true, result)
		})

		convey.Convey("redis error", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			req := &filter.RequestPb{}
			patches := [...]*Patches{
				ApplyFunc(isStateExist, func(taskCtx taskcontext.Context, funcName, stateID string) (bool, snerror.SNError) {
					err := snerror.New(500, "state client is error")
					return false, err
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			result := checkStateIsNotExistDuringCreatingState(taskCtx, stateID, req)
			assert.Equal(t, false, result)
		})
	})
}

func Test_checkStateIsExistDuringCreatingState(t *testing.T) {
	patches := [...]*Patches{
		ApplyFunc(isStateExist, func(taskCtx taskcontext.Context, funcName, stateID string) (bool, snerror.SNError) {
			return true, nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	taskCtx := testcontext.New(common.MethodReadState)
	stateID := "test1"
	req := &filter.RequestPb{}
	result := checkStateIsNotExistDuringCreatingState(taskCtx, stateID, req)
	assert.Equal(t, false, result)

	req = &filter.RequestPb{
		PodName: "PodName",
	}
	result = checkStateIsNotExistDuringCreatingState(taskCtx, stateID, req)
	assert.Equal(t, false, result)
}

func Test_checkIsNewStateKeyScenario(t *testing.T) {
	taskCtx := testcontext.New(common.MethodReadState)
	stateID := "test1"
	stateKey := "stateKey"
	result := isNewStateKeyScenario(taskCtx, stateID, stateKey)
	assert.Equal(t, false, result)
}

func Test_isStateExist(t *testing.T) {
	convey.Convey("Test_isStateExist", t, func() {
		convey.Convey("redis error, existInETCD is false", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			funcName := "test1"
			patches := [...]*Patches{
				ApplyFunc(CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
					funcName, stateID string) bool {
					return false
				}),
				ApplyFunc(stateclient.HExists, func(stateHashKey, stateID string) (bool, snerror.SNError) {
					err := snerror.New(500, "state client is error")
					return false, err
				}),
				ApplyFunc(recoverStateHashKey, func(stateHashKey, stateID string, taskCtx taskcontext.Context) {}),
				ApplyFunc(rollbackStateInfoInRedis, func(taskCtx taskcontext.Context, funcName, stateID string) {}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			result, err := isStateExist(taskCtx, funcName, stateID)
			assert.Equal(t, false, result)
			assert.Equal(t, "state client is error", err.Error())
		})

		convey.Convey("isExistInRedis true, existInETCD is true", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			funcName := "test1"
			patches := [...]*Patches{
				ApplyFunc(CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
					funcName, stateID string) bool {
					return true
				}),
				ApplyFunc(stateclient.HExists, func(stateHashKey, stateID string) (bool, snerror.SNError) {
					return true, nil
				}),
				ApplyFunc(recoverStateHashKey, func(stateHashKey, stateID string, taskCtx taskcontext.Context) {}),
				ApplyFunc(rollbackStateInfoInRedis, func(taskCtx taskcontext.Context, funcName, stateID string) {}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			result, err := isStateExist(taskCtx, funcName, stateID)
			assert.Equal(t, true, result)
			assert.Equal(t, nil, err)
		})

		convey.Convey("isExistInRedis false, existInETCD true", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			funcName := "test1"
			patches := [...]*Patches{
				ApplyFunc(stateclient.HExists, func(key string, field string) (bool, snerror.SNError) {
					return false, nil
				}),
				ApplyFunc(CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
					funcName, stateID string) bool {
					return true
				}),
				ApplyFunc(recoverStateHashKey, func(stateHashKey, stateID string, taskCtx taskcontext.Context) {}),
				ApplyFunc(rollbackStateInfoInRedis, func(taskCtx taskcontext.Context, funcName, stateID string) {}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			result, err := isStateExist(taskCtx, funcName, stateID)
			assert.Equal(t, true, result)
			assert.Equal(t, nil, err)
		})

		convey.Convey("isExistInRedis true, existInETCD false, config.Conf.PersistentState false", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			funcName := "test1"
			patches := [...]*Patches{
				ApplyFunc(stateclient.HExists, func(key string, field string) (bool, snerror.SNError) {
					return true, nil
				}),
				ApplyFunc(CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
					funcName, stateID string) bool {
					return false
				}),
				ApplyFunc(taskutil.DeleteStateInRedis, func(tenantID, serviceID, functionName, stateID string) snerror.SNError {
					var err snerror.SNError
					return err
				}),
				ApplyFunc(recoverStateHashKey, func(stateHashKey, stateID string, taskCtx taskcontext.Context) {}),
				ApplyFunc(rollbackStateInfoInRedis, func(taskCtx taskcontext.Context, funcName, stateID string) {}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			result, err := isStateExist(taskCtx, funcName, stateID)
			assert.Equal(t, false, result)
			assert.Equal(t, nil, err)
		})

		convey.Convey("isExistInRedis true, existInETCD false, config.Conf.PersistentState true", func() {
			taskCtx := testcontext.New(common.MethodReadState)
			stateID := "test1"
			funcName := "test1"
			patches := [...]*Patches{
				ApplyFunc(stateclient.HExists, func(key string, field string) (bool, snerror.SNError) {
					return true, nil
				}),
				ApplyFunc(CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
					funcName, stateID string) bool {
					return false
				}),
				ApplyFunc(taskutil.DeleteStateInRedis, func(tenantID, serviceID, functionName, stateID string) snerror.SNError {
					var err snerror.SNError
					return err
				}),
				ApplyFunc(taskConf.DefaultConfig, func() *taskConf.Config {
					return &taskConf.Config{
						MaxSingleWaitFutureCnt: taskConf.DefaultMaxSingleWaitFutureCnt,
						CallTimeout:            int64(taskConf.DefaultCallTimeout),
						ResolveDeadlock:        true,
						FutureTTL:              taskConf.DefaultFutureTTL,
						ExternalFutureTTL:      taskConf.DefaultExternalFutureTTL,
						MaxNumsOfStateID:       taskConf.DefaultMaxNumsOfStateID,
						PersistentState:        true,
					}
				}),
				ApplyFunc(sdk.DeleteState, func(stateValueKey string) snerror.SNError {
					var err snerror.SNError
					return err
				}),
				ApplyFunc(recoverStateHashKey, func(stateHashKey, stateID string, taskCtx taskcontext.Context) {}),
				ApplyFunc(rollbackStateInfoInRedis, func(taskCtx taskcontext.Context, funcName, stateID string) {}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			result, err := isStateExist(taskCtx, funcName, stateID)
			assert.Equal(t, false, result)
			assert.Equal(t, nil, err)
		})
	})
}

func Test_taskContextSetError(t *testing.T) {
	taskCtx := testcontext.New(common.MethodReadState)
	metadata := &metadata.Metadata{}
	err := common.NewTaskError(int32(1), "")
	taskContextSetError(taskCtx, metadata, err)
}

func Test_GetStateKey(t *testing.T) {
	convey.Convey("", t, func() {
		adaptor := &Adaptor{}
		taskCtx := testcontext.New(common.MethodFilter)
		req := &filter.RequestPb{}
		metadata := &metadata.Metadata{}
		_, flag := adaptor.getStateKey(taskCtx, req, metadata)
		assert.Equal(t, false, flag)
	})

	convey.Convey("getStateKey return nil", t, func() {
		adaptor := &Adaptor{}
		taskCtx := testcontext.New(common.MethodFilter)
		req := &filter.RequestPb{Key: "", PodName: "test1"}
		metadata := &metadata.Metadata{}
		patch := gomonkey.ApplyFunc(util.GetFuncStateKey, func(taskCtx taskcontext.Context, funcName string, clientInstance bridgetypes.ClientInstance) (string,
			common.TaskError) {
			return "", nil
		})
		defer patch.Reset()
		_, flag := adaptor.getStateKey(taskCtx, req, metadata)
		assert.Equal(t, true, flag)
	})

	convey.Convey("getStateKey return nil", t, func() {
		adaptor := &Adaptor{}
		taskCtx := testcontext.New(common.MethodFilter)
		req := &filter.RequestPb{Key: "", PodName: "test1"}
		metadata := &metadata.Metadata{}
		patch := gomonkey.ApplyFunc(util.GetFuncStateKey, func(taskCtx taskcontext.Context, funcName string, clientInstance bridgetypes.ClientInstance) (string, common.TaskError) {
			return "", common.NewTaskError(int32(0), "")
		})
		defer patch.Reset()
		_, flag := adaptor.getStateKey(taskCtx, req, metadata)
		assert.Equal(t, true, flag)
	})

	convey.Convey("getStateKey return nil", t, func() {
		adaptor := &Adaptor{}
		taskCtx := testcontext.New(common.MethodFilter)
		req := &filter.RequestPb{Key: "test1", PodName: "test1"}
		metadata := &metadata.Metadata{}
		patch := gomonkey.ApplyFunc(util.GetFuncStateKey, func(taskCtx taskcontext.Context, funcName string, clientInstance bridgetypes.ClientInstance) (string, common.TaskError) {
			return "", common.NewTaskError(int32(0), "")
		})
		defer patch.Reset()
		_, flag := adaptor.getStateKey(taskCtx, req, metadata)
		assert.Equal(t, true, flag)
	})
}

func Test_scaleUp(t *testing.T) {
	convey.Convey("makeScaleUpRequest return err", t, func() {
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		patch := gomonkey.ApplyFunc(makeScaleUpRequest, func(ctx context.Context, traceID string, scaleUpParam *metadata.Metadata) (*http.Request, error) {
			return nil, errors.New("fail")
		})
		defer patch.Reset()
		scaleUp(context.Background(), taskCtx, metadatas)
	})

	convey.Convey("makeScaleUpRequest return success", t, func() {
		client := &http.Client{}
		patches := []*Patches{
			ApplyFunc(busUtils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {}),
			ApplyFunc(busUtils.GetWorkerManagerClient, func() *http.Client {
				return client
			}),
			ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		scaleUp(context.Background(), taskCtx, metadatas)
	})

	convey.Convey("handleScaleUpResponse return err", t, func() {
		client := &http.Client{}
		patches := []*Patches{
			ApplyFunc(busUtils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {}),
			ApplyFunc(busUtils.GetWorkerManagerClient, func() *http.Client {
				return client
			}),
			ApplyFunc(handleScaleUpResponse, func(taskCtx taskcontext.Context, resp *http.Response, respBody []byte,
				scaleUpParam *metadata.Metadata) (getWorkerSuccessResponse, error) {
				return getWorkerSuccessResponse{}, errors.New("aaa")
			}),
			ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		scaleUp(context.Background(), taskCtx, metadatas)
	})

	convey.Convey("handleScaleUpResponse return success", t, func() {
		client := &http.Client{}
		patches := []*Patches{
			ApplyFunc(busUtils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {}),
			ApplyFunc(busUtils.GetWorkerManagerClient, func() *http.Client {
				return client
			}),
			ApplyFunc(handleScaleUpResponse, func(taskCtx taskcontext.Context, resp *http.Response, respBody []byte,
				scaleUpParam *metadata.Metadata) (getWorkerSuccessResponse, error) {
				return getWorkerSuccessResponse{}, nil
			}),
			ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		scaleUp(context.Background(), taskCtx, metadatas)
	})
}

type scaleTestPara struct {
	statusCode int
	body       []byte
	doRsp      error
}

var scaleTestParaInstance scaleTestPara

func mockActionClientDo(_ *http.Client, _ *http.Request) (*http.Response, error) {
	body := scaleTestParaInstance.body
	return &http.Response{
		StatusCode: scaleTestParaInstance.statusCode,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, scaleTestParaInstance.doRsp
}

func Test_makeScaleUpRequest(t *testing.T) {
	scaleUpParam := &metadata.Metadata{}
	scaleUpParam.FunctionName = "FunctionName"
	scaleUpParam.TenantID = "TenantID"
	scaleUpParam.StateID = "StateID"
	scaleUpParam.ServiceID = "ServiceID"
	scaleUpParam.StateKey = "StateKey"
	convey.Convey("makeScaleUpRequest marshal return error", t, func() {
		patches := []*Patches{
			ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return nil, errors.New("aaa")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		makeScaleUpRequest(context.Background(), "traceID", scaleUpParam)
	})
}

func Test_handleScaleUpResponse(t *testing.T) {
	type args struct {
		taskCtx      taskcontext.Context
		resp         *http.Response
		respBody     []byte
		scaleUpParam *metadata.Metadata
	}
	convey.Convey("handleScaleUpResponse unmarshal success", t, func() {
		resp := &http.Response{StatusCode: http.StatusAccepted}
		snErr := &snerror.BadResponse{Code: 0, Message: "aaa"}
		result, _ := json.Marshal(snErr)
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		handleScaleUpResponse(taskCtx, resp, result, metadatas)
	})

	convey.Convey("handleScaleUpResponse unmarshal error", t, func() {
		resp := &http.Response{StatusCode: http.StatusOK}
		patches := []*Patches{
			ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return errors.New("aaa")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		response, err := handleScaleUpResponse(taskCtx, resp, nil, metadatas)
		if err == nil {
			t.Errorf(" this should not be nil")
		}
		if response.Code != 0 || response.Message != "" {
			t.Errorf("this should be default set")
		}
	})

	convey.Convey("scale up response 0", t, func() {
		resp := &http.Response{StatusCode: http.StatusOK}
		worker := &worker{}
		scaleUpResp := getWorkerSuccessResponse{
			Code:   0,
			Worker: worker,
		}
		result, _ := json.Marshal(scaleUpResp)
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		handleScaleUpResponse(taskCtx, resp, result, metadatas)
	})

	convey.Convey("scale up response error", t, func() {
		resp := &http.Response{StatusCode: http.StatusOK}
		worker := &worker{}
		scaleUpResp := getWorkerSuccessResponse{
			Code:   1,
			Worker: worker,
		}
		result, _ := json.Marshal(scaleUpResp)
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		response, err := handleScaleUpResponse(taskCtx, resp, result, metadatas)
		if err == nil {
			t.Errorf(" this should not be nil")
		}
		if response.Code != 0 || response.Message != "" {
			t.Errorf("this should be default set")
		}
	})

	convey.Convey("scale up response 504211 error", t, func() {
		resp := &http.Response{StatusCode: http.StatusOK}
		worker := &worker{}
		scaleUpResp := getWorkerSuccessResponse{
			Code:   snerror.InnerRuntimeInitTimeoutCode,
			Worker: worker,
		}
		result, _ := json.Marshal(scaleUpResp)
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		response, err := handleScaleUpResponse(taskCtx, resp, result, metadatas)
		if err == nil {
			t.Errorf(" this should not be nil")
		}
		if response.Code != 0 || response.Message != "" {
			t.Errorf("this should be default set")
		}
	})

	convey.Convey("scale up response ok", t, func() {
		resp := &http.Response{StatusCode: http.StatusOK}
		worker := &worker{}
		scaleUpResp := getWorkerSuccessResponse{
			Code:   statusOKCode,
			Worker: worker,
		}
		result, _ := json.Marshal(scaleUpResp)
		taskCtx := testcontext.New(common.MethodFilter)
		metadatas := &metadata.Metadata{}
		handleScaleUpResponse(taskCtx, resp, result, metadatas)
	})
}

func Test_buildFilterMetadata(t *testing.T) {
	req := &filter.RequestPb{
		FuncName: "testFunc",
		Key:      "testKey",
		Creator:  "testCreator",
		Options: &filter.CreateOptions{
			Cpu:    2,
			Memory: 4096,
		},
	}
	stateID := "testStateID"
	taskCtx := testcontext.New(common.MethodFilter)
	convey.Convey("BuildFilterMetadata", t, func() {
		metaData := buildFilterMetadata(taskCtx, req, stateID)

		convey.Convey("Should set metadata fields correctly", func() {
			convey.So(metaData.FunctionName, convey.ShouldEqual, "testFunc")
			convey.So(metaData.StateID, convey.ShouldEqual, "testStateID")
			convey.So(metaData.StateKey, convey.ShouldEqual, "testKey")
			convey.So(metaData.Creator, convey.ShouldEqual, "testCreator")
			convey.So(metaData.CreateOptions.CPU, convey.ShouldEqual, 2)
			convey.So(metaData.CreateOptions.Memory, convey.ShouldEqual, 4096)
		})

		convey.Convey("Should handle nil Options correctly", func() {
			req.Options = nil
			metaData = buildFilterMetadata(taskCtx, req, stateID)
			convey.So(metaData.CreateOptions, convey.ShouldBeNil)
		})
	})
}

func Test_rollbackStateInfoInRedis(t *testing.T) {
	patch := [...]*Patches{
		ApplyFunc(CheckCacheConsistencyAndRepair, func(taskCtx taskcontext.Context, existInRedis bool,
			funcName, stateID string) bool {
			return true
		}),
		ApplyFunc(recoverStateHashKey, func(stateHashKey, stateID string, taskCtx taskcontext.Context) {}),
		ApplyFunc(rollbackStateInfoInRedis, func(taskCtx taskcontext.Context, funcName, stateID string) {}),
		ApplyFunc(taskutil.DeleteStateInRedis, func(tenantID, serviceID, funcName, stateID string) snerror.SNError {
			var err snerror.SNError
			return err
		}),
		ApplyFunc(stateclient.HExists, func(stateHashKey, stateID string) (bool, snerror.SNError) {
			return false, nil
		}),
		ApplyFunc(sdk.DeleteState, func(stateID string) snerror.SNError {
			var err snerror.SNError
			return err
		}),
		ApplyFunc(taskConf.DefaultConfig, func() *taskConf.Config {
			return &taskConf.Config{
				MaxSingleWaitFutureCnt: taskConf.DefaultMaxSingleWaitFutureCnt,
				CallTimeout:            int64(taskConf.DefaultCallTimeout),
				ResolveDeadlock:        true,
				FutureTTL:              taskConf.DefaultFutureTTL,
				ExternalFutureTTL:      taskConf.DefaultExternalFutureTTL,
				MaxNumsOfStateID:       taskConf.DefaultMaxNumsOfStateID,
				PersistentState:        true,
			}
		}),
	}
	defer func() {
		for _, p := range patch {
			p.Reset()
		}
	}()
	tc := &testcontext.Context{}
	rollbackStateInfoInRedis(tc, "funcName", "stateID")
}

func Test_recoverStateHashKey(t *testing.T) {
	patch := [...]*Patches{
		ApplyFunc(stateclient.HSetNX, func(key, filed, value string) (bool, snerror.SNError) {
			var err snerror.SNError
			return false, err
		}),
	}
	defer func() {
		for _, p := range patch {
			p.Reset()
		}
	}()
	tc := &testcontext.Context{}
	recoverStateHashKey("stateHashKey", "funcName", tc)
}
