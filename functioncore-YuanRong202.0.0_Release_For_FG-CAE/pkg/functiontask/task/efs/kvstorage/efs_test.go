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

package kvstorage

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	errors2 "k8s.io/kube-openapi/pkg/validation/errors"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/utils"
)

func TestEFS_GetRequest(t *testing.T) {
	Convey("TestEFS_GetRequest", t, func() {
		id := "xx"
		s := NewStorage()
		e := New(s)
		var md = &FutureMetadata{}
		data, _ := json.Marshal(md)
		s.Set(metadataPrefix+id, string(data), 0)
		resp := common.Response{}
		data2, _ := json.Marshal(resp)
		s.Set(getKey("", "", "", id), string(data2), 0)
		f := e.GetRequest("", "", id, nil)
		So(f.GetResponse().Code, ShouldEqual, common.EOK)
	})
}
func TestEFS_SetResult(t *testing.T) {
	Convey("TestEFS_SetResult", t, func() {
		s := NewStorage()
		e := New(s)
		f := snfuture.NewExternalEvent("xx")
		err := e.SetResult("", "", f, &common.Response{
			Code:    0,
			Message: "xxx",
		})
		So(err, ShouldBeNil)
	})
}
func TestEFS_ClearResult(t *testing.T) {
	Convey("TestEFS_ClearResult", t, func() {
		s := NewStorage()
		e := New(s)
		f := snfuture.NewExternalEvent("xx")
		err := e.ClearResult("", "", f)
		So(err, ShouldBeNil)
	})
}

func TestEFS_SetRequest(t *testing.T) {
	type fields struct {
		storage Storage
	}
	type args struct {
		tenantID    string
		serviceID   string
		sncFuture   *snfuture.Future
		reqFutureID string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test json marshal error
		{
			"case1",
			fields{},
			args{
				sncFuture: &snfuture.Future{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte(""), errors.New("marshal error")
					}),
				})
				return patches
			},
			true,
		},
		// test Set error
		{
			"case2",
			fields{
				storage: &InMemoryStorage{},
			},
			args{
				sncFuture: &snfuture.Future{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Set",
						func(_ *InMemoryStorage, key string, value string, expireTime int) common.TaskError {
							return &errors2.CompositeError{}
						}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case3",
			fields{
				storage: &InMemoryStorage{},
			},
			args{
				sncFuture: &snfuture.Future{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Set",
						func(_ *InMemoryStorage, key string, value string, expireTime int) common.TaskError {
							return nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EFS{
				storage: tt.fields.storage,
			}
			patches := tt.patchesFunc()
			if got := e.SetRequest(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture, tt.args.reqFutureID); (got != nil) != tt.want {
				t.Errorf("SetRequest() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestEFS_GetRequest1(t *testing.T) {
	type fields struct {
		storage Storage
	}
	type args struct {
		tenantID  string
		serviceID string
		id        snfuture.ID
		reqIDList common.ReqIDList
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test get error
		{
			"case1",
			fields{
				storage: &InMemoryStorage{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Get",
						func(_ *InMemoryStorage, key string) (string, common.TaskError) {
							return "", &errors2.CompositeError{}
						}),
				})
				return patches
			},
			false,
		},
		// test json unmarshal error
		{
			"case2",
			fields{
				storage: &InMemoryStorage{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Get",
						func(_ *InMemoryStorage, key string) (string, common.TaskError) {
							return "", nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("unmarshal error")
					}),
				})
				return patches
			},
			false,
		},
		// test res
		{
			"case3",
			fields{
				storage: &InMemoryStorage{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Get",
						func(_ *InMemoryStorage, key string) (string, common.TaskError) {
							return "", nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
				})
				return patches
			},
			true,
		},
		{
			"case4",
			fields{
				storage: &InMemoryStorage{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Get",
						func(_ *InMemoryStorage, key string) (string, common.TaskError) {
							return "test", nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
				})
				return patches
			},
			true,
		},
		// test exception error
		{
			"case5",
			fields{
				storage: &InMemoryStorage{},
			},
			args{
				id: snfuture.ID("test"),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Get",
						func(_ *InMemoryStorage, key string) (string, common.TaskError) {
							if key == metadataPrefix+snfuture.ID("test") {
								return "", nil
							} else {
								return "", &errors2.CompositeError{}
							}
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
				})
				return patches
			},
			true,
		},
		// test json unmarshal value data error
		{
			"case6",
			fields{
				storage: &InMemoryStorage{},
			},
			args{
				id: snfuture.ID("test"),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Get",
						func(_ *InMemoryStorage, key string) (string, common.TaskError) {
							if key == metadataPrefix+snfuture.ID("test") {
								return "a", nil
							}
							return "b", nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						if string(data) == "b" {
							return errors.New("json unmarshal error")
						}
						return nil
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EFS{
				storage: tt.fields.storage,
			}
			patches := tt.patchesFunc()
			if got := e.GetRequest(tt.args.tenantID, tt.args.serviceID, tt.args.id, tt.args.reqIDList); (got != nil) != tt.want {
				t.Errorf("GetRequest() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getKey(t *testing.T) {
	type args struct {
		stateID   string
		tenantID  string
		serviceID string
		id        snfuture.ID
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// test empty stateID
		{
			"case1",
			args{
				id: snfuture.ID("1"),
			},
			"value1",
		},
		// test stateID not empty
		{
			"case2",
			args{
				stateID: "test",
			},
			"{:}:value:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getKey(tt.args.stateID, tt.args.tenantID, tt.args.serviceID, tt.args.id); got != tt.want {
				t.Errorf("getKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEFS_SetResult1(t *testing.T) {
	type fields struct {
		storage Storage
	}
	type args struct {
		tenantID  string
		serviceID string
		sncFuture *snfuture.Future
		result    *common.Response
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test result code != 0
		{
			"case1",
			fields{},
			args{
				result: &common.Response{
					Code: 1,
				},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		// test json marshal error
		{
			"case2",
			fields{},
			args{
				result: &common.Response{
					Code: 0,
				},
				sncFuture: &snfuture.Future{
					Metadata: snfuture.Metadata{
						ReqIDList: common.ReqIDList{},
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte(""), errors.New("json marshal error")
					}),
				})
				return patches
			},
			true,
		},
		// test setErr
		{
			"case3",
			fields{
				storage: &InMemoryStorage{},
			},
			args{
				result: &common.Response{
					Code: 0,
				},
				sncFuture: &snfuture.Future{
					Metadata: snfuture.Metadata{
						ReqIDList: common.ReqIDList{},
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&InMemoryStorage{}), "Set",
						func(_ *InMemoryStorage, key string, value string, expireTime int) common.TaskError {
							return common.NewTaskError(400, "task error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EFS{
				storage: tt.fields.storage,
			}
			patches := tt.patchesFunc()
			if got := e.SetResult(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture, tt.args.result); (got != nil) != tt.want {
				t.Errorf("SetResult() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestEFS_ClearResult1(t *testing.T) {
	type fields struct {
		storage Storage
	}
	type args struct {
		tenantID  string
		serviceID string
		sncFuture *snfuture.Future
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			fields{
				storage: NewStorage(),
			},
			args{
				sncFuture: &snfuture.Future{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(NewStorage()), "Delete",
						func(_ *InMemoryStorage, key string) common.TaskError {
							return common.NewTaskError(200, "delete error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EFS{
				storage: tt.fields.storage,
			}
			patches := tt.patchesFunc()
			if got := e.ClearResult(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture); (got != nil) != tt.want {
				t.Errorf("ClearResult() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}
