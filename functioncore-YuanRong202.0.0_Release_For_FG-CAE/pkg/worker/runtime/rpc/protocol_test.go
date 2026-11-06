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

package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/runtime/rpc/pb"
	mockUtils "functioncore/test/utils"
)

func TestJSONProtocol_BuildLoadRequest(t *testing.T) {
	type args struct {
		c                *config.Configuration
		instanceFuncName string
		traceID          string
		extra            map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"test1", args{c: &config.Configuration{}, instanceFuncName: "test", traceID: "traceID"}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"test2 marshal error", args{c: &config.Configuration{}, instanceFuncName: "test", traceID: "traceID"}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			j := &JSONProtocol{}
			_, err := j.BuildLoadRequest(tt.args.c, tt.args.instanceFuncName, tt.args.traceID, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildLoadRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestJSONProtocol_BuildInitializerRequest(t *testing.T) {
	type args struct {
		traceID string
		extra   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"test1", args{traceID: "123", extra: map[string]string{"test": "test"}}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"test2 marshal error", args{traceID: "123", extra: map[string]string{"test": "test"}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			j := &JSONProtocol{}
			_, err := j.BuildInitializerRequest(tt.args.traceID, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildInitializerRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestProtoProtocol_BuildInitializerRequest(t *testing.T) {
	type args struct {
		traceID string
		extra   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 marshal error", args{traceID: "123", extra: map[string]string{"test": "test"}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
						return nil, errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &ProtoProtocol{}
			_, err := p.BuildInitializerRequest(tt.args.traceID, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildInitializerRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestJSONProtocol_BuildPreStopRequest(t *testing.T) {
	type args struct {
		traceID string
		extra   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"test1", args{traceID: "123", extra: map[string]string{"test": "test"}}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"test2 marshal error", args{traceID: "123", extra: map[string]string{"test": "test"}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			j := &JSONProtocol{}
			_, err := j.BuildPreStopRequest(tt.args.traceID, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildPreStopRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestProtoProtocol_BuildPreStopRequest(t *testing.T) {
	type args struct {
		traceID string
		extra   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 marshal error", args{traceID: "123", extra: map[string]string{"test": "test"}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
						return nil, errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &ProtoProtocol{}
			_, err := p.BuildPreStopRequest(tt.args.traceID, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildPreStopRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestProtoProtocol_ParsePreStopResponse(t *testing.T) {
	type args struct {
		response []byte
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 marshal error", args{[]byte("test")}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
						return errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &ProtoProtocol{}
			err := p.ParsePreStopResponse(tt.args.response)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePreStopResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestJSONProtocol_ParsePreStopResponse(t *testing.T) {
	rsp := PreStopResponse{}
	resp, _ := json.Marshal(rsp)
	type args struct {
		response []byte
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test1", args{response: resp}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"test2 marshal error", args{}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			j := &JSONProtocol{}
			err := j.ParsePreStopResponse(tt.args.response)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePreStopResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestProtoProtocol_ParseRuntimeLog(t *testing.T) {
	convey.Convey("TestProtoProtocol_ParseRuntimeLog", t, func() {
		convey.Convey("succeed to Parse Runtime Log", func() {
			log := &pb.FunctionLog{
				Timestamp: time.Now().String(),
			}
			logByte, _ := proto.Marshal(log)
			p := &ProtoProtocol{}
			_, err := p.ParseRuntimeLog(&functionlog.FunctionLog{
				Message: new(bytes.Buffer)}, logByte)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("succeed to Parse Runtime Log with std log", func() {
			log := &pb.FunctionLog{
				Timestamp: time.Now().String(),
			}
			logByte, _ := proto.Marshal(log)
			p := &ProtoProtocol{}
			_, err := p.ParseRuntimeLog(&functionlog.FunctionLog{
				Message: new(bytes.Buffer), IsStdLog: true}, logByte)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("failed to Unmarshal", func() {
			p := &ProtoProtocol{}
			_, err := p.ParseRuntimeLog(&functionlog.FunctionLog{
				Message: new(bytes.Buffer)}, []byte("test"))
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestJSONProtocol_BuildUpdateAgencyRequest(t *testing.T) {
	type args struct {
		traceID string
		extra   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"test1", args{traceID: "123", extra: map[string]string{"test": "test"}}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"test2 marshal error", args{traceID: "123", extra: map[string]string{"test": "test"}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			j := &JSONProtocol{}
			_, err := j.BuildUpdateAgencyRequest(tt.args.traceID, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildPreStopRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestProtoProtocol_BuildUpdateAgencyRequest(t *testing.T) {
	type args struct {
		traceID string
		extra   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 marshal error", args{traceID: "123", extra: map[string]string{"test": "test"}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
						return nil, errors.New("test err")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &ProtoProtocol{}
			_, err := p.BuildUpdateAgencyRequest(tt.args.traceID, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildPreStopRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}
