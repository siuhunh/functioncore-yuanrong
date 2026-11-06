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

package internal

import (
	"errors"

	"net/http"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/aliasroute"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	busUtils "functioncore/pkg/functionbus/utils"
	"functioncore/test/utils"
)

func TestBuildReqResult(t *testing.T) {
	result := BuildReqResult(&types.TCPResponse{})
	assert.NotNil(t, result.RspData)
}

func TestBuildReqResultError(t *testing.T) {
	patch := gomonkey.ApplyFunc(proto.Marshal, func(_ proto.Message) ([]byte, error) {
		return nil, errors.New("marshal error")
	})
	defer patch.Reset()
	result := BuildReqResult(&types.TCPResponse{})
	assert.Equal(t, result.ErrorCode, uint32(proxyerrors.InternalErrCode))
}

func TestBuildCallRequest(t *testing.T) {
	_, _, err := BuildCallRequest(&pb.Message_CallMessage{
		Dst: &pb.FunctionProperty{FunctionVersion: "latest"},
		Req: &pb.CallRequest{},
	})
	assert.NoError(t, err)

	_, _, err = BuildCallRequest(&pb.Message_CallMessage{
		Dst: &pb.FunctionProperty{TenantID: common.BusProxyTenantID},
		Req: &pb.CallRequest{},
	})
	assert.NoError(t, err)
}

func TestBuildCallRequestError(t *testing.T) {
	_, _, err := BuildCallRequest(&pb.Message_CallMessage{
		Dst: &pb.FunctionProperty{FunctionVersion: ""},
		Req: &pb.CallRequest{},
	})
	assert.NotNil(t, err)
}

func TestBuildCancelResult(t *testing.T) {
	result := BuildCancelResult(&types.TCPResponse{})
	assert.NotNil(t, result.RspData)
}

func TestBuildCancelResultError(t *testing.T) {
	patch := gomonkey.ApplyFunc(proto.Marshal, func(_ proto.Message) ([]byte, error) {
		return nil, errors.New("marshal error")
	})
	defer patch.Reset()
	result := BuildCancelResult(&types.TCPResponse{})
	assert.Equal(t, result.ErrorCode, uint32(proxyerrors.InternalErrCode))

}

func TestConvertFuncName(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc((*aliasroute.Aliases).GetFuncVersionURN, func(_ *aliasroute.Aliases, aliasURN string) string {
		return "sn:cn:yrk:tenantID:function:function1:myalias"
	})
	defer patches.Reset()

	funcName, funcVersion, err := ConvertFuncName("a", "function1:!alias", "")
	assert.NoError(t, err)
	assert.Equal(t, "function1", funcName)
	assert.Equal(t, "myalias", funcVersion)

	funcName, funcVersion, err = ConvertFuncName("a", "function1", "")
	assert.NoError(t, err)
	assert.Equal(t, "function1", funcName)
	assert.Equal(t, "latest", funcVersion)
}

func TestConvertErrorToHTTPResponseBody(t *testing.T) {
	data := ConvertErrorToHTTPResponseBody(0, "abc")
	assert.NotNil(t, data)
}

func TestBuildRecoverResult(t *testing.T) {
	convey.Convey("TestBuildRecoverResult", t, func() {
		msg := &types.TCPResponse{
			ErrorCode:    0,
			ErrorMessage: "",
			RawData:      []byte{1, 2, 3},
		}
		result := BuildRecoverResult(msg)
		convey.So(result.ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestBuildRecoverResultError(t *testing.T) {
	patch := gomonkey.ApplyFunc(proto.Marshal, func(_ proto.Message) ([]byte, error) {
		return nil, errors.New("marshal error")
	})
	defer patch.Reset()
	msg := &types.TCPResponse{
		ErrorCode:    0,
		ErrorMessage: "",
		RawData:      []byte{1, 2, 3},
	}
	result := BuildRecoverResult(msg)
	assert.Equal(t, result.ErrorCode, uint32(proxyerrors.InternalErrCode))
}

func TestConvertHTTPResponseStatusCode(t *testing.T) {
	convey.Convey("TestConvertHTTPResponseStatusCode", t, func() {
		convey.So(ConvertHTTPResponseStatusCode(proxyerrors.StateOKCode), convey.ShouldEqual, http.StatusOK)
		convey.So(ConvertHTTPResponseStatusCode(proxyerrors.ClientExitErrCode),
			convey.ShouldEqual, http.StatusInternalServerError)
	})
}

func TestBuildPosixReqResult(t *testing.T) {
	convey.Convey("TestBuildPosixReqResult case 1: failed to marshal", t, func() {
		patch := gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
			return []byte{}, errors.New("test")
		})
		defer func() {
			patch.Reset()
		}()

		result := BuildPosixReqResult(&types.TCPResponse{})
		convey.So(result.ErrorCode, convey.ShouldNotEqual, 0)
	})

	convey.Convey("TestBuildPosixReqResult case 2: succeed to BuildPosixReqResult", t, func() {
		patch := gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
			return []byte{}, nil
		})
		defer func() {
			patch.Reset()
		}()

		result := BuildPosixReqResult(&types.TCPResponse{})
		convey.So(result.ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestTestBuildPosixCallRequest(t *testing.T) {
	_, _, err := BuildPosixCallRequest(&pb.PosixCallRequest{
		FunctionVersion: "latest",
		RequestID:       "100",
	})
	assert.NoError(t, err)

	_, _, err = BuildPosixCallRequest(&pb.PosixCallRequest{
		RequestID: "xx100",
	})
	assert.Error(t, err)

	_, _, err = BuildPosixCallRequest(&pb.PosixCallRequest{
		TenantID:  common.BusProxyTenantID,
		RequestID: "100",
	})
	assert.NoError(t, err)

	_, _, err = BuildPosixCallRequest(&pb.PosixCallRequest{
		TenantID:  common.BusProxyTenantID,
		RequestID: "xx100",
	})
	assert.Error(t, err)

	_, _, err = BuildPosixCallRequest(&pb.PosixCallRequest{
		TenantID:  common.BusProxyTenantID,
		RequestID: "100",
		Args: []*pb.Arg{
			&pb.Arg{Value: []byte("test")},
		},
	})
	assert.NoError(t, err)
}

func Test_ConvertFuncName(t *testing.T) {
	convey.Convey("test ConvertFuncName", t, func() {
		convey.Convey("case1: routeVersion is empty, get metadata from etcd failed", func() {
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
			functionName, functionVersion, err := ConvertFuncName("tenantID", "bridgeFunc:!RELEASE", "")
			convey.So(functionName, convey.ShouldEqual, "bridgeFunc")
			convey.So(functionVersion, convey.ShouldEqual, "")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("case2: routeVersion is empty, get metadata from etcd ok, is bridgeFunc", func() {
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
			functionName, functionVersion, err := ConvertFuncName("tenantID", "bridgeFunc:!RELEASE", "")
			convey.So(functionName, convey.ShouldEqual, "bridgeFunc")
			convey.So(functionVersion, convey.ShouldEqual, "latest")
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case3: routeVersion is empty, get metadata from etcd ok, not bridgeFunc", func() {
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
			functionName, functionVersion, err := ConvertFuncName("tenantID", "bridgeFunc:!RELEASE", "")
			convey.So(functionName, convey.ShouldEqual, "bridgeFunc")
			convey.So(functionVersion, convey.ShouldEqual, "")
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}
