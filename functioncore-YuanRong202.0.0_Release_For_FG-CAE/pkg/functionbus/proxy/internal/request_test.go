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
