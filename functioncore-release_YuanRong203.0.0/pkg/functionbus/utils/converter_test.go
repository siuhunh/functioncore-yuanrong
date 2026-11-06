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
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"testing"

	"functioncore/pkg/functionbus/bus/pb"
	"github.com/stretchr/testify/assert"
)

func Test_StringToUint64WithRequestID(t *testing.T) {
	requestID, err := StringToUint64WithRequestID("100")
	assert.Equal(t, 100, int(requestID))
	assert.Nil(t, err)

	requestID, err = StringToUint64WithRequestID("xx100")
	assert.Equal(t, 0, int(requestID))
	assert.NotNil(t, err, "should return err")
}

func Test_FromCallResultPb(t *testing.T) {
	posixCallResult := &pb.PosixCallResult{
		InstanceID: "123",
		RequestID:  "100",
	}
	resp, err := FromCallResultPb(posixCallResult)
	assert.Equal(t, 100, int(resp.RequestID))
	assert.Nil(t, err)

	posixCallResult = &pb.PosixCallResult{
		InstanceID: "123",
		RequestID:  "xx100",
	}
	resp, err = FromCallResultPb(posixCallResult)
	assert.NotNil(t, err, "should return err")
}

func Test_ToCallResultPb(t *testing.T) {
	response := &types.TCPResponse{
		Summary: "summary",
	}
	posixResult := ToCallResultPb(response)
	assert.Equal(t, "summary", posixResult.Summary)
}

func Test_GetFuncPropertyFromPosixPb(t *testing.T) {
	req := &pb.PosixCallRequest{
		Function: "test",
	}
	funcProperty := GetFuncPropertyFromPosixPb(req)
	assert.Equal(t, "test", funcProperty.FunctionName)
}

func Test_FromPosixCallRequestPb(t *testing.T) {
	req := &pb.PosixCallRequest{
		Function:  "test",
		RequestID: "100",
		Args: []*pb.Arg{
			&pb.Arg{Value: []byte("test")},
		},
	}
	funcProperty := FromPosixCallRequestPb(req)
	assert.Equal(t, 100, int(funcProperty.RequestID))
	req = &pb.PosixCallRequest{
		Function:  "test",
		RequestID: "xx100",
		Args: []*pb.Arg{
			&pb.Arg{Value: []byte("test")},
		},
	}
	funcProperty = FromPosixCallRequestPb(req)
	assert.Equal(t, 0, int(funcProperty.RequestID))
}

func Test_ToPosixCallRequestPb(t *testing.T) {
	req := ToPosixCallRequestPb("traceID", &types.TCPRequest{}, &common.FunctionProperty{})
	assert.Equal(t, "traceID", req.TraceID)
}
