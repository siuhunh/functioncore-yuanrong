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

package tcpbus

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	commontypes "functioncore/pkg/common/types"
	buscommon "functioncore/pkg/functionbus/common"
	errcode "functioncore/pkg/functionbus/proxyerrors"
)

func TestNewVerifyTLSConfig(t *testing.T) {
	client := ClientOption{}
	config := newVerifyTLSConfig(client)
	if config.MinVersion != 0 {
		t.Errorf("creat wrong config")
	}

	client = ClientOption{
		Config: &tls.Config{
			MinVersion: 1,
		},
	}
	config = newVerifyTLSConfig(client)
	if config.MinVersion != 1 {
		t.Errorf("creat wrong config")
	}
}

func TestConnection_getAndCheckWorkerID(t *testing.T) {
	convey.Convey("TestConnection_getAndCheckWorkerID", t, func() {
		convey.Convey("TestConnection_getAndCheckWorkerID_success", func() {
			c := &Connection{}
			convey.So(c.getAndCheckWorkerID(), convey.ShouldBeNil)
			c.workerID = "defaultaz-#-pool25-800-768-fusion-12345-qwe12"
			rp := buscommon.RecoverProperty{
				Requests: make(map[string]*buscommon.RecoverTraceRecord, 0),
				PodName:  "pool25-800-768-fusion-12345-qwe12",
			}
			b, _ := json.Marshal(rp)
			defer gomonkey.ApplyMethod(reflect.TypeOf(c), "SendRecoverRequest", func(
				*Connection, context.Context, *commontypes.TCPRequest, string, string) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{RawData: b}, nil
			}).Reset()
			convey.So(c.getAndCheckWorkerID(), convey.ShouldBeNil)
		})

		convey.Convey("TestConnection_getAndCheckWorkerID_Failed", func() {
			c := &Connection{}
			c.workerID = "defaultaz-#-pool25-800-768-fusion-12345-qwe12"
			convey.So(c.getAndCheckWorkerID(), convey.ShouldNotBeNil)
			defer gomonkey.ApplyMethod(reflect.TypeOf(c), "SendRecoverRequest", func(
				*Connection, context.Context, *commontypes.TCPRequest, string, string) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{RawData: nil,
					ErrorCode: errcode.TimeoutErrCode, ErrorMessage: errcode.TimeoutErr}, nil
			}).Reset()
			err := c.getAndCheckWorkerID()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, errcode.TimeoutErr)
		})
	})
}

func TestInitPoolOptionsWithTLSAndVerify(t *testing.T) {
	opt := ClientOption{
		TLSEnable:  true,
		NeedVerify: true,
		ServerAddr: "localhost:8080",
	}
	receiveChan := make(chan interface{})
	connWG := &sync.WaitGroup{}
	isMsgChanOpen := int32(1)
	isConnectListened := int32(1)

	poolOpts := initPoolOptions(opt, receiveChan, connWG, isConnectListened)

	assert.NotNil(t, poolOpts.Dialer)
	assert.NotNil(t, poolOpts.ConnHandler)
	assert.Equal(t, defaultConns, poolOpts.MaxConns)
	assert.Equal(t, receiveChan, poolOpts.MsgChan)
	assert.Equal(t, &isMsgChanOpen, poolOpts.IsMsgChanOpen)
}

func TestInitPoolOptionsWithoutTLS(t *testing.T) {
	opt := ClientOption{
		TLSEnable:  false,
		ServerAddr: "localhost:8080",
	}
	receiveChan := make(chan interface{})
	connWG := &sync.WaitGroup{}
	isConnectListened := int32(1)

	poolOpts := initPoolOptions(opt, receiveChan, connWG, isConnectListened)

	assert.NotNil(t, poolOpts.Dialer)
	assert.NotNil(t, poolOpts.ConnHandler)
	assert.Equal(t, defaultConns, poolOpts.MaxConns)
	assert.Equal(t, receiveChan, poolOpts.MsgChan)
}
