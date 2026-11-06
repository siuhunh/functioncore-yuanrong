/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package frontend

import (
	"bufio"
	"clients/streamcache"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/model"
	"functioncore/pkg/frontend-invoke/statuscode"
	"functioncore/pkg/frontend-invoke/streamclient"
)

func TestProcessHTTPStreamRequest(t *testing.T) {
	c := &gin.Context{Request: &http.Request{Header: map[string][]string{}}}
	t.Run("failed to create producer", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{TraceID: "testTraceID"}
		responseWriter := &fakeResponseWriter{}
		c.Writer = responseWriter
		constant.FConfig.DataSystem.StreamEnable = true
		c.Request = &http.Request{Header: map[string][]string{constants.ContentType: {"multipart/form-data"}}}
		client := &streamclient.DataSystemStreamClient{}
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return client
		})
		patch.ApplyMethod(reflect.TypeOf(client), "CreateProducerByDefault",
			func(c *streamclient.DataSystemStreamClient, streamName string) (*streamcache.StreamProducer, error) {
				return nil, errors.New("error")
			})
		defer patch.Reset()
		err := processHTTPStreamRequest(c, reqInfo)
		assert.NotNil(t, err)
		expect, _ := json.Marshal(InvokeErrorResponse{Code: statuscode.CreateStreamProducerError,
			Message: errInter.Error()})
		assert.Equal(t, expect, responseWriter.body)
	})
	t.Run("failed to create client", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{TraceID: "testTraceID"}
		responseWriter := &fakeResponseWriter{}
		c.Writer = responseWriter
		constant.FConfig.DataSystem.StreamEnable = true
		c.Request = &http.Request{Header: map[string][]string{constants.ContentType: {"multipart/form-data"}}}
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return nil
		})
		defer patch.Reset()
		err := processHTTPStreamRequest(c, reqInfo)
		assert.NotNil(t, err)
		expect, _ := json.Marshal(InvokeErrorResponse{Code: statuscode.CreateStreamProducerError,
			Message: errInter.Error()})
		assert.Equal(t, expect, responseWriter.body)
	})
}

type MockReadCloser struct{}

func (m MockReadCloser) Close() error {
	return nil
}

func (m MockReadCloser) Read([]byte) (int, error) {
	return 0, nil
}

func TestSendDataToStream(t *testing.T) {
	t.Run("failed to read from http stream", func(t *testing.T) {
		producer := &streamcache.StreamProducer{}
		r := &MockReadCloser{}
		patch := gomonkey.ApplyFunc(io.ReadFull,
			func(r io.Reader, buf []byte) (n int, err error) {
				return 0, errors.New("error")
			})
		defer patch.Reset()
		err := sendBodyToStream(producer, r, "")
		assert.NotNil(t, err)
	})
	t.Run("failed to read from http stream", func(t *testing.T) {
		producer := &streamcache.StreamProducer{}
		r := &MockReadCloser{}
		patch := gomonkey.ApplyFunc(io.ReadFull,
			func(r io.Reader, buf []byte) (n int, err error) {
				return 1, nil
			})
		patch.ApplyFunc(streamclient.SendDataWithRetry, func(producer *streamcache.StreamProducer,
			ptr *uint8, size uint64) error {
			return errors.New("error")
		})
		defer patch.Reset()
		err := sendBodyToStream(producer, r, "")
		assert.NotNil(t, err)
	})
	t.Run("read until EOF", func(t *testing.T) {
		producer := &streamcache.StreamProducer{}
		r := &MockReadCloser{}
		patch := gomonkey.ApplyFunc(io.ReadFull,
			func(r io.Reader, buf []byte) (n int, err error) {
				buf = []byte{0}
				return 1, io.EOF
			})
		patch.ApplyFunc(streamclient.SendDataWithRetry, func(producer *streamcache.StreamProducer,
			ptr *uint8, size uint64) error {
			return nil
		})
		defer patch.Reset()
		err := sendBodyToStream(producer, r, "")
		assert.Nil(t, err)
	})
	t.Run("read until EOF", func(t *testing.T) {
		producer := &streamcache.StreamProducer{}
		r := &MockReadCloser{}
		patch := gomonkey.ApplyFunc(io.ReadFull,
			func(r io.Reader, buf []byte) (n int, err error) {
				buf = []byte{0}
				return 0, io.EOF
			})
		defer patch.Reset()
		err := sendBodyToStream(producer, r, "")
		assert.Nil(t, err)
	})
}

type fakeResponseWriter struct {
	close      bool
	header     http.Header
	body       []byte
	statusCode int
}

func (f *fakeResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	panic("implement me")
}

func (f *fakeResponseWriter) Flush() {}

func (f *fakeResponseWriter) CloseNotify() <-chan bool {
	return nil
}

func (f *fakeResponseWriter) Status() int {
	panic("implement me")
}

func (f *fakeResponseWriter) Size() int {
	panic("implement me")
}

func (f *fakeResponseWriter) WriteString(s string) (int, error) {
	panic("implement me")
}

func (f *fakeResponseWriter) Written() bool {
	panic("implement me")
}

func (f *fakeResponseWriter) WriteHeaderNow() {
	panic("implement me")
}

func (f *fakeResponseWriter) Pusher() http.Pusher {
	panic("implement me")
}

func (f *fakeResponseWriter) Header() http.Header {
	return f.header
}

func (f *fakeResponseWriter) Write(bytes []byte) (int, error) {
	if f.close {
		return 0, errors.New("fakeResponseWriter closed")
	}
	f.body = append(f.body, bytes...)
	return len(bytes), nil
}

func (f *fakeResponseWriter) WriteHeader(statusCode int) {
	f.statusCode = statusCode
}

func TestRequestBodyToStream(t *testing.T) {
	request := &MockReadCloser{}
	producer := &streamcache.StreamProducer{}
	client := &streamclient.DataSystemStreamClient{}
	t.Run("failed to query stream consumer", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{TraceID: "testTraceID"}
		patch := gomonkey.ApplyMethod(reflect.TypeOf(client), "QueryStreamConsumerExists",
			func(c *streamclient.DataSystemStreamClient, streamName string, timeout time.Duration) error {
				return errors.New("error")
			})
		patch.ApplyFunc(getRequestDeadline, func(reqInfo *model.FunctionRequestInfo) time.Duration {
			return 1 * time.Second
		})
		defer patch.Reset()
		requestBodyToStream(request, reqInfo, producer, client)
		assert.Equal(t, statuscode.QueryStreamCustomerError, int(reqInfo.RequestStreamErrorCode))
	})
	t.Run("failed to send data to stream", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{TraceID: "testTraceID"}
		patch := gomonkey.ApplyFunc(getRequestDeadline, func(reqInfo *model.FunctionRequestInfo) time.Duration {
			return 1 * time.Second
		})
		patch.ApplyMethod(reflect.TypeOf(client), "QueryStreamConsumerExists",
			func(c *streamclient.DataSystemStreamClient, streamName string, timeout time.Duration) error {
				return nil
			})
		patch.ApplyFunc(sendBodyToStream, func(producer *streamcache.StreamProducer, stream io.ReadCloser, traceID string) error {
			return errors.New("error")
		})
		defer func() {
			patch.Reset()
		}()
		requestBodyToStream(request, reqInfo, producer, client)
		assert.Equal(t, statuscode.SendDataToStreamError, int(reqInfo.RequestStreamErrorCode))
	})
}
