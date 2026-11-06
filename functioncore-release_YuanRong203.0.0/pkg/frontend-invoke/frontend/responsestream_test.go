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

package frontend

import (
	"clients/streamcache"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/model"
	"functioncore/pkg/frontend-invoke/streamclient"
)

type MockResponseWriter struct{}

func (m MockResponseWriter) Header() http.Header {
	return http.Header{}
}

func (m MockResponseWriter) Write([]byte) (int, error) {
	return 0, nil
}

func (m MockResponseWriter) WriteHeader(int) {
	return
}

func TestReceiveDataAndWrite(t *testing.T) {
	consumer := &streamcache.StreamConsumer{}
	w := &MockResponseWriter{}
	res := &responseInfo{}
	t.Run("failed to receive", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(streamclient.RecWithRetry,
			func(consumer *streamcache.StreamConsumer) ([]streamcache.Element, error) {
				return nil, errors.New("error")
			})
		defer patch.Reset()
		_, err := receiveAndWriteData(consumer, w, res)
		assert.NotNil(t, err)
	})
	t.Run("receive num 0", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(streamclient.RecWithRetry,
			func(consumer *streamcache.StreamConsumer) ([]streamcache.Element, error) {
				return []streamcache.Element{}, nil
			})
		defer patch.Reset()
		_, err := receiveAndWriteData(consumer, w, res)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "receive no data")
	})
	t.Run("succeed receive and write", func(t *testing.T) {
		data := []byte{0}
		patch := gomonkey.ApplyFunc(streamclient.RecWithRetry,
			func(consumer *streamcache.StreamConsumer) ([]streamcache.Element, error) {
				return []streamcache.Element{{
					Ptr:  &data[0],
					Size: 1}}, nil
			})
		patch.ApplyMethod(reflect.TypeOf(w), "Write", func(*MockResponseWriter, []byte) (int, error) {
			return 1, nil
		})
		defer patch.Reset()
		_, err := receiveAndWriteData(consumer, w, res)
		assert.Nil(t, err)
	})
}

func TestWriteStreamDataToResponse(t *testing.T) {
	consumer := &streamcache.StreamConsumer{}
	w := &MockResponseWriter{}
	res := &responseInfo{ResponseStreamName: "respStream", RequestID: "id"}
	t.Run("write timeout", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(receiveAndWriteData,
			func(*streamcache.StreamConsumer, http.ResponseWriter, *responseInfo) (uint64, error) {
				return 0, nil
			})
		defer patch.Reset()
		err := writeStreamDataToResponse(consumer, w, res, 1*time.Second)
		assert.NotNil(t, err)
		assert.Equal(t, "write stream respStream to response id timeout 1s", err.Error())
	})
	t.Run("write until EOF", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(receiveAndWriteData,
			func(*streamcache.StreamConsumer, http.ResponseWriter, *responseInfo) (uint64, error) {
				return 0, io.EOF
			})
		defer patch.Reset()
		err := writeStreamDataToResponse(consumer, w, res, 1*time.Second)
		assert.Nil(t, err)
	})
	t.Run("failed to write", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(receiveAndWriteData,
			func(*streamcache.StreamConsumer, http.ResponseWriter, *responseInfo) (uint64, error) {
				return 0, errors.New("error")
			})
		defer patch.Reset()
		err := writeStreamDataToResponse(consumer, w, res, 1*time.Second)
		assert.NotNil(t, err)
		assert.Equal(t, "error", err.Error())
	})
}

func TestProcessDownloadResponse(t *testing.T) {
	consumer := &streamcache.StreamConsumer{}
	t.Run("receive no element", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(streamclient.RecWithRetry,
			func(consumer *streamcache.StreamConsumer) ([]streamcache.Element, error) {
				return []streamcache.Element{}, nil
			})
		defer patch.Reset()
		err := processFrontendResponseStreamElements(consumer)
		assert.Nil(t, err)
	})
}

func TestStartListenFrontendResponseStream(t *testing.T) {
	client := &streamclient.DataSystemStreamClient{}
	constant.FConfig.DataSystem.IAMTokenEnable = true
	t.Run("not enable dataSystem", func(t *testing.T) {
		defer gomonkey.ApplyFunc(listenFrontendResponseStream,
			func(consumer *streamcache.StreamConsumer, stopCh chan struct{}, tenantID string) {}).Reset()
		constant.FConfig.DataSystem.StreamEnable = false
		tenantIDMap.Delete("tenantID")
		err := startListenFrontendResponseStream("tenantID")
		assert.Nil(t, err)
	})
	t.Run("start failed", func(t *testing.T) {
		defer gomonkey.ApplyFunc(listenFrontendResponseStream,
			func(consumer *streamcache.StreamConsumer, stopCh chan struct{}, tenantID string) {}).Reset()
		constant.FConfig.DataSystem.StreamEnable = true
		tenantIDMap.Delete("tenantID")
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return client
		})
		patch.ApplyMethod(reflect.TypeOf(client), "Subscribe",
			func(c *streamclient.DataSystemStreamClient, streamName string) (*streamcache.StreamConsumer, error) {
				return nil, errors.New("error")
			})
		defer patch.Reset()
		err := startListenFrontendResponseStream("tenantID")
		assert.NotNil(t, err)
	})
	t.Run("start success", func(t *testing.T) {
		defer gomonkey.ApplyFunc(listenFrontendResponseStream,
			func(consumer *streamcache.StreamConsumer, stopCh chan struct{}, tenantID string) {}).Reset()
		constant.FConfig.DataSystem.StreamEnable = true
		tenantIDMap.Delete("tenantID")
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return client
		})
		patch.ApplyMethod(reflect.TypeOf(client), "Subscribe",
			func(c *streamclient.DataSystemStreamClient, streamName string) (*streamcache.StreamConsumer, error) {
				return &streamcache.StreamConsumer{}, nil
			})
		patch.ApplyFunc(listenFrontendResponseStream, func(_ *streamcache.StreamConsumer, _ chan struct{}, _ string) {})
		defer patch.Reset()
		err := startListenFrontendResponseStream("tenantID")
		assert.Nil(t, err)

		// tenantID is already in map
		err = startListenFrontendResponseStream("tenantID")
		assert.Nil(t, err)
	})
	t.Run("different tenantID start success", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = true
		tenantIDMap.Delete("tenantID")
		defer gomonkey.ApplyFunc(subscribeFrontendResponseStream, func(tenantID string) (*streamcache.StreamConsumer, chan struct{}, error) {
			return nil, nil, nil
		}).Reset()
		clientCount := 0
		defer gomonkey.ApplyFunc(listenFrontendResponseStream, func(consumer *streamcache.StreamConsumer, stopCh chan struct{}, tenantID string) {
			clientCount++
		}).Reset()
		constant.FConfig.DataSystem.IAMTokenEnable = false
		err := startListenFrontendResponseStream("tenantID-1")
		assert.Nil(t, err)

		// tenantID is already in map
		err = startListenFrontendResponseStream("tenantID-2")
		assert.Nil(t, err)
		assert.Equal(t, 0, clientCount)

		err = startListenFrontendResponseStream("")
		assert.Nil(t, err)
		assert.Equal(t, 0, clientCount)
	})
	constant.FConfig.DataSystem.IAMTokenEnable = true
	t.Run("failed to create client", func(t *testing.T) {
		defer gomonkey.ApplyFunc(listenFrontendResponseStream,
			func(consumer *streamcache.StreamConsumer, stopCh chan struct{}, tenantID string) {}).Reset()
		constant.FConfig.DataSystem.StreamEnable = true
		tenantIDMap.Delete("tenantID")
		tenantIDMap.Delete("tenantID-1")
		tenantIDMap.Delete("tenantID-2")
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return nil
		})
		defer patch.Reset()
		err := startListenFrontendResponseStream("tenantID")
		assert.NotNil(t, err)
	})
}

func TestProcessResponseElement(t *testing.T) {
	testRequestID := "xxx-xxx-xx-xx"
	info := responseInfo{RequestID: testRequestID, ResponseStreamName: "resp-stream"}
	data, _ := json.Marshal(info)
	response := &ResponseStream{
		reqInfo:  &model.FunctionRequestInfo{Deadline: time.Now().Add(-1 * time.Second)},
		stopChan: NewStopChan(),
	}
	t.Run("failed to Unmarshal ResponseStream json str", func(t *testing.T) {
		streamElement := streamcache.Element{Ptr: &[]byte{1}[0], Size: 1}
		err := processResponseStreamElement(streamElement)
		assert.NotNil(t, err)
	})
	t.Run("not exists", func(t *testing.T) {
		streamElement := streamcache.Element{Ptr: &data[0], Size: uint64(len(data))}
		err := processResponseStreamElement(streamElement)
		assert.NotNil(t, err)
		errInfo := fmt.Sprintf("the response stream %s of request %s is not existed in responseStreamMap",
			info.ResponseStreamName, info.RequestID)
		assert.Equal(t, errInfo, err.Error())
	})
	t.Run("not responseStream struct type", func(t *testing.T) {
		defer func() {
			responseStreamMap.Delete(info.ResponseStreamName)
		}()
		streamElement := streamcache.Element{Ptr: &data[0], Size: uint64(len(data))}
		responseStreamMap.Store(info.ResponseStreamName, 1)
		err := processResponseStreamElement(streamElement)
		assert.NotNil(t, err)
		assert.Equal(t, "not responseStream struct type", err.Error())
	})
	t.Run("the request timeout", func(t *testing.T) {
		defer func() {
			responseStreamMap.Delete(info.ResponseStreamName)
		}()
		streamElement := streamcache.Element{Ptr: &data[0], Size: uint64(len(data))}
		responseStreamMap.Store(info.ResponseStreamName, response)
		err := processResponseStreamElement(streamElement)
		errInfo := fmt.Sprintf("the request %s has reached deadline ", testRequestID)
		assert.NotNil(t, err)
		assert.Equal(t, errInfo, err.Error())
	})
}

func TestWriteResponseStream(t *testing.T) {
	responseWriter := &fakeResponseWriter{header: map[string][]string{}}
	response := &ResponseStream{w: responseWriter, reqInfo: &model.FunctionRequestInfo{}}
	info := &responseInfo{
		ResponseStreamName: "testStream",
		StatusCode:         http.StatusOK,
		ResponseHeaders:    map[string][]string{"headerKey": {"headerValue"}},
	}
	client := &streamclient.DataSystemStreamClient{}
	t.Run("failed to subscribe ResponseStream stream", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return client
		})
		patch.ApplyMethod(reflect.TypeOf(client), "Subscribe",
			func(c *streamclient.DataSystemStreamClient, streamName string) (*streamcache.StreamConsumer, error) {
				return nil, errors.New("error")
			})
		defer patch.Reset()
		err := writeResponseStream(info, response, 1*time.Second)
		assert.NotNil(t, err)
	})
	t.Run("failed to writeStreamDataToResponse", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return client
		})
		patch.ApplyMethod(reflect.TypeOf(client), "Subscribe",
			func(c *streamclient.DataSystemStreamClient, streamName string) (*streamcache.StreamConsumer, error) {
				return &streamcache.StreamConsumer{}, nil
			})
		patch.ApplyFunc(writeStreamDataToResponse, func(consumer *streamcache.StreamConsumer, w http.ResponseWriter, res *responseInfo,
			timeout time.Duration) error {
			return errors.New("error")
		})
		defer func() {
			patch.Reset()
			responseWriter.statusCode = 0
		}()
		err := writeResponseStream(info, response, 1*time.Second)
		assert.Nil(t, err)
	})
	t.Run("succeed", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return client
		})
		patch.ApplyMethod(reflect.TypeOf(client), "Subscribe",
			func(c *streamclient.DataSystemStreamClient, streamName string) (*streamcache.StreamConsumer, error) {
				return &streamcache.StreamConsumer{}, nil
			})
		patch.ApplyFunc(writeStreamDataToResponse, func(consumer *streamcache.StreamConsumer, w http.ResponseWriter, res *responseInfo,
			timeout time.Duration) error {
			return nil
		})
		defer func() {
			patch.Reset()
			responseWriter.statusCode = 0
		}()
		err := writeResponseStream(info, response, 1*time.Second)
		assert.Nil(t, err)
		assert.Equal(t, http.StatusOK, responseWriter.statusCode)
	})
	t.Run("failed to create client", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return nil
		})
		defer patch.Reset()
		err := writeResponseStream(info, response, 1*time.Second)
		assert.NotNil(t, err)
	})
}

func TestListenFrontendResponseStream(t *testing.T) {
	consumer := &streamcache.StreamConsumer{}
	t.Run("list and stop", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(processFrontendResponseStreamElements, func(consumer *streamcache.StreamConsumer) error {
			return nil
		})
		stop := make(chan struct{})
		defer func() {
			patch.Reset()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			close(stop)
		}()
		listenFrontendResponseStream(consumer, stop, "tenantID")
	})
	t.Run("failed to process", func(t *testing.T) {
		stop := make(chan struct{})
		newConsumer := &streamcache.StreamConsumer{}
		alarmClient := &alarm.Client{}
		patch := gomonkey.ApplyFunc(processFrontendResponseStreamElements, func(consumer *streamcache.StreamConsumer) error {
			return errors.New("error")
		})
		patch.ApplyFunc(alarm.GetClient, func() *alarm.Client {
			return alarmClient
		})
		patch.ApplyMethod(reflect.TypeOf(alarmClient), "ClearAlarm",
			func(c *alarm.Client, almType alarm.Type) {})
		patch.ApplyFunc(subscribeFrontendResponseStream, func(_ string) (*streamcache.StreamConsumer, chan struct{}, error) {
			return newConsumer, stop, nil
		})
		defer func() {
			patch.Reset()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			close(stop)
			assert.Equal(t, consumer, newConsumer)
		}()
		listenFrontendResponseStream(consumer, stop, "tenantID")
	})
	t.Run("failed to Subscribe", func(t *testing.T) {
		client := &streamclient.DataSystemStreamClient{}
		patch := gomonkey.ApplyFunc(processFrontendResponseStreamElements, func(consumer *streamcache.StreamConsumer) error {
			return errors.New("error")
		})
		patch.ApplyFunc(streamclient.GetClient, func() *streamclient.DataSystemStreamClient {
			return client
		})
		patch.ApplyMethod(reflect.TypeOf(client), "Subscribe",
			func(c *streamclient.DataSystemStreamClient, streamName string) (*streamcache.StreamConsumer, error) {
				return nil, errors.New("error")
			})
		stop := make(chan struct{})
		defer func() {
			patch.Reset()
		}()
		go func() {
			time.Sleep(1 * time.Second)
			close(stop)
			assert.Equal(t, consumer, consumer)
		}()
		listenFrontendResponseStream(consumer, stop, "tenantID")
	})
}

func TestRegisterResponse(t *testing.T) {
	t.Run("not enable dataSystem", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = false
		reqInfo := &model.FunctionRequestInfo{}
		stopChan := RegisterResponse(MockResponseWriter{}, reqInfo)
		assert.Nil(t, stopChan)
		_, ok := responseStreamMap.Load(reqInfo.ResponseStreamName)
		assert.False(t, ok)
	})
	t.Run("enable dataSystem", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = true
		reqInfo := &model.FunctionRequestInfo{}
		stopChan := RegisterResponse(MockResponseWriter{}, reqInfo)
		assert.NotNil(t, stopChan)
		_, ok := responseStreamMap.Load(reqInfo.ResponseStreamName)
		assert.True(t, ok)
	})
}

func TestCheckIsResponseStream(t *testing.T) {
	t.Run("not enable datasystem", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = false
		result := CheckIsResponseStream("xxx")
		assert.False(t, result)
	})
	t.Run("not exist", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = true
		traceID := "not exist"
		result := CheckIsResponseStream(traceID)
		assert.False(t, result)
	})
	t.Run("not_response_stream_type", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = true
		traceID := "test_trace_id"
		responseStreamMap.Store(traceID, "not_response_stream_type")
		result := CheckIsResponseStream(traceID)
		assert.False(t, result)
	})
	t.Run("not stream", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = true
		traceID := "test_trace_id"
		responseStreamMap.Store(traceID, &ResponseStream{isStream: false})
		defer responseStreamMap.Delete(traceID)
		result := CheckIsResponseStream(traceID)
		assert.False(t, result)
	})
	t.Run("is stream", func(t *testing.T) {
		constant.FConfig.DataSystem.StreamEnable = true
		traceID := "test_trace_id"
		responseStreamMap.Store(traceID, &ResponseStream{isStream: true})
		defer responseStreamMap.Delete(traceID)
		result := CheckIsResponseStream(traceID)
		assert.True(t, result)
	})
}
