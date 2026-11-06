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

package stream

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"functioncore/pkg/common/snerror"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	stream "functioncore/pkg/common/stream/pb"
	"functioncore/test/utils"
)

func TestReceiveStream(t *testing.T) {
	convey.Convey("TestReceiveStream", t, func() {
		convey.Convey("invalid client option", func() {
			ReceiveStream(&ClientOption{})

			writer := &fakeResponseWriter{header: http.Header{}}
			ReceiveStream(&ClientOption{
				Writer:  writer,
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			})
			convey.So(writer.statusCode, convey.ShouldEqual, 500)
		})
		convey.Convey("newStreamHandlerConn error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return nil, nil, errors.New("newStreamHandlerConn error")
				}),
			})
			defer patches.ResetAll()
			writer := &fakeResponseWriter{header: http.Header{}}
			ReceiveStream(&ClientOption{
				Writer:  writer,
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			})
			convey.So(writer.statusCode, convey.ShouldEqual, 500)
		})

		convey.Convey("headerRecAndResp success", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}"}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			ReceiveStream(&ClientOption{})

			writer := &fakeResponseWriter{header: http.Header{}}
			ReceiveStream(&ClientOption{
				Writer:    writer,
				DialCtx:   context.Background(),
				CliCtx:    context.Background(),
				RequestID: "traceID001",
			})
			convey.So(writer.statusCode, convey.ShouldEqual, 0)
			convey.So(string(writer.body), convey.ShouldEqual, "{\"AA\": \"hello world\"}")
		})

		convey.Convey("headerRecAndResp error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{err: errors.New("receive error")}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			ReceiveStream(&ClientOption{})

			writer := &fakeResponseWriter{header: http.Header{}}
			ReceiveStream(&ClientOption{
				Writer:    writer,
				DialCtx:   context.Background(),
				CliCtx:    context.Background(),
				RequestID: "traceID001",
			})
			convey.So(writer.statusCode, convey.ShouldEqual, 0)
		})
	})
}

func TestGetRequestMetadata(t *testing.T) {
	convey.Convey("TestGetRequestMetadata", t, func() {
		option := CredentialOption{
			WorkflowRunID: "traceID001",
			Token:         "token",
			ProjectID:     "project001",
		}
		mm, err := option.GetRequestMetadata(context.Background())
		convey.So(err, convey.ShouldBeNil)
		convey.So(mm["Token"], convey.ShouldEqual, "token")
		convey.So(mm["project_id"], convey.ShouldEqual, "project001")
		convey.So(mm["workflow_run_id"], convey.ShouldEqual, "traceID001")
	})
}

func TestSendStream(t *testing.T) {
	convey.Convey("TestSendStream", t, func() {
		convey.Convey("invalid client option", func() {
			writer := &fakeResponseWriter{header: http.Header{}}
			code := SendStream(&ClientOption{}, writer, nil, nil)
			convey.So(code, convey.ShouldEqual, snerror.WorkerInternalErrorCode)
			code = SendStream(&ClientOption{DialCtx: context.Background()}, writer, nil, nil)
			convey.So(code, convey.ShouldEqual, snerror.WorkerInternalErrorCode)
		})
		convey.Convey("newStreamHandlerConn error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return nil, nil, errors.New("newStreamHandlerConn error")
				}),
			})
			defer patches.ResetAll()
			writer := &fakeResponseWriter{header: http.Header{}}
			code := SendStream(&ClientOption{
				Writer:  writer,
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, writer, nil, nil)
			convey.So(code, convey.ShouldEqual, snerror.StreamConnException)
		})

		convey.Convey("cli.SendHeader error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}",
						err: errors.New("test error")}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			code := SendStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, nil, nil, nil)
			convey.So(code, convey.ShouldEqual, snerror.StreamConnException)
		})

		convey.Convey("SendStream timeout", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}"}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			tmpTimer := time.NewTimer(50 * time.Millisecond)
			request := &http.Request{Header: make(http.Header)}
			request.Header.Add("ContentType", "octet/stream")
			writer := &fakeResponseWriter{header: http.Header{}}
			code := SendStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, writer, request, tmpTimer)
			convey.So(code, convey.ShouldEqual, snerror.RuntimeInvokeTimeoutCode)
		})

		convey.Convey("SendStream success", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}"}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			tmpTimer := time.NewTimer(3 * time.Second)
			request := &http.Request{Header: make(http.Header), Body: mockConn{}}
			request.Header.Add("ContentType", "octet/stream")
			writer := &fakeResponseWriter{header: http.Header{}}
			code := SendStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, writer, request, tmpTimer)
			convey.So(code, convey.ShouldEqual, 0)
		})
	})
}

func TestSendWebStream(t *testing.T) {
	convey.Convey("TestSendWebStream", t, func() {
		convey.Convey("invalid client option", func() {
			code, msg := SendWebStream(&ClientOption{}, nil)
			convey.So(code, convey.ShouldEqual, snerror.WorkerInternalErrorCode)
			convey.So(msg, convey.ShouldEqual, "invalid stream client option")
		})
		convey.Convey("newStreamHandlerConn error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return nil, nil, errors.New("newStreamHandlerConn error")
				}),
			})
			defer patches.ResetAll()
			code, msg := SendWebStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, nil)
			convey.So(code, convey.ShouldEqual, snerror.StreamConnException)
			convey.So(code, convey.ShouldNotEqual, snerror.StreamExceedLimitErrorCode)
			convey.So(msg, convey.ShouldEqual, snerror.ErrText(snerror.StreamConnException))
		})

		convey.Convey("cli.SendHeader error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}",
						err: errors.New("test error")}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			code, msg := SendWebStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, nil)
			convey.So(code, convey.ShouldEqual, snerror.StreamConnException)
			convey.So(msg, convey.ShouldEqual, snerror.ErrText(snerror.StreamConnException))
		})

		convey.Convey("SendWebStream success", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}"}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			response := &http.Response{Header: make(http.Header), Body: mockConn{}}
			response.Header.Add("ContentType", "octet/stream")
			code, _ := SendWebStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, response)
			convey.So(code, convey.ShouldEqual, 0)
		})

		convey.Convey("call invoke timeout", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}"}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			response := &http.Response{Header: make(http.Header), Body: mockErrConn{msg: "context deadline exceeded"}}
			response.Header.Add("ContentType", "octet/stream")
			code, _ := SendWebStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, response)
			convey.So(code, convey.ShouldEqual, snerror.RuntimeInvokeTimeoutCode)
		})
		convey.Convey("failed to read http response body", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(newStreamHandlerConn, func(input *ClientOption) (*grpc.ClientConn,
					stream.StreamHandlerClient, error) {
					return &grpc.ClientConn{}, mockStreamClient{response: "{\"AA\": \"hello world\"}"}, nil
				}),
				gomonkey.ApplyFunc((*grpc.ClientConn).Close, func(*grpc.ClientConn) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			response := &http.Response{Header: make(http.Header), Body: mockErrConn{msg: "internal error"}}
			response.Header.Add("ContentType", "octet/stream")
			code, _ := SendWebStream(&ClientOption{
				DialCtx: context.Background(),
				CliCtx:  context.Background(),
			}, response)
			convey.So(code, convey.ShouldEqual, snerror.WorkerInternalErrorCode)
		})
	})
}

type mockStreamClient struct {
	response string
	err      error
}

type mockStreamHandlerSendClient struct {
	response string
	err      error
}

func (s mockStreamHandlerSendClient) Send(request *stream.StreamRequest) error {
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (s mockStreamHandlerSendClient) CloseAndRecv() (*stream.StreamResponse, error) {
	return &stream.StreamResponse{}, nil
}

func (s mockStreamHandlerSendClient) Header() (metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}

func (s mockStreamHandlerSendClient) Trailer() metadata.MD {
	//TODO implement me
	panic("implement me")
}

func (s mockStreamHandlerSendClient) CloseSend() error {
	//TODO implement me
	panic("implement me")
}

func (s mockStreamHandlerSendClient) Context() context.Context {
	//TODO implement me
	panic("implement me")
}

func (s mockStreamHandlerSendClient) SendMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (s mockStreamHandlerSendClient) RecvMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (m mockStreamClient) Send(ctx context.Context, opts ...grpc.CallOption) (stream.StreamHandler_SendClient, error) {
	return mockStreamHandlerSendClient{}, nil
}

func (m mockStreamClient) Receive(ctx context.Context,
	in *stream.StreamRequest, opts ...grpc.CallOption) (stream.StreamHandler_ReceiveClient, error) {
	return mockStreamClient{response: m.response, err: m.err}, nil
}

func (m mockStreamClient) SendHeader(ctx context.Context, opts ...grpc.CallOption) (stream.StreamHandler_SendHeaderClient, error) {
	if m.err != nil {
		return nil, m.err
	}
	return mockStreamHandlerSendClient{}, nil
}

func (m mockStreamClient) ReceiveHeader(ctx context.Context, in *stream.StreamRequest, opts ...grpc.CallOption) (stream.StreamHandler_ReceiveHeaderClient, error) {
	return mockStreamClient{response: "{\"header01\":[\"value001\"]}"}, nil
}

var (
	flag = false
)

func (m mockStreamClient) Recv() (*stream.StreamResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	if flag {
		flag = false
		return nil, io.EOF
	}
	flag = true
	return &stream.StreamResponse{Stream: []byte(m.response)}, m.err
}

func (m mockStreamClient) Header() (metadata.MD, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockStreamClient) Trailer() metadata.MD {
	//TODO implement me
	panic("implement me")
}

func (m mockStreamClient) CloseSend() error {
	//TODO implement me
	panic("implement me")
}

func (m mockStreamClient) Context() context.Context {
	//TODO implement me
	panic("implement me")
}

func (mockStreamClient) SendMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (mockStreamClient) RecvMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

type fakeResponseWriter struct {
	close      bool
	header     http.Header
	body       []byte
	statusCode int
}

func (f *fakeResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeResponseWriter) Flush() {}

func (f *fakeResponseWriter) CloseNotify() <-chan bool {
	return nil
}

func (f *fakeResponseWriter) Status() int {
	//TODO implement me
	panic("implement me")
}

func (f *fakeResponseWriter) Size() int {
	//TODO implement me
	panic("implement me")
}

func (f *fakeResponseWriter) WriteString(s string) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeResponseWriter) Written() bool {
	//TODO implement me
	panic("implement me")
}

func (f *fakeResponseWriter) WriteHeaderNow() {
	//TODO implement me
	panic("implement me")
}

func (f *fakeResponseWriter) Pusher() http.Pusher {
	//TODO implement me
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

type mockConn struct {
	buf []byte
}

func (m mockConn) Read(p []byte) (n int, err error) {
	p = []byte("hello world!")
	return len(p), io.EOF
}

func (m mockConn) Close() error {
	return nil
}

type mockErrConn struct {
	buf []byte
	msg string
}

func (m mockErrConn) Read(p []byte) (n int, err error) {
	return 0, errors.New(m.msg)
}

func (m mockErrConn) Close() error {
	return nil
}
