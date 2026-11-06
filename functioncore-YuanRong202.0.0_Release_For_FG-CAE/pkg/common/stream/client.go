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

// Package stream process streaming data
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/stream/pb"
)

const (
	maxCallReceiveMsgSize = 200 * 1024 * 1024

	// DialTimeout timeout interval for creating a connection
	DialTimeout = 1 * time.Second
	// keepLiveTime send pings every 10 seconds if there is no activity
	keepLiveTime = 10 * time.Second
	// keepLiveTimeout wait 1 second for ping ack before considering the connection dead
	keepLiveTimeout = time.Second

	bufferSize = 65535

	streamLimit = 200 * 1024 * 1024

	contentLenFilter = "Content-Length"
)

// InvokeErrorResponse invoke error response
type InvokeErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ClientOption processes stream data for internal use
type ClientOption struct {
	Writer            gin.ResponseWriter
	DialCtx           context.Context
	CliCtx            context.Context
	Token             string
	ProjectID         string
	RequestID         string
	StreamHandlerAddr string
	StreamTransPort   string
}

// CredentialOption grpc Token
type CredentialOption struct {
	Token         string
	ProjectID     string
	WorkflowRunID string
}

// GetRequestMetadata get request metadata
func (c CredentialOption) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"Token": c.Token, "project_id": c.ProjectID, "workflow_run_id": c.WorkflowRunID}, nil
}

// RequireTransportSecurity require transport security
func (c CredentialOption) RequireTransportSecurity() bool {
	return false
}

// ReceiveStream receive stream
func ReceiveStream(option *ClientOption) bool {
	if option == nil || option.Writer == nil || option.DialCtx == nil || option.CliCtx == nil {
		log.GetLogger().Warnf("invalid client option")
		return false
	}
	conn, cli, err := newStreamHandlerConn(option)
	if err != nil {
		handleStreamInternalErr("failed to create stream client connection", option.RequestID, option.Writer)
		return true
	}
	defer conn.Close()
	receiveBodyHandler, err := cli.Receive(option.CliCtx, &pb.StreamRequest{})
	if err != nil {
		handleStreamInternalErr(snerror.ErrText(snerror.StreamConnException), option.RequestID, option.Writer)
		return true
	}
	receiveHeaderHandler, err := cli.ReceiveHeader(option.CliCtx, &pb.StreamRequest{})
	if err != nil {
		handleStreamInternalErr(snerror.ErrText(snerror.StreamConnException), option.RequestID, option.Writer)
		return true
	}
	if !headerRecAndResp(receiveHeaderHandler, option) || !bodyRecAndResp(receiveBodyHandler, option) {
		return false
	}
	return true
}

// SendStream -
func SendStream(option *ClientOption, w http.ResponseWriter, request *http.Request, timer *time.Timer) int32 {
	if option == nil || option.DialCtx == nil || option.CliCtx == nil {
		return snerror.WorkerInternalErrorCode
	}
	conn, cli, err := newStreamHandlerConn(option)
	if err != nil {
		return snerror.StreamConnException
	}
	defer conn.Close()
	sendHeaderHandler, err := cli.SendHeader(option.CliCtx)
	if err != nil {
		log.GetLogger().Errorf("failed to create send header client,"+
			"traceID: %s, err: %s", option.RequestID, err.Error())
		return snerror.StreamConnException
	}
	sendBodyHandler, err := cli.Send(option.CliCtx)
	if err != nil {
		log.GetLogger().Errorf("failed to create send client, "+
			"traceID: %s, err: %s", option.RequestID, err.Error())
		return snerror.StreamConnException
	}
	errCode, err := headerRecAndSend(sendHeaderHandler, w, request, timer)
	if err != nil {
		log.GetLogger().Errorf("failed to send header, "+
			"traceID: %s, err: %s", option.RequestID, err.Error())
	}
	if errCode != 0 {
		return errCode
	}
	code, err := bodyRecAndSend(sendBodyHandler, w, request, timer)
	if err != nil {
		log.GetLogger().Errorf("failed to send body, "+
			"traceID: %s, err: %s", option.RequestID, err.Error())
	}
	return code
}

// SendWebStream -
func SendWebStream(option *ClientOption, response *http.Response) (int32, string) {
	if option == nil || option.DialCtx == nil || option.CliCtx == nil {
		return snerror.WorkerInternalErrorCode, "invalid stream client option"
	}
	conn, cli, err := newStreamHandlerConn(option)
	if err != nil {
		return snerror.StreamConnException, snerror.ErrText(snerror.StreamConnException)
	}
	defer conn.Close()
	sendBodyHandler, err := cli.Send(option.CliCtx)
	if err != nil {
		log.GetLogger().Errorf("failed to create send client, "+
			"traceID: %s, err: %s", option.RequestID, err.Error())
		return snerror.StreamConnException, snerror.ErrText(snerror.StreamConnException)
	}
	sendHeaderHandler, err := cli.SendHeader(option.CliCtx)
	if err != nil {
		log.GetLogger().Errorf("failed to create send header client,"+
			"traceID: %s, err: %s", option.RequestID, err.Error())
		return snerror.StreamConnException, snerror.ErrText(snerror.StreamConnException)
	}

	errCode, err := webHeaderRecAndSend(sendHeaderHandler, response)
	if err != nil && errCode != 0 {
		log.GetLogger().Errorf("failed to send web header, "+
			"traceID: %s, err: %s", option.RequestID, err.Error())
		return errCode, err.Error()
	}
	code, err := webBodyRecAndSend(sendBodyHandler, response)
	if err != nil {
		log.GetLogger().Errorf("failed to send body, "+
			"traceID: %s, err: %s", option.RequestID, err.Error())
		return code, err.Error()
	}
	return code, ""
}

func webHeaderRecAndSend(client pb.StreamHandler_SendHeaderClient, response *http.Response) (int32, error) {
	header, err := json.Marshal(response.Header)
	if err != nil {
		log.GetLogger().Errorf("failed to Marshal request header, err: %s", err.Error())
		return snerror.WorkerInternalErrorCode, err
	}
	err = client.Send(&pb.StreamRequest{Stream: header})
	if err != nil {
		log.GetLogger().Errorf("failed to send stream header to streamClient, err: %s", err.Error())
		return snerror.StreamConnException, err
	}
	_, err = client.CloseAndRecv()
	if err != nil {
		log.GetLogger().Errorf("failed to Close the header send client, err: %s", err.Error())
		return snerror.StreamConnException, err
	}
	return 0, nil
}

func webBodyRecAndSend(client pb.StreamHandler_SendClient, response *http.Response) (int32, error) {
	totalSize := 0
	for {
		buf := make([]byte, 0, bufferSize)
		n, err := response.Body.Read(buf[len(buf):cap(buf)])
		if err != nil && err != io.EOF {
			if strings.Contains(err.Error(), "context deadline exceeded") {
				return snerror.RuntimeInvokeTimeoutCode, errors.New("call invoke timeout")
			}
			return snerror.WorkerInternalErrorCode, errors.New("failed to read http response body: " + err.Error())
		}
		buf = buf[:len(buf)+n]
		chunkSize := len(buf)
		code, sendErr := processSendEvent(client, buf, chunkSize)
		if sendErr != nil {
			return code, sendErr
		}
		totalSize += chunkSize
		if totalSize >= streamLimit {
			return snerror.IllegalArgumentErrorCode, errors.New("send stream exceed limit")
		}
		if errors.Is(err, io.EOF) {
			return processSendEnd(client)
		}
	}
}

func headerRecAndSend(sendHeaderHandler pb.StreamHandler_SendHeaderClient,
	w http.ResponseWriter, request *http.Request, timer *time.Timer) (int32, error) {
	for {
		select {
		case <-timer.C:
			writeHeaderAndBody(w, http.StatusRequestTimeout, []byte("invoke timeout"))
			return snerror.RuntimeInvokeTimeoutCode, errors.New("invoke timeout")
		default:
			header, err := json.Marshal(request.Header)
			if err != nil {
				log.GetLogger().Errorf("failed to Marshal request header, err: %s", err.Error())
				return snerror.WorkerInternalErrorCode, err
			}
			err = sendHeaderHandler.Send(&pb.StreamRequest{Stream: header})
			if err != nil {
				log.GetLogger().Errorf("failed to send stream header to streamClient, err: %s", err.Error())
				return snerror.StreamConnException, err
			}
			_, err = sendHeaderHandler.CloseAndRecv()
			if err != nil {
				log.GetLogger().Errorf("failed to Close the header send client, err: %s", err.Error())
				return snerror.StreamConnException, err
			}
			return 0, nil
		}
	}
}

func bodyRecAndSend(sendBodyHandler pb.StreamHandler_SendClient,
	w http.ResponseWriter, request *http.Request, timer *time.Timer) (int32, error) {
	totalSize := 0
	for {
		select {
		case <-timer.C:
			writeHeaderAndBody(w, http.StatusRequestTimeout, []byte("invoke timeout"))
			return snerror.RuntimeInvokeTimeoutCode, errors.New("invoke timeout")
		default:
			buf := make([]byte, 0, bufferSize)
			n, err := request.Body.Read(buf[len(buf):cap(buf)])
			if err != nil && !errors.Is(err, io.EOF) {
				writeHeaderAndBody(w, http.StatusBadRequest, []byte(fmt.Sprintf(
					"receive event response failed. err:%s", err.Error())))
				return snerror.FunctionExceptionCode, err
			}
			buf = buf[:len(buf)+n]
			chunkSize := len(buf)
			code, sendErr := processSendEvent(sendBodyHandler, buf, chunkSize)
			if sendErr != nil {
				return code, sendErr
			}
			totalSize += chunkSize
			if totalSize >= streamLimit {
				writeHeaderAndBody(w, http.StatusBadRequest, []byte(fmt.Sprintf(
					"send stream exceed limit %d", streamLimit)))
				return snerror.IllegalArgumentErrorCode, errors.New("send stream exceed limit")
			}
			if errors.Is(err, io.EOF) {
				return processSendEnd(sendBodyHandler)
			}
		}
	}
}

func processSendEvent(sendBodyHandler pb.StreamHandler_SendClient, buf []byte, chunkSize int) (int32, error) {
	if chunkSize > 0 {
		err := sendBodyHandler.Send(&pb.StreamRequest{Stream: buf})
		if err != nil {
			log.GetLogger().Errorf("failed to send stream to streamClient, err: %s", err.Error())
			return snerror.StreamConnException, err
		}
	}
	return 0, nil
}

func processSendEnd(sendBodyHandler pb.StreamHandler_SendClient) (int32, error) {
	_, err := sendBodyHandler.CloseAndRecv()
	if err != nil {
		log.GetLogger().Errorf("failed to Close the body send client, err: %s", err.Error())
		return snerror.StreamConnException, err
	}
	return 0, nil
}

func writeHeaderAndBody(w http.ResponseWriter, code int, body []byte) {
	w.WriteHeader(code)
	_, wErr := w.Write(body)
	if wErr != nil {
		log.GetLogger().Warnf("failed to write response, err: %s", wErr)
	}
}
func newStreamHandlerConn(input *ClientOption) (*grpc.ClientConn, pb.StreamHandlerClient, error) {
	token := &CredentialOption{
		Token:         input.Token,
		ProjectID:     input.ProjectID,
		WorkflowRunID: input.RequestID,
	}
	addr := fmt.Sprintf("%s:%s", input.StreamHandlerAddr, input.StreamTransPort)
	conn, err := grpc.DialContext(input.DialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(token), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxCallReceiveMsgSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepLiveTime,    // send pings every 10 seconds if there is no activity
			Timeout:             keepLiveTimeout, // wait 1 second for ping ack before considering the connection dead
			PermitWithoutStream: true,            // send pings even without active streams
		}))
	if err != nil {
		log.GetLogger().Errorf("failed to create stream client connection, "+
			"address: %s, err: %s", addr, err.Error())
		return nil, nil, err
	}
	return conn, pb.NewStreamHandlerClient(conn), nil
}

func headerRecAndResp(receiveHeaderHandler pb.StreamHandler_ReceiveHeaderClient, option *ClientOption) bool {
	for {
		select {
		case <-option.Writer.CloseNotify():
			log.GetLogger().Warnf("http response writer is closed, traceID: %s", option.RequestID)
			return false
		default:
			resp, err := receiveHeaderHandler.Recv()
			if errors.Is(err, io.EOF) {
				appendStreamHeader(option)
				return true
			}
			if err != nil {
				log.GetLogger().Warnf("failed to receive stream header, "+
					"traceID: %s, err: %s", option.RequestID, err.Error())
				return false
			}
			writeHeader(option, resp)
		}
	}
}

func appendStreamHeader(option *ClientOption) {
	if option == nil || option.Writer == nil {
		return
	}
	if option.Writer.Header().Get("Transfer-Encoding") == "" {
		option.Writer.Header().Set("Transfer-Encoding", "chunked")
	}
	if option.Writer.Header().Get("Content-Type") == "" {
		option.Writer.Header().Set("Content-Type", "application/octet-stream")
	}
}

func bodyRecAndResp(receiveHandler pb.StreamHandler_ReceiveClient, option *ClientOption) bool {
	for {
		select {
		case <-option.Writer.CloseNotify():
			log.GetLogger().Warnf("http response writer is closed, traceID: %s", option.RequestID)
			return false
		default:
			resp, err := receiveHandler.Recv()
			if errors.Is(err, io.EOF) {
				log.GetLogger().Infof("succeed to handle stream transfer, traceID %s", option.RequestID)
				return true
			}
			if err != nil {
				log.GetLogger().Warnf("failed to receive stream body, "+
					"traceID: %s, err: %s", option.RequestID, err.Error())
				return false
			}
			if resp != nil && len(resp.Stream) != 0 {
				writeBody(option, resp)
			}
		}
	}
}
func writeHeader(option *ClientOption, resp *pb.StreamResponse) {
	receivedHeader := make(map[string][]string)
	if err := json.Unmarshal(resp.Stream, &receivedHeader); err != nil {
		log.GetLogger().Errorf("failed to marshal response header, "+
			"traceID %s, err: %s", option.RequestID, err.Error())
		return
	}
	for key, value := range receivedHeader {
		for _, val := range value {
			if !strings.EqualFold(key, contentLenFilter) {
				option.Writer.Header().Add(key, val)
			}
		}
	}
}

func writeBody(option *ClientOption, resp *pb.StreamResponse) {
	_, err := option.Writer.Write(resp.Stream)
	if err != nil {
		log.GetLogger().Errorf("failed to write stream response, "+
			"traceID: %s, err: %s", option.RequestID, err.Error())
		return
	}
	option.Writer.Flush()
}

func handleStreamInternalErr(message string, traceID string, rw http.ResponseWriter) {
	rw.WriteHeader(http.StatusInternalServerError)
	data, err := json.Marshal(InvokeErrorResponse{
		Code:    snerror.StreamConnException,
		Message: message,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal response data, traceID %s, err: %s", traceID, err)
		return
	}
	_, err = rw.Write(data)
	if err != nil {
		log.GetLogger().Warnf("failed to write response body, traceID %s, err: %s", traceID, err)
		return
	}
	log.GetLogger().Infof("succeed to handle stream internal error, traceID %s", traceID)
}
