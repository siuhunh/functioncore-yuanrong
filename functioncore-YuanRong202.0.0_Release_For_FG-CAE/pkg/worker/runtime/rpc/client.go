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

package rpc

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/invokectx"
	cc "functioncore/pkg/worker/runtime/rpc/connect"
	"functioncore/pkg/worker/runtime/rpc/pb"
	"functioncore/pkg/worker/utils"
)

// AbstractProcessor define processor of message from server.
type AbstractProcessor interface {
	// ProcessCall process "call message" from local function to other function.
	ProcessCall(request *pb.CallRequest) (*pb.CallResponse, error)

	// ProcessLog process real time log of function form server,
	// no need to return response or error because server not care.
	ProcessLog(log []byte) error

	// ProcessMetrics process real time metrics form server,
	// no need to return response or error because server not care.
	ProcessMetrics(metrics *pb.Metrics)

	// ProcessCancelRequest process cancel request from runtime
	ProcessCancelRequest(request *pb.CancelRequest) (*pb.CancelResponse, error)

	RecvInvoke(ic invokectx.InvokeCtx, rsp []byte, err error)

	// ProcessRuntimeLog process real time log of function form server,
	// no need to return response or error because server not care.
	ProcessRuntimeLog(log []byte) error
}

// Client client of rpc
type Client struct {
	conn               *cc.TinyComm
	receiver           *Receiver
	invokeTimeout      time.Duration
	initializerTimeout time.Duration
	restoreHookTimeout time.Duration
	preStopTimeout     time.Duration
}

// BuildOption build option of client
type BuildOption struct {
	Address        string
	ConnectType    string
	ReconnectTimes int
	IsDirectFunc   bool
}

// Option client option
type Option func(*BuildOption)

// ConnectType connect type tcp or unix
func ConnectType(language, socketType string) Option {
	log.GetLogger().Infof("connType | %s | %s", language, socketType)
	if socketType == worker.UnixDomainSocketType {
		if strings.Contains(language, "java") {
			return func(s *BuildOption) {
				s.ConnectType = worker.UnixDomainSocketType
				s.Address = fmt.Sprintf("/home/snuser/socket/%s.sock", language)
			}
		}
	}
	return func(s *BuildOption) {
		s.ConnectType = worker.DefaultSocketType
	}
}

// ReconnectTimes reconnect times
func ReconnectTimes(times int) Option {
	return func(s *BuildOption) {
		s.ReconnectTimes = times
	}
}

// CheckJavaxLanguage specialize times
func CheckJavaxLanguage(language string) Option {
	return func(s *BuildOption) {
		s.IsDirectFunc = utils.IsDirectFunc(language)
	}
}

// NewClient new a rpc client to send message
func NewClient(address string, timeout int64, options ...func(*BuildOption)) (*Client, error) {
	receiver := &Receiver{}

	opt := &BuildOption{}
	for _, option := range options {
		option(opt)
	}

	if opt.ConnectType == worker.UnixDomainSocketType {
		address = opt.Address
	}
	log.GetLogger().Infof("newClient | %s | %s", address, opt.ConnectType)
	buildOpt := cc.BuildOptions{
		ServerAddr:     address,
		Network:        opt.ConnectType,
		ReconnectTimes: opt.ReconnectTimes,
		IsDirectFunc:   opt.IsDirectFunc,
	}
	receiver.isDirectFunc = opt.IsDirectFunc
	tinyComm, err := cc.Build(receiver, buildOpt)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:          tinyComm,
		invokeTimeout: time.Duration(timeout) * time.Second,
		receiver:      receiver,
	}, nil
}

// SetProcessor set processor of message from runtime
func (c *Client) SetProcessor(processor AbstractProcessor) {
	c.receiver.processor = processor
}

// ProcessInitializer process initializer message,
func (c *Client) ProcessInitializer(request []byte) ([]byte, error) {
	reqMeta := initializerRequestTypeBytes
	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(c.initializerTimeout, reqMeta, request)
	if err != nil {
		log.GetLogger().Warnf("initServerFail | %s", err.Error())
		return nil, err
	}

	return rsp, nil
}

// ProcessRestoreHook process restore hook message,
func (c *Client) ProcessRestoreHook(request []byte) ([]byte, error) {
	reqMeta := restoreHookRequestTypeBytes
	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(c.restoreHookTimeout, reqMeta, request)
	if err != nil {
		log.GetLogger().Errorf("failed to restore hook server: %s", err.Error())
		return nil, err
	}

	return rsp, nil
}

// ProcessPreStop process preStop message,
func (c *Client) ProcessPreStop(request []byte) ([]byte, error) {
	reqMeta := preStopRequestTypeBytes
	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(c.preStopTimeout, reqMeta, request)
	if err != nil {
		log.GetLogger().Errorf("failed to preStop user function: %s", err.Error())
		return nil, err
	}

	return rsp, nil
}

// ProcessUpdateAgency process update agency
func (c *Client) ProcessUpdateAgency(request []byte) ([]byte, error) {
	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(updateAgencyTimeout*time.Second, updateAgencyRequestTypeBytes, request)
	if err != nil {
		log.GetLogger().Errorf("failed to update agency user function: %s", err.Error())
		return nil, err
	}
	return rsp, nil
}

// SendInvoke send invoke message to server
func (c *Client) SendInvoke(ic invokectx.InvokeCtx, request []byte, timeout time.Duration) {
	c.conn.SendRequest(ic, timeout, invokeRequestTypeBytes, request)
}

// ProcessCancel process cancel request message
// receive cancel from bus and send it to runtime
func (c *Client) ProcessCancel(request *pb.CancelRequest) (*pb.CancelResponse, error) {
	req, err := proto.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal cancel request: %s", err.Error())
		return nil, err
	}
	reqMeta := cancelRequestTypeBytes

	_, resp, err := c.conn.Request(c.invokeTimeout, reqMeta, req)
	if err != nil {
		log.GetLogger().Errorf("failed to send cancel request: %s", err.Error())
		newErr, ok := err.(snerror.SNError)
		if !ok {
			return nil, err
		}
		if newErr.Code() == snerror.RuntimeInvokeTimeoutCode {
			return nil, snerror.New(snerror.RuntimeInvokeTimeoutCode,
				fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), c.invokeTimeout))
		}
		return nil, err
	}

	var cancelResp pb.CancelResponse
	if err = proto.Unmarshal(resp, &cancelResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal cancel response: %s", err.Error())
		return nil, err
	}
	return &cancelResp, nil
}

// LoadFunction load function in runtime,
// send function info to server and wait for response
func (c *Client) LoadFunction(request []byte, timeout time.Duration) ([]byte, error) {
	reqMeta := loadRequestTypeBytes
	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(timeout, reqMeta, request)
	if err != nil {
		log.GetLogger().Errorf("failed to load function in server: %s", err.Error())
		newErr, ok := err.(snerror.SNError)
		if !ok {
			return nil, err
		}
		if newErr.Code() == snerror.RuntimeInvokeTimeoutCode {
			return nil, snerror.New(snerror.RuntimeLoadTimeoutCode,
				fmt.Sprintf(snerror.ErrText(snerror.RuntimeLoadTimeoutCode), timeout))
		}
		return nil, err
	}
	return rsp, nil
}

// CheckHeath check health of runtime, send probe to server and wait for response
func (c *Client) CheckHeath(timeout time.Duration) (*pb.HealthResponse, error) {
	reqMeta := healthRequestTypeBytes

	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(timeout, reqMeta, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to check health in server: %s", err.Error())
		newErr, ok := err.(snerror.SNError)
		if !ok {
			return nil, err
		}
		if newErr.Code() == snerror.RuntimeInvokeTimeoutCode {
			return nil, snerror.New(snerror.RuntimeInvokeTimeoutCode,
				fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), timeout))
		}
		return nil, err
	}

	// Unmarshal response and return
	var healthResponse pb.HealthResponse
	if err = proto.Unmarshal(rsp, &healthResponse); err != nil {
		log.GetLogger().Errorf("failed to unmarshal check-health response: %s", err.Error())
		return nil, err
	}
	return &healthResponse, nil
}

// CheckStartupHeath check startup health of runtime, send probe to server and wait for response
func (c *Client) CheckStartupHeath(timeout time.Duration) (*pb.HealthResponse, error) {
	reqMeta := startupHealthRequestTypeBytes

	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(timeout, reqMeta, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to check startup health in server: %s", err.Error())
		newErr, ok := err.(snerror.SNError)
		if !ok {
			return nil, err
		}
		if newErr.Code() == snerror.RuntimeInvokeTimeoutCode {
			return nil, snerror.New(snerror.RuntimeInvokeTimeoutCode,
				fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), timeout))
		}
		return nil, err
	}

	// Unmarshal response and return
	var healthResponse pb.HealthResponse
	if err = proto.Unmarshal(rsp, &healthResponse); err != nil {
		log.GetLogger().Errorf("failed to unmarshal check-startup-health response: %s", err.Error())
		return nil, err
	}
	return &healthResponse, nil
}

// ReportCustomMetrics report custom metrics
func (c *Client) ReportCustomMetrics(timeout int) ([]byte, error) {
	reqMeta := customMetricsReportTypeBytes

	reportCustomMetrics := time.Duration(timeout) * time.Second
	// Request server/runtime and wait response sync
	_, rsp, err := c.conn.Request(reportCustomMetrics, reqMeta, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to get customReportMetrics in server: %s", err.Error())
		newErr, ok := err.(snerror.SNError)
		if !ok {
			return nil, err
		}
		if newErr.Code() == snerror.RuntimeInvokeTimeoutCode {
			return nil, snerror.New(snerror.RuntimeInvokeTimeoutCode,
				fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), reportCustomMetrics))
		}
		return nil, err
	}

	// Unmarshal response and return
	var customReportMetricsResponse pb.CustomMetricsReportResponse
	if err = proto.Unmarshal(rsp, &customReportMetricsResponse); err != nil {
		log.GetLogger().Errorf("failed to unmarshal customReportMetricsResponse response: %s", err.Error())
		return nil, err
	}

	if customReportMetricsResponse.GetError() != nil {
		log.GetLogger().Errorf("report custom metrics response error: %s",
			customReportMetricsResponse.GetError().GetMessage())
		return nil, errors.New(customReportMetricsResponse.GetError().GetMessage())
	}
	return customReportMetricsResponse.GetResult(), nil
}

// SetInvokeTimeout set max wait time of invoking process, return error if timeout
func (c *Client) SetInvokeTimeout(timeout time.Duration) {
	c.invokeTimeout = timeout
}

// SetInitializerTimeout set max wait time of initializer process, return error if timeout
func (c *Client) SetInitializerTimeout(timeout time.Duration) {
	c.initializerTimeout = timeout
}

// SetRestoreHookTimeout set max wait time of restore hook process, return error if timeout
func (c *Client) SetRestoreHookTimeout(timeout time.Duration) {
	c.restoreHookTimeout = timeout
}

// SetPreStopTimeout set max wait time of pre stop process, return error if timeout
func (c *Client) SetPreStopTimeout(timeout time.Duration) {
	c.preStopTimeout = timeout
}

// Receiver receive message from transport client
type Receiver struct {
	processor    AbstractProcessor
	isDirectFunc bool
}

// HandleRequest handle request message from transport client
// @return: metaData, message, error
func (r *Receiver) HandleRequest(rawMeta []byte, message []byte) ([]byte, []byte, error) {
	meta := unmarshalMetaData(rawMeta)
	switch meta.messageType {
	case CallRequestType:
		var callRequest pb.CallRequest
		if err := proto.Unmarshal(message, &callRequest); err != nil {
			log.GetLogger().Errorf("failed to unmarshal call request: %s", err.Error())
			return nil, nil, err
		}
		callResponse, err := r.processor.ProcessCall(&callRequest)
		if err != nil {
			log.GetLogger().Errorf("failed to process call request: %s", err.Error())
			return nil, nil, err
		}
		response, err := proto.Marshal(callResponse)
		if err != nil {
			log.GetLogger().Errorf("failed to marshal call response: %s", err.Error())
			return nil, nil, err
		}
		// No need to write metadata when response because sync process
		return callResponseTypeBytes, response, nil
	case CancelRequestType:
		metaData, response, err := r.handleCancelRequest(message)
		if err != nil {
			log.GetLogger().Errorf("failed to process cancel request: %s", err.Error())
			return nil, nil, err
		}
		return metaData, response, nil
	default:
		log.GetLogger().Warnf("unknown message type %d", meta.messageType)
		return nil, nil, errors.New("unknown message type")
	}
}

func (r *Receiver) handleCancelRequest(message []byte) ([]byte, []byte, error) {
	var cancelRequest pb.CancelRequest
	if err := proto.Unmarshal(message, &cancelRequest); err != nil {
		log.GetLogger().Errorf("failed to unmarshal cancelRequest: %s", err.Error())
		return nil, nil, err
	}
	cancelResponse, err := r.processor.ProcessCancelRequest(&cancelRequest)
	if err != nil {
		log.GetLogger().Errorf("failed to process cancelRequest: %s", err.Error())
		return nil, nil, err
	}
	response, err := proto.Marshal(cancelResponse)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal cancelResponse: %s", err.Error())
		return nil, nil, err
	}
	return cancelResponseTypeBytes, response, nil
}

// HandlePost handle post message from transport client.
// No blocking or time-consuming in function
func (r *Receiver) HandlePost(rawMeta []byte, message []byte) error {
	meta := unmarshalMetaData(rawMeta)
	var err error
	switch meta.messageType {
	case FunctionLogType:
		if r.isDirectFunc {
			err = r.processor.ProcessRuntimeLog(message)
		} else {
			err = r.processor.ProcessLog(message)
		}
	case MetricsType:
		var metrics pb.Metrics
		if err = proto.Unmarshal(message, &metrics); err != nil {
			log.GetLogger().Errorf("failed to unmarshal metrics: %s", err.Error())
			break
		}
		r.processor.ProcessMetrics(&metrics)
	default:
		log.GetLogger().Warnf("unknown post message")
	}
	return err
}

// RecvRequest -
func (r *Receiver) RecvRequest(ic invokectx.InvokeCtx, metaData, payload []byte, err error) {
	if err != nil {
		log.GetLogger().Warnf("failed to invoke server: %s", err.Error())
	}
	r.processor.RecvInvoke(ic, payload, err)
}

// rpc message type
const (
	UndefinedType byte = iota
	InvokeRequestType
	InvokeResponseType
	LoadRequestType
	LoadResponseType
	HealthRequestType
	HealthResponseType
	CallRequestType
	CallResponseType
	FunctionLogType
	MetricsType
	ReservedForDeprecatedInstanceRequestType  // deprecated, InstanceRequestType
	ReservedForDeprecatedInstanceResponseType // deprecated, InstanceResponseType
	InitializerRequestType
	InitializerResponseType
	CancelRequestType
	CancelResponseType
	RestoreHookRequestType
	RestoreHookResponseType
	PreStopRequestType
	PreStopResponseType
	CustomMetricsReportType
	updateAgencyRequestType
	StartupHealthRequestType
)

type metaData struct {
	messageType byte
}

const (
	metaDataLen = 1
)

var (
	initializerRequestTypeBytes   = []byte{InitializerRequestType}
	healthRequestTypeBytes        = []byte{HealthRequestType}
	startupHealthRequestTypeBytes = []byte{StartupHealthRequestType}
	cancelResponseTypeBytes       = []byte{CancelResponseType}
	cancelRequestTypeBytes        = []byte{CancelRequestType}
	callResponseTypeBytes         = []byte{CallResponseType}
	loadRequestTypeBytes          = []byte{LoadRequestType}
	invokeRequestTypeBytes        = []byte{InvokeRequestType}
	restoreHookRequestTypeBytes   = []byte{RestoreHookRequestType}
	preStopRequestTypeBytes       = []byte{PreStopRequestType}
	// customMetricsReportTypeBytes runtime invoke type of custom metrics report
	customMetricsReportTypeBytes = []byte{CustomMetricsReportType}
	updateAgencyRequestTypeBytes = []byte{updateAgencyRequestType}
)

// MetaData can be marshal by proto buffer
func unmarshalMetaData(input []byte) *metaData {
	messageType := UndefinedType

	if len(input) >= metaDataLen {
		messageType = input[0]
	} else {
		log.GetLogger().Warnf("meta data empty")
	}

	return &metaData{
		messageType: messageType,
	}
}
