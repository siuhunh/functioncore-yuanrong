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
	"bytes"
	"clients/streamcache"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/model"
	"functioncore/pkg/frontend-invoke/statuscode"
	"functioncore/pkg/frontend-invoke/streamclient"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	failedRetrySubscribeSleepTime = 1 * time.Second
	zeroElementSleepTimeLimit     = 15 * time.Second
)

var (
	responseStreamMap          sync.Map
	eof                        = []byte("EOF")
	frontendResponseStreamName = os.Getenv("HOSTNAME")
	errRecNodata               = errors.New("receive no data")
	tenantIDMap                sync.Map
)

// ResponseStream -
type ResponseStream struct {
	w            http.ResponseWriter
	reqInfo      *model.FunctionRequestInfo
	stopChan     *StreamStopChan
	isStream     bool
	processTimes int32
}

// StreamStopChan -
type StreamStopChan struct {
	C    chan struct{}
	once sync.Once
}

// NewStopChan -
func NewStopChan() *StreamStopChan {
	return &StreamStopChan{C: make(chan struct{})}
}

// SafeClose -
func (mc *StreamStopChan) SafeClose() {
	mc.once.Do(func() {
		close(mc.C)
	})
}

// responseInfo -
type responseInfo struct {
	StatusCode         int                 `json:"statusCode"`
	RequestID          string              `json:"requestID"`
	ResponseHeaders    map[string][]string `json:"responseHeaders"`
	ResponseStreamName string              `json:"responseStreamName"`
	TenantID           string              `json:"tenantID"`
}

// RegisterResponse -
func RegisterResponse(w http.ResponseWriter, reqInfo *model.FunctionRequestInfo) *StreamStopChan {
	if !constant.FConfig.DataSystem.StreamEnable {
		return nil
	}
	reqInfo.ResponseStreamName = uuid.New().String()
	responseStopChan := NewStopChan()
	r := &ResponseStream{w: w, reqInfo: reqInfo, stopChan: responseStopChan}
	responseStreamMap.Store(reqInfo.ResponseStreamName, r)
	return responseStopChan
}

// ReleaseResponse -
func ReleaseResponse(responseStreamName string) {
	if !constant.FConfig.DataSystem.StreamEnable {
		return
	}
	responseStreamMap.Delete(responseStreamName)
}

// CheckIsResponseStream -
func CheckIsResponseStream(responseStreamName string) bool {
	if !constant.FConfig.DataSystem.StreamEnable {
		return false
	}
	v, ok := responseStreamMap.Load(responseStreamName)
	if !ok {
		return false
	}
	res, ok := v.(*ResponseStream)
	if !ok {
		return false
	}
	return res.isStream
}

func startListenFrontendResponseStream(tenantID string) error {
	tenantID = util.GetStreamClientTenantID(tenantID)
	if !constant.FConfig.DataSystem.StreamEnable {
		log.GetLogger().Warn("not enable to support stream of DataSystem")
		return nil
	}
	_, loaded := tenantIDMap.LoadOrStore(tenantID, struct{}{})
	if loaded {
		return nil
	}
	consumer, closeCh, err := subscribeFrontendResponseStream(tenantID)
	if err != nil {
		tenantIDMap.Delete(tenantID)
		return err
	}
	go listenFrontendResponseStream(consumer, closeCh, tenantID)
	return nil
}

func subscribeFrontendResponseStream(tenantID string) (*streamcache.StreamConsumer, chan struct{}, error) {
	client := streamclient.GetClient(tenantID)
	if client == nil {
		errMsg := fmt.Sprintf("stream client is nil, tenantID: %s", tenantID)
		log.GetLogger().Error(errMsg)
		return nil, nil, errors.New(errMsg)
	}
	consumer, err := client.Subscribe(frontendResponseStreamName)
	if err != nil {
		log.GetLogger().Errorf("failed to subscribe stream %s, tenantID: %s, err:%s",
			frontendResponseStreamName, tenantID, err.Error())
		return nil, nil, err
	}
	return consumer, client.GetCloseCh(), nil
}

func listenFrontendResponseStream(consumer *streamcache.StreamConsumer, stopCh chan struct{}, tenantID string) {
	defer func() {
		streamclient.CloseConsumer(consumer)
		tenantIDMap.Delete(tenantID)
	}()
	for {
		select {
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("received termination signal, shutdown")
			}
			return
		default:
			processFrontendResponseStreamElementsWithRecover(consumer, &stopCh, tenantID)
		}
	}
}

func processFrontendResponseStreamElementsWithRecover(consumer *streamcache.StreamConsumer, stopCh *chan struct{},
	tenantID string) {
	if consumer != nil {
		err := processFrontendResponseStreamElements(consumer)
		if err == nil {
			return
		}
		log.GetLogger().Errorf("failed to receive and process elements from stream %s, err %s",
			frontendResponseStreamName, err.Error())
	}
	newConsumer, newStopCh, err := subscribeFrontendResponseStream(tenantID)
	if err != nil {
		log.GetLogger().Errorf("failed to retry subscribe stream %s, err:%s",
			frontendResponseStreamName, err.Error())
		time.Sleep(failedRetrySubscribeSleepTime)
		return
	}
	streamclient.CloseConsumer(consumer)
	consumer = newConsumer
	stopCh = &newStopCh
	alarm.GetClient().ClearAlarm(alarm.StreamClient)
	log.GetLogger().Infof("succeed to retry subscribe stream %s", frontendResponseStreamName)
}

func processFrontendResponseStreamElements(consumer *streamcache.StreamConsumer) error {
	elements, err := streamclient.RecWithRetry(consumer)
	if err != nil {
		log.GetLogger().Errorf("failed to receive elements from stream %s ,err %s",
			frontendResponseStreamName, err.Error())
		return err
	}
	if len(elements) == 0 {
		time.Sleep(streamclient.GetZeroSleepTime())
		return nil
	}
	for _, e := range elements {
		if err := processResponseStreamElement(e); err != nil {
			log.GetLogger().Errorf("failed to process frontend response stream element id %d, err %s",
				e.Id, err.Error())
		}
	}
	return nil
}

func processResponseStreamElement(element streamcache.Element) error {
	responseInfoStr := string(unsafe.Slice(element.Ptr, element.Size))
	info := &responseInfo{}
	if err := json.Unmarshal([]byte(responseInfoStr), info); err != nil {
		log.GetLogger().Errorf("failed to unmarshal responseInfoStr, err %s", err.Error())
		return err
	}
	log.GetLogger().Infof("receive a response stream %s of the request %s from frontend stream %s",
		info.ResponseStreamName, info.RequestID, frontendResponseStreamName)
	v, ok := responseStreamMap.Load(info.ResponseStreamName)
	if !ok {
		errInfo := fmt.Sprintf("the response stream %s of request %s is not existed in responseStreamMap",
			info.ResponseStreamName, info.RequestID)
		return errors.New(errInfo)
	}
	resp, ok := v.(*ResponseStream)
	if !ok {
		return errors.New("not responseStream struct type")
	}
	resp.isStream = true
	atomic.AddInt32(&resp.processTimes, 1)
	if atomic.LoadInt32(&resp.processTimes) > 1 {
		return errors.New("the response stream was already processed")
	}
	start := time.Now()
	timeout := resp.reqInfo.Deadline.Sub(start)
	if timeout <= 0 {
		resp.stopChan.SafeClose()
		errInfo := fmt.Sprintf("the request %s has reached deadline ", info.RequestID)
		return errors.New(errInfo)
	}
	go writeResponseStreamTask(info, resp, timeout)
	return nil
}

func writeResponseStreamTask(info *responseInfo, resp *ResponseStream, timeout time.Duration) {
	log.GetLogger().Infof("start to write stream %s data to response of the request %s",
		info.ResponseStreamName, info.RequestID)
	if err := writeResponseStream(info, resp, timeout); err != nil {
		log.GetLogger().Errorf("failed to write stream %s data to response of the request %s, err %s",
			info.ResponseStreamName, info.RequestID, err.Error())
		setLogAndSendResponse(statuscode.InternalError, statuscode.WriteResponseError, errInter, resp.reqInfo, resp.w)
	}
	resp.stopChan.SafeClose()
}

func writeResponseStream(info *responseInfo, r *ResponseStream, timeout time.Duration) error {
	client := streamclient.GetClient(info.TenantID)
	if client == nil {
		errMsg := fmt.Sprintf("stream client is nil, tenantID: %s", info.TenantID)
		log.GetLogger().Error(errMsg)
		return errors.New(errMsg)
	}
	consumer, err := client.Subscribe(info.ResponseStreamName)
	if err != nil {
		log.GetLogger().Errorf("failed to subscribe response stream %s, err:%s",
			info.ResponseStreamName, err.Error())
		return err
	}
	defer streamclient.CloseConsumer(consumer)
	writeResponseStreamHeaders(info, r.w)
	r.w.WriteHeader(info.StatusCode)
	if err := writeStreamDataToResponse(consumer, r.w, info, timeout); err != nil {
		// already write response header, no need to write again by setLogAndSendResponse
		log.GetLogger().Errorf("failed to write stream %s data to response, err %s",
			info.ResponseStreamName, err.Error())
	}
	return nil
}

func writeResponseStreamHeaders(responseElement *responseInfo, w http.ResponseWriter) {
	for key, value := range responseElement.ResponseHeaders {
		for _, v := range value {
			w.Header().Add(key, v)
		}
	}
	w.WriteHeader(responseElement.StatusCode)
}

func writeStreamDataToResponse(consumer *streamcache.StreamConsumer, w http.ResponseWriter,
	info *responseInfo, timeout time.Duration) error {
	writeTimoutTimer := time.NewTimer(timeout)
	start := time.Now()
	totalSize := uint64(0)
	recNoDataCount := 0
	for {
		select {
		case <-writeTimoutTimer.C:
			errStr := fmt.Sprintf("write stream %s to response %s timeout %v",
				info.ResponseStreamName, info.RequestID, timeout)
			log.GetLogger().Error(errStr)
			return errors.New(errStr)
		default:
			num, err := receiveAndWriteData(consumer, w, info)
			totalSize += num
			if err == nil {
				recNoDataCount = 0
				continue
			}
			if errors.Is(err, errRecNodata) &&
				time.Duration(recNoDataCount)*streamclient.GetZeroSleepTime() < zeroElementSleepTimeLimit {
				log.GetLogger().Warnf("receive no data from response stream %s for %d consecutive times.",
					info.ResponseStreamName, recNoDataCount+1)
				time.Sleep(streamclient.GetZeroSleepTime())
				recNoDataCount++
				continue
			}
			if err == io.EOF {
				totalCost := time.Since(start)
				log.GetLogger().Infof("succeed write stream %s to response %s cost %v, total size %d",
					info.ResponseStreamName, info.RequestID, totalCost, totalSize)
				return nil
			}
			return err
		}
	}
}

func receiveAndWriteData(consumer *streamcache.StreamConsumer,
	w http.ResponseWriter, info *responseInfo) (uint64, error) {
	elements, err := streamclient.RecWithRetry(consumer)
	if err != nil {
		log.GetLogger().Errorf("failed to receive elements from response stream %s ,err %s",
			info.ResponseStreamName, err.Error())
		return 0, err
	}
	if len(elements) == 0 {
		return 0, errRecNodata
	}
	recSize := uint64(0)
	for _, e := range elements {
		data := unsafe.Slice(e.Ptr, e.Size)
		recSize += e.Size
		if len(data) == len(eof) && bytes.Equal(data, eof) {
			log.GetLogger().Infof("receive EOF of response stream %s", info.ResponseStreamName)
			return recSize, io.EOF
		}
		if _, err := w.Write(data); err != nil {
			log.GetLogger().Errorf("failed to write data to response %s", info.RequestID)
			return recSize, err
		}
	}
	return recSize, nil
}
