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
	"clients/streamcache"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/frontend-invoke/model"
	"functioncore/pkg/frontend-invoke/statuscode"
	"functioncore/pkg/frontend-invoke/streamclient"
)

const (
	httpStreamContentLength = 10 * 1024 * 2024
	readLimitSize           = streamclient.PageSize - 4*1024
)

var (
	errInter = errors.New("internal error")
)

func processHTTPStreamRequest(c *gin.Context, reqInfo *model.FunctionRequestInfo) error {
	reqInfo.RequestStreamName = uuid.New().String()
	c.Request.Header.Set(constants.HeaderRequestStreamName, reqInfo.RequestStreamName)
	client := streamclient.GetClient(reqInfo.TenantID)
	if client == nil {
		errMsg := fmt.Sprintf("stream client is nil, tenantID: %s", reqInfo.TenantID)
		log.GetLogger().Error(errMsg)
		setLogAndSendResponse(statuscode.InternalError, statuscode.CreateStreamProducerError,
			errInter, reqInfo, c.Writer)
		return errors.New(errMsg)
	}
	producer, err := client.CreateProducerByDefault(reqInfo.RequestStreamName)
	if err != nil {
		log.GetLogger().Errorf("failed to create request %s stream %s producer, err %s",
			reqInfo.TraceID, reqInfo.RequestStreamName, err.Error())
		setLogAndSendResponse(statuscode.InternalError, statuscode.CreateStreamProducerError,
			errInter, reqInfo, c.Writer)
		return err
	}
	log.GetLogger().Infof("succeed to create request %s stream %s producer",
		reqInfo.TraceID, reqInfo.RequestStreamName)
	// Note: Do not pass gin.Context to the goroutine. The gin.Context object maybe reused in other request handler.
	go requestBodyToStream(c.Request.Body, reqInfo, producer, client)
	return nil
}

func requestBodyToStream(stream io.ReadCloser, reqInfo *model.FunctionRequestInfo,
	producer *streamcache.StreamProducer, client *streamclient.DataSystemStreamClient) {
	timeout := getRequestDeadline(reqInfo)
	defer streamclient.CloseProducer(producer)
	if err := client.QueryStreamConsumerExists(reqInfo.RequestStreamName, timeout); err != nil {
		log.GetLogger().Errorf("failed to query request %s consumer of stream %s, err %s",
			reqInfo.TraceID, reqInfo.RequestStreamName, err.Error())
		reqInfo.SetRequestStreamErrorCode(statuscode.QueryStreamCustomerError)
		return
	}
	if err := sendBodyToStream(producer, stream, reqInfo.TraceID); err != nil {
		log.GetLogger().Errorf("failed to send data to request %s stream %s, err %s",
			reqInfo.TraceID, reqInfo.RequestStreamName, err.Error())
		reqInfo.SetRequestStreamErrorCode(statuscode.SendDataToStreamError)
		return
	}
}

func sendBodyToStream(producer *streamcache.StreamProducer, stream io.ReadCloser, traceID string) error {
	buf := make([]byte, readLimitSize)
	lastAction := ""
	defer func() {
		producer.Flush()
		if err := stream.Close(); err != nil {
			log.GetLogger().Warnf("failed to close request %s body", traceID)
		}
		log.GetLogger().Infof("send request %s body to stream %s", traceID, lastAction)
	}()
	start := time.Now()
	totalSize := 0
	for {
		lastAction = fmt.Sprintf("lastAction: %s, already send size %d", "io.ReadFull", totalSize)
		n, err := io.ReadFull(stream, buf)
		if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.GetLogger().Errorf("failed to read from body, err %s", err.Error())
			return err
		}
		if n > 0 {
			totalSize += n
			data := buf[:n]
			lastAction = fmt.Sprintf("lastAction: %s, already send size %d", "sendData", totalSize)
			if err := streamclient.SendDataWithRetry(producer, &data[0], uint64(len(data))); err != nil {
				log.GetLogger().Errorf("failed to send data to stream, err %s", err.Error())
				return err
			}
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			totalCost := time.Since(start)
			log.GetLogger().Infof("succeed to send http stream request %s to ds-worker cost %v, total size %d",
				traceID, totalCost, totalSize)
			break
		}
	}
	return nil
}
