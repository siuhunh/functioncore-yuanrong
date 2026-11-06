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

package custom

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/stream"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/custom/config"
)

const (
	readTimeout     = 5 * time.Second
	headerMapASize  = 6
	bitSize         = 64
	getEventTimeout = 180
	megabytes       = 1024 * 1024

	bufferSize = 4096
)

var (
	serverManager = struct {
		sync.Once
		singleton *server
	}{}
)

// Server of Custom runtime
type server struct {
	httpServer   *http.Server
	serverConfig *config.ServerConfiguration

	sync.RWMutex
	eventManager *EventManager

	closeServerFlag bool
	healthy         bool

	maxRequestBodySize int
	streamResponse     bool
}

func getServer(address string) (*server, error) {
	serverConfig, err := config.GetServerConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get serverConfig, err: %s", err.Error())
		return nil, err
	}
	conf, err := config.GetRuntimeConfig()
	if conf == nil {
		log.GetLogger().Errorf("failed to get runtime config, err: %s", err.Error())
		return nil, err
	}
	num, err := strconv.ParseFloat(conf.MaxRequestBodySize, bitSize)
	if err != nil {
		log.GetLogger().Errorf("failed to strconv maxRequestBodySize %s", conf.MaxRequestBodySize)
		return nil, err
	}
	serverManager.Do(func() {
		serverManager.singleton = &server{healthy: true}
		serverManager.singleton.httpServer = &http.Server{
			Handler:      newHandler(serverManager.singleton, serverConfig),
			Addr:         address,
			WriteTimeout: waitRequestTimeout,
			ReadTimeout:  readTimeout,
		}
		serverManager.singleton.eventManager = NewEventManager()
		serverManager.singleton.serverConfig = serverConfig
		serverManager.singleton.maxRequestBodySize = int(num * megabytes)
	})
	return serverManager.singleton, nil
}

func getServerAddress(port int32) string {
	return fmt.Sprintf("localhost:%d", port)
}

func newHandler(instance *server, serverConfig *config.ServerConfiguration) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	urls := serverConfig.ServerUrl
	if urls.ReadyPath != "" {
		router.POST(urls.ReadyPath, func(c *gin.Context) {
			instance.Ready(c.Writer, c.Request)
		})
	}
	router.GET(urls.NextPath, func(c *gin.Context) {
		instance.NextRequest(c.Writer, c.Request)
	})
	router.POST(urls.ResponsePath, func(c *gin.Context) {
		instance.ReplyResult(c.Writer, c.Request)
	})
	router.POST(urls.ErrorPath, func(c *gin.Context) {
		instance.ReplyError(c.Writer, c.Request)
	})
	return router
}

// Start listen and serve http request
func (s *server) Start() {
	err := s.httpServer.ListenAndServe()
	if err != nil && !s.closeServerFlag {
		log.GetLogger().Errorf("http server listenAndServe error: %s", err.Error())

		s.healthy = false
	}
}

// Ready handle "init/ready" request from runtime
func (s *server) Ready(w http.ResponseWriter, r *http.Request) {
	ClientReady()
}

func getNextRequest(s *server, w http.ResponseWriter, r *http.Request) {
	clientClosed := r.Context()
	timer := time.NewTimer(getEventTimeout * time.Second)
Loop:
	for {
		select {
		case request, ok := <-s.eventManager.requestChan:
			if !ok {
				s.processChannelClosed(w, r)
				break Loop
			}
			if request == nil {
				log.GetLogger().Debugf("get request nil")
				w.WriteHeader(http.StatusNoContent)
				break Loop
			}
			if !s.eventManager.isAvailable(request.TraceID) {
				log.GetLogger().Warnf("current traceID %s is not in waiters map", request.TraceID)
				timer.Reset(getEventTimeout * time.Second)
				continue
			}
			headers := s.setResponseHeader(request)
			for k, v := range headers {
				w.Header().Add(k, v)
			}
			if clientClosed.Err() != nil {
				s.processClientClosed(w, r, request)
				break Loop
			}
			_, err := w.Write(request.Event)
			if err != nil {
				log.GetLogger().Errorf("failed to write response, err: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			break Loop
		case _, ok := <-clientClosed.Done():
			if !ok {
				log.GetLogger().Warnf("clientClosed chan closed")
			}
			s.processClientClosed(w, r, nil)
			break Loop
		case <-timer.C:
			s.processTimeOut(w, r)
			break Loop
		}
		timer.Reset(getEventTimeout * time.Second)
	}
	timer.Stop()
}

// NextRequest handle "invocation/next" request from runtime
func (s *server) NextRequest(w http.ResponseWriter, r *http.Request) {
	if s.serverConfig.ServerUrl.ReadyPath == "" {
		s.Ready(w, r)
	}
	getNextRequest(s, w, r)
}

// ReplyResult runtime reply result to worker
func (s *server) ReplyResult(writer http.ResponseWriter, request *http.Request) {
	s.processResult(writer, request, false)
}

// ReplyError runtime reply error to worker
func (s *server) ReplyError(writer http.ResponseWriter, request *http.Request) {
	s.processResult(writer, request, true)
}

func (s *server) processResult(w http.ResponseWriter, request *http.Request, isError bool) {
	requestID := request.Header.Get(constants.HeaderRequestID)
	if requestID == "" {
		urls := strings.Split(request.RequestURI, "/")
		requestID = urls[len(urls)-1]
	}
	defer request.Body.Close()
	log.GetLogger().Infof("custom runtime put response '%s'.", requestID)
	if s.streamResponse {
		s.processStreamEvent(w, request, isError, requestID)
		return
	}

	respBytes, err := ioutil.ReadAll(request.Body)
	if err != nil {
		log.GetLogger().Errorf("get response from runtime error: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		_, wErr := w.Write([]byte("get response from runtime error."))
		if wErr != nil {
			log.GetLogger().Errorf("failed to write response, err: %s", err)
		}
		return
	}
	response := &runtime.InvokeResponse{}
	// Check response validity
	if len(respBytes) > s.maxRequestBodySize {
		errorMessage :=
			fmt.Sprintf(snerror.ErrText(snerror.IllegalArgumentErrorCode), len(respBytes), s.maxRequestBodySize)
		response.Error = &runtime.Error{
			Code:    snerror.IllegalArgumentErrorCode,
			Message: errorMessage,
		}
	} else {
		if isError {
			response.Error = &runtime.Error{
				Code:    snerror.FunctionExceptionCode,
				Message: string(respBytes),
			}
		} else {
			response.Result = respBytes
		}
	}

	err = s.eventManager.PutResponse(requestID, response)
	if err != nil {
		s.processPutResponseError(w, request, requestID, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) processStreamEvent(w http.ResponseWriter, request *http.Request, isError bool, requestID string) {
	s.eventManager.waitMap.Lock()
	timer, ok := s.eventManager.waitMap.timers[requestID]
	s.eventManager.waitMap.Unlock()
	if !ok || timer == nil {
		log.GetLogger().Errorf("invoke timer isn't exist, traceID %s", requestID)
		s.processPutResponseError(w, request, requestID, errors.New("invoke timer isn't exist"))
		return
	}
	s.eventManager.waitMap.Lock()
	option, ok := s.eventManager.waitMap.streamOptions[requestID]
	s.eventManager.waitMap.Unlock()
	if !ok || option == nil {
		log.GetLogger().Errorf("stream client option isn't exist, traceID %s", requestID)
		s.processPutResponseError(w, request, requestID, errors.New("stream client option isn't exist"))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), stream.DialTimeout)
	defer cancel()
	option.DialCtx = ctx
	option.CliCtx = context.Background()
	code := stream.SendStream(option, w, request, timer)
	response := &runtime.InvokeResponse{}
	if code != 0 {
		if code == snerror.IllegalArgumentErrorCode {
			response.Error = &runtime.Error{
				Code:    code,
				Message: snerror.ErrText(snerror.StreamExceedLimitErrorCode),
			}
		} else {
			response.Error = &runtime.Error{
				Code:    code,
				Message: snerror.ErrText(int(code)),
			}
		}
	} else if isError {
		response.Error = &runtime.Error{
			Code: snerror.FunctionExceptionCode,
		}
	}
	err := s.eventManager.PutResponse(requestID, response)
	if err != nil {
		s.processPutResponseError(w, request, requestID, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) processChannelClosed(w http.ResponseWriter, r *http.Request) {
	log.GetLogger().Errorf("Request channel closed unexpectedly")
	w.WriteHeader(http.StatusInternalServerError)
	_, err := w.Write([]byte("get request failed"))
	if err != nil {
		log.GetLogger().Errorf("failed to write response failed, err: %s", err)
	}
}

func (s *server) processClientClosed(w http.ResponseWriter, r *http.Request, request *runtime.InvokeRequest) {
	log.GetLogger().Warnf("client connection closed before sending response")
	if request != nil {
		select {
		case s.eventManager.requestChan <- request:
		default:
			log.GetLogger().Errorf("failed to repush request '%s' to queue "+
				"after client connection closed.", request.TraceID)
		}
	}
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("client closed"))
	if err != nil {
		log.GetLogger().Errorf("failed to write response, err: %s", err)
	}
}

func (s *server) processTimeOut(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusRequestTimeout)
	_, err := w.Write([]byte("no available function request"))
	if err != nil {
		log.GetLogger().Errorf("failed to write response, err: %s", err)
	}
}

func (s *server) processPutResponseError(w http.ResponseWriter, r *http.Request, requestID string, err error) {
	log.GetLogger().Errorf("failed to response request '%s', err: %s", requestID, err)
	w.WriteHeader(http.StatusBadRequest)
	_, wErr := w.Write([]byte(fmt.Sprintf("receive event response failed. err:%s", err.Error())))
	if wErr != nil {
		log.GetLogger().Errorf("failed to write response, err: %s", wErr)
	}
}

func (s *server) setResponseHeader(request *runtime.InvokeRequest) map[string]string {
	extraMap := request.Extra
	headerMap := make(map[string]string, headerMapASize)
	headers := s.serverConfig.ServerHeaders
	headerMap[headers.HeaderRequestIDKey] = request.TraceID
	if headers.HeaderAccessKey != "" {
		if accessKey, ok := extraMap[constants.HeaderAccessKey]; ok {
			headerMap[headers.HeaderAccessKey] = accessKey
		}
	}
	if headers.HeaderSecretKey != "" {
		if secretKey, ok := extraMap[constants.HeaderSecretKey]; ok {
			headerMap[headers.HeaderSecretKey] = secretKey
		}
	}
	if headers.HeaderSecurityAccessKey != "" {
		if securityAccessKey, ok := extraMap[constants.HeaderSecurityAccessKey]; ok {
			headerMap[headers.HeaderSecurityAccessKey] = securityAccessKey
		}
	}
	if headers.HeaderSecuritySecretKey != "" {
		if securitySecretKey, ok := extraMap[constants.HeaderSecuritySecretKey]; ok {
			headerMap[headers.HeaderSecuritySecretKey] = securitySecretKey
		}
	}
	if headers.HeaderOriginVersionTag != "" {
		if originVersionTag, ok := extraMap[constants.HeaderOriginVersionTag]; ok {
			headerMap[headers.HeaderOriginVersionTag] = originVersionTag
		}
	}
	if headers.HeaderWorkflowID != "" {
		if workflowID, ok := extraMap[constants.HeaderWorkflowID]; ok {
			headerMap[headers.HeaderWorkflowID] = workflowID
		}
	}
	if headers.HeaderWorkflowRunID != "" {
		if workflowRunID, ok := extraMap[constants.HeaderWorkflowRunID]; ok {
			headerMap[headers.HeaderWorkflowRunID] = workflowRunID
		}
	}
	if headers.HeaderWorkflowStateID != "" {
		if workflowStateID, ok := extraMap[constants.HeaderWorkflowStateID]; ok {
			headerMap[headers.HeaderWorkflowStateID] = workflowStateID
		}
	}
	if headers.HeaderAuthToken != "" {
		if authToken, ok := extraMap[constants.HeaderAuthToken]; ok {
			headerMap[headers.HeaderAuthToken] = authToken
		}
	}
	if headers.HeaderSecurityToken != "" {
		if securityToken, ok := extraMap[constants.HeaderSecurityToken]; ok {
			headerMap[headers.HeaderSecurityToken] = securityToken
		}
	}
	return headerMap
}
