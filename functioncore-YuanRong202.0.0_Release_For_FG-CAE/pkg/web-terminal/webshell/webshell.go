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

// Package webshell define function to exec webshell
package webshell

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"k8s.io/apimachinery/pkg/util/httpstream"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/web-terminal/config"
	"functioncore/pkg/web-terminal/constant"
)

// HandleWebShell handler for cae request
func HandleWebShell(c *gin.Context) {
	traceID := initTraceID(c.Request)
	reqInfo := &RequestInfo{TraceID: traceID}
	if requestCheck(c, reqInfo) {
		return
	}
	filterRequest(c, reqInfo)
	err := processRequest(c, reqInfo)
	if err != nil {
		return
	}
}

func filterRequest(c *gin.Context, reqInfo *RequestInfo) {
	reqInfo.ClusterID = c.Param(constant.ClusterIDURNParam)
	reqInfo.Namespace = c.Param(constant.NamespaceIDURNParam)
	reqInfo.PodName = c.Param(constant.PodNameURNParam)
	reqInfo.Container = constant.RuntimeContainerName
	baseCommand := c.Request.URL.Query().Get(constant.QueryCommand)
	if baseCommand == "" {
		baseCommand = "bash"
	}
	reqInfo.Command = baseCommand
	log.GetLogger().Debugf("webshell request clusterID: %s, namespace: %s, podName: %s, baseCommand: %s, traceID: %s",
		reqInfo.ClusterID, reqInfo.Namespace, reqInfo.PodName, reqInfo.Command, reqInfo.TraceID)
}

func processRequest(c *gin.Context, reqInfo *RequestInfo) error {
	manager := GetSessionMgrInstance()
	k8sConn, err := WssConnFactory(K8s).GetWssConn(reqInfo)
	if err != nil {
		log.GetLogger().Errorf(" websocket connect to k8s failed, err: %s", err.Error())
		ResponseError(c.Writer, err, reqInfo.TraceID)
		return err
	}
	upgrader := websocket.Upgrader{
		HandshakeTimeout: constant.HandshakeTimeOut,
		ReadBufferSize:   constant.DefaultBufferSize,
		WriteBufferSize:  constant.DefaultBufferSize,
	}
	caeConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.GetLogger().Errorf("upgrade websocket request failed, err: %s", err.Error())
		ResponseError(c.Writer, err, reqInfo.TraceID)
		return err
	}
	targetID := fmt.Sprintf("%s_%s_%s", reqInfo.ClusterID, reqInfo.Namespace, reqInfo.PodName)
	sessionPair := manager.CreateSessionPair(reqInfo.TraceID, targetID, caeConn, k8sConn)
	sessionPair.HandleMessage()
	return nil
}

func initTraceID(r *http.Request) string {
	traceID := r.Header.Get(constants.HeaderRequestID)
	if traceID == "" {
		traceID = r.Header.Get(constants.HeaderTraceID)
	}
	switch {
	case len(traceID) > constant.MaxTraceIDLength:
		traceID = traceID[:constant.MaxTraceIDLength]
	case traceID == "":
		traceID = uuid.New().String()
	default:
	}
	r.Header.Set(constants.HeaderTraceID, traceID)
	return traceID
}

func requestCheck(c *gin.Context, reqInfo *RequestInfo) bool {
	err := paramCheck(c, reqInfo)
	if err != nil {
		ResponseError(c.Writer, err, reqInfo.TraceID)
		log.GetLogger().Errorf("failed to check param, traceID: %s, err: %s", reqInfo.TraceID, err.Error())
		return true
	}
	err = authCheck(c.Request)
	if err != nil {
		ResponseErrorMsg(c.Writer, http.StatusUnauthorized, "Unauthorized", reqInfo.TraceID)
		log.GetLogger().Errorf("failed to authenticate request, traceID %s:, err: %s", reqInfo.TraceID, err.Error())
		return true
	}
	if !httpstream.IsUpgradeRequest(c.Request) {
		ResponseErrorMsg(c.Writer, http.StatusMethodNotAllowed, "request was not an upgrade", reqInfo.TraceID)
		log.GetLogger().Errorf("request was not an upgrade, traceID %s", reqInfo.TraceID)
		return true
	}
	if GetSessionMgrInstance().IsReachMaxConnection() {
		ResponseErrorMsg(c.Writer, http.StatusInternalServerError, "reach max sessions pair", reqInfo.TraceID)
		log.GetLogger().Errorf("reach max sessions pair, traceID %s", reqInfo.TraceID)
		return true
	}
	return false
}

func paramCheck(c *gin.Context, reqInfo *RequestInfo) error {
	if c.Param(constant.ClusterIDURNParam) == "" {
		return fmt.Errorf("clusterID is empty, traceID: %s", reqInfo.TraceID)
	}
	if c.Param(constant.NamespaceIDURNParam) == "" {
		return fmt.Errorf("namespace is empty, traceID: %s", reqInfo.TraceID)
	}
	if c.Param(constant.PodNameURNParam) == "" {
		return fmt.Errorf("podName is empty, traceID: %s", reqInfo.TraceID)
	}
	return nil
}

func authCheck(r *http.Request) error {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config, err: %s", err.Error())
		return err
	}
	if !cfg.AuthenticationEnable {
		return nil
	}
	requestSign := r.Header.Get(constants.HeaderAuthorization)
	timestamp := r.Header.Get(constants.HeaderAuthTimestamp)
	err = localauth.AuthCheckLocally(cfg.LocalAuth.AKey, cfg.LocalAuth.SKey, requestSign,
		timestamp, cfg.LocalAuth.Duration)
	if err != nil {
		log.GetLogger().Errorf("failed to check authorization of URL locally, error: %s", err.Error())
		return err
	}
	return nil
}
