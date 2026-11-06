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

// Package vpa for processing vertical pod autoscaling request from worker manager or slo agent
package vpa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

// SyncFinishedMu is used to lock sync status when bus and slo agent restarting
var (
	SyncFinishedMu            sync.RWMutex
	SyncFinished              bool
	msgInstanceAlreadyCreated = []byte("instance is already created")
)

func init() {
	workerbroker.SetSendInstanceInfoToSLOFunc(SendInstanceInfoToSLOAgent)
}

// SendInstanceInfoToSLOAgent register function instance to SLO agent
func SendInstanceInfoToSLOAgent(info vpa.FuncInstanceInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal instance info of %s, err: %s", info.PodName, err.Error())
		return err
	}
	traceID := uuid.New().String()
	log.GetLogger().Infof("start to register ready instance to slo, %s %s", info.PodName, traceID)
	// notify slo agent
	sloRequest, err := http.NewRequest("POST", vpa.SLOAgentInstanceInfoURL, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("failed to new reqeust to slo, %s %s", info.PodName, err.Error())
		return err
	}
	sloRequest.Header.Set(constants.HeaderTraceID, traceID)
	resp, err := utils.GetSLOAgentClient().Do(sloRequest)
	if err != nil {
		log.GetLogger().Errorf("failed to send request to slo, %s, %s, %s", info.PodName, traceID, err.Error())
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if body, err := io.ReadAll(resp.Body); err == nil {
			log.GetLogger().Warnf("SLO agent return error: %s, inner code: %s, pod %s",
				body, resp.Header.Get(constants.HeaderInnerCode), info.PodName)
			if bytes.Contains(body, msgInstanceAlreadyCreated) {
				return nil
			}
		}
		return fmt.Errorf("slo return with status code %d error code %s",
			resp.StatusCode, resp.Header.Get(constants.HeaderInnerCode))
	}
	return nil
}

// RegisterFunctionInstanceMonitorInSLOAgent register function instance monitor in SLO agent
func RegisterFunctionInstanceMonitorInSLOAgent(payload []byte, traceID string) snerror.SNError {
	// store the info in cache
	req := &vpa.FuncInstanceInfo{}
	if err := json.Unmarshal(payload, req); err != nil {
		return snerror.New(
			proxyerrors.VPARegisterFunctionInstanceErrCode,
			"failed to unmarshal request body from worker manager",
		)
	}
	log.GetLogger().Infof("start to handle vpa registration request, traceID: %s, action: %s", traceID, req.Action)
	if req.Action == vpa.FuncInsReady {
		errorCode, errorMsg := workerbroker.GetInstance().SetVPAInfo(req.PodName, req)
		if errorCode != 0 {
			return snerror.New(
				proxyerrors.VPARegisterFunctionInstanceErrCode,
				errorMsg,
			)
		}
	}
	// notify slo agent
	sloRequest, err := http.NewRequest("POST", vpa.SLOAgentInstanceInfoURL, bytes.NewBuffer(payload))
	if err != nil {
		return snerror.New(
			proxyerrors.VPARegisterFunctionInstanceErrCode,
			fmt.Sprintf("failed to create SLO agent request, inner error message: %s", err.Error()),
		)
	}
	sloRequest.Header.Set(constants.HeaderTraceID, traceID)
	resp, err := utils.GetSLOAgentClient().Do(sloRequest)
	if err != nil {
		return snerror.New(
			proxyerrors.VPARegisterFunctionInstanceErrCode,
			fmt.Sprintf("failed to send request to SLO agent, inner error message: %s", err.Error()),
		)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if body, err := io.ReadAll(resp.Body); err == nil {
			log.GetLogger().Errorf("SLO agent return error: %s, inner code: %s",
				string(body), resp.Header.Get(constants.HeaderInnerCode))
			if bytes.Contains(body, msgInstanceAlreadyCreated) {
				return nil
			}
		}
		return snerror.New(
			proxyerrors.VPARegisterFunctionInstanceErrCode,
			fmt.Sprintf("failed to send request to SLO agent, status code: %d, inner error code: %s",
				resp.StatusCode, resp.Header.Get(constants.HeaderInnerCode)),
		)
	}
	return nil
}

// RecoverFunctionInstanceMonitorInSLOAgent return all function instance info after SLO agent restart
func RecoverFunctionInstanceMonitorInSLOAgent(ctx *fasthttp.RequestCtx, traceID string) snerror.SNError {
	if !getSyncFinishedStatus() {
		return sendEmptyVpaInstanceInfo(ctx, traceID)
	}

	vpaInfo := workerbroker.GetInstance().GetAllInstanceVPAInfo(traceID)
	log.GetLogger().Infof("%d function instance info will be sent to recovering SLO agent",
		len(vpaInfo.FuncInstanceInfo))
	vpaBodyPayload, err := json.Marshal(vpaInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal vpa info in bus: %s, traceID: %s", err.Error(), traceID)
		return snerror.New(
			proxyerrors.VPARecoverFunctionInstanceCacheErrCode,
			fmt.Sprintf("failed to marshal vpa info in bus, inner error message: %s", err.Error()),
		)
	}
	if _, err := ctx.Write(vpaBodyPayload); err != nil {
		log.GetLogger().Errorf("failed to write response body: %s, traceID: %s", err.Error(), traceID)
		return snerror.New(
			proxyerrors.VPARecoverFunctionInstanceCacheErrCode,
			fmt.Sprintf("failed to write response body, inner error message: %s", err.Error()),
		)
	}
	return nil
}

func sendEmptyVpaInstanceInfo(ctx *fasthttp.RequestCtx, traceID string) snerror.SNError {
	vpaInfo := &vpa.FuncInstanceInfoWrapper{
		FuncInstanceInfo: []*vpa.FuncInstanceInfo{},
	}
	log.GetLogger().Info("sync function instance still not finished, will be send empty instance")
	vpaBodyPayload, err := json.Marshal(vpaInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal empty vpa info in bus: %s, traceID: %s", err.Error(), traceID)
		return snerror.New(
			proxyerrors.VPARecoverFunctionInstanceCacheErrCode,
			fmt.Sprintf("failed to marshal vpa info in bus, inner error message: %s", err.Error()),
		)
	}
	if _, err := ctx.Write(vpaBodyPayload); err != nil {
		log.GetLogger().Errorf("failed to write empty response body: %s, traceID: %s", err.Error(), traceID)
		return snerror.New(
			proxyerrors.VPARecoverFunctionInstanceCacheErrCode,
			fmt.Sprintf("failed to write response body, inner error message: %s", err.Error()),
		)
	}
	return nil
}

// GetCurrentResourceView retrieve resource view during VPA
func GetCurrentResourceView(traceID string) ([]byte, snerror.SNError) {
	sloRequest, err := http.NewRequest("GET", vpa.SLOAgentResourceInfoURL, nil)
	if err != nil {
		return nil, snerror.New(
			proxyerrors.VPASyncResourceViewErrCode,
			fmt.Sprintf("failed to make request to SLO agent when syncing resource view, "+
				"inner error message: %s", err.Error()),
		)
	}
	sloRequest.Header.Set(constants.HeaderTraceID, traceID)
	resp, err := utils.GetSLOAgentClient().Do(sloRequest)
	if err != nil {
		return nil, snerror.New(
			proxyerrors.VPASyncResourceViewErrCode,
			fmt.Sprintf("failed to send request to SLO agent when syncing resource view, "+
				"inner error message: %s", err.Error()),
		)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, snerror.New(
			proxyerrors.VPASyncResourceViewErrCode,
			fmt.Sprintf("failed to read response body, error: %s", err.Error()),
		)
	}
	if resp.StatusCode != http.StatusOK || len(payload) == 0 {
		return nil, snerror.New(
			proxyerrors.VPASyncResourceViewErrCode,
			fmt.Sprintf("failed to request SLO agent, status code: %d, error message: %s",
				resp.StatusCode, string(payload)),
		)
	}
	return payload, nil
}

// SyncResourceView sync resource view to worker manager after GetCurrentResourceView
func SyncResourceView(payload []byte, traceID string) snerror.SNError {
	if payload == nil {
		return snerror.New(proxyerrors.VPASyncResourceViewErrCode,
			"nil resource view payload got from SLO agent")
	}
	url := wmhealth.GetWorkerManagerBaseURL() + vpa.WorkerManagerResourceInfoURLSuffix
	sloRequest, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return snerror.New(
			proxyerrors.VPASyncResourceViewErrCode,
			fmt.Sprintf("failed to make request when syncing resource view to worker manager, "+
				"inner error message: %s", err.Error()),
		)
	}
	authorization, timestamp := utils.GenerateAuthorization()
	sloRequest.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	sloRequest.Header.Set(constants.Authorization, authorization)
	sloRequest.Header.Set(constants.HeaderTraceID, traceID)
	resp, err := utils.GetWorkerManagerClient().Do(sloRequest)
	if err != nil {
		return snerror.New(
			proxyerrors.VPASyncResourceViewErrCode,
			fmt.Sprintf("failed to send request to worker manager server when syncing resource view, "+
				"inner error message: %s", err.Error()),
		)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return snerror.New(proxyerrors.VPASyncResourceViewErrCode,
			fmt.Sprintf("worker manager return non-200 status code, statusCode: %d", resp.StatusCode))
	}
	return nil
}

// DeleteFuncInsInSLOAgentCallBack -
func DeleteFuncInsInSLOAgentCallBack(payload []byte, workerID string) {
	if common.FConfig == nil || common.FConfig.VPAConfig == nil || common.FConfig.VPAConfig.Enable == 0 {
		return
	}
	if len(payload) == 0 {
		log.GetLogger().Warnf("nil payload when handling function instance deletion, workerID: %s", workerID)
		return
	}
	sloRequest, err := http.NewRequest("POST", vpa.SLOAgentInstanceInfoURL, bytes.NewBuffer(payload))
	if err != nil {
		log.GetLogger().Errorf("failed to create deletion worker monitor request, workerID: %s, error: %s",
			workerID, err.Error())
		return
	}
	traceID := uuid.New().String()
	sloRequest.Header.Set(constants.HeaderTraceID, traceID)
	resp, err := utils.GetSLOAgentClient().Do(sloRequest)
	if err != nil {
		log.GetLogger().Errorf("failed to send deletion worker monitor request, workerID: %s, error: %s",
			workerID, err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.GetLogger().Errorf("failed to read body from SLO agent response")
			return
		}
		log.GetLogger().Errorf("SLO agent failed to process deletion worker monitor request, workerID: %s, "+
			"statusCode: %d, errorCode: %s, errorMessage: %s", workerID, resp.StatusCode,
			resp.Header.Get(constants.HeaderInnerCode), string(body))
		return
	}
	log.GetLogger().Infof("succeed to delete function instance monitor in SLO agent, workerID: %s, traceID: %s",
		workerID, traceID)
}

// SetSyncFinishedStatus set sync finished status when finished to sync all function instance info
// only used by recovering scenario
func SetSyncFinishedStatus(finished bool) {
	SyncFinishedMu.Lock()
	defer SyncFinishedMu.Unlock()
	SyncFinished = finished
}

func getSyncFinishedStatus() bool {
	SyncFinishedMu.RLock()
	defer SyncFinishedMu.RUnlock()
	return SyncFinished
}

// SyncWithSLOAgentWhenSyncFinished sync vpa info with slo-agent after router-etcd synced
func SyncWithSLOAgentWhenSyncFinished(finished bool) {
	if !finished || common.FConfig == nil || common.FConfig.VPAConfig == nil ||
		common.FConfig.VPAConfig.Enable == 0 {
		return
	}
	if err := SyncWithSLOAgent(); err != nil {
		log.GetLogger().Warnf("sync with SLO agent failed with error: %s", err.Error())
	}
}

// SyncWithSLOAgent sync instance vpa info with slo-agent
func SyncWithSLOAgent() error {
	traceID := uuid.New().String()
	log.GetLogger().Infof("start to sync with slo-agent, traceID: %s", traceID)
	data, snErr := GetCurrentResourceView(traceID)
	if snErr != nil {
		log.GetLogger().Warnf("failed to get current resource view, traceID: %s, err: %s", traceID, snErr.Error())
		return snErr
	}
	resp := SLOViewResp{}
	err := json.Unmarshal(data, &resp)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal slo view resp, traceID: %s, err: %s", traceID, err.Error())
		return err
	}
	currentIns := workerbroker.GetInstance().GetAllInstanceVPAInfo(traceID)
	curMap := make(map[string]struct{})
	for _, ins := range currentIns.FuncInstanceInfo {
		curMap[ins.PodName] = struct{}{}
		ins.Action = vpa.FuncInsReady
		err = SendInstanceInfoToSLOAgent(*ins)
		if err != nil {
			log.GetLogger().Warnf("failed to sync instance, pod %s, err: %s", ins.PodName, err.Error())
			continue
		}
		log.GetLogger().Infof("succeed to sync instance, pod %s", ins.PodName)
	}

	for _, ins := range resp.InstanceList.VpaInsView {
		if _, ok := curMap[ins.PodName]; !ok {
			ins.Action = vpa.FuncInsDelete
			err = SendInstanceInfoToSLOAgent(ins)
			if err != nil {
				log.GetLogger().Warnf("failed to del instance when sync, pod %s, err: %s", ins.PodName, err.Error())
				continue
			}
			log.GetLogger().Infof("succeed to del instance when sync, pod %s", ins.PodName)
		}
	}
	return nil
}

// SLOViewResp defines response of slo-agent view
type SLOViewResp struct {
	// instance resource view
	InstanceList InsView `json:"instanceList,omitempty"`
}

// InsView defines func instance vpa list
type InsView struct {
	VpaInsView []vpa.FuncInstanceInfo `json:"vpaInsView,omitempty"`
}
