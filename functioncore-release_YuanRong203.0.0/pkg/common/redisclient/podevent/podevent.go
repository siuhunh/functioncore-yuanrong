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

// Package podevent cae pod event
package podevent

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/strutil"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

const (
	defaultLen          = 20
	redisContextTimeout = 100 * time.Millisecond

	// PullingStage -
	PullingStage = "PodPullingStage"
	// SpecializationStage -
	SpecializationStage = "PodSpecializationStage"
	// RunningStage -
	RunningStage = "PodRunningStage"
	// ExitingStage -
	ExitingStage = "PodExitingStage"

	// EventInfoType -
	EventInfoType = "Info"
	// EventErrorType -
	EventErrorType = "Error"

	// PodPullFailed -
	PodPullFailed = "PodPullFailed"
	// PodSpecializeFailed -
	PodSpecializeFailed = "PodSpecializeFailed"
	// PodScaleDown -
	PodScaleDown = "PodScaleDown"
	// PodScaleUp -
	PodScaleUp = "PodScaleUp"
	// PodRunningError -
	PodRunningError = "PodRunningError"

	healthcheckStr = "health check"

	failedPullPodInternalCode = "30001"

	checkpointPrefix = "checkpoint"
	deletePrefix     = "delete"
	uploadPrefix     = "upload"
)

// EventInfo -
type EventInfo struct {
	PodName         string `json:"podName"`
	PodIP           string `json:"podIP"`
	EnvName         string `json:"envName"`
	EnvID           string `json:"envId"`
	ComponentName   string `json:"componentName"`
	ComponentID     string `json:"componentId"`
	ApplicationName string `json:"applicationName"`
	ApplicationID   string `json:"applicationId"`
	UploadTime      int64  `json:"uploadTime"`
	EventStageInfo
}

// EventStageInfo -
type EventStageInfo struct {
	ErrorMessage string `json:"errorMessage"`
	EventStage   string `json:"eventStage"`
	EventSummary string `json:"eventSummary"`
	EventType    string `json:"eventType"`
}

// GetErrorMessage -
func (e *EventStageInfo) GetErrorMessage(traceID string, err error) {
	if err == nil || len(e.ErrorMessage) != 0 {
		return
	}
	var errMsg string
	if snErr, ok := err.(snerror.SNError); ok {
		snErrPrefix := snerror.ErrText(snErr.Code()) + ", error: "
		errMsg = strings.TrimPrefix(snErr.Error(), snErrPrefix)
	} else {
		errMsg = err.Error()
	}

	switch e.EventStage {
	case PullingStage:
		if strings.Contains(errMsg, "ImagePullBackOff") ||
			strings.Contains(errMsg, "environment variable") {
			e.ErrorMessage = "Failed to pull the pod, " + errMsg
		} else {
			e.ErrorMessage = "Failed to pull the pod, errorCode: " + failedPullPodInternalCode
		}
	case SpecializationStage:
		if strings.Contains(errMsg, healthcheckStr) {
			e.ErrorMessage = "Failed to generalize pod, " + errMsg
		} else {
			e.ErrorMessage = "Failed to generalize pod, traceID: " + traceID
		}
	case RunningStage, ExitingStage:
		e.ErrorMessage = "An error occurred, " + errMsg
	default:
		e.ErrorMessage = errMsg
	}
}

// NewPodErrorInfo -
func NewPodErrorInfo(properties map[string]string, podName string, podIP string, stageInfo EventStageInfo) EventInfo {
	return EventInfo{
		PodName:         podName,
		PodIP:           podIP,
		EnvName:         properties[constants.CAEPropertiesEnvName],
		ComponentName:   properties[constants.CAEPropertiesComponentName],
		ApplicationName: properties[constants.CAEPropertiesAppName],
		EnvID:           properties[constants.CAEPropertiesEnvID],
		ComponentID:     properties[constants.CAEPropertiesComponentID],
		ApplicationID:   properties[constants.CAEPropertiesAppID],
		UploadTime:      time.Now().Unix(),
		EventStageInfo:  stageInfo,
	}
}

// ReportPodEventToRedis -
func ReportPodEventToRedis(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
	eventInfo EventInfo) {
	log.GetLogger().Debugf("report pod errMsg To Redis, URNWithVersion %s, podName %s, errMsg %s,"+
		" minInstanceNum %d", URNWithVersion, eventInfo.PodName, eventInfo.ErrorMessage, minInstanceNum)
	if c == nil {
		log.GetLogger().Errorf("redis is not ready when save error message to redis")
		return
	}
	data, err := json.Marshal(eventInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to json marshal PodErrorInfo, %s", err.Error())
		return
	}
	// In normal cases, the Redis lock is required. Currently, the Redis lock is not added. Considering that the pop
	// header element is displayed in the case of multiple concurrent requests, this problem does not affect the system.
	ctx := context.Background()
	lenCtx, cancel := context.WithTimeout(ctx, redisContextTimeout)
	curlength, err := c.LLen(lenCtx, URNWithVersion).Result()
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to LLen, urn: %s, podName: %s, err: %s", URNWithVersion,
			eventInfo.PodName, err.Error())
		return
	}
	pushCtx, cancel := context.WithTimeout(ctx, redisContextTimeout)
	err = c.RPush(pushCtx, URNWithVersion, data).Err()
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to RPush, urn: %s, podName: %s, err: %s",
			URNWithVersion, eventInfo.PodName, err.Error())
		return
	}
	length := minInstanceNum + defaultLen
	if curlength+1 > length {
		popNum := int(curlength + 1 - length)
		log.GetLogger().Debugf("urn %s, popNum %d, minInstanceNum %d", URNWithVersion, popNum, minInstanceNum)
		for ; popNum > 0; popNum-- {
			popCtx, cancel := context.WithTimeout(ctx, redisContextTimeout)
			err = c.LPop(popCtx, URNWithVersion).Err()
			cancel()
			if err != nil {
				log.GetLogger().Errorf("failed to LPopCount, urn: %s, podName: %s, err: %s",
					URNWithVersion, eventInfo.PodName, err.Error())
				continue
			}
		}
	}
}

// ReportCaePodEvent -
func ReportCaePodEvent(pod *corev1.Pod, service *faasfunction.FunctionServiceSpec, stageInfo *EventStageInfo,
	traceID string, eventErr error) {
	var businessType string
	var funcName string
	var properties map[string]string
	var functionVersionURN string
	var minInsNum int64
	if pod == nil || len(pod.Name) == 0 {
		log.GetLogger().Warnf("invalid pod")
		return
	}
	if service == nil {
		log.GetLogger().Infof("funcName is deleted, podname: %s, traceID %s", pod.Name, traceID)
		funcName = pod.ObjectMeta.Labels["funcName"]
		businessType = pod.ObjectMeta.Labels["businessType"]
		functionVersionURN = fmt.Sprintf("sn:cn:%s:%s:function:%s:%s",
			pod.ObjectMeta.Labels["businessID"], pod.ObjectMeta.Labels["tenantID"], pod.ObjectMeta.Annotations["funcName"],
			pod.ObjectMeta.Labels["version"])
		intNums, err := strconv.Atoi(pod.ObjectMeta.Annotations[constants.MinInsNum])
		minInsNum = int64(intNums)
		if err != nil {
			log.GetLogger().Errorf("failed to Atoi, MinInsNum %S, podName: %s, funcName: %s, err: %s",
				pod.ObjectMeta.Annotations[constants.MinInsNum], pod.Name, funcName, err.Error())
			return
		}
		if err = json.Unmarshal(strutil.StringToBytes(pod.ObjectMeta.Annotations[constants.Properties]),
			&properties); err != nil {
			log.GetLogger().Errorf("failed to unmarshal properties data, podName: %s, funcName: %s, err: %s",
				pod.Name, funcName, err.Error())
			return
		}
	} else {
		businessType = service.Configuration.BusinessType
		funcName = service.Configuration.FunctionName
		properties = service.Configuration.Properties
		functionVersionURN = service.Configuration.FunctionVersionURN
		minInsNum = service.Configuration.AutoScalerConfig.MinimumScale
	}

	// exclude snapshot checkpoint/delete/upload pod event
	if commonUtils.IsCAEFunc(businessType) && !strings.Contains(pod.Name, checkpointPrefix) &&
		!strings.Contains(pod.Name, deletePrefix) && !strings.Contains(pod.Name, uploadPrefix) {
		stageInfo.GetErrorMessage(traceID, eventErr)
		podErrInfo := NewPodErrorInfo(properties, pod.Name, pod.Status.PodIP, *stageInfo)
		ReportPodEventToRedis(redisclient.GetRedisCmd(), functionVersionURN, minInsNum, podErrInfo)
	}
}

// ReportScaleDownCaeEvent -
func ReportScaleDownCaeEvent(pod *corev1.Pod, functionService *faasfunction.FunctionService) {
	businessType := ""
	var spec *faasfunction.FunctionServiceSpec
	if functionService != nil {
		spec = &functionService.Spec
	}
	if pod == nil || len(pod.Name) == 0 {
		log.GetLogger().Warnf("pod is nil or podName is empty")
		return
	}
	podName := pod.Name
	businessType = pod.Labels["businessType"]

	if commonUtils.IsCAEFunc(businessType) {
		// The Case pod was initially set with TerminationGracePeriodSeconds as 920 seconds.
		// During a forced deletion, the TerminationGracePeriodSeconds will be set to 0.
		if pod.DeletionTimestamp != nil && pod.Spec.TerminationGracePeriodSeconds != nil &&
			*pod.Spec.TerminationGracePeriodSeconds == 0 {
			log.GetLogger().Infof("the pod %s is being force deleted, no need to report events.", pod.Name)
			return
		}

		stageInfo := &EventStageInfo{
			EventStage:   ExitingStage,
			EventSummary: PodScaleDown,
			EventType:    EventInfoType,
			ErrorMessage: fmt.Sprintf("The pod %s is being scaled down and will exit", podName),
		}
		ReportCaePodEvent(pod, spec, stageInfo, "", nil)
	}
}
