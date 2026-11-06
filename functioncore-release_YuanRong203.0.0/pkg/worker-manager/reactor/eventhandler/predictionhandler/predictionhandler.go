/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package predictionhandler -
package predictionhandler

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/utils"
)

const (
	redisContextTimeout = 100 * time.Millisecond
	minuteToSecond      = 60
)

var redisKey = "prediction_function_instance_" + os.Getenv("CLUSTER_ID")

// PredictionHandler define tenant metadata handler
type PredictionHandler struct {
	podFetcher  *podfetcher.PodFetcher
	redisClient *redisclient.Client
	timer       *time.Timer
}

// NewEventHandler new event handler
func NewEventHandler() *PredictionHandler {
	podFetcher := podfetcher.GetPodFetcher()
	if podFetcher == nil {
		log.GetLogger().Errorf("failed to get podFetcher")
		return nil
	}
	handler := &PredictionHandler{
		podFetcher:  podFetcher,
		redisClient: monitoring.GetRedisClient(),
	}
	return handler
}

func (h *PredictionHandler) getPredictionInfoFromRedis() map[string]*config.PredictionInfo {
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	value, err := h.redisClient.Get(ctx, redisKey).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to get key: %s, error: %s", redisKey, err.Error())
		return nil
	}
	var predictionMap map[string]*config.PredictionInfo
	err = json.Unmarshal([]byte(value), &predictionMap)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal prediction redis value, err: %s", err.Error())
		return nil
	}
	return predictionMap
}

// HandleEvent -
func (h *PredictionHandler) HandleEvent(event evs.Event) error {
	if !utils.GetSmartWarmerEnable() {
		log.GetLogger().Warnf("smartWarmer is disable")
		return nil
	}
	log.GetLogger().Infof("start to process instance prediction event, resource key: %s, type %s",
		event.KeyRef, event.EventType)
	if event.Kind != utils.PredictionKind {
		return errors.New("failed to match event kind to instance prediction")
	}
	if event.EventType == evs.Deleted {
		return nil
	}
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return errors.New("invalid value type")
	}
	predictionNotify := &config.PredictionNotify{}
	err := json.Unmarshal(ev.Value, predictionNotify)
	if err != nil {
		log.GetLogger().Errorf("unmarshal prediction notify with key: %s and error: %s", ev.Key, err.Error())
		return err
	}
	if predictionNotify.ValidTimeMinute <= 0 {
		return errors.New("invalid validTime: " + strconv.Itoa(predictionNotify.ValidTimeMinute))
	}

	predictionMap := h.getPredictionInfoFromRedis()
	pastTime := time.Now().Unix() - predictionNotify.PredictionTime
	if pastTime < 0 {
		return errors.New("invalid prediction time: " + strconv.Itoa(int(predictionNotify.PredictionTime)))
	}
	currentValidTime := int64(predictionNotify.ValidTimeMinute*minuteToSecond) - pastTime
	if currentValidTime <= 0 {
		log.GetLogger().Warnf("prediction has expired")
		h.updatePrewarmPool(predictionMap, true)
		return nil
	}
	h.updatePrewarmPool(predictionMap, false)

	if h.timer != nil {
		h.timer.Stop()
	}

	h.timer = time.AfterFunc(time.Duration(currentValidTime)*time.Second, func() {
		log.GetLogger().Infof("prediction valid time is up to %d second ,begin to clear instance",
			currentValidTime)
		h.updatePrewarmPool(predictionMap, true)
	})
	h.deleteUnPredictionPods(predictionMap)
	return nil
}

func (h *PredictionHandler) updatePrewarmPool(predictionMap map[string]*config.PredictionInfo, clear bool) {
	// key： yrk/12345678901234561234567890123456/0@default@nodejstest/latest
	for key, predictionInfo := range predictionMap {
		log.GetLogger().Infof("key is %s, prediction num is: %d", key, predictionInfo.Num)
		elements := strings.Split(key, "/")
		if len(elements) != utils.URNIndexFour {
			log.GetLogger().Errorf("illegal function key: %s", key)
			continue
		}
		funcSvcID := utils.CrName(elements[utils.URNIndexZero], elements[utils.URNIndexOne],
			elements[utils.URNIndexTwo], elements[utils.URNIndexThree])

		var predictionNum int
		if clear {
			// clear prewarm pool
			predictionNum = 0
		} else {
			predictionNum = predictionInfo.Num
		}

		action := cache.WorkerCache.HandlePrediction(funcSvcID, predictionNum, elements[utils.URNIndexTwo])
		h.podFetcher.DoActions(funcSvcID, action, cache.Metadata{Status: cache.NotChanged})
	}
	return
}

func (h *PredictionHandler) deleteUnPredictionPods(predictionMap map[string]*config.PredictionInfo) {
	pods := cache.WorkerCache.GetUnpredictedPods(predictionMap)
	for i := 0; i < len(pods); i++ {
		if pods[i] == nil {
			log.GetLogger().Error("nil pod")
			continue
		}
		go h.podFetcher.VpcHandler.IdleDeletePatServiceList(pods[i].Name)
		go h.podFetcher.DeletePodByK8S(pods[i].Name, "terminate-"+uuid.New().String())
	}
}
