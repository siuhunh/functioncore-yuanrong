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

// Package scaler -
package scaler

import (
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/functiontypes"
)

var (
	scaleMgr *ScaleMgr
)

// ScaleMgr -
type ScaleMgr struct {
	config        *config.Config
	stopCh        <-chan struct{}
	scalerGroup   map[string]*FunctionScaler
	scaleMgrGuard sync.Mutex
}

// CreateScaleMgr -
func CreateScaleMgr(stopCh <-chan struct{}) error {
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	scaleMgr = &ScaleMgr{
		config:      cfg,
		stopCh:      stopCh,
		scalerGroup: make(map[string]*FunctionScaler, 100),
	}
	err = scaleMgr.Start()
	if err != nil {
		log.GetLogger().Errorf("failed to start scaleMgr, err: %s", err)
		return err
	}
	return nil
}

// GetScaleManager returns the scaleMgr
func GetScaleManager() *ScaleMgr {
	return scaleMgr
}

// UpdateScalerGroup -
func (s *ScaleMgr) UpdateScalerGroup(functionSvcID string, asbresConfig functiontypes.AsbResConfig) {
	s.scaleMgrGuard.Lock()
	metricConfig := config.MetricConfig{
		TargetCPUUtilPercentage: atomic.LoadInt64(&asbresConfig.TargetCPUUtilPercentage),
		TargetMemUtilPercentage: atomic.LoadInt64(&asbresConfig.TargetMemUtilPercentage),
		SyncPeriod:              atomic.LoadInt64(&s.config.SyncPeriod),
		DownscaleStabilization:  atomic.LoadInt64(&s.config.DownscaleStabilization),
		ScaleUpTolerance:        s.config.ScaleUpTolerance,
		ScaleDownTolerance:      s.config.ScaleDownTolerance,
	}
	scaler := &FunctionScaler{
		functionSvcID:      functionSvcID,
		resiliencyStrategy: createStrategy(asbresConfig.ScaleStrategy, functionSvcID, metricConfig),
	}
	s.scalerGroup[functionSvcID] = scaler
	s.scaleMgrGuard.Unlock()
	return
}

// RemoveScaler -
func (s *ScaleMgr) RemoveScaler(functionSvcID string) *ScaleMgr {
	s.scaleMgrGuard.Lock()
	defer s.scaleMgrGuard.Unlock()
	delete(s.scalerGroup, functionSvcID)
	return scaleMgr
}

// Start -
func (s *ScaleMgr) Start() error {
	cache.GetInstance().FunctionMetaCache.Range(func(k, v interface{}) bool {
		functionSvcID, ok := k.(string)
		if !ok {
			return false
		}
		functionEtcdInfo, ok := v.(*functiontypes.FunctionEtcdInfo)
		if !ok {
			return false
		}
		asbresConfig := functionEtcdInfo.ExtendedMetaData.AsbResConfig
		s.scaleMgrGuard.Lock()
		metricConfig := config.MetricConfig{
			TargetCPUUtilPercentage: atomic.LoadInt64(&asbresConfig.TargetCPUUtilPercentage),
			TargetMemUtilPercentage: atomic.LoadInt64(&asbresConfig.TargetMemUtilPercentage),
			SyncPeriod:              atomic.LoadInt64(&s.config.SyncPeriod),
			DownscaleStabilization:  atomic.LoadInt64(&s.config.DownscaleStabilization),
			ScaleUpTolerance:        s.config.ScaleUpTolerance,
			ScaleDownTolerance:      s.config.ScaleDownTolerance,
		}
		scaler := &FunctionScaler{
			functionSvcID:      functionSvcID,
			resiliencyStrategy: createStrategy(asbresConfig.ScaleStrategy, functionSvcID, metricConfig),
		}
		s.scalerGroup[functionSvcID] = scaler
		if !ok {
			return false
		}
		s.scaleMgrGuard.Unlock()
		return true
	})
	go s.Run()
	return nil
}

// Run -
func (s *ScaleMgr) Run() {
	ticker := time.NewTicker(time.Duration(s.config.SyncPeriod) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			cache.UpdateFuncInsCache()
			actions := s.processScalerGroup()
			DoActions(actions)
		}
	}
}

func (s *ScaleMgr) processScalerGroup() map[string]Action {
	actions := make(map[string]Action)
	for _, scaler := range s.scalerGroup {
		instanceMeta := cache.GetInstance().GetInstanceMetaCache(scaler.functionSvcID)
		if instanceMeta == nil {
			log.GetLogger().Errorf("failed to get instance meta for functionSvcID: %s", scaler.functionSvcID)
			continue
		}
		minInsCount := cache.GetInstance().GetInstanceMetaCache(scaler.functionSvcID).MinInstance
		maxInsCount := cache.GetInstance().GetInstanceMetaCache(scaler.functionSvcID).MaxInstance
		curInsCount := cache.GetInstance().GetFuncInstanceCurrentCount(scaler.functionSvcID)
		totalExpected, needScale := s.needScaleAction(minInsCount, maxInsCount, curInsCount, scaler)
		log.GetLogger().Infof("needScale is %t minInsCount is %d, maxInsCount is %d, "+
			"curInsCount is %d for %s totalExpected is %d",
			needScale, minInsCount, maxInsCount, curInsCount, scaler.functionSvcID, totalExpected)
		if !needScale {
			continue
		}
		scaler.generateActions(totalExpected, curInsCount, actions)
	}
	return actions
}

func (s *ScaleMgr) needScaleAction(minInsCount int64, maxInsCount int64, curInsCount int64,
	scaler *FunctionScaler) (int64, bool) {
	var expected int64
	if curInsCount < minInsCount {
		// when curInsCount < minInsCount, required instances need to be scaled up by worker-manager
		log.GetLogger().Infof("curInsCount %d for %s smaller than minInsCount %d", curInsCount,
			scaler.functionSvcID, minInsCount)
		return expected, false
	}

	if curInsCount-minInsCount > maxInsCount {
		// maxInsCount does not include reserved instance(aka min instance), only if
		// curInsCount-minInsCount > maxInsCount, extra instances need to be scaled down by worker-manager
		log.GetLogger().Infof("curInsCount-minInsCount %d for %s equals or greater than maxInsCount %d",
			curInsCount-minInsCount, scaler.functionSvcID, maxInsCount)
		return expected, false
	}

	// when minIns < curInsCount < maxIns, required instances need to be calculated by ASBRes
	expected, err := scaler.resiliencyStrategy.calExpectedScaleNums()
	if err != nil {
		log.GetLogger().Errorf("failed to cal expected scale nums, err: %s", err.Error())
		return expected, false
	}
	if expected == 0 && curInsCount == 0 {
		log.GetLogger().Infof("curInsCount is %d for %s expected is %d", curInsCount, scaler.functionSvcID, expected)
		return expected, false
	}
	if expected <= minInsCount {
		expected = minInsCount
		log.GetLogger().Infof("expected is %d for %s smaller than minInsCount %d use minInsCount",
			expected, scaler.functionSvcID, minInsCount, maxInsCount)
	}
	return expected, true
}
