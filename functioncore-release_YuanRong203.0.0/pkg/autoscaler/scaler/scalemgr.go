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
	"functioncore/pkg/autoscaler/instance"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/functiontypes"
)

var (
	scaleMgr *ScaleMgr
)

const alarmTime = 5 * time.Minute

// ScaleMgr -
type ScaleMgr struct {
	config       *config.Config
	stopCh       <-chan struct{}
	scalerGroup  sync.Map
	alarmed      bool
	preAlarmTime time.Time
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
		scalerGroup: sync.Map{},
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

	metricConfig := s.createMetricConfig(asbresConfig)
	scaler := &FunctionScaler{
		functionSvcID:      functionSvcID,
		resiliencyStrategy: createStrategy(asbresConfig, functionSvcID, metricConfig),
	}
	s.scalerGroup.Store(functionSvcID, scaler)

	return
}

// RemoveScaler -
func (s *ScaleMgr) RemoveScaler(functionSvcID string) *ScaleMgr {
	s.scalerGroup.Delete(functionSvcID)
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

		metricConfig := s.createMetricConfig(asbresConfig)
		scaler := &FunctionScaler{
			functionSvcID:      functionSvcID,
			resiliencyStrategy: createStrategy(asbresConfig, functionSvcID, metricConfig),
		}
		s.scalerGroup.Store(functionSvcID, scaler)
		if !ok {
			return false
		}

		return true
	})
	go s.Run()
	return nil
}

func (s *ScaleMgr) createMetricConfig(asbresConfig functiontypes.AsbResConfig) config.MetricConfig {
	metricConfig := config.MetricConfig{
		TargetCPUUtilPercentage: atomic.LoadInt64(&asbresConfig.TargetCPUUtilPercentage),
		TargetMemUtilPercentage: atomic.LoadInt64(&asbresConfig.TargetMemUtilPercentage),
		SyncPeriod:              atomic.LoadInt64(&s.config.SyncPeriod),
		DownscaleStabilization:  atomic.LoadInt64(&s.config.DownscaleStabilization),
		PredictEnable:           s.config.PredictConfig.PredictEnable,
		ScaleUpTolerance:        s.config.ScaleUpTolerance,
		ScaleDownTolerance:      s.config.ScaleDownTolerance,
	}
	return metricConfig
}

// Run -
func (s *ScaleMgr) Run() {
	// 60s
	ticker := time.NewTicker(time.Duration(s.config.SyncPeriod) * time.Second)
	defer ticker.Stop()

	log.GetLogger().Infof("start to run instance handler job, syncPeriod is %d", s.config.SyncPeriod)
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			instance.UpdateFuncInsCache()
			actions := s.processScalerGroup()
			DoActions(actions)
		}
	}
}

func (s *ScaleMgr) processScalerGroup() map[string]Action {
	actions := make(map[string]Action)
	needAlarm := false
	s.scalerGroup.Range(func(key, value interface{}) bool {
		scaler, ok := value.(*FunctionScaler)
		if !ok {
			log.GetLogger().Errorf("FunctionScaler cache type err, functionSvcID: %v", key)
			return true
		}
		availableCluster, found := getAvailableClusters(scaler.functionSvcID)
		if !found {
			needAlarm = true
			log.GetLogger().Errorf("no cae available key found for functionSvcID: %s", scaler.functionSvcID)
		}
		if len(availableCluster) == 0 {
			log.GetLogger().Infof("function not available cluster, no need action. functionSvcID:%s.",
				scaler.functionSvcID)
			return true
		}
		instanceMeta := cache.GetInstance().GetInstanceMetaCache(scaler.functionSvcID)
		if instanceMeta == nil {
			log.GetLogger().Errorf("failed to get instance meta for functionSvcID: %s", scaler.functionSvcID)
			return true
		}
		minInsCount := instanceMeta.MinInstance
		maxInsCount := instanceMeta.MaxInstance
		curInsCount, scalingNum := cache.GetInstance().GetFuncInstanceCurrentCount(scaler.functionSvcID)
		expectedMinIns := cache.GetInstance().GetFuncExpectedMinIns(scaler.functionSvcID)
		expectedSnapshot := cache.GetInstance().GetFuncExpectedSnapshot(scaler.functionSvcID)

		if minInsCount > expectedMinIns {
			log.GetLogger().Errorf("wrong min instance config for %s, need %d actual %d", scaler.functionSvcID,
				minInsCount, expectedMinIns)
			needAlarm = true
		}
		meta, ok := cache.GetInstance().GetFunctionMeta(scaler.functionSvcID)
		if ok && meta.SnapshotMetaData.BucketID != "" && expectedSnapshot == 0 {
			needAlarm = true
			log.GetLogger().Errorf("wrong snapshot config for %s", scaler.functionSvcID)
		}

		totalExpected, expectedForPredict, needScale := s.needScaleAction(minInsCount, scalingNum, curInsCount, scaler)
		log.GetLogger().Infof("needScale is %t minInsCount is %d, maxInsCount is %d, scalingNum is %d, "+
			"curInsCount is %d for %s totalExpected is %d, expectedForPredict is %d", needScale, minInsCount,
			maxInsCount, scalingNum, curInsCount, scaler.functionSvcID, totalExpected, expectedForPredict)

		if !needScale {
			return true
		}
		// 给出 哪个函数需要扩容或缩容几个实例的动作
		scaler.generateActions(totalExpected, expectedForPredict, curInsCount, scalingNum, actions)
		return true
	})

	s.alarm(needAlarm)

	return actions
}

func (s *ScaleMgr) alarm(needAlarm bool) {
	if needAlarm && !s.alarmed && s.preAlarmTime.IsZero() {
		s.preAlarmTime = time.Now()
	}
	if needAlarm && !s.alarmed && time.Since(s.preAlarmTime) >= alarmTime {
		s.alarmed = true
		alarm.GetClient().ReportAlarm(alarm.CAEAvailableCluster, "some cae instance config wrong")
	}

	if !needAlarm && s.alarmed {
		s.alarmed = false
		s.preAlarmTime = time.Time{}
		alarm.GetClient().ClearAlarm(alarm.CAEAvailableCluster)
	}
}

func getAvailableClusters(functionSvcID string) ([]string, bool) {
	funcMetadata, exists := cache.GetInstance().GetFunctionMeta(functionSvcID)
	if !exists {
		log.GetLogger().Warnf("the funcMetadata of instanceKey:%s is not exists in cache", functionSvcID)
		return []string{}, false
	}
	funcAvailableClusters, found := instance.GetFuncAvailableClusters(funcMetadata)

	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get autoscaler config, error: %s", err.Error())
		return []string{}, found
	}
	asbAvailableCluster := conf.Clusters
	log.GetLogger().Infof("funcURN[%s]: funcAvailableClusters: %v, asbConfigAvailableCluster: %v",
		funcMetadata.FuncMetaData.VersionURN, funcAvailableClusters, asbAvailableCluster)
	return instance.IntersectArray(funcAvailableClusters, asbAvailableCluster), found
}

func (s *ScaleMgr) needScaleAction(minInsCount int64, scalingNum int64, curInsCount int64,
	scaler *FunctionScaler) (int64, int64, bool) {
	var (
		expected           int64
		expectedForPredict int64
	)
	if curInsCount+scalingNum < minInsCount {
		// when curInsCount < minInsCount, required instances need to be scaled up by worker-manager
		log.GetLogger().Infof("curInsCount %d, scalingNum %d for %s smaller than minInsCount %d", curInsCount,
			scalingNum, scaler.functionSvcID, minInsCount)
		return expected, expectedForPredict, false
	}

	// when minIns < curInsCount < maxIns, required instances need to be calculated by ASBRes
	expected, expectedForPredict, err := scaler.resiliencyStrategy.calExpectedScaleNums()

	if err != nil {
		log.GetLogger().Errorf("failed to cal expected scale nums, err: %s", err.Error())
		return expected, expectedForPredict, false
	}
	if expected == 0 && curInsCount == 0 && expectedForPredict == 0 {
		log.GetLogger().Infof("curInsCount is %d for %s expected is %d, expectedForPredict is %d", curInsCount,
			scaler.functionSvcID, expected, expectedForPredict)
		return expected, expectedForPredict, false
	}
	// 预留实例由 worker-manager拉起
	if expected <= minInsCount {
		expected = minInsCount
		log.GetLogger().Infof("expected is %d for %s smaller than minInsCount %d use minInsCount ",
			expected, scaler.functionSvcID, minInsCount)
	}

	// if expectedForPredict is zero means no need consider predict
	if expectedForPredict > 0 && expectedForPredict < minInsCount {
		log.GetLogger().Infof("expectedForPredict is %d for %s smaller than minInsCount %d use minInsCount ",
			expected, scaler.functionSvcID, minInsCount)
		expectedForPredict = minInsCount
	}

	return expected, expectedForPredict, true
}

func stringsContains(strs []string, s string) bool {
	for _, str := range strs {
		if s == str {
			return true
		}
	}
	return false
}
