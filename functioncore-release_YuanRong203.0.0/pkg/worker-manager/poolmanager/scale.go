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

// Package poolmanager manage pool
package poolmanager

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/utils"
)

const (
	// update replicas interval when scale up
	updateReplicasInterval          = 1000 * time.Millisecond
	defaultScalingNum               = 1
	defaultWorkerNum                = 2
	defaultReactiveScalingUpWindow  = 5
	defaultPoolRolloutCheckInterval = 30
)

var (
	scTicker      map[standard]*time.Ticker
	scTickerMutex sync.Mutex
)

// ScalingConfig scale up and down config
type ScalingConfig struct {
	standard                  standard
	replicas                  int32
	poolSizeLimit             int32
	waitingGeneralizedReqNums int32
	basePoolSize              int32
	// current scaling config
	elasticScalingCfg      *config.ElasticScalingConfig
	elasticScalingCfgMutex sync.RWMutex
	// proactive scaling config
	scalingMutex              sync.Mutex
	scalingPlan               []int64
	enableFusionPolicy        int32
	scalingUpTriggerCounter   int32
	scalingUpTriggerThreshold int32
	cooldownPeriod            int32
	proactiveMode             int32
}

func init() {
	if scTicker == nil {
		scTicker = make(map[standard]*time.Ticker)
	}
	config.RegisterHotLoadScaleDownTickerCallBack(hotLoadScaleDownTicker)
}

func updateScaleDownInterval(scaleDownInterval int) {
	scTickerMutex.Lock()
	defer scTickerMutex.Unlock()
	for _, scaleDownIntervalTicker := range scTicker {
		if scaleDownIntervalTicker == nil {
			continue
		}
		scaleDownIntervalTicker.Reset(time.Duration(scaleDownInterval) * time.Second)
	}
}

func hotLoadScaleDownTicker(config *config.FaasConfig, loadConfig *config.HotLoadConfig) {
	if loadConfig.ElasticScalingConfig == nil {
		return
	}
	oldConfig := config.ElasticScalingConfig
	hotLoadScalingConfig := loadConfig.ElasticScalingConfig
	oldInterval := oldConfig.ScaleDownInterval
	newInterval := hotLoadScalingConfig.ScaleDownInterval
	if oldInterval != newInterval {
		log.GetLogger().Infof("update ScaleDownInterval from old: %d seconds, new: %d seconds",
			oldInterval, newInterval)
		oldConfig.ScaleDownInterval = newInterval
		updateScaleDownInterval(newInterval)
	}
}

func newScalingConfig(poolSize int32, poolSizeLimit int32, standard standard) *ScalingConfig {
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config when setting pool scaling config for %s", standard)
		return nil
	}
	scalingUpTriggerThreshold := faasConfig.PoolMgrProactiveConfig.ScalingUpTriggerTimeThreshold
	enableFusionPolicy := faasConfig.PoolMgrProactiveConfig.EnableFusionPolicy
	return &ScalingConfig{
		elasticScalingCfg:         config.GetElasticScalingConfig(),
		replicas:                  poolSize,
		basePoolSize:              poolSize,
		standard:                  standard,
		poolSizeLimit:             poolSizeLimit,
		scalingUpTriggerCounter:   0,
		scalingUpTriggerThreshold: scalingUpTriggerThreshold,
		enableFusionPolicy:        enableFusionPolicy,
	}
}

func (sc *ScalingConfig) monitorScalingDown(stopCh <-chan struct{}) {
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("snapshot manager mode, skip monitorScalingDown")
		return
	}
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	if sc.getElasticScalingCfg() == nil {
		log.GetLogger().Errorf("elastic scaling config is nil")
		return
	}
	log.GetLogger().Infof("scale down ticker gets to work with standard: %s", sc.standard)

	scTickerMutex.Lock()
	if scTicker == nil {
		scTicker = make(map[standard]*time.Ticker)
	}
	scaleDownIntervalTicker, ok := scTicker[sc.standard]
	if !ok {
		scaleDownIntervalTicker = time.NewTicker(time.Duration(sc.scaleDownInterval()) * time.Second)
		scTicker[sc.standard] = scaleDownIntervalTicker
	}
	scTickerMutex.Unlock()
	for {
		select {
		case <-scaleDownIntervalTicker.C:
			replicas := atomic.LoadInt32(&sc.replicas)
			if replicas <= sc.effectiveThresholdReplicas() {
				log.GetLogger().Debugf("replicas: %d is less than threshold: %d, skip to check scale "+
					"down with standard: %s", replicas, sc.effectiveThresholdReplicas(), sc.standard)
				return
			}
			if sc.checkScaleDownIfInProactiveMode() {
				log.GetLogger().Infof("scaling plan is available now, skip scale down for %s", sc.standard)
				continue
			}
			sc.checkScaleDown()
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed with standard: %s", sc.standard)
			}
			scaleDownIntervalTicker.Stop()
			log.GetLogger().Infof("scale down ticker is stopped with standard: %s", sc.standard)
			return
		}
	}
}

func (p *Pool) monitorPoolReplicas(stopCh <-chan struct{}) {
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("snapshot manager mode, skip monitorPoolReplicas")
		return
	}
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	oldReplicas := p.scalingConfig.basePoolSize
	expectReplicas := atomic.LoadInt32(&p.scalingConfig.replicas)
	log.GetLogger().Infof("begin to monitor pool replicas with standard: %s", p.Standard)
	count := 0
	p.resetHangingTime()
	tick := time.NewTicker(updateReplicasInterval)
	for {
		select {
		case <-tick.C:
			count++
			expectReplicas, oldReplicas = p.updateReplicasWithExpect(expectReplicas, oldReplicas)
			if count < defaultPoolRolloutCheckInterval {
				continue
			}
			count = 0
			poolMgr.checkAndRolloutDeploy(p)
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed with standard: %s", p.Standard)
			}
			tick.Stop()
			log.GetLogger().Infof("stop monitoring pool replicas with standard: %s", p.Standard)
			return
		}
	}
}

func (p *Pool) updateReplicasWithExpect(expectReplicas, oldReplicas int32) (int32, int32) {
	if expectReplicas != oldReplicas {
		if err := poolMgr.updateDeployReplicas(p, expectReplicas); err != nil {
			log.GetLogger().Warnf("update pool replicas: %d error: %s with standard: %s",
				expectReplicas, err.Error(), p.Standard)
			return expectReplicas, oldReplicas
		}
		oldReplicas = expectReplicas
		log.GetLogger().Infof("succeed to update pool replicas with %d and standard: %s",
			expectReplicas, p.Standard)
	}
	expectReplicas = atomic.LoadInt32(&p.scalingConfig.replicas)
	return expectReplicas, oldReplicas
}

func (sc *ScalingConfig) considerScaleUp(req common.PodRequest, labels map[string]string) {
	if labels == nil {
		labels = make(map[string]string, utils.DefaultMapSize)
	}
	replicas := atomic.LoadInt32(&sc.replicas)
	if sc.getElasticScalingCfg() == nil || replicas <= sc.effectiveThresholdReplicas() {
		return
	}
	if cache.IsOverload(sc.thresholdResourceUsage(), cache.WithLabelValueEq(labels),
		cache.WithLabelValueIn(constants.NodeRoleLabelKey, constants.GetValidNodeRoleValues()...)) {
		log.GetLogger().Warnf("cluster average usage exceeded limit: %f will not scale up with "+
			"waiting request: %d, standard: %s, cluster labels: %+v", sc.thresholdResourceUsage(),
			atomic.LoadInt32(&sc.waitingGeneralizedReqNums), sc.standard, labels)
		return
	}
	loadCondition, ratioCondition := checkCondition(req)
	sc.setScalingCfg(loadCondition, ratioCondition)
	sc.scaleUp()
}

func (sc *ScalingConfig) scaleUp() {
	actualReplicas := atomic.LoadInt32(&sc.replicas)
	waitingReqNums := atomic.LoadInt32(&sc.waitingGeneralizedReqNums)
	// no need to scale up
	if actualReplicas >= sc.maxScaleUpPodNums() || actualReplicas >= waitingReqNums {
		log.GetLogger().Infof("no need to scale up with actual replicas: %d, waiting request: %d, standard: %s",
			actualReplicas, waitingReqNums, sc.standard)
		return
	}
	// reach scale up number limit
	if actualReplicas >= sc.basePoolSize && sc.poolSizeLimit > 0 && actualReplicas >= sc.poolSizeLimit {
		log.GetLogger().Warnf("reach scale up number limit %d with actual replicas: %d, basePoolSize: %d, "+
			"standard: %s", sc.poolSizeLimit, actualReplicas, sc.basePoolSize, sc.standard)
		return
	}
	scaleUpNums := sc.getScaleUpNums()
	if scaleUpNums == 0 {
		log.GetLogger().Infof("no need to scale up with actual replicas: %d, waiting request: %d, standard: %s",
			actualReplicas, waitingReqNums, sc.standard)
		return
	}
	expectReplicas := actualReplicas + scaleUpNums
	// actual replicas is not more than waiting request nums
	// make sure that expect replicas is not more than waiting request nums
	extraScaleUpNums := int32(math.Ceil(sc.extraScaleUpPodNumsPercentage() * float64(actualReplicas)))
	maxExpectReplicas := waitingReqNums + extraScaleUpNums
	if expectReplicas > maxExpectReplicas {
		expectReplicas = maxExpectReplicas
	}
	if sc.poolSizeLimit > 0 && expectReplicas >= sc.poolSizeLimit {
		expectReplicas = sc.poolSizeLimit
		log.GetLogger().Warnf("reach max scale up nums: %d with standard: %s", sc.poolSizeLimit, sc.standard)
	}
	maxScaleUpPodNums := sc.maxScaleUpPodNums()
	if expectReplicas > maxScaleUpPodNums {
		expectReplicas = maxScaleUpPodNums
		log.GetLogger().Warnf("reach max scale up nums: %d with standard: %s", maxScaleUpPodNums, sc.standard)
	}
	atomic.StoreInt32(&sc.replicas, expectReplicas)
	log.GetLogger().Infof("scale up nums: %d, waiting request: %d, replicas: %d with standard: %s",
		expectReplicas-actualReplicas, waitingReqNums, expectReplicas, sc.standard)
	sc.checkScalingUpCounterIfInProactiveMode(expectReplicas)
}

func (sc *ScalingConfig) checkScaleDown() {
	scaleDownNums := sc.getScaleDownNums()
	if scaleDownNums == 0 {
		return
	}
	actualReplicas := atomic.LoadInt32(&sc.replicas)
	// no need to scale down
	if actualReplicas == sc.basePoolSize {
		return
	}
	expectReplicas := actualReplicas - scaleDownNums
	if expectReplicas < sc.basePoolSize {
		expectReplicas = sc.basePoolSize
		log.GetLogger().Warnf("scale down expect replicas less than base pool size with standard: %s", sc.standard)
	}
	atomic.StoreInt32(&sc.replicas, expectReplicas)
	log.GetLogger().Infof("scale down nums: %d, waiting request: %d, replicas: %d with standard: %s",
		expectReplicas-actualReplicas, atomic.LoadInt32(&sc.waitingGeneralizedReqNums), sc.replicas, sc.standard)
}

func (sc *ScalingConfig) getScaleUpNums() int32 {
	replicas := atomic.LoadInt32(&sc.replicas)
	waitingGeneralizedReqNums := atomic.LoadInt32(&sc.waitingGeneralizedReqNums)
	// waitingGeneralizedReqNums/replicas > UsedPodNumsExceedPercentage will scale up
	thresholdPodNums := float64(replicas) * sc.usedPodNumsExceedPercentage()
	log.GetLogger().Debugf("scale up waiting request nums: %d, threshold pod nums: %f for standard: %s",
		waitingGeneralizedReqNums, thresholdPodNums, sc.standard)
	if utils.FloatAccuracy.SmallerOrEqual(float64(waitingGeneralizedReqNums), thresholdPodNums) {
		return 0
	}
	if replicas == 0 {
		return int32(math.Ceil(sc.maxTransitionScaleUpPercentage() *
			float64(waitingGeneralizedReqNums)))
	}
	return int32(math.Ceil(sc.scaleUpPodNumsPercentage() * float64(replicas)))
}

func (sc *ScalingConfig) getScaleDownNums() int32 {
	replicas := atomic.LoadInt32(&sc.replicas)
	waitingGeneralizedReqNums := atomic.LoadInt32(&sc.waitingGeneralizedReqNums)
	// waitingGeneralizedReqNums/replicas < ScaleDownPodNumsPercentage will scale down
	thresholdPodNums := float64(replicas) * sc.usedPodNumsBelowPercentage()
	log.GetLogger().Debugf("scale down waiting request nums: %d, threshold pod nums: %f for standard: %s",
		waitingGeneralizedReqNums, thresholdPodNums, sc.standard)
	if utils.FloatAccuracy.GreaterOrEqual(float64(waitingGeneralizedReqNums), thresholdPodNums) {
		return 0
	}
	return int32(math.Ceil(sc.scaleDownPodNumsPercentage() * float64(replicas)))
}

// first bool represent status of proxy: free or heavy
// second bool represent status of instance added: free or heavy
func checkCondition(req common.PodRequest) (bool, bool) {
	currentWorkerNum := req.CurrentWorkerNum
	scalingWorkersNum := req.ScalingWorkersNum
	if currentWorkerNum == 0 {
		// don't set load status in cold start condition
		return false, monitoring.GetRatioStatus()
	}
	if scalingWorkersNum/(scalingWorkersNum+currentWorkerNum) >= defaultScalingNum/defaultWorkerNum {
		return true, monitoring.GetRatioStatus()
	}
	return false, monitoring.GetRatioStatus()
}

func (sc *ScalingConfig) setScalingCfg(loadCondition, ratioCondition bool) {
	sc.elasticScalingCfgMutex.Lock()
	defer sc.elasticScalingCfgMutex.Unlock()
	if loadCondition {
		log.GetLogger().Debug("proxy is under heavy load")
		sc.elasticScalingCfg.UsedPodNumsExceedPercentage = sc.elasticScalingCfg.UsedPodNumsExceedHeavyPercentage
		if ratioCondition {
			log.GetLogger().Debug("ratio is under heavy status")
			sc.elasticScalingCfg.ScaleUpPodNumsPercentage = sc.elasticScalingCfg.ScaleUpPodNumsHeavyPercentage
		} else {
			sc.elasticScalingCfg.ScaleUpPodNumsPercentage = sc.elasticScalingCfg.ScaleUpPodNumsFreePercentage
		}
	} else {
		log.GetLogger().Debug("proxy is under free load")
		sc.elasticScalingCfg.UsedPodNumsExceedPercentage = sc.elasticScalingCfg.UsedPodNumsExceedFreePercentage
		if ratioCondition {
			log.GetLogger().Debug("ratio is under heavy status")
			sc.elasticScalingCfg.ScaleUpPodNumsPercentage = sc.elasticScalingCfg.ScaleUpPodNumsHeavyPercentage
		} else {
			sc.elasticScalingCfg.ScaleUpPodNumsPercentage = sc.elasticScalingCfg.ScaleUpPodNumsFreePercentage
		}
	}
}

func (sc *ScalingConfig) checkScaleDownIfInProactiveMode() bool {
	isAlreadyInCooldownPeriod := atomic.LoadInt32(&sc.cooldownPeriod) > 0
	isProactiveMode := atomic.LoadInt32(&sc.proactiveMode) > 0
	sc.scalingMutex.Lock()
	scalingPlanAvailable := sc.scalingPlan != nil && len(sc.scalingPlan) > 0
	sc.scalingMutex.Unlock()
	return isProactiveMode && scalingPlanAvailable && !isAlreadyInCooldownPeriod
}

func (sc *ScalingConfig) checkScalingUpCounterIfInProactiveMode(expectedReplica int32) {
	sc.scalingMutex.Lock()
	scalingPlanAvailable := sc.scalingPlan != nil && len(sc.scalingPlan) > 0
	for i := range sc.scalingPlan {
		sc.scalingPlan[i] = int64(expectedReplica)
		if i >= defaultReactiveScalingUpWindow {
			break
		}
	}
	sc.scalingMutex.Unlock()
	isAlreadyInCooldownPeriod := atomic.LoadInt32(&sc.cooldownPeriod) > 0
	enableFusionPolicy := atomic.LoadInt32(&sc.enableFusionPolicy) > 0
	isProactiveMode := atomic.LoadInt32(&sc.proactiveMode) > 0
	if !isProactiveMode || !scalingPlanAvailable {
		log.GetLogger().Infof("%s is in reactive mode or scaling plan is unavailable, "+
			"so skip to check counter status", sc.standard)
		return
	}
	if !enableFusionPolicy {
		log.GetLogger().Infof("%s turns off fusion policy for pool prediction", sc.standard)
		return
	}
	if enableFusionPolicy && isAlreadyInCooldownPeriod {
		log.GetLogger().Infof("%s is already in cooldown time and skip to check counter status", sc.standard)
		return
	}
	counter := atomic.AddInt32(&sc.scalingUpTriggerCounter, 1)
	threshold := atomic.LoadInt32(&sc.scalingUpTriggerThreshold)
	if counter > threshold {
		atomic.StoreInt32(&sc.scalingUpTriggerCounter, 0)
		log.GetLogger().Infof("%s scales up too many times reactively, return to reactive mode", sc.standard)
		faasConfig, err := config.GetConfig()
		if err != nil {
			log.GetLogger().Errorf("fail to get faas config, fail to set cooldown time for %s", sc.standard)
			return
		}
		atomic.StoreInt32(&sc.cooldownPeriod, faasConfig.PoolMgrProactiveConfig.CooldownTime)
		sc.setElasticScalingCfg(config.GetElasticScalingConfig())
		go func() {
			timer := time.NewTimer(time.Duration(faasConfig.PoolMgrProactiveConfig.CooldownTime) * time.Minute)
			<-timer.C
			timer.Stop()
			atomic.StoreInt32(&sc.cooldownPeriod, 0)
			log.GetLogger().Infof("%s pool start to listen prediction result, return to proactive mode",
				sc.standard)
		}()
	}
}

func (sc *ScalingConfig) applyScalingPlanPeriodically(stopCh <-chan struct{}, poolStandard standard) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config, fail to start goroutine to apply scaling plan for %s",
			sc.standard)
		return
	}
	poolMgrConfig := faasConfig.PoolMgrProactiveConfig
	tick := time.NewTicker(time.Duration(poolMgrConfig.ApplyPredictionResultToPoolInterval) * time.Minute)
	for {
		select {
		case <-tick.C:
			sc.applyScalingPlan(poolStandard)
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed when applying prediction result")
			}
			tick.Stop()
			log.GetLogger().Infof("stop applying prediction result")
			return
		}
	}
}

func (sc *ScalingConfig) applyScalingPlan(poolStandard standard) {
	// drop prediction result if trigger several times of reactive scaling up during a period
	isInCooldownPeriod := atomic.LoadInt32(&sc.cooldownPeriod) > 0
	isProactiveMode := atomic.LoadInt32(&sc.proactiveMode) > 0
	if !isProactiveMode || isInCooldownPeriod {
		sc.scalingMutex.Lock()
		sc.scalingPlan = nil
		sc.scalingMutex.Unlock()
		sc.setElasticScalingCfg(config.GetElasticScalingConfig())
		log.GetLogger().Infof("%s is in cooldown period or in reactive mode, so return to reactive mode",
			poolStandard)
		return
	}
	// extract expected replica config in advance for observability in productive env
	expectedReplica := int32(-1)
	sc.scalingMutex.Lock()
	if sc.scalingPlan != nil && len(sc.scalingPlan) > 0 {
		log.GetLogger().Infof("current scaling plan for %s: %v", poolStandard, sc.scalingPlan)
		expectedReplica = sc.resizeReplicaInProactiveMode()
		sc.scalingPlan = sc.scalingPlan[1:]
		log.GetLogger().Infof("after resize pool size for %s: %d", poolStandard, expectedReplica)
	}
	sc.scalingMutex.Unlock()
	if expectedReplica <= 0 {
		sc.setElasticScalingCfg(config.GetElasticScalingConfig())
		log.GetLogger().Infof("no available prediction result for %s can be used", poolStandard)
		return
	}
	// reload scaling config in case hot load happens
	newFaasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config, fail to apply scaling plan for %s", sc.standard)
		return
	}
	sc.setElasticScalingCfg(newFaasConfig.ProactiveElasticScalingConfig)
	atomic.StoreInt32(&sc.replicas, expectedReplica)
	log.GetLogger().Infof("set scaling plan for %s successfully: %d", poolStandard, expectedReplica)
	return
}

func (sc *ScalingConfig) resetScalingUpCounter(stopCh <-chan struct{}, poolStandard standard) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config, "+
			"fail to start goroutine to reset scaling up counter for %s", sc.standard)
		return
	}
	poolMgrConfig := faasConfig.PoolMgrProactiveConfig
	tick := time.NewTicker(time.Duration(poolMgrConfig.ReactiveScalingUpResetInterval) * time.Minute)
	for {
		select {
		case <-tick.C:
			scalingUpTime := atomic.LoadInt32(&sc.scalingUpTriggerCounter)
			log.GetLogger().Infof("reset scaling up counter successfully: %s, it scaled up %d times "+
				"in the last check period", poolStandard, scalingUpTime)
			atomic.StoreInt32(&sc.scalingUpTriggerCounter, 0)
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed when resetting scaling up counter")
			}
			tick.Stop()
			log.GetLogger().Infof("stop resetting scaling up counter")
			return
		}
	}
}

func (sc *ScalingConfig) resizeReplicaInProactiveMode() int32 {
	expectedReplica := int32(sc.scalingPlan[0])
	faasConfig, err := config.GetConfig()
	lowerFactor := 1.0
	upperFactor := 1.0
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config, use default value(1.0) as the factor")
	} else {
		poolMgrConfig := faasConfig.PoolMgrProactiveConfig
		lowerFactor = poolMgrConfig.LowerBoundFactor
		upperFactor = poolMgrConfig.UpperBoundFactor
	}
	lower := float64(sc.basePoolSize) * lowerFactor
	upper := float64(sc.poolSizeLimit) * upperFactor
	if expectedReplica < int32(math.Ceil(lower)) {
		expectedReplica = int32(math.Ceil(lower))
		log.GetLogger().Infof("resize predicted replica to lower bound, before resizing: %d", expectedReplica)
	}
	if expectedReplica > int32(math.Ceil(upper)) {
		expectedReplica = int32(math.Ceil(upper))
		log.GetLogger().Infof("resize predicted replica to upper bound, before resizing: %d", expectedReplica)
	}
	return expectedReplica
}

func (sc *ScalingConfig) setElasticScalingCfg(newConfig *config.ElasticScalingConfig) {
	sc.elasticScalingCfgMutex.Lock()
	defer sc.elasticScalingCfgMutex.Unlock()
	sc.elasticScalingCfg = newConfig
}

func (sc *ScalingConfig) getElasticScalingCfg() *config.ElasticScalingConfig {
	sc.elasticScalingCfgMutex.Lock()
	defer sc.elasticScalingCfgMutex.Unlock()
	return sc.elasticScalingCfg
}

func (sc *ScalingConfig) usedPodNumsExceedPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.UsedPodNumsExceedPercentage
}

func (sc *ScalingConfig) scaleUpPodNumsPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.ScaleUpPodNumsPercentage
}

func (sc *ScalingConfig) extraScaleUpPodNumsPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.ExtraScaleUpPodNumsPercentage
}

func (sc *ScalingConfig) usedPodNumsBelowPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.UsedPodNumsBelowPercentage
}

func (sc *ScalingConfig) scaleDownPodNumsPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.ScaleDownPodNumsPercentage
}

func (sc *ScalingConfig) effectiveThresholdReplicas() int32 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.EffectiveThresholdReplicas
}

func (sc *ScalingConfig) maxScaleUpPodNums() int32 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.MaxScaleUpPodNums
}

func (sc *ScalingConfig) scaleDownInterval() int {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.ScaleDownInterval
}

func (sc *ScalingConfig) thresholdResourceUsage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.ThresholdResourceUsage
}

func (sc *ScalingConfig) usedPodNumsExceedFreePercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.UsedPodNumsExceedFreePercentage
}

func (sc *ScalingConfig) usedPodNumsExceedHeavyPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.UsedPodNumsExceedHeavyPercentage
}

func (sc *ScalingConfig) scaleUpPodNumsFreePercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.ScaleUpPodNumsFreePercentage
}

func (sc *ScalingConfig) scaleUpPodNumsHeavyPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.ScaleUpPodNumsHeavyPercentage
}

func (sc *ScalingConfig) maxTransitionScaleUpPercentage() float64 {
	sc.elasticScalingCfgMutex.RLock()
	defer sc.elasticScalingCfgMutex.RUnlock()
	return sc.elasticScalingCfg.MaxTransitionScaleUpPercentage
}
