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

// Package autoscaler controls the scaling of function instances from a cluster's perspective.
package autoscaler

import (
	"context"
	"reflect"
	"sync"
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache/willdeleteworker"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

// Config is the configuration of auto scaler
type Config struct {
	Executor           *Executor
	WindowSlotInterval time.Duration
	WindowSlotNum      int
	JudgeInterval      time.Duration
}

// DefaultConfig is the default configuration of auto scaler
var DefaultConfig = Config{
	Executor:           DefaultExecutor,
	WindowSlotInterval: 1 * time.Minute,
	WindowSlotNum:      15,
	JudgeInterval:      5 * time.Minute,
}

// PendingRequestCache is cache for recording pending request report, key is funcSvcName,
// value is FuncRequestReportMetric slice
var PendingRequestCache sync.Map

// AutoScaler is a cluster-wise worker auto scaler. It receives metric reports from proxies and decides whether a
// function needs to scale up/down. Currently, only scale down is considered.
type AutoScaler struct {
	window   *slideWindow
	fa       FunctionAction
	selector ScaleStrategySelector
	cancel   context.CancelFunc
}

// FuncRequestReportMetric used to record request report metric for func
type FuncRequestReportMetric struct {
	PendingRequests int
	ReportTimestamp int64
}

// FunctionAction is used to retrieve the current status and do some actions to a function. Actions should run FAST
// because they will be in the same goroutine.
type FunctionAction interface {
	FuncSvcName() string
	GetWorkerInstances() (map[string]v1alpha1.WorkerInstance, error)
	GetSpec() (faasfunction.FunctionServiceSpec, error)
	DeleteInstances(instances []v1alpha1.WorkerInstance)
	WillDeleteInstances(instances []v1alpha1.WorkerInstance)
}

// NewAutoScaler creates a cluster-wise worker auto scaler
func NewAutoScaler(fa FunctionAction, selector ScaleStrategySelector) *AutoScaler {
	return NewAutoScalerWithConfig(fa, selector, DefaultConfig)
}

// NewAutoScalerWithConfig creates a cluster-wise worker auto scaler
func NewAutoScalerWithConfig(fa FunctionAction, selector ScaleStrategySelector, cfg Config,
) *AutoScaler {
	as := &AutoScaler{
		window:   newSlideWindow(cfg.Executor, cfg.WindowSlotInterval, cfg.WindowSlotNum),
		fa:       fa,
		selector: selector,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cfg.Executor.Every(ctx, cfg.JudgeInterval, func(ctx context.Context) {
		as.judge(ctx)
	})
	as.cancel = cancel

	return as
}

// AddMetrics -
func (as *AutoScaler) AddMetrics(ctx context.Context, view workermanager.WorkerView) error {
	var adds []slideWindowVisitor
	currentTimestamp := time.Now().UTC().Unix()

	log.GetLogger().Debugf("receive metrics for funcSvc %s, instanceID %s at %d, %+v", as.fa.FuncSvcName(),
		view.InstanceID, currentTimestamp, view.Metrics)
	for _, metric := range view.Metrics {
		adds = append(adds, &slideWindowAdd{
			currentTimestamp: currentTimestamp,
			recordTimestamp:  metric.SecondsTimestamp,
			instanceID:       view.InstanceID,
			val:              metric,
		})
	}
	err := as.window.async(ctx, adds...)
	log.GetLogger().Debugf("add metrics for funcSvc %s, instance %s finished", as.fa.FuncSvcName(), view.InstanceID)
	return err
}

// RemoveWorkerInstanceMetrics -
func (as *AutoScaler) RemoveWorkerInstanceMetrics(ctx context.Context, instanceID string) error {
	if as == nil {
		return nil
	}
	err := as.window.async(ctx, &slideWindowDelete{
		key: instanceID,
	})
	log.GetLogger().Debugf("remove metrics for funcSvc %s, instance %s", as.fa.FuncSvcName(), instanceID)
	return err
}

// Close terminates the auto scaler
func (as *AutoScaler) Close() {
	as.window.close()
	as.cancel()
}

func (as *AutoScaler) judge(ctx context.Context) {
	workers, err := as.fa.GetWorkerInstances()
	if err != nil {
		log.GetLogger().Warnf("failed to get worker instances for funcSvc: %s, %s", as.fa.FuncSvcName(),
			err.Error())
		return
	}
	confirmValidInstance(workers)
	if len(workers) == 0 {
		// Current function has no workers. Do nothing.
		return
	}

	sc, err := newScaleStrategyContext(ctx, as, workers)
	if err != nil {
		log.GetLogger().Warnf("failed to new strategy context for funcSvc: %s, %s", as.fa.FuncSvcName(),
			err.Error())
		return
	}
	strategy := as.selector.SelectStrategy(sc)
	scaleDownWorkers, err := strategy.JudgeScaleDown(sc)
	if err != nil {
		log.GetLogger().Warnf("failed to judge scale down for funcSvc: %s, %s", as.fa.FuncSvcName(), err.Error())
	}
	if etcd3.GetRouterEtcdClient().GetEtcdStatusNow() && len(scaleDownWorkers) != 0 {
		as.fa.WillDeleteInstances(scaleDownWorkers)
		// record to cache
		willdeleteworker.GetCache().AddWillDeleteWorkerInstances(sc.GetFuncSvcName(), scaleDownWorkers)
	}

	deleteNoReportMetricsFunc()
}

func confirmValidInstance(workers map[string]v1alpha1.WorkerInstance) {
	for InstanceID, instance := range workers {
		if instance.InstanceID == "" || reflect.DeepEqual(instance, v1alpha1.WorkerInstance{}) {
			delete(workers, InstanceID)
		}
	}
}

// GetFuncAllPendingRequest get all pending requests for a function
func GetFuncAllPendingRequest(funcSvcName string) int {
	currentTimeStamp := time.Now().Add(-DefaultConfig.JudgeInterval).UTC().Unix()
	requestSum := 0
	if objects, exist := PendingRequestCache.Load(funcSvcName); !exist {
	} else {
		if requestReportMetrics, ok := objects.([]FuncRequestReportMetric); ok {
			unexpiredMetrics := make([]FuncRequestReportMetric, 0)
			for _, metric := range requestReportMetrics {
				if currentTimeStamp-metric.ReportTimestamp < 0 {
					requestSum = requestSum + metric.PendingRequests
					// record unexpired data
					unexpiredMetrics = append(unexpiredMetrics, metric)
				}
			}
			PendingRequestCache.Store(funcSvcName, unexpiredMetrics)
		}
	}
	return requestSum
}

// deleteNoReportMetricsFunc will delete func without request report metrics
func deleteNoReportMetricsFunc() {
	PendingRequestCache.Range(func(key, value interface{}) bool {
		if requestReportMetrics, ok := value.([]FuncRequestReportMetric); ok {
			if len(requestReportMetrics) == 0 {
				PendingRequestCache.Delete(key)
			}
		}
		return true
	})
}
