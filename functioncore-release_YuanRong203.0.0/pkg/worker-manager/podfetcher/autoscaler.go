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

package podfetcher

import (
	"context"
	"fmt"
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/autoscaler/strategy"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

type autoScalerAction struct {
	funcSvcName string
	functionKey string
}

// FuncSvcName implements autoscaler.FunctionAction
func (a *autoScalerAction) FuncSvcName() string {
	return a.funcSvcName
}

// GetWorkerInstances implements autoscaler.FunctionAction
func (a *autoScalerAction) GetWorkerInstances() (map[string]v1alpha1.WorkerInstance, error) {
	workerIns, _, _ := cache.WorkerCache.GetInstanceMap(a.funcSvcName)
	return workerIns, nil
}

// GetSpec implements autoscaler.FunctionAction
func (a *autoScalerAction) GetSpec() (faasfunction.FunctionServiceSpec, error) {
	fs := cache.WorkerCache.GetFuncMeta(a.funcSvcName)
	if fs == nil {
		log.GetLogger().Errorf("failed to get function meta from cache, funcSvcName: %s", a.funcSvcName)
		return faasfunction.FunctionServiceSpec{}, fmt.Errorf("function meta is nil")
	}
	return fs.Spec, nil
}

// DeleteInstances implements autoscaler.FunctionAction
func (a *autoScalerAction) DeleteInstances(workers []v1alpha1.WorkerInstance) {
	for _, worker := range workers {
		log.GetLogger().Infof("scale down worker name: %s, applier: %s, funcSvc: %s", worker.InstanceID,
			worker.Applier, a.funcSvcName)
	}
	if config.EnableRadicalAutoscaler() {
		a.radicalDelete(workers)
	} else {
		a.normalDelete(workers)
	}
}

func (a *autoScalerAction) WillDeleteInstances(workers []v1alpha1.WorkerInstance) {
	for _, worker := range workers {
		log.GetLogger().Infof("worker %s will be tagged and will be deleted , applier: %s, funcSvc: %s, "+
			"podName: %s", worker.InstanceID, worker.Applier, a.funcSvcName, worker.PodName)
	}
	a.tagWillDelete(workers)
}

func (a *autoScalerAction) tagWillDelete(workers []v1alpha1.WorkerInstance) {
	go func() {
		for _, worker := range workers {
			worker.WillDelete = true
			etcdhandler.PutInstanceToETCD(context.Background(), worker)
		}
	}()
}

func (a *autoScalerAction) normalDelete(workers []v1alpha1.WorkerInstance) {
	go func() {
		for _, worker := range workers {
			GetPodFetcher().DeletePodByWorkerInstance(a.funcSvcName, utils.FunctionNameSpace(), "autoscaler", worker)
		}
	}()
}

func (a *autoScalerAction) radicalDelete(workers []v1alpha1.WorkerInstance) {
	cache.WorkerCache.HandleInstanceAutoScaleDown(a.funcSvcName, &cache.AutoScaleDownMeta{Instances: workers})

	for i := range workers {
		go func(worker v1alpha1.WorkerInstance) {
			etcdhandler.DeleteInstanceFromETCD(context.Background(), worker)
			GetPodFetcher().DeletePodByWorkerInstance(a.funcSvcName, utils.FunctionNameSpace(), "autoscaler", worker)
		}(workers[i])
	}
}

// AddAutoScalerMetrics -
func AddAutoScalerMetrics(ctx context.Context, view workermanager.WorkerView) {
	funcSvcName := utils.CrName(view.BusinessID, view.TenantID, view.FuncName, view.FuncVersion)
	as, ok := cache.GetOrCreateAutoScaler(funcSvcName, func() *autoscaler.AutoScaler {
		functionKey := utils.FunctionKey(view.BusinessID, view.TenantID, view.FuncName, view.FuncVersion)
		return autoscaler.NewAutoScaler(&autoScalerAction{funcSvcName: funcSvcName, functionKey: functionKey},
			strategy.NewSelector())
	})
	if !ok {
		log.GetLogger().Warnf("failed to get or create autoscaler for funcSvcName: %s", funcSvcName)
		return
	}

	as.AddMetrics(ctx, view)
}

// AddPendingRequestMetrics is to store pending request metrics
func AddPendingRequestMetrics(view workermanager.PendingRequestView) {
	funcSvcName := utils.CrName(view.BusinessID, view.TenantID, view.FuncName, view.FuncVersion)
	// add request report metric
	currentTimestamp := time.Now().Add(-autoscaler.DefaultConfig.JudgeInterval).UTC().Unix()
	// timeout, no record
	if currentTimestamp-view.ReportTimestamp > 0 {
		return
	}
	metrics := make([]autoscaler.FuncRequestReportMetric, 0)
	if objects, exist := autoscaler.PendingRequestCache.Load(funcSvcName); exist {
		if requestMetrics, ok := objects.([]autoscaler.FuncRequestReportMetric); ok {
			requestMetrics = append(requestMetrics, autoscaler.FuncRequestReportMetric{
				PendingRequests: view.PendingRequests,
				ReportTimestamp: view.ReportTimestamp,
			})
			metrics = requestMetrics
		}
	} else {
		metrics = append(metrics, autoscaler.FuncRequestReportMetric{
			PendingRequests: view.PendingRequests,
			ReportTimestamp: view.ReportTimestamp,
		})
	}
	if len(metrics) != 0 {
		autoscaler.PendingRequestCache.Store(funcSvcName, metrics)
		log.GetLogger().Debugf("succeed to add pending request metric, funcName %s, businessID %s, tenantID %s, "+
			"funcVersion %s, pendingRequest %d, reportTimestamp %d", view.FuncName, view.BusinessID, view.TenantID,
			view.FuncVersion, view.PendingRequests, view.ReportTimestamp)
	}
}
