/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package instance -
package instance

import (
	"net/http"
	"sync"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/workermanagerutils"
)

const (
	defaultPoolLen = 5
	maxPoolLen     = 100
)

// Job -
type Job struct {
	funcSvc         string
	workerManagerIP string
}

// QueryPool -
type QueryPool struct {
	// key : funcSvc -> cluster -> data
	instanceData     map[string]map[string]types.QueryInstanceResponse
	instanceDataLock sync.RWMutex
	poolLen          int
	wg               sync.WaitGroup
}

func createQueryPool() *QueryPool {
	return &QueryPool{
		instanceData:     make(map[string]map[string]types.QueryInstanceResponse),
		instanceDataLock: sync.RWMutex{},
		poolLen:          defaultPoolLen,
		wg:               sync.WaitGroup{},
	}
}

// WithPoolLen -
func (q *QueryPool) WithPoolLen(poolLen int) *QueryPool {
	if poolLen <= 0 || poolLen > maxPoolLen {
		log.GetLogger().Warnf("newPoolLen not right, newPooLen is %d", poolLen)
		return q
	}
	log.GetLogger().Debugf("withPoolLen: %d", poolLen)
	q.poolLen = poolLen
	return q
}

// StartWorking -
func (q *QueryPool) StartWorking() {
	log.GetLogger().Debugf("start working, poolLen is %d", q.poolLen)
	jobs := make(chan Job, q.poolLen)

	for i := 0; i < q.poolLen; i++ {
		q.wg.Add(1)
		go q.workerOfQueryInstance(i, jobs)
	}

	cache.GetInstance().FunctionMetaCache.Range(func(key, value interface{}) bool {
		funcSvc, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to string: %v", value)
			return true
		}
		for _, cluster := range clustermgr.GetClusterMgr().GetAllAvailableCluster() {
			jobs <- Job{
				funcSvc:         funcSvc,
				workerManagerIP: cluster.ActiveNodes.IP,
			}
			log.GetLogger().Debugf("job pushed, funcSvc is %s, ip is %s", funcSvc, cluster.ActiveNodes.IP)
		}
		return true
	})

	// make sure the for range in goroutine of WorkerOfQueryInstance finished, avoid the goroutine leaks
	close(jobs)
	// wait all data refresh job finished
	q.wg.Wait()
	// handle all data
	q.updateDataToCache()
}

func (q *QueryPool) workerOfQueryInstance(workerID int, jobs <-chan Job) {
	log.GetLogger().Debugf("workerOfQueryInstance created, workerID is %d", workerID)
	defer q.wg.Done()

	for job := range jobs {
		log.GetLogger().Debugf("query start, workerID: %d, job: %+v", workerID, job)
		// 1. send request
		queryInsResp, err := workermanagerutils.QueryInstancesFromWmByIP(job.funcSvc, job.workerManagerIP,
			config.EnableHTTPS(), config.GetLocalAuth())
		if err != nil {
			log.GetLogger().Errorf("failed to query funcInsCache for worker manager node: %s, "+
				"funcSvcID: %s,workerID: %d, error: %s", job.workerManagerIP, job.funcSvc, workerID, err.Error())
			continue
		}
		log.GetLogger().Debugf("funcSvcID: %s, query instance response: %+v", job.funcSvc, queryInsResp)
		if queryInsResp.Code != http.StatusOK {
			log.GetLogger().Errorf("code of queryInstanceResponse is not OK "+
				"for worker manager node: %s, workerID: %d, funcSvcID: %s, code: %s, message: %s",
				job.workerManagerIP, workerID, job.funcSvc, queryInsResp.Code, queryInsResp.Message)
			continue
		}

		log.GetLogger().Debugf("query finished, workerID: %d, job: %+v, queryInsResp: %+v",
			workerID, job, queryInsResp)

		q.addDataToInstanceData(job.funcSvc, job.workerManagerIP, queryInsResp)
	}
}

func (q *QueryPool) addDataToInstanceData(funcSvc, workerManagerIP string, queryResponse types.QueryInstanceResponse) {
	q.instanceDataLock.Lock()
	defer q.instanceDataLock.Unlock()

	if _, exist := q.instanceData[funcSvc]; !exist {
		q.instanceData[funcSvc] = make(map[string]types.QueryInstanceResponse)
	}

	q.instanceData[funcSvc][workerManagerIP] = queryResponse
}

func (q *QueryPool) updateDataToCache() {
	q.instanceDataLock.RLock()
	defer q.instanceDataLock.RUnlock()

	for funcSvc, clusterData := range q.instanceData {
		totalCount, minInsCount, snapshotCount, scalingNum, insCache := 0, 0, 0, int64(0), &config.InstanceCache{}

		for _, queryInsResp := range clusterData {
			clusterInsCache := config.ClusterInsCache{}
			clusterInsCache.ClusterID = queryInsResp.Instances.ClusterID

			clusterInsCache.ScalingNum = queryInsResp.Instances.ScalingNum
			scalingNum += clusterInsCache.ScalingNum

			clusterInsCache.Count = len(queryInsResp.Instances.PodsName)
			totalCount += clusterInsCache.Count
			minInsCount += queryInsResp.Instances.ExpectedMinIns
			snapshotCount += queryInsResp.Instances.ExpectedSnap
			insCache.ClusterInsCache = append(insCache.ClusterInsCache, clusterInsCache)
		}

		insCache.ScalingNum = scalingNum
		insCache.TotalCurrentCount = int64(totalCount)
		insCache.ExpectedMinIns = int64(minInsCount)
		insCache.ExpectedSnapshot = int64(snapshotCount)
		log.GetLogger().Debugf("func %s store data to function instance cache, %+v", funcSvc, insCache)
		cache.GetInstance().FunctionInstanceCache.Store(funcSvc, insCache)
	}

}
