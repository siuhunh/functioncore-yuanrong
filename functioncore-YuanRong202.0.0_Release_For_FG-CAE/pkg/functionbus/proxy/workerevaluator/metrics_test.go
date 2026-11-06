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

package workerevaluator

import (
	"testing"
	"time"
)

func TestWorkerEvaluator_AddMetrics(t *testing.T) {
	setup()
	defer tearDown()

	GetInstance().getOrAddWorkerMetrics("testA-instance-001", WithFunctionKeyWithResource("testA"), WithNodeIP("127.0.0.1"), WithLocalWorker(false))
	var metrics *WorkerMetrics
	if metrics = GetInstance().getWorkerMetrics("testA-instance-001"); metrics == nil {
		t.Errorf("AddMetrics failed")
		return
	}
	if metrics.FuncKeyWithResource != "testA" || metrics.ID != "testA-instance-001" {
		t.Error("AddMetrics failed, value error")
		return
	}
	GetInstance().DeleteWorkerMetrics("testA-instance-001", "testA")
}

func TestWorkerEvaluator_DeleteMetrics(t *testing.T) {
	setup()
	defer tearDown()

	workerID := "testA-instance-001"
	functionKey := "testA"

	GetInstance().DeleteWorkerMetrics(workerID, functionKey)
	GetInstance().getOrAddWorkerMetrics("testA-instance-001", WithFunctionKeyWithResource(functionKey), WithNodeIP("127.0.0.1"), WithLocalWorker(false))
	GetInstance().AddWorker(workerID, WithFunctionKeyWithResource(functionKey), WithNodeIP("127.0.0.1"), WithLocalWorker(false))
	metrics := GetInstance().getWorkerMetrics(workerID)
	t.Log("add workerID", metrics.ID)
	if metrics == nil {
		t.Errorf("add workerID:%s metrics failed", workerID)
		return
	}

	GetInstance().DeleteWorkerMetrics(workerID, functionKey)
	time.Sleep(100 * time.Millisecond)
	metrics = GetInstance().getWorkerMetrics(workerID)
	if metrics != nil {
		t.Errorf("delete workerID:%s metrics failed", workerID)
		return
	}
}
