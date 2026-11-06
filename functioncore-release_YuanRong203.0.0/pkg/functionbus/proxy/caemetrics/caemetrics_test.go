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

package caemetrics

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"functioncore/pkg/common/queue"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/podevent"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
)

func TestGetNodeResourceUsageInstance(t *testing.T) {
	tests := []struct {
		name    string
		wantNil bool
	}{
		{
			"test get node resource",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetCaeMetricsHandler(); (got == nil) != tt.wantNil {
				t.Errorf("GetCaeMetricsHandler() = %v, want %v", got, tt.wantNil)
			}
		})
	}
}

func TestNodeResourceUsage_Store(t *testing.T) {
	n := &Handler{
		resourceMutex: sync.RWMutex{},
		dataLen:       3,
		resources:     map[funcKey]map[podName]List{},
	}

	internal.SaveOwnedInstances("defaultaz-#-podA", "", true)
	internal.SaveOwnedInstances("defaultaz-#-podB", "", true)

	n.Store(common.CaeMetrics{
		FunctionKey:  "funcA",
		PodName:      "podA",
		CPUUsageRate: 1,
	})

	if len(n.resources) != 1 || len(n.resources["funcA"]["podA"]) != 1 {
		t.Errorf("first store Error, resources: %v", n.resources)
	}

	n.Store(common.CaeMetrics{
		FunctionKey:  "funcA",
		PodName:      "podA",
		CPUUsageRate: 2,
	})

	if len(n.resources) != 1 || len(n.resources["funcA"]["podA"]) != 2 {
		t.Errorf("second store Error, resources: %v", n.resources)
	}

	n.Store(common.CaeMetrics{
		FunctionKey:  "funcA",
		PodName:      "podA",
		CPUUsageRate: 3,
	})

	if len(n.resources) != 1 || len(n.resources["funcA"]["podA"]) != 3 {
		t.Errorf("third store Error, resources: %v", n.resources)
	}

	n.Store(common.CaeMetrics{
		FunctionKey:  "funcA",
		PodName:      "podA",
		CPUUsageRate: 4,
	})

	if len(n.resources) != 1 || len(n.resources["funcA"]["podA"]) != 3 {
		t.Errorf("fourth store Error, resources: %v", n.resources)
	}
	resourceItem := []Metrics(n.resources["funcA"]["podA"])[2]
	if resourceItem.CPUUsageRate != 4 {
		t.Errorf("fourth store cpuusage error, resources: %v", n.resources)
	}

	n.Store(common.CaeMetrics{
		FunctionKey:  "funcA",
		PodName:      "podB",
		CPUUsageRate: 5,
	})
	if len(n.resources) != 1 || len(n.resources["funcA"]) != 2 {
		t.Errorf("fifth store cpuusage error, resources: %v", n.resources)
	}

	internal.DeleteOwnedInstances("defaultaz-#-podA")

	n.Store(common.CaeMetrics{
		FunctionKey:  "funcA",
		PodName:      "podA",
		CPUUsageRate: 10,
	})
	fmt.Println(n.resources)
	if len(n.resources) != 1 || n.resources["funcA"]["podA"][len(n.resources["funcA"]["podA"])-1].CPUUsageRate == 10 {
		t.Errorf("fifth store cpuusage error, resources: %v", n.resources)
	}
}

func TestNodeResourceUsage_StoreWithProperties(t *testing.T) {
	n := &Handler{
		resourceMutex: sync.RWMutex{},
		dataLen:       3,
		resources:     map[funcKey]map[podName]List{},
	}

	internal.SaveOwnedInstances("defaultaz-#-podA", "", true)

	n.Store(common.CaeMetrics{
		FunctionKey:  "funcA",
		PodName:      "podA",
		CPUUsageRate: 1,
		Properties:   map[string]string{"key": "value"},
	})

	if len(n.resources) != 1 || len(n.resources["funcA"]["podA"]) != 1 || n.resources["funcA"]["podA"][0].Properties["key"] != "value" {
		t.Errorf("first store Error, resources: %v", n.resources)
	}
}

func TestResourceList_getLatestDataTime(t *testing.T) {
	rl := List{}
	rTime := rl.getLatestDataTime()
	if rTime != 0 || len(rl) != 0 {
		t.Errorf("first get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, Metrics{ReportTime: 100})
	rTime = rl.getLatestDataTime()
	if rTime != 100 || len(rl) != 1 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, Metrics{ReportTime: 130})
	rTime = rl.getLatestDataTime()
	if rTime != 130 || len(rl) != 2 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, Metrics{ReportTime: 150})
	rTime = rl.getLatestDataTime()
	if rTime != 150 || len(rl) != 3 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, Metrics{ReportTime: 170})
	rTime = rl.getLatestDataTime()
	if rTime != 170 || len(rl) != 3 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}
}

func TestNodeResourceUsage_RemoveByInstance(t *testing.T) {
	n := &Handler{
		resourceMutex: sync.RWMutex{},
	}
	resource := make(map[funcKey]map[podName]List)
	podMapA := make(map[podName]List)
	podMapA["podA"] = List{}
	podMapA["podB"] = List{}
	podMapB := make(map[podName]List)
	podMapB["podC"] = List{}
	podMapB["podD"] = List{}
	resource["funcA"] = podMapA
	resource["funcB"] = podMapB
	n.resources = resource

	n.RemoveByInstance("funcA", "defaultaz-#-podA")
	if len(n.resources["funcA"]) != 1 {
		t.Errorf("first delete remove by funcKey Error, resources: %v", n.resources)
	}

	n.RemoveByInstance("funcA", "defaultaz-#-podB")
	if len(n.resources["funcA"]) != 0 || len(n.resources) != 1 {
		t.Errorf("send delete remove by funcKey Error, resources: %v", n.resources)
	}

	n.RemoveByInstance("funcB", "defaultaz-#-podE")
	if len(n.resources["funcB"]) != 2 || len(n.resources) != 1 {
		t.Errorf("third delete remove by funcKey Error, resources: %v", n.resources)
	}

}

func TestNodeResourceUsage_RegisterReportCallBack(t *testing.T) {
	type args struct {
		callback func(c *redisclient.Client, caeMetricsBytes []byte)
	}
	tests := []struct {
		name    string
		args    args
		wantNil bool
	}{
		{
			"test register call back",
			args{
				func(c *redisclient.Client, caeMetricsBytes []byte) {
					return
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Handler{}
			if got := r.RegisterReportCallBack(tt.args.callback); (got.uploadCallBack == nil) != tt.wantNil {
				t.Errorf("RegisterReportCallBack() = %v, wantNil %v", got, tt.wantNil)
			}
		})
	}
}

func TestNodeResourceUsage_RemoveByFuncKey(t *testing.T) {
	n := &Handler{
		resourceMutex: sync.RWMutex{},
	}
	resource := make(map[funcKey]map[podName]List)
	podMapA := make(map[podName]List)
	podMapB := make(map[podName]List)
	resource["funcA"] = podMapA
	resource["funcB"] = podMapB
	n.resources = resource

	n.RemoveByFuncKey("funcA")
	if len(n.resources) != 1 {
		t.Errorf("first delete remove by funcKey Error, resources: %v", n.resources)
	}

	n.RemoveByFuncKey("funcA")
	if len(n.resources) != 1 {
		t.Errorf("second delete remove by funcKey Error, resources: %v", n.resources)
	}

	n.RemoveByFuncKey("funcB")
	if len(n.resources) != 0 {
		t.Errorf("third delete remove by funcKey Error, resources: %v", n.resources)
	}
}

func TestNodeResourceUsage_WithDataLen(t *testing.T) {
	n := &Handler{}
	n.WithDataLen(5)
	if n.dataLen != 5 {
		t.Errorf("change data len error, resources: %v", n.resources)
	}

	n.WithDataLen(6)
	if n.dataLen != 6 {
		t.Errorf("third change data len error, resources: %v", n.resources)
	}
}

func TestNodeResourceUsage_WithMetricsClient(t *testing.T) {
	n := &Handler{}
	n.WithMetricsClient(&redisclient.Client{})
	if n.metricsClient == nil {
		t.Errorf("change metrics client error, resources: %v", n.resources)
	}
}

func TestNodeResourceUsage_WithStopCh(t *testing.T) {
	n := &Handler{}
	n.WithStopCh(make(chan struct{}))
	if n.stopCh == nil {
		t.Errorf("change stop chan error, resources: %v", n.resources)
	}
}

func TestNodeResourceUsage_WithTickerTime(t *testing.T) {
	n := &Handler{}
	n.WithTickerTime(10)
	if n.tickerTime != 10 {
		t.Errorf("change ticker time error, resources: %v", n.resources)
	}
}

func TestNodeResourceUsage_getResource(t *testing.T) {
	n := &Handler{
		resources:     make(map[funcKey]map[podName]List),
		resourceMutex: sync.RWMutex{},
	}

	resource := n.getResource()
	if resource == nil {
		t.Errorf("change metrics client error, resources: %v", n.resources)
	}
}

func TestNodeResourceUsage_storeResourceUsageToRedis(t *testing.T) {
	type fields struct {
		resources      map[funcKey]map[podName]List
		resourceMutex  sync.RWMutex
		tickerTime     int64
		dataLen        int64
		stopCh         chan struct{}
		metricsClient  *redisclient.Client
		uploadCallBack func(c *redisclient.Client, caeMetricsBytes []byte)
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			"test store to redis with wrong tick",
			fields{
				resources:     map[funcKey]map[podName]List{},
				resourceMutex: sync.RWMutex{},
				tickerTime:    0,
			},
		},
		{
			"test store to redis with wrong stopChan",
			fields{
				resources:     map[funcKey]map[podName]List{},
				resourceMutex: sync.RWMutex{},
				tickerTime:    10,
				stopCh:        nil,
			},
		},
		{
			"test store to redis with all line",
			fields{
				resources:     map[funcKey]map[podName]List{},
				resourceMutex: sync.RWMutex{},
				tickerTime:    1,
				stopCh:        make(chan struct{}),
				uploadCallBack: func(c *redisclient.Client, caeMetricsBytes []byte) {
					return
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Handler{
				resources:       tt.fields.resources,
				resourceMutex:   tt.fields.resourceMutex,
				tickerTime:      tt.fields.tickerTime,
				dataLen:         tt.fields.dataLen,
				stopCh:          tt.fields.stopCh,
				metricsClient:   tt.fields.metricsClient,
				uploadCallBack:  tt.fields.uploadCallBack,
				podErrInfoQueue: queue.NewFifoQueue(),
			}
			if tt.fields.stopCh != nil {
				go func() {
					time.Sleep(2 * time.Second)
					tt.fields.stopCh <- struct{}{}
				}()
			}
			r.storeCaeMetricsToRedis()
		})
	}
}

func Test_transformToFunctionKey(t *testing.T) {
	type args struct {
		funcUrnWithVersion string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "1",
			args: args{
				funcUrnWithVersion: "sn:cn:yr:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@default@shy4:latest",
			},
			want: "7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/0@default@shy4/latest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transformToFunctionKey(tt.args.funcUrnWithVersion); got != tt.want {
				t.Errorf("transformToFunctionKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandler_storeErrPodMetrics(t *testing.T) {
	n := &Handler{
		resources:       make(map[funcKey]map[podName]List),
		resourceMutex:   sync.RWMutex{},
		queueMutex:      sync.RWMutex{},
		podErrInfoQueue: queue.NewFifoQueue(),
		dataLen:         2,
	}
	internal.SaveOwnedInstances("defaultaz-#-podA", "", true)
	internal.SaveOwnedInstances("defaultaz-#-podB", "", true)
	n.PushPodErrMetrics("sn:cn:yr:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@default@a:latest", podevent.EventInfo{
		PodName: "podA",
	})
	n.PushPodErrMetrics("sn:cn:yr:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@default@b:latest", podevent.EventInfo{
		PodName: "podB",
	})
	pairList := n.storeErrPodMetrics()
	if pairList[0].podName != "podA" || pairList[1].podName != "podB" {
		t.Errorf("pairList = %v not match", pairList)
	}
}
