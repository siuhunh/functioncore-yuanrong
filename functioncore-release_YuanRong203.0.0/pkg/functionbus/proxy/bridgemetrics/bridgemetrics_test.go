package bridgemetrics

import (
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	testutils "functioncore/test/utils"
)

func TestGetBridgeMetricsHandler(t *testing.T) {
	tests := []struct {
		name    string
		wantNil bool
	}{
		{
			"test get and crate bridge metrics handler",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetBridgeMetricsHandler(); (got == nil) != tt.wantNil {
				t.Errorf("GetBridgeMetricsHandler() = %v, want %v", got, tt.wantNil)
			}
		})
	}
}

func TestHandler_RegisterReportCallBack(t *testing.T) {
	type args struct {
		callback func(c *redisclient.Client, bridgeMetricsBytes []byte)
	}
	tests := []struct {
		name    string
		args    args
		wantNil bool
	}{
		{
			"test register call back ok",
			args{
				func(c *redisclient.Client, caeMetricsBytes []byte) {
					return
				},
			},
			false,
		},
		{
			"test register call back nil",
			args{},
			true,
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

func TestHandler_RemoveByFuncKey(t *testing.T) {
	n := &Handler{
		resourceMutex: sync.RWMutex{},
	}
	resource := make(map[funcKey]map[podName]List)
	podMapA := make(map[podName]List)
	podMapB := make(map[podName]List)
	resource["funcA"] = podMapA
	resource["funcB"] = podMapB
	n.metrics = resource

	n.RemoveByFuncKey("funcKeyNotExist")
	if len(n.metrics) != 2 {
		t.Errorf("delete not exit funcKey by funcKey Error, metrics: %v", n.metrics)
	}

	n.RemoveByFuncKey("funcA")
	if len(n.metrics) != 1 {
		t.Errorf("first delete remove by funcKey Error, metrics: %v", n.metrics)
	}

	n.RemoveByFuncKey("funcA")
	if len(n.metrics) != 1 {
		t.Errorf("second delete remove by funcKey Error, metrics: %v", n.metrics)
	}

	n.RemoveByFuncKey("funcB")
	if len(n.metrics) != 0 {
		t.Errorf("third delete remove by funcKey Error, metrics: %v", n.metrics)
	}
}

func TestHandler_RemoveByInstance(t *testing.T) {
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
	n.metrics = resource

	n.RemoveByInstance("funcA", "defaultaz-#-podA")
	if len(n.metrics["funcA"]) != 1 || len(n.metrics) != 2 {
		t.Errorf("first delete remove by instance Error, metrics: %v", n.metrics)
	}

	n.RemoveByInstance("funcA", "defaultaz-#-podB")
	if len(n.metrics["funcA"]) != 0 || len(n.metrics) != 1 {
		t.Errorf("send delete remove by instance Error, metrics: %v", n.metrics)
	}

	n.RemoveByInstance("funcB", "defaultaz-#-podNotExist")
	if len(n.metrics["funcB"]) != 2 || len(n.metrics) != 1 {
		t.Errorf("third delete remove by instance Error, metrics: %v", n.metrics)
	}
}

func TestHandler_Store(t *testing.T) {
	convey.Convey("test not owned instance", t, func() {
		patches := testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(internal.IsOwnedInstancesExist, func(instanceID string) bool {
				if instanceID == "defaultaz-#-bridgePodName" {
					return false
				}
				return true
			}),
		})
		handler := &Handler{
			resourceMutex: sync.RWMutex{},
			dataLen:       3,
			metrics:       map[funcKey]map[podName]List{},
		}
		handler.Store(common.BridgeFuncMetrics{
			PodName: "bridgePodName",
		})
		convey.So(len(handler.metrics), convey.ShouldEqual, 0)
	})
	convey.Convey("test function metrics store success", t, func() {
		patches := testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(internal.IsOwnedInstancesExist, func(instanceID string) bool {
				return instanceID == "defaultaz-#-bridgePodName" ||
					instanceID == "defaultaz-#-bridgePodName-1" ||
					instanceID == "defaultaz-#-bridgePodName-2"
			}),
		})
		handler := &Handler{
			resourceMutex: sync.RWMutex{},
			dataLen:       3,
			metrics:       map[funcKey]map[podName]List{},
		}
		bridgeMetrics := common.BridgeFuncMetrics{
			PodName:  "bridgePodName",
			Package:  "default",
			FuncName: "bridgeName",
			TenantID: "c53626012ba84727b938ca8bf03108ef",
			Version:  "latest",
		}
		// case 1: first store, this function metrics data is empty in this node
		handler.Store(bridgeMetrics)
		convey.So(len(handler.metrics), convey.ShouldEqual, 1)
		podMap := handler.metrics[("c53626012ba84727b938ca8bf03108ef/0@default@bridgeName/latest")]
		convey.So(len(podMap), convey.ShouldEqual, 1)
		bridgeMetricsList := podMap["bridgePodName"]
		convey.So(len(bridgeMetricsList), convey.ShouldEqual, 1)

		// case 2: second store, this function metrics data not empty in this node, the same pod name
		handler.Store(bridgeMetrics)
		convey.So(len(handler.metrics), convey.ShouldEqual, 1)
		podMap = handler.metrics[("c53626012ba84727b938ca8bf03108ef/0@default@bridgeName/latest")]
		convey.So(len(podMap), convey.ShouldEqual, 1)
		bridgeMetricsList = podMap["bridgePodName"]
		convey.So(len(bridgeMetricsList), convey.ShouldEqual, 2)

		// case 3: the same pod name store several times, length of bridgeMetricsList should be h.dataLen
		for i := 0; i < 10; i++ {
			handler.Store(bridgeMetrics)
		}
		convey.So(len(handler.metrics), convey.ShouldEqual, 1)
		podMap = handler.metrics[("c53626012ba84727b938ca8bf03108ef/0@default@bridgeName/latest")]
		convey.So(len(podMap), convey.ShouldEqual, 1)
		bridgeMetricsList = podMap["bridgePodName"]
		convey.So(len(bridgeMetricsList), convey.ShouldEqual, handler.dataLen)

		// case 4: the function exist, but pod not exist
		handler.Store(common.BridgeFuncMetrics{
			PodName:  "bridgePodName-1",
			Package:  "default",
			FuncName: "bridgeName",
			TenantID: "c53626012ba84727b938ca8bf03108ef",
			Version:  "latest",
		})
		convey.So(len(handler.metrics), convey.ShouldEqual, 1)
		podMap = handler.metrics[("c53626012ba84727b938ca8bf03108ef/0@default@bridgeName/latest")]
		convey.So(len(podMap), convey.ShouldEqual, 2)
		bridgeMetricsList = podMap["bridgePodName-1"]
		convey.So(len(bridgeMetricsList), convey.ShouldEqual, 1)

		// case 5: store another function
		handler.Store(common.BridgeFuncMetrics{
			PodName:  "bridgePodName-2",
			Package:  "autotest",
			FuncName: "bridgeName-1",
			TenantID: "c53626012ba84727b938ca8bf03108ef",
			Version:  "latest",
		})
		convey.So(len(handler.metrics), convey.ShouldEqual, 2)
		podMap = handler.metrics[("c53626012ba84727b938ca8bf03108ef/0@autotest@bridgeName-1/latest")]
		convey.So(len(podMap), convey.ShouldEqual, 1)
		bridgeMetricsList = podMap["bridgePodName-2"]
		convey.So(len(bridgeMetricsList), convey.ShouldEqual, 1)
	})
}

func TestHandler_WithDataLen(t *testing.T) {
	handler := &Handler{
		dataLen: defaultDataLen,
	}

	handler.WithDataLen(0)
	if handler.dataLen != defaultDataLen {
		t.Errorf("change length with 0 error, handler.dataLen: %d", handler.dataLen)
	}

	handler.WithDataLen(5)
	if handler.dataLen != 5 {
		t.Errorf("change data len larger than 0 error, handler.dataLen: %d", handler.dataLen)
	}

	handler.WithDataLen(-1)
	if handler.dataLen != 5 {
		t.Errorf("change data len smaller than 0 error, handler.dataLen: %d", handler.dataLen)
	}
}

func TestHandler_WithMetricsClient(t *testing.T) {
	tests := []struct {
		name    string
		args    *redisclient.Client
		wantNil bool
	}{
		{
			name:    "test redis client not nil",
			args:    &redisclient.Client{},
			wantNil: false,
		},
		{
			name:    "test redis client nil",
			args:    nil,
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{}
			h.WithMetricsClient(tt.args)
			if got := h.metricsClient; (got == nil) != tt.wantNil {
				t.Errorf("WithMetricsClient() = %v, want %v", got, tt.wantNil)
			}
		})
	}
}

func TestHandler_WithTickerTime(t *testing.T) {
	handler := &Handler{
		tickerTime: defaultTick,
	}

	handler.WithTickerTime(0)
	if handler.tickerTime != defaultTick {
		t.Errorf("change ticker timer with 0 error, handler.tickerTime: %d", handler.tickerTime)
	}

	handler.WithTickerTime(5)
	if handler.tickerTime != 5 {
		t.Errorf("change ticker timer larger than 0 error, handler.tickerTime: %d", handler.tickerTime)
	}

	handler.WithTickerTime(-1)
	if handler.tickerTime != 5 {
		t.Errorf("change ticker timer smaller than 0 error, handler.tickerTime: %d", handler.tickerTime)
	}
}

func TestHandler_storeBridgeMetricsToRedis(t *testing.T) {
	isUploadCallBackCalled := false
	type fields struct {
		resources      map[funcKey]map[podName]List
		resourceMutex  sync.RWMutex
		tickerTime     int64
		dataLen        int64
		stopCh         chan struct{}
		metricsClient  *redisclient.Client
		uploadCallBack func(c *redisclient.Client, caeMetricsBytes []byte)
	}
	resources := make(map[funcKey]map[podName]List, 2)
	bridgeMetricsList := List{}
	newPodMap := make(map[podName]List)
	newPodMap["podName"] = bridgeMetricsList
	resources["funcKey"] = newPodMap
	tests := []struct {
		name                   string
		fields                 fields
		isUploadCallBackCalled bool
	}{
		{
			"test store to redis with wrong tick",
			fields{
				resources:     resources,
				resourceMutex: sync.RWMutex{},
				tickerTime:    0,
				uploadCallBack: func(c *redisclient.Client, caeMetricsBytes []byte) {
					isUploadCallBackCalled = false
					return
				},
			},
			false,
		},
		{
			"test store to redis with wrong stopChan",
			fields{
				resources:     resources,
				resourceMutex: sync.RWMutex{},
				tickerTime:    10,
				stopCh:        nil,
				uploadCallBack: func(c *redisclient.Client, caeMetricsBytes []byte) {
					isUploadCallBackCalled = false
					return
				},
			},
			false,
		},
		{
			"test store to redis with all line",
			fields{
				resources:     resources,
				resourceMutex: sync.RWMutex{},
				tickerTime:    1,
				stopCh:        make(chan struct{}),
				uploadCallBack: func(c *redisclient.Client, caeMetricsBytes []byte) {
					isUploadCallBackCalled = true
					return
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Handler{
				metrics:        tt.fields.resources,
				resourceMutex:  tt.fields.resourceMutex,
				tickerTime:     tt.fields.tickerTime,
				dataLen:        tt.fields.dataLen,
				stopCh:         tt.fields.stopCh,
				metricsClient:  tt.fields.metricsClient,
				uploadCallBack: tt.fields.uploadCallBack,
			}

			if tt.fields.stopCh != nil {
				go func() {
					time.Sleep(2 * time.Second)
					tt.fields.stopCh <- struct{}{}
				}()
			}
			isUploadCallBackCalled = false
			r.storeBridgeMetricsToRedis()
			if tt.isUploadCallBackCalled != isUploadCallBackCalled {
				t.Errorf("storeBridgeMetricsToRedis error, wantCount: %t, count: %t", tt.isUploadCallBackCalled, isUploadCallBackCalled)
			}
		})
	}
}

func TestList_getLatestDataTime(t *testing.T) {
	rl := List{}
	rTime := rl.getLatestDataTime()
	if rTime != 0 || len(rl) != 0 {
		t.Errorf("first get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, common.BridgeFuncMetrics{TimeStamp: 100})
	rTime = rl.getLatestDataTime()
	if rTime != 100 || len(rl) != 1 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, common.BridgeFuncMetrics{TimeStamp: 130})
	rTime = rl.getLatestDataTime()
	if rTime != 130 || len(rl) != 2 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, common.BridgeFuncMetrics{TimeStamp: 150})
	rTime = rl.getLatestDataTime()
	if rTime != 150 || len(rl) != 3 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}

	rl.storeDataWithDataLen(3, common.BridgeFuncMetrics{TimeStamp: 170})
	rTime = rl.getLatestDataTime()
	if rTime != 170 || len(rl) != 3 {
		t.Errorf("second get latestDatTime error: %v", rl)
	}
}
