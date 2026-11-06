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

package runtime

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/tcpbustool"
	buscommon "functioncore/pkg/functionbus/common"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
)

const (
	convertToCPU = 10000000
	// case: 100m->0.1u
	cpuDivisor                = 1000
	uint64size                = 8
	totalSize                 = uint64size + uint64size
	byteToMB                  = 1024 * 1024
	defaultLogMetricsTick     = 60
	workerCPULimitKey         = "WORKER_CPU_LIMIT"
	workerMemoryLimitKey      = "WORKER_MEMORY_LIMIT"
	defaultReportTimeout      = 100 * time.Millisecond
	defaultSuicideTimeout     = 30
	defaultRuntimeMetricsTick = 2 * time.Millisecond
	metricsFile               = "/home/sn/metrics/metrics"

	defaultFileSystemCapacity   = 512   // 512Mb
	expansionFileSystemCapacity = 10240 // 10Gb
	reportTickerTime            = 1 * time.Minute
	fullFuncNameSegLens         = 3
	funcNameIdx                 = 2
	packageIdx                  = 1
	bridgeTypeIdx               = 2
	recordCPUInfoSleepTime      = 10 * time.Millisecond
)

const (
	usedLimitRate             = 1000
	millPerSecond             = 1000
	millisecondsToNanoseconds = 1000000
	executionTimePrecision    = 3
)

var (
	logMetricsOnce              sync.Once
	reportResourceUsageOnce     sync.Once
	reportCaeMetricsOnce        sync.Once
	reportBridgeFuncMetricsOnce sync.Once
	suicideTimeout              = defaultSuicideTimeout * time.Minute
	properties                  map[string]string
	startTime                   int64
)

func parseLogMetricsTick(tickStr string) int64 {
	if val, err := strconv.Atoi(tickStr); err == nil {
		return int64(val)
	}
	return defaultLogMetricsTick
}

// LogMetrics logs metrics info
func LogMetrics(tickStr string) {
	logMetricsOnce.Do(func() {
		go logMetrics(time.Duration(parseLogMetricsTick(tickStr)) * time.Second)
	})
}

func logMetrics(interval time.Duration) {
	if interval <= 0 {
		log.GetLogger().Warnf("logMetrics disabled, interval: %d", interval)
		return
	}
	stopCh := signals.WaitForSignal()
	if stopCh == nil {
		return
	}
	log.GetLogger().Infof("start to logMetrics with interval %s", interval)
	lastCPU := RunMetrics.GetCurrentCPU()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cpuUsed := RunMetrics.CalAvgCPUUsed(lastCPU)
			lastCPU = RunMetrics.GetCurrentCPU()
			log.GetLogger().Infof("runtimeResUsage | cpu %.3f | mem %.3f | disk %.3f",
				cpuUsed, RunMetrics.GetMemoryUsed(nil), RunMetrics.GetStorageUsed())
		case <-stopCh:
			log.GetLogger().Infof("logMetrics stopped")
			return
		}
	}
}

// ReportCaeMetrics report cae metrics to bus-proxy
func ReportCaeMetrics(tickStr string, enableTmpExpansion bool) {
	reportCaeMetricsOnce.Do(func() {
		go reportCaeMetrics(time.Duration(parseLogMetricsTick(tickStr))*time.Second,
			os.Getenv("PodName"),
			enableTmpExpansion)
	})
}

// ReportBridgeFuncMetrics report bridgeFunc metrics to bus-proxy
func ReportBridgeFuncMetrics(runtimeConfig config.RuntimeConfig, callback ReportBridgeFuncMetricsCallBack) {
	reportBridgeFuncMetricsCallBack = callback
	reportBridgeFuncMetricsOnce.Do(func() {
		// for FG
		go reportBridgeMetrics(runtimeConfig, reportTickerTime, false)
		// for bridgeManager
		go reportBridgeMetrics(runtimeConfig, time.Duration(runtimeConfig.CustomMetricsReport.CollectTime)*time.Second, true)
	})
}

// ReportCaeMetricsCallBack -
type ReportCaeMetricsCallBack func(resourceUsage *buscommon.CaeMetrics, reportTimeout time.Duration) error

var reportCaeMetricsCallBack ReportCaeMetricsCallBack

// RegisterReportCaeMetricsCallBack -
func RegisterReportCaeMetricsCallBack(callback ReportCaeMetricsCallBack) {
	reportCaeMetricsCallBack = callback
}

// ReportBridgeFuncMetricsCallBack -
type ReportBridgeFuncMetricsCallBack func(metrics *buscommon.BridgeFuncMetrics, reportTimeout time.Duration) error

var reportBridgeFuncMetricsCallBack ReportBridgeFuncMetricsCallBack

func reportCaeMetrics(interval time.Duration, podName string, enableTmpExpansion bool) {
	if interval <= 0 {
		log.GetLogger().Warnf("report resource cae metrics disabled, interval: %d", interval)
		return
	}
	stopCh := signals.WaitForSignal()
	if stopCh == nil {
		log.GetLogger().Errorf("report resource cae metrics not start because stopCh is nil")
		return
	}

	log.GetLogger().Infof("start to report cae metrics with interval %s", interval)
	lastCPU := RunMetrics.GetCurrentCPU()
	lastCaeMetrics := RunMetrics.GetCurrentCaeMetrics()
	go recordCPUInfo(lastCPU)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	functionKey := urnutils.GetFuncKey(utils.LocalFuncURN.TenantID, utils.LocalFuncURN.Name, utils.LocalFuncURN.Version)

	fileSystemCapacity := float64(defaultFileSystemCapacity)
	if enableTmpExpansion {
		fileSystemCapacity = float64(expansionFileSystemCapacity)
	}

	for {
		select {
		case <-ticker.C:
			cpuUsed := RunMetrics.CalAvgCPUUsed(lastCPU)
			lastCPU = RunMetrics.GetCurrentCPU()
			memoryUsed := RunMetrics.GetMemoryUsed(nil)
			caeMetrics := RunMetrics.CalAvgCaeMetricsData(lastCaeMetrics)
			lastCaeMetrics = RunMetrics.GetCurrentCaeMetrics()
			customMetrics := RunMetrics.GetAndClearCustomReportMetrics()
			storageUsed := RunMetrics.GetStorageUsed()
			log.GetLogger().Debugf("report CAE resource usage (CPU,Memory) | %f | %f |", cpuUsed, memoryUsed)
			cpuLimit, memoryLimit := utils.GetCPUMemory()
			err := reportCaeMetricsCallBack(
				buscommon.InitCaeMetrics().WithFunctionKey(functionKey).WithPodName(podName).WithCustomMetrics(customMetrics).
					WithAvgCaeMetrics(caeMetrics).WithProperties(properties).WithCPUMetrics(cpuUsed*usedLimitRate, float64(cpuLimit)).
					WithMemoryMetrics(memoryUsed, float64(memoryLimit)).WithFileSystemData(storageUsed, fileSystemCapacity),
				defaultReportTimeout)
			if err != nil {
				log.GetLogger().Errorf("report resource usage error: %s", err.Error())
			}
		case <-stopCh:
			log.GetLogger().Infof("report resource usage stopped")
			return
		}
	}
}

func reportBridgeMetrics(runtimeConfig config.RuntimeConfig, reportTime time.Duration, forBridgeManager bool) {
	if reportTime <= 0 {
		log.GetLogger().Warnf("report bridge metrics disabled, interval: %v, forBridgeManager: %t",
			reportTime, forBridgeManager)
		return
	}
	podName = os.Getenv("PodName")
	stopCh := signals.WaitForSignal()
	if stopCh == nil {
		log.GetLogger().Warnf("report bridgeFunc resource metrics not start because stopCh is nil")
		return
	}
	moduleParts := strings.Split(runtimeConfig.EntryFile, ".")
	bridgeType := "default"
	// com.huawei.xxx.cache.FGCacheHandler, need 'cache'
	if len(moduleParts) > 1 {
		bridgeType = moduleParts[len(moduleParts)-bridgeTypeIdx]
	}
	log.GetLogger().Infof("start to report bridgeFunc metrics, bridgeType %s, forBridgeManager %t",
		bridgeType, forBridgeManager)
	ticker := time.NewTicker(reportTime)
	defer ticker.Stop()
	lastCPU := RunMetrics.GetCurrentCPU()
	for {
		select {
		case <-ticker.C:
			cpuUsed := RunMetrics.CalAvgCPUUsed(lastCPU)
			lastCPU = RunMetrics.GetCurrentCPU()
			metrics := constructBridgeMetrics(cpuUsed, bridgeType, forBridgeManager)
			parts := strings.Split(RunMetrics.baseURN.Name, "@")
			if len(parts) == fullFuncNameSegLens {
				metrics.Package = parts[packageIdx]
				metrics.FuncName = parts[funcNameIdx]
			}
			err := reportBridgeFuncMetricsCallBack(metrics, defaultReportTimeout)
			if err != nil {
				log.GetLogger().Warnf("report bridgeFunc resource usage error: %s, forBridge %t", err.Error(), forBridgeManager)
			}
		case <-stopCh:
			log.GetLogger().Infof("report bridgeFunc resource usage stopped, forBridge %t", forBridgeManager)
			return
		}
	}
}

func constructBridgeMetrics(cpuUsed float64, bridgeType string, forBridgeManager bool) *buscommon.BridgeFuncMetrics {
	cpuLimit, memoryLimit := utils.GetCPUMemory()
	customMetrics := tcpbustool.CustomReportMetrics{}
	if forBridgeManager {
		customMetrics = RunMetrics.GetAndClearCustomReportMetrics()
	} else {
		customMetrics = calculateBridgeResourceIndex()
	}
	return &buscommon.BridgeFuncMetrics{
		TimeStamp:           time.Now().Unix(),
		PodName:             podName,
		MemoryUsage:         RunMetrics.GetMemoryUsed(nil),
		MemoryCapacity:      float64(memoryLimit),
		CPUUsage:            cpuUsed * usedLimitRate,
		CPUCapacity:         float64(cpuLimit),
		TotalFailedCount:    RunMetrics.totalFailedCount,
		TotalReqCount:       RunMetrics.totalReqCount,
		AverageTimeCost:     RunMetrics.AverageTimeCost(),
		TenantID:            RunMetrics.baseURN.TenantID,
		Version:             RunMetrics.baseURN.Version,
		BridgeType:          bridgeType,
		CustomReportMetrics: customMetrics,
		ForBridgeManager:    forBridgeManager,
	}
}

func getBridgeReportMetrics() []types.BridgeReportMetrics {
	var reportMetrics []types.BridgeReportMetrics
	customMetrics := RunMetrics.GetAndClearBridgeResourceIndexMetrics()

	for _, customMetric := range customMetrics {
		metric := types.BridgeReportMetrics{}

		err := json.Unmarshal([]byte(customMetric.Val), &metric)
		if err != nil {
			log.GetLogger().Warnf("Failed to unmarshal bridge report metrics: %s", err.Error())
			continue
		}
		reportMetrics = append(reportMetrics, metric)
	}
	return reportMetrics
}

func calculateBridgeResourceIndex() tcpbustool.CustomReportMetrics {
	reportMetrics := getBridgeReportMetrics()
	log.GetLogger().Debugf("bridgeReportMetrics is %v", reportMetrics)

	if len(reportMetrics) == 0 {
		return tcpbustool.CustomReportMetrics{}
	}

	bridgeReSourceMetric := buildBridgeResourceMetrics(reportMetrics)
	resBody, err := json.Marshal(bridgeReSourceMetric)
	if err != nil {
		log.GetLogger().Warnf("failed to Marshal bridgeReSourceMetric %s", err.Error())
	}

	var customReportMetrics tcpbustool.CustomReportMetrics
	customReportMetric := tcpbustool.CustomReportMetric{
		Val:              string(resBody),
		CustomReportTime: time.Now().Unix(),
	}
	customReportMetrics = append(customReportMetrics, customReportMetric)

	return customReportMetrics
}

func buildBridgeResourceMetrics(reportMetrics []types.BridgeReportMetrics) types.BridgeResourceMetrics {
	var maxActive, minActive int32 = math.MinInt32, math.MaxInt32
	var totalExecuteTime int64
	var executeTimeCount int
	var maxExecuteTime, minExecuteTime int64 = math.MinInt64, math.MaxInt64

	for _, metric := range reportMetrics {
		if metric.NumActive > maxActive {
			maxActive = metric.NumActive
		}
		if metric.NumActive < minActive {
			minActive = metric.NumActive
		}
		for _, executeTime := range metric.ExecuteTimeList {
			if executeTime > maxExecuteTime {
				maxExecuteTime = executeTime
			}
			if executeTime < minExecuteTime {
				minExecuteTime = executeTime
			}
			totalExecuteTime += executeTime
			executeTimeCount++
		}
	}

	var averageExecuteTime float64
	if executeTimeCount > 0 {
		averageExecuteTime = float64(totalExecuteTime) / float64(executeTimeCount) / float64(millisecondsToNanoseconds)
	}

	if maxActive == math.MinInt32 {
		maxActive = 0
	}
	if minActive == math.MaxInt32 {
		minActive = 0
	}

	if maxExecuteTime == math.MinInt64 {
		maxExecuteTime = 0
	}
	if minExecuteTime == math.MaxInt64 {
		minExecuteTime = 0
	}

	bridgeReSourceMetric := types.BridgeResourceMetrics{
		MaxActiveNumber:     maxActive,
		MinActiveNumber:     minActive,
		CurrentActiveNumber: reportMetrics[len(reportMetrics)-1].NumActive,
		MaxTotal:            reportMetrics[0].MaxTotal,
		MaxExecuteTime: commonutils.Round(float64(maxExecuteTime)/float64(millisecondsToNanoseconds),
			executionTimePrecision),
		MinExecuteTime: commonutils.Round(float64(minExecuteTime)/float64(millisecondsToNanoseconds),
			executionTimePrecision),
		AverageExecuteTime: commonutils.Round(averageExecuteTime, executionTimePrecision),
	}
	return bridgeReSourceMetric
}

func recordCPUInfo(lastCPU CPUUsed) {
	// to calculate cpu used info, we need a time gap. cpu info collected per 2ms, 10ms is enough
	// log cpu used info in 10ms after load function finished (earlier than metric which is reported after 15s)
	time.Sleep(recordCPUInfoSleepTime)
	cpuUsed := RunMetrics.CalAvgCPUUsed(lastCPU)
	log.GetLogger().Infof("Current CPU used info: %f", cpuUsed)
}

// ReportResourceUsage report resource usage to bus-proxy
func ReportResourceUsage(tickStr string) {
	reportResourceUsageOnce.Do(func() {
		startTime = time.Now().Unix()
		go reportResourceUsage(time.Duration(parseLogMetricsTick(tickStr))*time.Second, os.Getenv("PodName"))
	})
}

// ReportResourceUsageCallBack -
type ReportResourceUsageCallBack func(resourceUsage *buscommon.ResourceUsage, reportTimeout time.Duration) error

var reportResourceUsageCallBack ReportResourceUsageCallBack

// RegisterResourceUsageCallBack -
func RegisterResourceUsageCallBack(callback ReportResourceUsageCallBack) {
	reportResourceUsageCallBack = callback
}

func reportResourceUsage(interval time.Duration, podName string) {
	if interval <= 0 {
		log.GetLogger().Warnf("report resource usage disabled, interval: %d", interval)
		return
	}
	stopCh := signals.WaitForSignal()
	if stopCh == nil {
		log.GetLogger().Errorf("report resource usage not start because stopCh is nil")
		return
	}
	log.GetLogger().Infof("start to report resource with interval %s", interval)
	lastCPU := RunMetrics.GetCurrentCPU()
	lastCaeMetrics := RunMetrics.GetCurrentCaeMetrics()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	functionKey := urnutils.GetFuncKey(utils.LocalFuncURN.TenantID, utils.LocalFuncURN.Name, utils.LocalFuncURN.Version)
	for {
		select {
		case <-ticker.C:
			cpuUsed := RunMetrics.CalAvgCPUUsed(lastCPU)
			lastCPU = RunMetrics.GetCurrentCPU()
			memoryUsed := RunMetrics.GetMemoryUsed(nil)

			caeMetrics := RunMetrics.CalAvgCaeMetricsData(lastCaeMetrics)
			lastCaeMetrics = RunMetrics.GetCurrentCaeMetrics()
			customMetrics := RunMetrics.GetAndClearResourceUsageCustomMetrics()

			log.GetLogger().Debugf("report resource usage (CPU,Memory) | %f | %f |", cpuUsed, memoryUsed)
			cpuLimit, memoryLimit := utils.GetCPUMemory()
			resourceUsage := &buscommon.ResourceUsage{
				FunctionKey: functionKey, PodName: podName, CPUUsage: cpuUsed, MemoryUsage: memoryUsed,
				CPULimit: cpuLimit, MemoryLimit: memoryLimit, ReportTime: time.Now().Unix(), StartTime: startTime,
				ReceiveBytesPerSecond:         caeMetrics.ReceiveBytesPerSecond,
				ReceivePackagePerSecond:       caeMetrics.ReceivePackagePerSecond,
				TransmitBytesPerSecond:        caeMetrics.TransmitBytesPerSecond,
				TransmitPackagePerSecond:      caeMetrics.TransmitPackagePerSecond,
				FileSystemReadBytesPerSecond:  caeMetrics.FileSystemReadBytesPerSecond,
				FileSystemWriteBytesPerSecond: caeMetrics.FileSystemWriteBytesPerSecond,
				CustomReportMetrics:           customMetrics,
			}
			err := reportResourceUsageCallBack(resourceUsage, defaultReportTimeout)
			if err != nil {
				log.GetLogger().Errorf("report resource usage error: %s", err.Error())
			}
		case <-stopCh:
			log.GetLogger().Infof("report resource usage stopped")
			return
		}
	}
}

// Metrics runtime request metrics
type Metrics struct {
	tracer                               *futureTracer
	suicideTimer                         *time.Timer
	suicideTimerMux                      sync.Mutex
	lastMemoryUsed                       float64
	memoryUsed                           atomic.Value
	oomMemoryUsed                        atomic.Value
	requestFinishTime                    time.Time
	requestCount                         int64
	wsConnCount                          int64
	totalReqCount                        int64
	totalFailedCount                     int64
	timeCostAnalyzer                     *TimeCostAnalyzer
	isFunctionLoaded                     bool
	cpuUsed                              atomic.Value
	storageUsed                          atomic.Value
	caeMetrics                           atomic.Value
	isDirectFunc                         bool
	customReportMetrics                  tcpbustool.CustomReportMetrics
	customReportMetricsLock              sync.Mutex
	bridgeResourceIndexMetrics           tcpbustool.CustomReportMetrics
	bridgeResourceIndexMetricsLock       sync.Mutex
	resourceUsageCustomReportMetrics     tcpbustool.CustomReportMetrics
	resourceUsageCustomReportMetricsLock sync.Mutex
	baseURN                              urnutils.BaseURN
}

// TimeCostAnalyzer -
type TimeCostAnalyzer struct {
	capacity      int
	override      bool
	idx           int
	timeCost      []float64
	totalTimeCost float64
	count         int
	mtx           sync.RWMutex
}

func createTimeCostCalculator(len int) *TimeCostAnalyzer {
	return &TimeCostAnalyzer{
		timeCost: make([]float64, len),
		capacity: len,
	}
}

func (tcc *TimeCostAnalyzer) recordTimeCost(timeCost float64) {
	tcc.mtx.Lock()
	defer tcc.mtx.Unlock()
	if tcc.override {
		tcc.totalTimeCost += timeCost
		tcc.totalTimeCost -= tcc.timeCost[tcc.idx]
	} else {
		tcc.totalTimeCost += timeCost
		tcc.count++
	}
	tcc.timeCost[tcc.idx] = timeCost
	tcc.idx++
	if tcc.idx >= tcc.capacity {
		tcc.idx = 0
		tcc.override = true
		tcc.count = tcc.capacity
	}
}

func (tcc *TimeCostAnalyzer) averageTimeCost() float64 {
	tcc.mtx.RLock()
	defer tcc.mtx.RUnlock()
	if tcc.count == 0 {
		return 0
	}
	return tcc.totalTimeCost / float64(tcc.count)
}

// CPUUsed -
type CPUUsed struct {
	CPUUsedNs        uint64
	TotalTimestampNs uint64
}

// Encode encodes CPUUsed to bytes
func (c *CPUUsed) Encode() []byte {
	data := make([]byte, totalSize, totalSize)
	binary.BigEndian.PutUint64(data, c.CPUUsedNs)
	binary.BigEndian.PutUint64(data[uint64size:], c.TotalTimestampNs)
	return data
}

// DecodeFrom decodes CPUUsed from bytes
func (c *CPUUsed) DecodeFrom(b []byte) {
	c.CPUUsedNs = binary.BigEndian.Uint64(b)
	c.TotalTimestampNs = binary.BigEndian.Uint64(b[uint64size:])
}

// GetRequestFinishTime set request finish time
func (m *Metrics) GetRequestFinishTime() time.Time {
	return m.requestFinishTime
}

// SetFunctionLoaded -
func (m *Metrics) SetFunctionLoaded() {
	m.isFunctionLoaded = true
}

// SetDirectFuncFlag -
func (m *Metrics) SetDirectFuncFlag() {
	m.isDirectFunc = true
}

// SetBaseURN -
func (m *Metrics) SetBaseURN(baseURN urnutils.BaseURN) {
	m.baseURN = baseURN
}

// IncreaseRequestCount increase current request count
func (m *Metrics) IncreaseRequestCount(futureID string) {
	m.tracer.increaseRequestCount(futureID)
	cnt := atomic.AddInt64(&m.requestCount, 1)
	if PodApplier != constants.WorkerManagerApplier && cnt == 1 {
		m.suicideTimerMux.Lock()
		timer := m.suicideTimer
		m.suicideTimer = nil
		m.suicideTimerMux.Unlock()
		if timer != nil {
			timer.Stop()
		}
	}
}

// IncrWsConnCount increase websocket conn count
func (m *Metrics) IncrWsConnCount() {
	atomic.AddInt64(&m.wsConnCount, 1)
}

// DecrWsConnCount decrease websocket conn count
func (m *Metrics) DecrWsConnCount() {
	atomic.AddInt64(&m.wsConnCount, -1)
}

// GetWsConnCount get websocket conn count
func (m *Metrics) GetWsConnCount() int {
	return int(atomic.LoadInt64(&m.wsConnCount))
}

// IncreaseTotalReqCount increase total request count
func (m *Metrics) IncreaseTotalReqCount() {
	atomic.AddInt64(&m.totalReqCount, 1)
}

// IncreaseTotalFailedCount increase total failed request count
func (m *Metrics) IncreaseTotalFailedCount() {
	atomic.AddInt64(&m.totalFailedCount, 1)
}

// RecordTimeCost -
func (m *Metrics) RecordTimeCost(timeCost float64) {
	m.timeCostAnalyzer.recordTimeCost(timeCost)
}

// AverageTimeCost -
func (m *Metrics) AverageTimeCost() float64 {
	return m.timeCostAnalyzer.averageTimeCost()
}

// DecreaseRequestCount decrease current request count
func (m *Metrics) DecreaseRequestCount(futureID string) {
	m.tracer.decreaseRequestCount(futureID)
	cnt := atomic.AddInt64(&m.requestCount, -1)
	if cnt == 0 {
		m.resetMemoryUsed()
		m.IdleCheckSuicide()
	}
	m.requestFinishTime = time.Now()
}

// SetSuicideTimeout set suicide timeout
func SetSuicideTimeout(timeout int) {
	if timeout > 0 {
		log.GetLogger().Debugf("Set suicide timeout succeed, timeout is: %d", timeout)
		suicideTimeout = time.Duration(timeout) * time.Minute
	}
}

// IdleCheckSuicide check idle worker
func (m *Metrics) IdleCheckSuicide() {
	// check to insure scaled
	if PodApplier != constants.WorkerManagerApplier && PodApplier != constants.PredictionApplier && !m.isDirectFunc {
		m.suicideTimerMux.Lock()
		if m.suicideTimer == nil {
			m.suicideTimer = time.AfterFunc(suicideTimeout, func() {
				log.GetLogger().Warnf("No request for long time, suicide to recycle resources")
				CloseExitChannel()
				close(recycleSignal)
			})
		}
		m.suicideTimerMux.Unlock()
	}
}

// GetRequestCount get current request count
func (m *Metrics) GetRequestCount() int {
	return int(atomic.LoadInt64(&m.requestCount))
}

// GetRecoverProperty -
func (m *Metrics) GetRecoverProperty() buscommon.RecoverProperty {
	return m.tracer.getRecoverProperty()
}

// AddTraceRecord adds a trace record (caller and callee's futureID)
func (m *Metrics) AddTraceRecord(futureID, callee string) {
	m.tracer.add(futureID, callee)
}

// RemoveTraceRecord -
func (m *Metrics) RemoveTraceRecord(futureID, callee string) {
	m.tracer.remove(futureID, callee)
}

// Cancel marks the request as "canceled". It returns a slice of callees associates to this request.
func (m *Metrics) Cancel(futureID string) ([]string, bool) {
	return m.tracer.cancel(futureID)
}

// IsCanceled -
func (m *Metrics) IsCanceled(futureID string) (bool, error) {
	return m.tracer.isCanceled(futureID)
}

// resetMemoryUsed no request now, prepare to rerecord memory used
func (m *Metrics) resetMemoryUsed() {
	// keep last value to insure memory used not equal to 0 always
	memoryUsed, flag := m.memoryUsed.Load().(float64)
	if flag && memoryUsed != 0.0 {
		m.lastMemoryUsed = memoryUsed
		m.memoryUsed.Store(0.0)
	}
}

func (m *Metrics) updateOOMMemoryUsed(memoryUsed float64) {
	// keep last value to insure memory used not equal to 0 always
	if m.oomMemoryUsed.Load() == nil {
		m.oomMemoryUsed.Store(memoryUsed)
		return
	}
	oomMemoryUsed, flag := m.oomMemoryUsed.Load().(float64)
	if flag {
		if memoryUsed >= oomMemoryUsed {
			m.oomMemoryUsed.Store(memoryUsed)
		}
	}
}

// UpdateMemoryUsed update memory used when requesting
func (m *Metrics) UpdateMemoryUsed(memory float64) {
	if !m.isFunctionLoaded {
		m.lastMemoryUsed = memory
		return
	}
	if m.memoryUsed.Load() == nil {
		m.memoryUsed.Store(memory)
		return
	}
	defer func() {
		m.updateOOMMemoryUsed(memory)
	}()

	if m.GetRequestCount() == 0 && !m.isDirectFunc {
		m.lastMemoryUsed = memory
		return
	}

	curMemoryUsed, flag := m.memoryUsed.Load().(float64)
	if flag {
		if memory > curMemoryUsed {
			m.memoryUsed.Store(memory)
		}
	}
}

// UpdateCPUUsed update cpu used nanoseconds when requesting
func (m *Metrics) UpdateCPUUsed(cpuUsed CPUUsed) {
	m.cpuUsed.Store(cpuUsed)
}

// UpdateCaeMetrics update cae metrics when requesting
func (m *Metrics) UpdateCaeMetrics(caeMetrics monitor.CaeMetrics) {
	m.caeMetrics.Store(caeMetrics)
}

// UpdateCustomReportMetrics update cae metrics when requesting
func (m *Metrics) UpdateCustomReportMetrics(customReportMetric tcpbustool.CustomReportMetric) {
	m.customReportMetricsLock.Lock()
	defer m.customReportMetricsLock.Unlock()

	m.customReportMetrics = append(m.customReportMetrics, customReportMetric)
}

// GetAndClearCustomReportMetrics get custom report metrics
func (m *Metrics) GetAndClearCustomReportMetrics() []tcpbustool.CustomReportMetric {
	customReportMetricData := make([]tcpbustool.CustomReportMetric, 0, len(m.customReportMetrics))

	m.customReportMetricsLock.Lock()
	defer m.customReportMetricsLock.Unlock()

	for _, customMetric := range m.customReportMetrics {
		customReportMetricData = append(customReportMetricData, customMetric)
	}

	m.customReportMetrics = make([]tcpbustool.CustomReportMetric, 0)

	return customReportMetricData
}

// UpdateBridgeResourceIndexMetrics update bridge resource index
func (m *Metrics) UpdateBridgeResourceIndexMetrics(customReportMetric tcpbustool.CustomReportMetric) {
	m.bridgeResourceIndexMetricsLock.Lock()
	defer m.bridgeResourceIndexMetricsLock.Unlock()

	m.bridgeResourceIndexMetrics = append(m.bridgeResourceIndexMetrics, customReportMetric)
}

// UpdateResourceUsageCustomMetrics update resource custom metrics
func (m *Metrics) UpdateResourceUsageCustomMetrics(customReportMetric tcpbustool.CustomReportMetric) {
	m.resourceUsageCustomReportMetricsLock.Lock()
	defer m.resourceUsageCustomReportMetricsLock.Unlock()

	m.resourceUsageCustomReportMetrics = append(m.resourceUsageCustomReportMetrics, customReportMetric)
}

// GetAndClearBridgeResourceIndexMetrics get bridge resource index
func (m *Metrics) GetAndClearBridgeResourceIndexMetrics() []tcpbustool.CustomReportMetric {
	bridgeResourceIndexData := make([]tcpbustool.CustomReportMetric, 0, len(m.bridgeResourceIndexMetrics))

	m.bridgeResourceIndexMetricsLock.Lock()
	defer m.bridgeResourceIndexMetricsLock.Unlock()

	for _, customMetric := range m.bridgeResourceIndexMetrics {
		bridgeResourceIndexData = append(bridgeResourceIndexData, customMetric)
	}

	m.bridgeResourceIndexMetrics = make([]tcpbustool.CustomReportMetric, 0)

	return bridgeResourceIndexData
}

// GetAndClearResourceUsageCustomMetrics get resource custom metrics
func (m *Metrics) GetAndClearResourceUsageCustomMetrics() tcpbustool.CustomReportMetrics {
	resourceUsageCustomIndexData := make(tcpbustool.CustomReportMetrics, 0, len(m.resourceUsageCustomReportMetrics))

	m.resourceUsageCustomReportMetricsLock.Lock()
	defer m.resourceUsageCustomReportMetricsLock.Unlock()

	for _, customMetric := range m.resourceUsageCustomReportMetrics {
		resourceUsageCustomIndexData = append(resourceUsageCustomIndexData, customMetric)
	}

	m.resourceUsageCustomReportMetrics = make(tcpbustool.CustomReportMetrics, 0)

	return resourceUsageCustomIndexData
}

func max(value ...float64) float64 {
	m := 0.0
	for _, i := range value {
		if i > m {
			m = i
		}
	}
	return m
}

// GetMemoryUsed get used memory
func (m *Metrics) GetMemoryUsed(err error) float64 {
	tmpMemoryUsed, flag := m.memoryUsed.Load().(float64)
	oomMemoryUsed, oomMemflag := m.oomMemoryUsed.Load().(float64)
	if snerror.IsOOMErrorCode(err) && flag && oomMemflag {
		currentMemory := max(tmpMemoryUsed, oomMemoryUsed, m.lastMemoryUsed)
		return currentMemory
	}
	m.oomMemoryUsed.Store(0.0)
	if flag && tmpMemoryUsed != 0.0 {
		return tmpMemoryUsed
	}
	return m.lastMemoryUsed
}

// GetCurrentCPU get current cpu metrics of runtime
func (m *Metrics) GetCurrentCPU() CPUUsed {
	cpuUsed, ok := m.cpuUsed.Load().(CPUUsed)
	if !ok {
		return CPUUsed{}
	}
	return cpuUsed
}

// GetCurrentCaeMetrics get current cae metrics of runtime
func (m *Metrics) GetCurrentCaeMetrics() monitor.CaeMetrics {
	caeMetrics, ok := m.caeMetrics.Load().(monitor.CaeMetrics)
	if !ok {
		return monitor.CaeMetrics{}
	}
	return caeMetrics
}

// CalAvgCPUUsed calculate avg cpu usage between current and pre
func (m *Metrics) CalAvgCPUUsed(pre CPUUsed) float64 {
	if pre.TotalTimestampNs < 1 {
		return 0
	}
	current := m.GetCurrentCPU()
	if current.TotalTimestampNs-pre.TotalTimestampNs < 1 {
		return 0
	}
	cpuValue := float64((current.CPUUsedNs-pre.CPUUsedNs)*uint64(runtime.NumCPU())) /
		float64(current.TotalTimestampNs-pre.TotalTimestampNs) / convertToCPU

	if cpuValue > maxCPU {
		cpuValue = maxCPU
	}
	return cpuValue
}

// CalAvgCaeMetricsData get avf cae metrics data
func (m *Metrics) CalAvgCaeMetricsData(lastCaeMetrics monitor.CaeMetrics) buscommon.AvgCaeMetrics {
	avgCaeMetricsData := buscommon.AvgCaeMetrics{}
	current := m.GetCurrentCaeMetrics()
	if current.TimestampMs-lastCaeMetrics.TimestampMs <= 0 {
		log.GetLogger().Errorf("failed to cal avg cae metrics dta because time not right, before: %d, now: %d",
			lastCaeMetrics.TimestampMs, current.TimestampMs)
		return avgCaeMetricsData
	}

	timeDiff := float64(current.TimestampMs-lastCaeMetrics.TimestampMs) / float64(millPerSecond)
	avgCaeMetricsData.ReceiveBytesPerSecond = float64(current.ReceiveBytes-lastCaeMetrics.ReceiveBytes) / timeDiff
	avgCaeMetricsData.ReceivePackagePerSecond = float64(current.ReceivePackage-lastCaeMetrics.ReceivePackage) / timeDiff
	avgCaeMetricsData.TransmitBytesPerSecond = float64(current.TransmitBytes-lastCaeMetrics.TransmitBytes) / timeDiff
	avgCaeMetricsData.TransmitPackagePerSecond = float64(current.TransmitPackage-lastCaeMetrics.TransmitPackage) / timeDiff
	diffFileSystemReadBytes := current.FileSystemReadBytes - lastCaeMetrics.FileSystemReadBytes
	avgCaeMetricsData.FileSystemReadBytesPerSecond = float64(diffFileSystemReadBytes) / timeDiff
	diffFileSystemWriteBytes := current.FileSystemWriteBytes - lastCaeMetrics.FileSystemWriteBytes
	avgCaeMetricsData.FileSystemWriteBytesPerSecond = float64(diffFileSystemWriteBytes) / timeDiff
	return avgCaeMetricsData
}

// UpdateStorageUsed updates usage of storage in bytes
func (m *Metrics) UpdateStorageUsed(used int64) {
	m.storageUsed.Store(float64(used) / byteToMB)
}

// GetStorageUsed returns usage of storage in bytes
func (m *Metrics) GetStorageUsed() float64 {
	val, ok := m.storageUsed.Load().(float64)
	if !ok {
		return 0
	}
	return val
}

var maxCPU = parseMaxCPU()

func parseMaxCPU() float64 {
	cpu, _ := utils.GetCPUMemory()
	return float64(cpu) / cpuDivisor
}

type runtimeMetricsMgr struct {
	runtimeMetrics *runtimeMetrics
	metricsPool    *sync.Pool
	file           *os.File
}

type runtimeMetrics struct {
	memorySize       int
	memoryUsed       float64
	cpuUsedNs        uint64
	timestampNs      uint64
	gpuMemorySize    int
	ephemeralStorage int
	storageUsed      float64
}

// InitRuntimeMetrics -
func InitRuntimeMetrics(conf *config.Configuration, quit chan int) error {
	runtimeMetricsMgr := &runtimeMetricsMgr{
		runtimeMetrics: &runtimeMetrics{
			memorySize:       int(conf.RuntimeConfig.ResourceMetaData.Mem),
			gpuMemorySize:    int(conf.ResourceMetaData.GpuMemory),
			ephemeralStorage: int(conf.ResourceMetaData.EphemeralStorage),
		},
		metricsPool: &sync.Pool{New: func() interface{} {
			return &bytes.Buffer{}
		}},
	}
	file, err := os.Create(metricsFile)
	if err != nil {
		log.GetLogger().Errorf("record loaded error: %s", err.Error())
		return err
	}
	runtimeMetricsMgr.file = file
	RunMetrics.SetFunctionLoaded()
	RunMetrics.SetDirectFuncFlag()
	go runtimeMetricsMgr.reportRuntimeMetrics(quit)
	return nil
}

func (m *runtimeMetricsMgr) updateRuntimeMetrics(memoryUsed, storageUsed float64, cpuUsedInfo CPUUsed) {
	m.runtimeMetrics.memoryUsed = memoryUsed
	m.runtimeMetrics.cpuUsedNs = cpuUsedInfo.CPUUsedNs
	m.runtimeMetrics.timestampNs = cpuUsedInfo.TotalTimestampNs
	m.runtimeMetrics.storageUsed = storageUsed
}

func (m *runtimeMetricsMgr) encodeRuntimeMetrics(buf *bytes.Buffer) []byte {
	buf.WriteString(` {"memorySize" : `)
	buf.WriteString(strconv.FormatInt(int64(m.runtimeMetrics.memorySize), base))
	buf.WriteString(", ")
	buf.WriteString(`"memoryUsed" : `)
	buf.WriteString(strconv.FormatFloat(m.runtimeMetrics.memoryUsed, 'f', precision, bitSize))
	buf.WriteString(", ")
	buf.WriteString(`"cpuUsedNs" : `)
	buf.WriteString(strconv.FormatUint(m.runtimeMetrics.cpuUsedNs, base))
	buf.WriteString(", ")
	buf.WriteString(`"timestampNs" : `)
	buf.WriteString(strconv.FormatUint(m.runtimeMetrics.timestampNs, base))
	buf.WriteString(", ")
	buf.WriteString(`"gpuMemorySize" : `)
	buf.WriteString(strconv.FormatInt(int64(m.runtimeMetrics.gpuMemorySize), base))
	buf.WriteString(", ")
	buf.WriteString(`"EphemeralStorage" : `)
	buf.WriteString(strconv.FormatInt(int64(m.runtimeMetrics.ephemeralStorage), base))
	buf.WriteString(", ")
	buf.WriteString(`"storageUsed" : `)
	buf.WriteString(strconv.FormatFloat(m.runtimeMetrics.storageUsed, 'f', precision, bitSize))
	buf.WriteString("}")
	return buf.Bytes()
}

func (m *runtimeMetricsMgr) reportRuntimeMetrics(quit chan int) {
	var preLen int
	ticker := time.NewTicker(defaultRuntimeMetricsTick)
	stopCh := signals.WaitForSignal()
	if stopCh == nil {
		log.GetLogger().Errorf("report resource usage not start because stopCh is nil")
		return
	}
	for {
		select {
		case <-ticker.C:
			curCPUUsed := RunMetrics.GetCurrentCPU()
			m.updateRuntimeMetrics(RunMetrics.GetMemoryUsed(nil), RunMetrics.GetStorageUsed(), curCPUUsed)
			metricsBuf, ok := m.metricsPool.Get().(*bytes.Buffer)
			if !ok {
				log.GetLogger().Errorf("failed to assert bytes.Buffer")
				continue
			}
			metricsBuf.Reset()
			metrics := m.encodeRuntimeMetrics(metricsBuf)
			if preLen > len(metrics) {
				emptyByte := []byte{' '}
				fillingByte := bytes.Repeat(emptyByte, preLen-len(metrics))
				metrics = append(metrics, fillingByte...)
			}
			preLen = len(metrics)
			_, err := m.file.Seek(0, io.SeekStart)
			if err != nil {
				log.GetLogger().Errorf("failed to seek, err: %s", err.Error())
				m.metricsPool.Put(metricsBuf)
				continue
			}
			buf := bufio.NewWriter(m.file)
			_, err = buf.Write(metrics)
			flushErr := buf.Flush()
			if err != nil || flushErr != nil {
				log.GetLogger().Errorf("failed to flush metrics file")
				m.metricsPool.Put(metricsBuf)
				continue
			}
			m.metricsPool.Put(metricsBuf)
		case <-quit:
			ticker.Stop()
			m.file.Close()
			return
		case <-stopCh:
			log.GetLogger().Info("worker is exiting")
			ticker.Stop()
			m.file.Close()
			return
		}
	}
}

// SetProperties set properties of cae metrics
func SetProperties(inputProperties map[string]string) {
	properties = inputProperties
}

// GetProperties get properties
func GetProperties() map[string]string {
	return properties
}
