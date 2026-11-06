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
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/urnutils"
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
)

const (
	usedLimitRate = 1000
	millPerSecond = 1000
)

var (
	logMetricsOnce          sync.Once
	reportResourceUsageOnce sync.Once
	reportCaeMetricsOnce    sync.Once
	cpuLimit                int64
	memoryLimit             int64
	suicideTimeout          = defaultSuicideTimeout * time.Minute
	properties              map[string]string
)

func init() {
	cpuLimitStr := os.Getenv(workerCPULimitKey)
	var err error
	cpuLimit, err = strconv.ParseInt(cpuLimitStr, base, bitSize)
	if err != nil {
		log.GetLogger().Errorf("failed to parse cpuLimit, report resource usage err: %s", err.Error())
		return
	}
	memoryLimitStr := os.Getenv(workerMemoryLimitKey)
	memoryLimit, err = strconv.ParseInt(memoryLimitStr, base, bitSize)
	if err != nil {
		log.GetLogger().Errorf("failed to parse memoryLimit, report resource usage err: %s", err.Error())
		return
	}
}

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

// ReportCaeMetricsCallBack -
type ReportCaeMetricsCallBack func(resourceUsage *buscommon.CaeMetrics, reportTimeout time.Duration) error

var reportCaeMetricsCallBack ReportCaeMetricsCallBack

// RegisterReportCaeMetricsCallBack -
func RegisterReportCaeMetricsCallBack(callback ReportCaeMetricsCallBack) {
	reportCaeMetricsCallBack = callback
}

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

// ReportResourceUsage report resource usage to bus-proxy
func ReportResourceUsage(tickStr string) {
	reportResourceUsageOnce.Do(func() {
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
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	functionKey := urnutils.GetFuncKey(utils.LocalFuncURN.TenantID, utils.LocalFuncURN.Name, utils.LocalFuncURN.Version)
	for {
		select {
		case <-ticker.C:
			cpuUsed := RunMetrics.CalAvgCPUUsed(lastCPU)
			lastCPU = RunMetrics.GetCurrentCPU()
			memoryUsed := RunMetrics.GetMemoryUsed(nil)
			log.GetLogger().Debugf("report resource usage (CPU,Memory) | %f | %f |", cpuUsed, memoryUsed)

			resourceUsage := &buscommon.ResourceUsage{
				FunctionKey: functionKey,
				PodName:     podName,
				CPUUsage:    cpuUsed,
				MemoryUsage: memoryUsed,
				CPULimit:    cpuLimit,
				MemoryLimit: memoryLimit,
				ReportTime:  time.Now().Unix(),
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
	tracer                  *futureTracer
	suicideTimer            *time.Timer
	suicideTimerMux         sync.Mutex
	lastMemoryUsed          float64
	memoryUsed              atomic.Value
	oomMemoryUsed           atomic.Value
	requestFinishTime       time.Time
	requestCount            int64
	isFunctionLoaded        bool
	cpuUsed                 atomic.Value
	storageUsed             atomic.Value
	caeMetrics              atomic.Value
	isDirectFunc            bool
	customReportMetrics     tcpbustool.CustomReportMetrics
	customReportMetricsLock sync.Mutex
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

// SetFunctionJavaxFlag -
func (m *Metrics) SetFunctionJavaxFlag() {
	m.isDirectFunc = true
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
func InitRuntimeMetrics(conf *config.Configuration) error {
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
	RunMetrics.SetFunctionJavaxFlag()
	go runtimeMetricsMgr.reportRuntimeMetrics()
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

func (m *runtimeMetricsMgr) reportRuntimeMetrics() {
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
