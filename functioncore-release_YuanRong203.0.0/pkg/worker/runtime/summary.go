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
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/utils"
)

const (
	// 1ms（1,000us）
	meteringGranularity = 1000
	// MicroToMilliSecond is used for Microsecond to MilliSecond
	MicroToMilliSecond = 1000
	// FuncDigestLength -
	FuncDigestLength = 32
	precision        = 3
	bitSize          = 64
	base             = 10
)

var (
	// InitializerDuration -
	InitializerDuration time.Duration
	initOnce            sync.Once
	// StartupDuration -
	StartupDuration time.Duration
	startupOnce     sync.Once

	// RestoreDuration -
	RestoreDuration time.Duration
	restoreOnce     sync.Once
	// ExtensionDuration -
	ExtensionDuration time.Duration
	extensionOnce     sync.Once

	loadFlag      = uint32(0)
	initFlag      = uint32(0)
	restoreFlag   = uint32(0)
	extensionFlag = uint32(0)
	podName       = os.Getenv("PodName")

	hcsMode = os.Getenv("HCS_MODE") == "true"
)

var summaryRecoder = struct {
	m map[string]*Summary
	sync.Mutex
}{
	m:     make(map[string]*Summary),
	Mutex: sync.Mutex{},
}

// RecodeSummary -
func RecodeSummary(traceID string, summarySpan *Summary) {
	summaryRecoder.Lock()
	defer summaryRecoder.Unlock()
	_, ok := summaryRecoder.m[traceID]
	if !ok {
		summaryRecoder.m[traceID] = summarySpan
		return
	}
	log.GetLogger().Infof("traceID %s summarySpan is exist")
}

// DeleteSummary -
func DeleteSummary(traceID string) {
	summaryRecoder.Lock()
	defer summaryRecoder.Unlock()
	_, ok := summaryRecoder.m[traceID]
	if !ok {
		return
	}
	delete(summaryRecoder.m, traceID)
}

// GetSummary -
func GetSummary(traceID string) *Summary {
	summaryRecoder.Lock()
	defer summaryRecoder.Unlock()
	summarySpan, ok := summaryRecoder.m[traceID]
	if !ok {
		log.GetLogger().Infof("traceID %s summarySpan is not exist")
		return nil
	}
	return summarySpan
}

// Summary -
type Summary struct {
	err       *Error
	stage     string
	traceID   string
	timeout   time.Duration
	startTime time.Time
	endTime   time.Time
	// the duration endTime - startTime
	subDuration time.Duration
	cpuUsed     CPUUsed

	footerDuration        float64
	footerBillingDuration int

	SummaryInfo snerror.SummaryInfo
}

// NewSummarySpan -
func NewSummarySpan(stage, traceID string, c *config.Configuration, timeout time.Duration) *Summary {
	info := snerror.SummaryInfo{
		MemorySize: int(c.ResourceMetaData.Mem),
	}
	// In fact, FuncDigest is not checked in FunctionGraph V2
	if len(c.CodeSha512) > FuncDigestLength {
		info.FuncDigest = c.CodeSha512[0 : FuncDigestLength-1]
	} else {
		info.FuncDigest = c.CodeSha512
	}
	info.GPUMemorySize = int(c.ResourceMetaData.GpuMemory)
	info.EphemeralStorage = int(c.ResourceMetaData.EphemeralStorage)
	summary := &Summary{
		stage:       stage,
		traceID:     traceID,
		timeout:     timeout,
		startTime:   time.Now().UTC(),
		SummaryInfo: info,
		cpuUsed:     RunMetrics.GetCurrentCPU(),
	}
	// Preset endTime is startTime+Timeout. If the actual endTime is earlier than the Preset endTime,
	// the endTime should be set to the actual endTime.
	summary.endTime = summary.startTime.Add(summary.timeout)
	return summary
}

// StartTime -
func (i *Summary) StartTime() time.Time {
	return i.startTime
}

// EndTime -
func (i *Summary) EndTime() time.Time {
	return i.endTime
}

// SubDuration -
func (i *Summary) SubDuration() time.Duration {
	return i.subDuration
}

// GenerateLogHeader -
func (i *Summary) GenerateLogHeader(logHeader *functionlog.FunctionLog) {
	timeStr := i.startTime.Format(functionlog.FuncLogTimeFormat)
	timeLogStr := i.startTime.Format(functionlog.NanoLogLayout)

	logHeader.Time = timeStr
	logHeader.NanoTime = timeLogStr
	logHeader.Level = "INFO"
	logHeader.Message.WriteString(timeStr)
	logHeader.Message.WriteString(" Start ")
	logHeader.Message.WriteString(i.stage)
	logHeader.Message.WriteString(" request '")
	logHeader.Message.WriteString(i.traceID)
	logHeader.Message.WriteString("', version: ")
	logHeader.Message.WriteString(utils.LocalFuncURN.Version)
}

func (i *Summary) isFirstLoadAndInitOrInvokeFinish() bool {
	if i.stage == constants.InvokeStage {
		return true
	}
	if (i.stage == constants.LoadStage || i.stage == constants.CheckpointLoadStage) &&
		atomic.LoadUint32(&loadFlag) == 0 {
		atomic.StoreUint32(&loadFlag, 1)
		return true
	}
	if (i.stage == constants.InitializeStage || i.stage == constants.CheckpointInitStage) &&
		atomic.LoadUint32(&initFlag) == 0 {
		atomic.StoreUint32(&initFlag, 1)
		return true
	}
	if i.stage == constants.RestoreStage && atomic.LoadUint32(&restoreFlag) == 0 {
		atomic.StoreUint32(&restoreFlag, 1)
		return true
	}
	if i.stage == constants.ExtensionStage && atomic.LoadUint32(&extensionFlag) == 0 {
		atomic.StoreUint32(&extensionFlag, 1)
		return true
	}
	return false
}

// Finish -
func (i *Summary) Finish(err *Error) {
	i.FinishWithEndTime(err, time.Now().UTC())
}

// FinishWithEndTime -
func (i *Summary) FinishWithEndTime(err *Error, actualEndTime time.Time) {
	var billingDuration time.Duration
	if actualEndTime.Before(i.endTime) {
		i.endTime = actualEndTime
	}
	i.subDuration, billingDuration = i.endTime.Sub(i.startTime), i.timeout
	if i.subDuration >= i.timeout {
		i.subDuration = i.timeout
	} else {
		billingDuration = CalculateBillingDuration(i.subDuration)
	}
	i.SummaryInfo.Duration = float64(i.subDuration/time.Microsecond) / MicroToMilliSecond
	i.SummaryInfo.BillingDuration = int(billingDuration / time.Millisecond)
	i.SummaryInfo.MemoryUsed = RunMetrics.GetMemoryUsed(changeErrorStructToError(err))
	avgCPUUsed := RunMetrics.CalAvgCPUUsed(i.cpuUsed)
	if avgCPUUsed != 0 {
		i.SummaryInfo.CPUUsed = avgCPUUsed
	}
	i.SummaryInfo.StorageUsed = RunMetrics.GetStorageUsed()
	// Do not calculate the load/init/restore time for multiple times when the initialization fails.
	if !i.isFirstLoadAndInitOrInvokeFinish() {
		i.footerDuration = i.SummaryInfo.Duration
		i.footerBillingDuration = i.SummaryInfo.BillingDuration
		i.err = err
		return
	}
	i.footerDuration = i.SummaryInfo.Duration
	i.footerBillingDuration = i.SummaryInfo.BillingDuration
	i.err = err

	// If the startup duration time is greater than 0, add it to the duration.
	if StartupDuration > 0 && PodApplier != constants.WorkerManagerApplier {
		i.addStartUpDuration()
	}

	// If the initialization duration time is greater than 0, add it to the duration.
	// InitializerDuration is not included in invoke LoggerFooter.
	if InitializerDuration > 0 && PodApplier != constants.WorkerManagerApplier {
		i.addInitDuration()
	}
	if RestoreDuration > 0 && PodApplier != constants.WorkerManagerApplier {
		i.addRestoreDuration()
	}
	if ExtensionDuration > 0 && PodApplier != constants.WorkerManagerApplier {
		i.addExtensionDuration()
	}
}

// GenerateLogFooter -
func (i *Summary) GenerateLogFooter(logFooter *functionlog.FunctionLog) {
	timeStr := i.endTime.Format(functionlog.FuncLogTimeFormat)
	timeLogStr := i.endTime.Format(functionlog.NanoLogLayout)
	logFooter.Time = timeStr
	logFooter.NanoTime = timeLogStr
	logFooter.IsFinishedLog = true
	if i.err != nil {
		logFooter.Level = "ERROR"
		logFooter.ErrorType = getErrorType(i.err)
	} else {
		logFooter.Level = "INFO"
	}
	if i.footerDuration == 0 {
		i.footerDuration = i.SummaryInfo.Duration
		i.footerBillingDuration = i.SummaryInfo.BillingDuration
	}
	i.generateFooterMessage(logFooter.Message, timeStr)
	logFooter.ReportSummary = true
	logFooter.Duration = i.footerDuration
	logFooter.CPUUsed = i.SummaryInfo.CPUUsed
	logFooter.MemoryUsed = i.SummaryInfo.MemoryUsed
	logFooter.StorageUsed = i.SummaryInfo.StorageUsed
}

func (i *Summary) generateFooterMessage(buf *bytes.Buffer, timeStr string) {
	buf.WriteString(timeStr)
	buf.WriteString(" Finish ")
	buf.WriteString(i.stage)
	buf.WriteString(" request '")
	buf.WriteString(i.traceID)
	buf.WriteString("'")
	if i.err != nil {
		buf.WriteString("(")
		buf.WriteString(getErrorInfo(i.err, i.stage))
		buf.WriteString(")")
	}
	buf.WriteString(", duration: ")
	buf.WriteString(strconv.FormatFloat(i.footerDuration, 'f', precision, bitSize))
	if (i.stage == constants.InvokeStage || i.err != nil) && !hcsMode {
		buf.WriteString("ms, billing duration: ")
		buf.WriteString(strconv.Itoa(i.footerBillingDuration))
	}
	buf.WriteString("ms, memory used: ")
	buf.WriteString(strconv.FormatFloat(i.SummaryInfo.MemoryUsed, 'f', precision, bitSize))
	if (i.stage == constants.InvokeStage || i.err != nil) && !hcsMode {
		_, billingMemory := utils.GetCPUMemory()
		buf.WriteString("MB, billing memory: ")
		buf.WriteString(strconv.FormatInt(billingMemory, base))
	}
	buf.WriteString("MB")
	if i.SummaryInfo.CPUUsed != 0 && (i.stage == constants.InvokeStage || i.err != nil) {
		buf.WriteString(", cpu used: ")
		buf.WriteString(strconv.FormatFloat(i.SummaryInfo.CPUUsed, 'f', precision, bitSize))
		buf.WriteString("U")
	}
	buf.WriteString(", storage used: ")
	buf.WriteString(strconv.FormatFloat(i.SummaryInfo.StorageUsed, 'f', precision, bitSize))
	buf.WriteString("MB")
	if i.stage != constants.InvokeStage {
		buf.WriteString(".")
	}
}

func (i *Summary) addExtensionDuration() {
	extensionOnce.Do(func() {
		i.subDuration = i.subDuration + ExtensionDuration
		i.SummaryInfo.Duration = float64(i.subDuration/time.Microsecond) / MicroToMilliSecond
		billingDuration := CalculateBillingDuration(i.subDuration)
		i.SummaryInfo.BillingDuration = int(billingDuration / time.Millisecond)
	})
}

func (i *Summary) addRestoreDuration() {
	restoreOnce.Do(func() {
		i.subDuration = i.subDuration + RestoreDuration
		i.SummaryInfo.Duration = float64(i.subDuration/time.Microsecond) / MicroToMilliSecond
		billingDuration := CalculateBillingDuration(i.subDuration)
		i.SummaryInfo.BillingDuration = int(billingDuration / time.Millisecond)
	})
}

func (i *Summary) addInitDuration() {
	initOnce.Do(func() {
		i.subDuration = i.subDuration + InitializerDuration
		i.SummaryInfo.Duration = float64(i.subDuration/time.Microsecond) / MicroToMilliSecond
		billingDuration := CalculateBillingDuration(i.subDuration)
		i.SummaryInfo.BillingDuration = int(billingDuration / time.Millisecond)
	})
}

func (i *Summary) addStartUpDuration() {
	startupOnce.Do(func() {
		var billingDuration time.Duration
		i.subDuration, billingDuration = i.subDuration+StartupDuration, i.timeout
		if i.subDuration >= i.timeout {
			i.subDuration = i.timeout
		} else {
			billingDuration = CalculateBillingDuration(i.subDuration)
		}
		i.SummaryInfo.Duration = float64(i.subDuration/time.Microsecond) / MicroToMilliSecond
		i.SummaryInfo.BillingDuration = int(billingDuration / time.Millisecond)
	})
}

// CalculateBillingDuration -
func CalculateBillingDuration(duration time.Duration) time.Duration {
	durationInUs := duration.Microseconds() // 100ms（100,000us）
	billingUnits := durationInUs / meteringGranularity
	if durationInUs%meteringGranularity > 0 {
		billingUnits++
	}
	return time.Duration(billingUnits) * meteringGranularity * time.Microsecond
}

func getErrorInfo(err *Error, stage string) string {
	var result string
	switch err.Code {
	case snerror.RuntimeLoadTimeoutCode, snerror.RuntimeInvokeTimeoutCode, snerror.RuntimeInitTimeoutCode,
		snerror.RuntimeRestoreHookTimeoutCode:
		result = fmt.Sprintf("%s Timeout", stage)
	case snerror.RuntimeProcessExit:
		result = fmt.Sprintf("%s Failed:RuntimeProcessExited", stage)
	case snerror.DiskUsageExceed:
		result = fmt.Sprintf("%s Failed:DiskUsageExceedLimit", stage)
	case snerror.RequestCanceled:
		result = fmt.Sprintf("%s Failed:RequestCanceled", stage)
	case snerror.RuntimeUnHealthy:
		result = fmt.Sprintf("%s Failed:RuntimeUnHealthy", stage)
	case snerror.RuntimeMemoryExceedLimit:
		result = fmt.Sprintf("%s Failed:RuntimeMemoryExceedLimit", stage)
	case snerror.INodeUsageExceed:
		result = fmt.Sprintf("%s Failed:InodeUsageExceedLimit", stage)
	default:
		result = fmt.Sprintf("%s Failed", stage)
	}
	return result
}

func getErrorType(err *Error) string {
	errorType := ""
	if err == nil {
		return errorType
	}

	switch err.Code {
	case snerror.RuntimeLoadTimeoutCode, snerror.RuntimeInvokeTimeoutCode, snerror.RuntimeInitTimeoutCode,
		snerror.RuntimeRestoreHookTimeoutCode:
		errorType = "TimeOut"
	case snerror.DiskUsageExceed:
		errorType = "DiskTmpUsageOutOfRange"
	case snerror.RequestCanceled:
		errorType = "RequestCanceled"
	case snerror.RuntimeUnHealthy:
		errorType = "RuntimeUnHealthy"
	case snerror.RuntimeMemoryExceedLimit:
		errorType = "MemoryExceedLimit"
	default:
		errorType = "RunningError"
	}

	return errorType
}

// SetErr -
func (i *Summary) SetErr(err *Error) {
	i.err = err
}

// GetErr -
func (i *Summary) GetErr() *Error {
	return i.err
}

// GetTraceID -
func (i *Summary) GetTraceID() string {
	return i.traceID
}

// GenerateSummaryInfo -
func GenerateSummaryInfo(conf *config.Configuration, CPUUsedNs, TotalTimestampNs uint64,
	err error) snerror.SummaryInfo {
	summaryInfo := snerror.SummaryInfo{}
	if len(conf.RuntimeConfig.CodeSha512) > FuncDigestLength {
		summaryInfo.FuncDigest = conf.RuntimeConfig.CodeSha512[0 : FuncDigestLength-1]
	} else {
		summaryInfo.FuncDigest = conf.RuntimeConfig.CodeSha512
	}
	preUsed := CPUUsed{CPUUsedNs: CPUUsedNs, TotalTimestampNs: TotalTimestampNs}
	summaryInfo.MemorySize = int(conf.RuntimeConfig.ResourceMetaData.Mem)
	summaryInfo.PodName = podName
	summaryInfo.GPUMemorySize = int(conf.ResourceMetaData.GpuMemory)
	summaryInfo.EphemeralStorage = int(conf.ResourceMetaData.EphemeralStorage)
	summaryInfo.StorageUsed = RunMetrics.GetStorageUsed()
	summaryInfo.CPUUsed = RunMetrics.CalAvgCPUUsed(preUsed)
	summaryInfo.MemoryUsed = RunMetrics.GetMemoryUsed(err)
	return summaryInfo
}
