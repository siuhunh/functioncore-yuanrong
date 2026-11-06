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

package invoke

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/types/workertypes"
	"functioncore/pkg/common/urnutils"
	buscommon "functioncore/pkg/functionbus/common"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/utils"
)

const (
	defaultReportTimeWindowsPeriod = 5
	minuteMilliseconds             = 60000
	reportRuntimeContextTimeout    = 1000 * time.Millisecond
	consumerBridgeFunctionKeyWords = "consumer"
)

var (
	periodStartTime     time.Time
	periodEndTime       = time.Now()
	tmpBillingStartTime = time.Time{}
	tmpBillingEndTime   = time.Time{}
	maxBillingDuration  = minuteMilliseconds * defaultReportTimeWindowsPeriod
	gatheredTimeWindows []workertypes.TimeWindow
)

func (s *Server) startReportTimeWindowsTask(period int) {
	log.GetLogger().Infof("start the task of reporting the ConcurrentResourceTimeWindows")
	functionURN := utils.LocalFuncURN.String()
	podName := os.Getenv("PodName")
	if period == 0 {
		period = defaultReportTimeWindowsPeriod
	}
	maxBillingDuration = period * minuteMilliseconds
	runtime.ConResource.SetMaxBillingDuration(maxBillingDuration)
	tick := time.NewTicker(time.Duration(period) * time.Minute)
	for {
		select {
		case <-tick.C:
			if err := s.ReportConcurrentResourceTimeWindows(functionURN, podName); err != nil {
				log.GetLogger().Warnf("failed to ReportConcurrentResourceTimeWindows, %s", err.Error())
			}
		case _, ok := <-runtime.ProcessExitSignal:
			if !ok {
				tick.Stop()
			}
			log.GetLogger().Infof("stop the task of reporting the ConcurrentResourceTimeWindows")
			return
		}
	}
}

func isInvokeExist(timeWindow []workertypes.TimeWindow) bool {
	for _, value := range timeWindow {
		if value.Stage == constants.InvokeStage {
			return true
		}
	}
	return false
}

func configStartTime() {
	if tmpBillingStartTime.IsZero() {
		periodStartTime = periodEndTime
	} else {
		// [tmpBillingStartTime, now] will be billing
		periodStartTime = tmpBillingStartTime
		tmpBillingStartTime = time.Time{}
	}
}

func configEndTime() {
	if tmpBillingEndTime.IsZero() {
		periodEndTime = time.Now()
	} else {
		// [preEndTime, tmpBillingEndTime] will be billing
		periodEndTime = tmpBillingEndTime
	}
}

func (s *Server) generateRuntimeMetricsProperty(functionURN, podName string) (buscommon.RuntimeMetricsProperty, error) {
	configStartTime()
	configEndTime()
	allDuration := getAllDurationWithPeriod(periodStartTime, periodEndTime)
	timeWindows := runtime.ConResource.GetTimeWindows(periodStartTime, periodEndTime)
	num := len(timeWindows)
	if runtime.PodApplier != constants.WorkerManagerApplier {
		if num == 0 {
			log.GetLogger().Infof("timeWindowsEmpty | %s | %s", urnutils.FuncURNtoShort(functionURN), podName)
			return buscommon.RuntimeMetricsProperty{}, errors.New("timeWindows is empty")
		}
	} else {
		if num == 0 || !isInvokeExist(timeWindows) {
			log.GetLogger().Infof("timeWindows is empty, num: %d", num)
			gatheredTimeWindow := generateTimeWindows(periodStartTime, periodEndTime, allDuration)
			timeWindows = append(timeWindows, gatheredTimeWindow)
		}
	}
	timeWindowsLogMessage, err := json.Marshal(timeWindows)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal timeWindows, %s", err.Error())
		return buscommon.RuntimeMetricsProperty{}, err
	}
	s.billingLog.WriteLog("ConcurrentResourceTimeWindows", string(timeWindowsLogMessage))
	runtime.ConResource.RemoveReportedTimeWindows(len(timeWindows))
	// Combine multiple time windows into one
	gatheredTimeWindows = gatherTimeWindows(timeWindows, allDuration)
	data, err := json.Marshal(gatheredTimeWindows)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal gatheredTimeWindows, %s", err.Error())
		return buscommon.RuntimeMetricsProperty{}, err
	}
	isTriggeredBridgeFunction := s.conf.RuntimeConfig.IsBridgeFunction &&
		!strings.Contains(s.conf.RuntimeConfig.EntryFile+s.conf.RuntimeConfig.HandlerName,
			consumerBridgeFunctionKeyWords)
	runtimeMetricsProperty := buscommon.RuntimeMetricsProperty{
		PodName:                   podName,
		FunctionURN:               functionURN,
		MetricsName:               runtime.MetricsSourceName,
		MetricsValue:              string(data),
		EphemeralStorage:          s.conf.RuntimeConfig.ResourceMetaData.EphemeralStorage,
		GPUMem:                    s.conf.RuntimeConfig.ResourceMetaData.GpuMemory,
		IsTriggeredBridgeFunction: isTriggeredBridgeFunction,
		IsBridgeFunction:          s.conf.RuntimeConfig.IsBridgeFunction,
	}
	return runtimeMetricsProperty, nil
}

func (s *Server) generateRuntimePreStopMetricsProperty(functionURN, podName string) (*buscommon.RuntimeMetricsProperty,
	error) {
	configStartTime()
	configEndTime()
	allDuration := getAllDurationWithPeriod(periodStartTime, periodEndTime)
	timeWindows := make([]workertypes.TimeWindow, 1)
	timeWindows[0].Stage = constants.PreStopStage
	timeWindows[0].BillingDuration = allDuration
	timeWindows[0].BusyDuration = allDuration
	timeWindows[0].StartTime = periodStartTime.UnixMilli()
	timeWindows[0].EndTime = timeWindows[0].StartTime + int64(allDuration)
	timeWindows[0].IdleDuration = 0

	timeWindowsLogMessage, err := json.Marshal(timeWindows)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal timeWindows, %s", err.Error())
		return &buscommon.RuntimeMetricsProperty{}, err
	}
	s.billingLog.WriteLog("ConcurrentResourceTimeWindows", string(timeWindowsLogMessage))
	runtime.ConResource.RemoveReportedTimeWindows(len(timeWindows))
	// Combine multiple time windows into one
	gatheredTimeWindows = gatherPreStopTimeWindows(timeWindows, allDuration)
	data, err := json.Marshal(gatheredTimeWindows)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal gatheredTimeWindows, %s", err.Error())
		return &buscommon.RuntimeMetricsProperty{}, err
	}

	isTriggeredBridgeFunction := s.conf.RuntimeConfig.IsBridgeFunction &&
		!strings.Contains(s.conf.RuntimeConfig.EntryFile+s.conf.RuntimeConfig.HandlerName,
			consumerBridgeFunctionKeyWords)
	runtimeMetricsProperty := &buscommon.RuntimeMetricsProperty{
		FunctionURN:               functionURN,
		PodName:                   podName,
		MetricsName:               runtime.MetricsSourceName,
		MetricsValue:              string(data),
		GPUMem:                    s.conf.RuntimeConfig.ResourceMetaData.GpuMemory,
		EphemeralStorage:          s.conf.RuntimeConfig.ResourceMetaData.EphemeralStorage,
		IsTriggeredBridgeFunction: isTriggeredBridgeFunction,
		IsBridgeFunction:          s.conf.RuntimeConfig.IsBridgeFunction,
	}
	return runtimeMetricsProperty, nil
}

// ReportConcurrentResourceTimeWindows -
func (s *Server) ReportConcurrentResourceTimeWindows(functionURN, podName string) error {
	log.GetLogger().Debugf("started to report ConcurrentResourceTimeWindows")
	defer func() {
		runtime.SetReservedInstanceModeInCurrentTimeWindow()
	}()

	runtimeMetricsProperty, err := s.generateRuntimeMetricsProperty(functionURN, podName)
	if err != nil {
		if err.Error() == "timeWindows is empty" {
			log.GetLogger().Debugf("current timeWindows is empty.")
			return nil
		}
		log.GetLogger().Errorf("failed to generate RuntimeMetricsProperty, %s.", err.Error())
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), reportRuntimeContextTimeout)
	defer cancel()
	if s.bus == nil {
		return errors.New("connection with bus is nil")
	}
	response, err := s.bus.ReportRuntimeMetrics(ctx, &runtimeMetricsProperty)
	if err != nil {
		log.GetLogger().Warnf("failed to report runtime metrics %s, with err:%s",
			runtimeMetricsProperty.MetricsValue, err.Error())
		return err
	}
	if response.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report runtime metrics %s, "+
			"with errorCode:  %d, errorMessage:  %s",
			runtimeMetricsProperty.MetricsValue, response.ErrorCode, response.ErrorMessage)
		return err
	}
	removeReportedTimeWindows(len(gatheredTimeWindows))
	// succeeded to report
	log.GetLogger().Debugf("succeeded to report UsedResourceTimeWindows")
	return nil
}

// ReportPreStopInvokeTimeWindows -
func (s *Server) ReportPreStopInvokeTimeWindows(functionURN, podName string) error {
	log.GetLogger().Info("started to report ReportPreStopInvokeTimeWindows")
	defer func() {
		runtime.SetReservedInstanceModeInCurrentTimeWindow()
	}()

	runtimeMetricsProperty, err := s.generateRuntimePreStopMetricsProperty(functionURN, podName)
	if err != nil {
		if err.Error() == "timeWindows is empty" {
			log.GetLogger().Info("current timeWindows is empty")
			return nil
		}
		log.GetLogger().Errorf("failed to generate RuntimeMetricsProperty, %s", err.Error())
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), reportRuntimeContextTimeout)
	defer cancel()
	response, err := s.bus.ReportRuntimeMetrics(ctx, runtimeMetricsProperty)
	if err != nil {
		log.GetLogger().Warnf("failed to report runtime metrics %s, %s",
			runtimeMetricsProperty.MetricsValue, err.Error())
		return err
	}
	if response.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report runtime metrics %s, "+
			"errorCode: %d, errorMessage: %s",
			runtimeMetricsProperty.MetricsValue, response.ErrorCode, response.ErrorMessage)
		return err
	}
	removeReportedTimeWindows(len(gatheredTimeWindows))
	// succeeded to report
	log.GetLogger().Info("succeeded to report UsedResourceTimeWindows")
	return nil
}

func removeReportedTimeWindows(reportedNums int) {
	if reportedNums < len(gatheredTimeWindows) {
		gatheredTimeWindows = gatheredTimeWindows[reportedNums:]
		return
	}
	gatheredTimeWindows = nil
}

func generateTimeWindows(startTime, endTime time.Time, allDuration int) workertypes.TimeWindow {
	gatheredTimeWindow := workertypes.TimeWindow{
		StartTime:       timeutil.UnixMillisecond(startTime),
		EndTime:         timeutil.UnixMillisecond(endTime),
		BillingDuration: 0,
		BusyDuration:    0,
		IdleDuration:    allDuration,
		Stage:           constants.InvokeStage,
	}
	return gatheredTimeWindow
}

func getAllDurationWithPeriod(startTime, endTime time.Time) int {
	allDuration := int(runtime.CalculateBillingDuration(endTime.Sub(startTime)) / time.Millisecond)
	if allDuration >= maxBillingDuration {
		allDuration = maxBillingDuration
	}
	log.GetLogger().Infof("curAllDuration %d", allDuration)
	return allDuration
}

func gatherTimeWindows(timeWindows []workertypes.TimeWindow, allDuration int) []workertypes.TimeWindow {
	num := len(timeWindows)
	if timeWindows[0].Stage == constants.LoadStage {
		// The time window of the Load stage cannot be gathered with the time window of the invocation stage.
		gatheredTimeWindows = append(gatheredTimeWindows, timeWindows[0])
		if num == 1 {
			return gatheredTimeWindows
		}
		timeWindows = timeWindows[1:]
		num--
	}
	if timeWindows[0].Stage == constants.ExtensionStage {
		// The time window of the extension stage cannot be gathered with the time window of the invocation stage.
		gatheredTimeWindows = append(gatheredTimeWindows, timeWindows[0])
		if num == 1 {
			return gatheredTimeWindows
		}
		timeWindows = timeWindows[1:]
		num--
	}
	if timeWindows[0].Stage == constants.InitializeStage {
		// The time window of the initialization stage cannot be gathered with the time window of the invocation stage.
		gatheredTimeWindows = append(gatheredTimeWindows, timeWindows[0])
		if num == 1 {
			return gatheredTimeWindows
		}
		timeWindows = timeWindows[1:]
		num--
	}

	// gather all invoke BillingDuration
	var invokeTotalBillingDuration int
	for _, timeWindow := range timeWindows {
		if timeWindow.Stage == constants.InvokeStage {
			invokeTotalBillingDuration += timeWindow.BillingDuration
		}
	}
	if invokeTotalBillingDuration > allDuration {
		invokeTotalBillingDuration = allDuration
	}
	gatheredTimeWindow := workertypes.TimeWindow{
		StartTime:       timeWindows[0].StartTime,
		EndTime:         timeWindows[num-1].EndTime,
		BillingDuration: invokeTotalBillingDuration,
		BusyDuration:    invokeTotalBillingDuration,
		IdleDuration:    allDuration - invokeTotalBillingDuration,
		Stage:           constants.InvokeStage,
	}
	gatheredTimeWindows = append(gatheredTimeWindows, gatheredTimeWindow)
	return gatheredTimeWindows
}

func gatherPreStopTimeWindows(timeWindows []workertypes.TimeWindow, allDuration int) []workertypes.TimeWindow {
	if timeWindows[0].Stage == constants.PreStopStage {
		// The time window of the preStop stage cannot be gathered with the time window of the invocation stage.
		gatheredTimeWindows = append(gatheredTimeWindows, timeWindows[0])
	}

	return gatheredTimeWindows
}

// SetReservedInstanceStartTime -
func SetReservedInstanceStartTime() {
	periodStartTime = time.Now()
	periodEndTime = time.Now()
	log.GetLogger().Infof("set reserved instance start time")
}

// StartReservedInstanceFee -
func StartReservedInstanceFee() {
	if tmpBillingStartTime.IsZero() && !tmpBillingEndTime.IsZero() {
		tmpBillingStartTime = time.Now()
		tmpBillingEndTime = time.Time{}
		log.GetLogger().Infof("start to billing reserved instance fee")
	}
}

// StopReservedInstanceFee -
func StopReservedInstanceFee() {
	if tmpBillingEndTime.IsZero() {
		tmpBillingEndTime = time.Now()
		log.GetLogger().Infof("stop to billing reserved instance fee")
	}
}
