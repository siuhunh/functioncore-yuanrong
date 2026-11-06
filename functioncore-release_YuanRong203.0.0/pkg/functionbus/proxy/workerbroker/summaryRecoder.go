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

package workerbroker

import (
	"encoding/json"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
)

const (
	defaultReportTimeWindowsPeriod = 5 * time.Minute
	defaultCacheLength             = 100
	// 1ms（1,000us）
	meteringGranularity = 1000
)

// TimeWindow -
type TimeWindow struct {
	StartTime       int64  `json:"startTime"`       // unit: ms
	EndTime         int64  `json:"endTime"`         // unit: ms
	BillingDuration int    `json:"billingDuration"` // Charging time (unit: ms)
	BusyDuration    int    `json:"busyDuration"`    // instance busy time (unit: ms)
	IdleDuration    int    `json:"idleDuration"`    // instance idle time (unit: ms)
	Stage           string `json:"stage"`           // invoke or init
}

type stageSummaryInfo struct {
	startTime time.Time
	endTime   time.Time
	stageCost int64
	stage     string
}

// SummaryRecorder is used for bus billing
type SummaryRecorder struct {
	invokeSummaryInfo    []stageSummaryInfo
	nonInvokeSummaryInfo []stageSummaryInfo
	reportSummary        []TimeWindow
	summaryMux           sync.Mutex
	podName              string
	reqNum               uint64
	funcKeyWithResource  string
	runtimeMetrics       common.RuntimeMetricsProperty
	workerID             string
}

func newSummaryRecorder(broker *workerBroker, runtimeMetrics common.RuntimeMetricsProperty,
	stopCh chan struct{}) *SummaryRecorder {
	summary := &SummaryRecorder{
		workerID:            broker.workerID,
		funcKeyWithResource: broker.funcKeyWithResource,
		runtimeMetrics:      runtimeMetrics,
	}
	go summary.reportBillingInfo(stopCh)
	return summary
}

// StartBilling is used for starting req billing
func (s *SummaryRecorder) StartBilling(stage string, concurrency uint32, number uint64) {
	if stage == "" || ((stage == constants.InvokeStage || stage == constants.InitializeStage) && concurrency <= 1) {
		return
	}
	s.summaryMux.Lock()
	defer s.summaryMux.Unlock()
	if stage == constants.InvokeStage && concurrency > 1 {
		if s.reqNum == 0 {
			summary := stageSummaryInfo{
				stage:     stage,
				startTime: time.Now(),
			}
			s.invokeSummaryInfo = append(s.invokeSummaryInfo, summary)
		}
		s.reqNum = s.reqNum + number
		return
	}
	summary := stageSummaryInfo{
		stage:     stage,
		startTime: time.Now(),
	}
	s.nonInvokeSummaryInfo = append(s.nonInvokeSummaryInfo, summary)
	return
}

// StopBilling is used for stopping req billing
func (s *SummaryRecorder) StopBilling(stage string, concurrency uint32) {
	if stage == "" || ((stage == constants.InvokeStage || stage == constants.InitializeStage) && concurrency <= 1) {
		return
	}
	s.summaryMux.Lock()
	defer s.summaryMux.Unlock()
	if stage == constants.InvokeStage && concurrency > 1 {
		s.reqNum--
		invokeLength := len(s.invokeSummaryInfo)
		if (s.reqNum == 0 && invokeLength == 0) ||
			(s.reqNum == 0 && invokeLength != 0 && !s.invokeSummaryInfo[invokeLength-1].endTime.IsZero()) {
			log.GetLogger().Errorf("failed to stop bill, summaryInfo is empty or is finished, podName: %s", s.podName)
			return
		}
		if s.reqNum == 0 && invokeLength != 0 &&
			s.invokeSummaryInfo[invokeLength-1].endTime.IsZero() {
			s.invokeSummaryInfo[invokeLength-1].endTime = time.Now()
			subDuration := s.invokeSummaryInfo[invokeLength-1].endTime.Sub(s.invokeSummaryInfo[invokeLength-1].startTime)
			s.invokeSummaryInfo[invokeLength-1].stageCost = int64(CalculateBillingDuration(subDuration) / time.Millisecond)
		}
		return
	}
	nonInvokeLength := len(s.nonInvokeSummaryInfo)
	if (nonInvokeLength == 0) || (nonInvokeLength != 0 &&
		!s.nonInvokeSummaryInfo[nonInvokeLength-1].endTime.IsZero()) {
		log.GetLogger().Errorf("failed to stop bill, summaryInfo is empty or is finished, podName: %s", s.podName)
		return
	}
	if nonInvokeLength != 0 &&
		s.nonInvokeSummaryInfo[nonInvokeLength-1].endTime.IsZero() {
		s.nonInvokeSummaryInfo[nonInvokeLength-1].endTime = time.Now()
		subDuration := s.nonInvokeSummaryInfo[nonInvokeLength-1].endTime.Sub(
			s.nonInvokeSummaryInfo[nonInvokeLength-1].startTime)
		s.nonInvokeSummaryInfo[nonInvokeLength-1].stageCost = int64(CalculateBillingDuration(subDuration) / time.Millisecond)
	}
	return
}

func (s *SummaryRecorder) mergeInvokeStageBillingData() {
	var haveInvokeReq bool
	s.summaryMux.Lock()
	defer s.summaryMux.Unlock()
	invokeLength := len(s.invokeSummaryInfo)
	if invokeLength == 0 {
		return
	}
	if invokeLength > 0 && s.invokeSummaryInfo[len(s.invokeSummaryInfo)-1].endTime.IsZero() {
		s.invokeSummaryInfo[invokeLength-1].endTime = time.Now()
		subDuration := s.invokeSummaryInfo[invokeLength-1].endTime.Sub(s.invokeSummaryInfo[invokeLength-1].startTime)
		s.invokeSummaryInfo[len(s.invokeSummaryInfo)-1].stageCost =
			int64(CalculateBillingDuration(subDuration) / time.Millisecond)
		haveInvokeReq = true
	}
	var allCost int
	for _, value := range s.invokeSummaryInfo {
		allCost += int(value.stageCost)
	}
	invokeSummaryInfo := TimeWindow{
		StartTime:       timeutil.UnixMillisecond(s.invokeSummaryInfo[0].startTime),
		EndTime:         timeutil.UnixMillisecond(s.invokeSummaryInfo[invokeLength-1].endTime),
		Stage:           constants.InvokeStage,
		BillingDuration: allCost,
		BusyDuration:    allCost,
	}
	s.reportSummary = append(s.reportSummary, invokeSummaryInfo)
	if haveInvokeReq {
		s.invokeSummaryInfo = []stageSummaryInfo{
			{
				startTime: time.Now(),
				stage:     constants.InvokeStage,
			},
		}
	} else {
		s.invokeSummaryInfo = []stageSummaryInfo{}
	}
}

func (s *SummaryRecorder) mergeNonInvokeStageBillingData() {
	var haveNonInvokeReq bool
	var curReportTimeWindow []TimeWindow
	mergedStageTimeWindow := make(map[string]TimeWindow)
	s.summaryMux.Lock()
	defer s.summaryMux.Unlock()
	nonInvokeLength := len(s.nonInvokeSummaryInfo)
	var curStage string
	if nonInvokeLength == 0 {
		return
	}
	if len(s.nonInvokeSummaryInfo) > 0 && s.nonInvokeSummaryInfo[len(s.nonInvokeSummaryInfo)-1].endTime.IsZero() {
		s.nonInvokeSummaryInfo[nonInvokeLength-1].endTime = time.Now()
		subDuration := s.nonInvokeSummaryInfo[nonInvokeLength-1].endTime.Sub(
			s.nonInvokeSummaryInfo[nonInvokeLength-1].startTime)
		s.nonInvokeSummaryInfo[nonInvokeLength-1].stageCost = int64(CalculateBillingDuration(subDuration) / time.Millisecond)
		haveNonInvokeReq = true
		curStage = s.nonInvokeSummaryInfo[nonInvokeLength-1].stage
	}
	for _, value := range s.nonInvokeSummaryInfo {
		summaryInfo, ok := mergedStageTimeWindow[value.stage]
		if !ok {
			mergedStageTimeWindow[value.stage] = TimeWindow{
				StartTime:       timeutil.UnixMillisecond(value.startTime),
				EndTime:         timeutil.UnixMillisecond(value.endTime),
				Stage:           value.stage,
				BusyDuration:    int(value.stageCost),
				BillingDuration: int(value.stageCost),
			}
		} else {
			summaryInfo.EndTime = timeutil.UnixMillisecond(value.endTime)
			summaryInfo.BusyDuration = summaryInfo.BusyDuration + int(value.stageCost)
			summaryInfo.BillingDuration = summaryInfo.BillingDuration + int(value.stageCost)
			mergedStageTimeWindow[value.stage] = summaryInfo
		}
	}
	for _, value := range mergedStageTimeWindow {
		curReportTimeWindow = append(curReportTimeWindow, value)
	}
	s.reportSummary = append(s.reportSummary, curReportTimeWindow...)
	if haveNonInvokeReq {
		s.nonInvokeSummaryInfo = []stageSummaryInfo{
			{
				startTime: time.Now(),
				stage:     curStage,
			},
		}
	} else {
		s.nonInvokeSummaryInfo = []stageSummaryInfo{}
	}
}

func (s *SummaryRecorder) mergeAndReportBillData() error {
	s.mergeNonInvokeStageBillingData()
	s.mergeInvokeStageBillingData()
	s.summaryMux.Lock()
	summaryLength := len(s.reportSummary)
	if summaryLength == 0 {
		s.summaryMux.Unlock()
		return nil
	}
	if len(s.reportSummary) >= defaultCacheLength {
		s.reportSummary = s.reportSummary[len(s.reportSummary)-defaultCacheLength : len(s.reportSummary)]
	}
	data, err := json.Marshal(s.reportSummary)
	s.summaryMux.Unlock()
	if err != nil {
		log.GetLogger().Errorf("failed to marshal reportSummary, workerID: %s, err: %s", s.workerID, err.Error())
		return err
	}
	s.runtimeMetrics.MetricsValue = string(data)
	log.GetLogger().Debugf("current %s billing data %s", s.workerID, string(data))
	if err := workerevaluator.Client.ReportUsedResourceTimeWindows(s.runtimeMetrics,
		s.funcKeyWithResource); err != nil {
		log.GetLogger().Errorf("failed to report usedResourceTimeWindows, "+
			"workerID: %s, functionKey %s, %s", s.workerID,
			urnutils.AnonymizeTenantKey(s.funcKeyWithResource), err.Error())
		return err
	}
	s.summaryMux.Lock()
	s.reportSummary = []TimeWindow{}
	s.summaryMux.Unlock()
	return nil
}

func (s *SummaryRecorder) reportBillingInfo(stopCh chan struct{}) {
	log.GetLogger().Infof("start the task of reporting the %s billing data", s.workerID)
	tick := time.NewTicker(defaultReportTimeWindowsPeriod)
	for {
		select {
		case <-tick.C:
			if err := s.mergeAndReportBillData(); err != nil {
				log.GetLogger().Warnf("failed to reportBillData, podName: %s, err %s", s.podName, err.Error())
			}
		case <-stopCh:
			if err := s.mergeAndReportBillData(); err != nil {
				log.GetLogger().Warnf("failed to reportBillData, podName: %s, err %s", s.podName, err.Error())
			}
			tick.Stop()
			log.GetLogger().Infof("stop the task of reporting the %s billing data", s.workerID)
			return
		}
	}
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
