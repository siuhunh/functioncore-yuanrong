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

// Package monitor monitors and controls resource usage
package monitor

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
)

// MemMonitor monitor memory usage
type MemMonitor interface {
	// Allow returns whether you can take some memory to use (in bytes)
	Allow(uint64) bool
	// AllowByLowerThreshold returns whether a function can take some memory to use (in bytes)
	AllowByLowerThreshold(string, string, uint64) bool
	// ReleaseFunctionMem release function mem when request finished
	ReleaseFunctionMem(urn string, size uint64)
	// AllowWithCondition returns whether you can take some memory to use (in bytes)
	AllowWithCondition(func() bool) bool
}

const (
	defaultMemoryRefreshInterval = 50
	highMemoryPercent            = 0.9
	statefulHighMemPercent       = 0.9
	base                         = 10
	bitSize                      = 64
	lowerMemoryPercent           = 0.7
	bodyThreshold                = 10000
	memoryPath                   = "/sys/fs/cgroup/memory/memory.stat"
)

var (
	memory = struct {
		sync.Once
		monitor *memMonitor
		err     error
	}{
		monitor: &memMonitor{},
	}
	mu                        sync.Mutex
	curMemUsageReachThreshold uint32
)

var (
	config = &types.MemoryControlConfig{
		LowerMemoryPercent:     lowerMemoryPercent,
		HighMemoryPercent:      highMemoryPercent,
		StatefulHighMemPercent: statefulHighMemPercent,
		BodyThreshold:          bodyThreshold,
		MemDetectIntervalMs:    defaultMemoryRefreshInterval,
	}
)

type memMonitor struct {
	enable            bool
	used              uint64
	threshold         uint64
	statefulThreshold uint64
	stopCh            chan struct{}
	lowerThreshold    uint64
	memMapMutex       sync.Mutex
	functionMemMap    map[string]uint64
	totalMemCnt       uint64
	isStateful        bool
}

// SetMemoryControlConfig set memory control config from different service
func SetMemoryControlConfig(memoryControlConfig *types.MemoryControlConfig) {
	if memoryControlConfig == nil {
		return
	}
	if memoryControlConfig.LowerMemoryPercent > 0 {
		config.LowerMemoryPercent = memoryControlConfig.LowerMemoryPercent
	}
	if memoryControlConfig.BodyThreshold > 0 {
		config.BodyThreshold = memoryControlConfig.BodyThreshold
	}
	if memoryControlConfig.MemDetectIntervalMs > 0 {
		config.MemDetectIntervalMs = memoryControlConfig.MemDetectIntervalMs
	}
	if memoryControlConfig.HighMemoryPercent > 0 {
		config.HighMemoryPercent = memoryControlConfig.HighMemoryPercent
	}
	if memoryControlConfig.StatefulHighMemPercent > 0 {
		config.StatefulHighMemPercent = memoryControlConfig.StatefulHighMemPercent
	}
	log.GetLogger().Infof("LowerMemoryPercent %f, HighMemoryPercent %f, "+
		"StatefulHighMemPercent %f, BodyThreshold %d, MemDetectIntervalMs %d",
		config.LowerMemoryPercent, config.HighMemoryPercent,
		config.StatefulHighMemPercent, config.BodyThreshold, config.MemDetectIntervalMs)

	if memory.monitor != nil {
		memory.monitor.updateConfig()
	}
}

// InitMemMonitor initialize global memory monitor
func InitMemMonitor() error {
	memory.Do(func() {
		memory.err = memory.monitor.init()
	})
	return memory.err
}

// GetMemInstance returns global memory monitor
func GetMemInstance() MemMonitor {
	return memory.monitor
}

func setMemMonitorStateful(stateful bool) MemMonitor {
	v := memory.monitor
	v.isStateful = stateful
	return v
}

func readValue(path string) (uint64, error) {
	v, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return parseValue(strings.TrimSpace(string(v)), base, bitSize)
}

func parseValue(s string, base, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, base, bitSize)
	if err != nil {
		intValue, intErr := strconv.ParseInt(s, base, bitSize)
		if intErr == nil && intValue < 0 {
			return 0, nil
		}
		if intErr != nil &&
			intErr.(*strconv.NumError).Err == strconv.ErrRange &&
			intValue < 0 {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

// refresh actual memory usage
func (m *memMonitor) refreshActualMemoryUsage(curPid int, pageSize uint64) {
	interval := config.MemDetectIntervalMs
	parser, err := NewCGroupMemoryParser()
	if err != nil {
		log.GetLogger().Warnf("failed to create cgroup memory parser: %s", err.Error())
		return
	}
	defer parser.Close()
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			val, err := parser.Read()
			if err != nil {
				log.GetLogger().Errorf("GetSystemMemoryUsed failed, err: %s", err.Error())
				continue
			}
			used, ok := val.(uint64)
			if !ok {
				log.GetLogger().Errorf("GetSystemMemoryUsed failed, err: failed to assert parser data")
				continue
			}
			atomic.StoreUint64(&m.used, used)
			curRise := atomic.LoadUint32(&curMemUsageReachThreshold)
			if used >= m.threshold && curRise == 0 {
				atomic.StoreUint32(&curMemUsageReachThreshold, 1)
			} else if used < m.threshold && curRise == 1 {
				atomic.StoreUint32(&curMemUsageReachThreshold, 0)
			}
			if interval != config.MemDetectIntervalMs {
				log.GetLogger().Infof("MemDetectIntervalMs updated, old: %d, new: %d, reset timer",
					interval, config.MemDetectIntervalMs)
				interval = config.MemDetectIntervalMs
				ticker.Reset(time.Duration(interval) * time.Millisecond)
			}
		case <-m.stopCh:
			log.GetLogger().Info("memory monitor stopped")
			ticker.Stop()
			return
		}
	}
}

func (m *memMonitor) init() error {
	memLimit, err := readValue("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		log.GetLogger().Warn("failed to read limit_in_bytes")
		return nil
	}

	m.threshold = uint64(float64(memLimit) * config.HighMemoryPercent)
	m.statefulThreshold = uint64(float64(memLimit) * config.StatefulHighMemPercent)
	m.enable = true
	m.memMapMutex = sync.Mutex{}
	m.functionMemMap = map[string]uint64{}
	m.lowerThreshold = uint64(float64(memLimit) * config.LowerMemoryPercent)
	log.GetLogger().Infof("memory threshold is %d, stateful memory threshold is %d, lowerThreshold is %d",
		m.threshold, m.statefulThreshold, m.lowerThreshold)

	m.stopCh = make(chan struct{})

	curPid := os.Getpid()
	pageSize := uint64(os.Getpagesize())

	go m.refreshActualMemoryUsage(curPid, pageSize)

	return nil
}

// Allow returns whether you can take some memory to use (in bytes)
func (m *memMonitor) Allow(want uint64) bool {
	if !m.enable {
		return true
	}
	for {
		threshold := m.threshold
		if m.isStateful {
			threshold = m.statefulThreshold
		}
		current := atomic.LoadUint64(&m.used)
		if current > threshold || want > threshold-current {
			log.GetLogger().Errorf("memory threshold triggered, current=%d want=%d threshold=%d",
				current, want, threshold)
			return false
		}
		if atomic.CompareAndSwapUint64(&m.used, current, current+want) {
			return true
		}
	}
}

func (m *memMonitor) AllowWithCondition(condition func() bool) bool {
	if !m.enable {
		return true
	}
	current := atomic.LoadUint64(&m.used)
	if current > m.threshold {
		log.GetLogger().Errorf("memory threshold triggered, current=%d threshold=%d",
			current, m.threshold)
		return false
	} else if current > m.lowerThreshold {
		log.GetLogger().Errorf("lower memory threshold triggered, current=%d lowerThreshold=%d", current, m.lowerThreshold)
		if condition == nil || !condition() {
			log.GetLogger().Errorf("the condition is not met, the request is rejected, "+
				"current=%d lowerThreshold=%d", current, m.lowerThreshold)
			return false
		}
	}
	return true
}

func (m *memMonitor) increaseMemCnt(size uint64) {
	m.totalMemCnt += size
}

func (m *memMonitor) decreaseMemCnt(size uint64) {
	if m.totalMemCnt < size {
		log.GetLogger().Warnf("invalid mem cnt %d, size %d", m.totalMemCnt, size)
		m.totalMemCnt = 0
	} else {
		m.totalMemCnt -= size
	}
}

// ReleaseFunctionMem release function mem when function req finished
func (m *memMonitor) ReleaseFunctionMem(urn string, size uint64) {
	if !m.enable || size <= config.BodyThreshold {
		return
	}

	m.memMapMutex.Lock()
	defer m.memMapMutex.Unlock()

	memUsed, ok := m.functionMemMap[urn]
	if !ok {
		return
	}

	m.decreaseMemCnt(size)
	if memUsed <= size {
		delete(m.functionMemMap, urn)
	} else {
		m.functionMemMap[urn] = memUsed - size
	}
}

// mallocFunctionMem malloc function mem when function req enter
func (m *memMonitor) mallocFunctionMem(urn string, realSize uint64) {
	m.increaseMemCnt(realSize)
	memUsed, ok := m.functionMemMap[urn]
	if !ok {
		m.functionMemMap[urn] = realSize
	} else {
		m.functionMemMap[urn] = memUsed + realSize
	}
}

// AllowByLowerThreshold control memory use by LowerThreshold
// if used memory > LowerThreshold and function mem use > average, this function just return heavy load
func (m *memMonitor) AllowByLowerThreshold(urn string, traceID string, size uint64) bool {
	if !m.enable || size <= config.BodyThreshold {
		return true
	}

	m.memMapMutex.Lock()
	defer m.memMapMutex.Unlock()
	// if current mem lower than lowerThreshold, allow
	current := atomic.LoadUint64(&m.used)
	if current <= m.lowerThreshold && m.totalMemCnt <= m.lowerThreshold {
		m.mallocFunctionMem(urn, size)
		return true
	}

	memUsed, ok := m.functionMemMap[urn]
	// if it's new function, allow
	if !ok {
		m.increaseMemCnt(size)
		m.functionMemMap[urn] = size
		return true
	}

	functionNum := uint64(len(m.functionMemMap))
	// if function use mem lower than averageMem allow
	averageMem := m.totalMemCnt / (functionNum + 1)
	if memUsed <= averageMem {
		m.increaseMemCnt(size)
		m.functionMemMap[urn] = memUsed + size
		return true
	}

	log.GetLogger().Errorf("lower memory threshold triggered, currentFromSys=%d,currentFromEvaluator=%d,"+
		"lowerThreshold=%d,functionUsed=%d,functionNum=%d,traceID=%s,bodyLength=%d",
		current, m.totalMemCnt, m.lowerThreshold, memUsed, functionNum, traceID, size)
	return false
}

func (m *memMonitor) updateConfig() {
	memLimit, err := readValue("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		log.GetLogger().Warn("failed to read limit_in_bytes")
		return
	}

	m.threshold = uint64(float64(memLimit) * config.HighMemoryPercent)
	m.statefulThreshold = uint64(float64(memLimit) * config.StatefulHighMemPercent)
	m.lowerThreshold = uint64(float64(memLimit) * config.LowerMemoryPercent)

	log.GetLogger().Infof("config updated, memory threshold is %d, stateful memory threshold is %d,lowerThreshold is %d",
		m.threshold, m.statefulThreshold, m.lowerThreshold)
}

// IsAllowByMemory returns whether you can take some memory to use
func IsAllowByMemory(urn string, memoryWant uint64, traceID string) bool {
	if !GetMemInstance().Allow(memoryWant) {
		log.GetLogger().Errorf("request is limited by higher threshold, urn %s traceID %s want %d",
			urn, traceID, memoryWant)
		return false
	}

	if !GetMemInstance().AllowByLowerThreshold(urn, traceID, memoryWant) {
		log.GetLogger().Errorf("request is limited by lower threshold, urn %s traceID %s want %d",
			urn, traceID, memoryWant)
		return false
	}

	return true
}

// DistinguishMemAllowByStateful separate the req function req from stateful and non-stateful
func DistinguishMemAllowByStateful(urn string, memoryWant uint64, traceID string, isStateful bool) bool {
	mu.Lock()
	defer mu.Unlock()
	if !isStateful {
		return IsAllowByMemory(urn, memoryWant, traceID)
	}

	defer memory.monitor.resetMemStateful()

	if !setMemMonitorStateful(isStateful).Allow(memoryWant) {
		log.GetLogger().Errorf("stateful request is limited by higher stateful threshold, urn %s traceID %s want %d",
			urn, traceID, memoryWant)
		return false
	}
	return true
}

func (m *memMonitor) resetMemStateful() {
	m.isStateful = false
}

// StatefulFunctionReqMemSeparator bus proxy separate the request of stateful function invoke
func StatefulFunctionReqMemSeparator(isStateful bool, memoryUsed uint64) bool {
	mu.Lock()
	defer mu.Unlock()
	if !isStateful {
		return GetMemInstance().Allow(memoryUsed)
	}
	defer memory.monitor.resetMemStateful()
	if !setMemMonitorStateful(isStateful).Allow(memoryUsed) {
		return false
	}
	return true
}

// ReachMemoryThreshold -
func ReachMemoryThreshold() bool {
	curRise := atomic.LoadUint32(&curMemUsageReachThreshold)
	return curRise != 0
}
