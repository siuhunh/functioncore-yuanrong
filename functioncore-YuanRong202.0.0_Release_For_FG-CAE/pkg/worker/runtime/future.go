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
	"container/list"
	"fmt"
	"sync"

	"functioncore/pkg/common/logger/log"
	buscommon "functioncore/pkg/functionbus/common"
)

type futureTracer struct {
	traces map[string]*trace // caller's futureID => callee's futureIDs
	mux    sync.Mutex
}

type trace struct {
	records  *list.List
	m        map[string]*list.Element
	canceled bool
}

func newFutureTracer() *futureTracer {
	return &futureTracer{
		traces: make(map[string]*trace),
	}
}

func (f *futureTracer) increaseRequestCount(caller string) int {
	f.mux.Lock()
	res := len(f.traces)
	_, ok := f.traces[caller]
	if ok {
		f.mux.Unlock()
		log.GetLogger().Warnf("failed to increase request count %s. caller already exists.", caller)
		return res
	}

	f.traces[caller] = &trace{
		records: list.New(),
		m:       make(map[string]*list.Element, 1),
	}
	f.mux.Unlock()
	return res + 1
}

func (f *futureTracer) decreaseRequestCount(caller string) int {
	f.mux.Lock()
	delete(f.traces, caller)
	res := len(f.traces)
	f.mux.Unlock()
	return res
}

func (f *futureTracer) getRequestCount() int {
	f.mux.Lock()
	res := len(f.traces)
	f.mux.Unlock()
	return res
}

func (f *futureTracer) getRecoverProperty() buscommon.RecoverProperty {
	f.mux.Lock()
	res := buscommon.RecoverProperty{
		Requests: make(map[string]*buscommon.RecoverTraceRecord, len(f.traces)),
	}
	for futureID, trace := range f.traces {
		records := f.getRecordsWithLock(trace)
		res.Requests[futureID] = &buscommon.RecoverTraceRecord{
			Records: records,
		}
	}
	f.mux.Unlock()
	return res
}

// Add adds a trace record (caller and callee's futureID)
func (f *futureTracer) add(caller, callee string) {
	f.mux.Lock()
	defer f.mux.Unlock()

	t, ok := f.traces[caller]
	if !ok {
		log.GetLogger().Warnf("failed to remove from caller %s. caller does not exist.", caller)
		return
	}

	if _, ok := t.m[callee]; ok {
		log.GetLogger().Warnf("failed to add to caller %s. callee %s already exists.", caller, callee)
		return
	}
	ele := t.records.PushBack(callee)
	t.m[callee] = ele
}

// Remove removes a trace record
func (f *futureTracer) remove(caller, callee string) {
	f.mux.Lock()
	defer f.mux.Unlock()

	t, ok := f.traces[caller]
	if !ok {
		log.GetLogger().Warnf("failed to remove from caller %s. caller does not exist.", caller)
		return
	}

	ele, ok := t.m[callee]
	if !ok {
		log.GetLogger().Warnf("failed to remove from caller %s. callee %s does not exist.", caller, callee)
		return
	}

	_ = t.records.Remove(ele)
}

func (f *futureTracer) cancel(caller string) ([]string, bool) {
	f.mux.Lock()
	defer f.mux.Unlock()
	t, ok := f.traces[caller]
	if !ok {
		log.GetLogger().Warnf("failed to get caller %s. caller does not exist.", caller)
		return nil, true
	}
	if t.canceled {
		return nil, true
	}

	t.canceled = true
	return f.getRecordsWithLock(t), false
}

func (f *futureTracer) isCanceled(caller string) (bool, error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	t, ok := f.traces[caller]
	if !ok {
		log.GetLogger().Warnf("failed to get caller %s. caller does not exist.", caller)
		return false, fmt.Errorf("unknown caller futureID: %s", caller)
	}

	return t.canceled, nil
}

func (f *futureTracer) getRecordsWithLock(t *trace) []string {
	res := make([]string, 0, t.records.Len())
	for ele := t.records.Front(); ele != nil; ele = ele.Next() {
		callee, ok := ele.Value.(string)
		if !ok {
			log.GetLogger().Warnf("failed to assert")
			return nil
		}
		res = append(res, callee)
	}
	return res
}
