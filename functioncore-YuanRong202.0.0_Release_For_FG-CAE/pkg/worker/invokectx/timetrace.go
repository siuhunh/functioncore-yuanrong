/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package invokectx

import (
	"time"

	"functioncore/pkg/common/logger/log"
)

const (
	beforeElapsed = "("
	afterElapsed  = ") "
)

var tcGetter = DefaultMetrics.Register(func() Poolable { return NewTimeTraceContext() })

// GetTimeTraceContext retrives the Context from invokeCtx
func GetTimeTraceContext(ic InvokeCtx) *TimeTraceContext {
	ctxIf := tcGetter(ic)
	ctx, ok := ctxIf.(*TimeTraceContext)
	if !ok {
		log.GetLogger().Errorf("failed to assert TimeTraceContext")
		return nil
	}
	return ctx
}

type timeTraceElem struct {
	start         time.Time
	elapsed       time.Duration
	cachedElapsed string
}

// TimeTraceContext provides a way to collect time traces using invokeCtx
type TimeTraceContext struct {
	m map[string]*timeTraceElem
}

// NewTimeTraceContext -
func NewTimeTraceContext() *TimeTraceContext {
	return &TimeTraceContext{
		m: make(map[string]*timeTraceElem),
	}
}

// Reset implements inovkectx.Poolable
func (c *TimeTraceContext) Reset() {
	for _, elem := range c.m {
		elem.elapsed = 0
	}
}

// Start marks the start time of 'name'
func (c *TimeTraceContext) Start(name string, t time.Time) {
	e, ok := c.m[name]
	if !ok {
		e = &timeTraceElem{}
		c.m[name] = e
	}
	e.start = t
}

// End marks the end time of 'name'
func (c *TimeTraceContext) End(name string, t time.Time) {
	e, ok := c.m[name]
	if !ok {
		log.GetLogger().Warnf("key %s does not exist", name)
		return
	}
	e.elapsed = t.Sub(e.start)
}

// GetElapsed -
func (c *TimeTraceContext) GetElapsed(name string) time.Duration {
	e, ok := c.m[name]
	if !ok {
		return 0
	}
	return e.elapsed
}
