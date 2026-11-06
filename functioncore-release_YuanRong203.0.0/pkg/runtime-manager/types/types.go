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

// Package types describe the instruction type
package types

import (
	"sync/atomic"

	"functioncore/pkg/worker/config"
)

// AtomicErrorCode -
type AtomicErrorCode struct {
	code int64
}

// Override update date error code
func (a *AtomicErrorCode) Override(code int) {
	atomic.StoreInt64(&a.code, int64(code))
}

// Set -
func (a *AtomicErrorCode) Set(code int) {
	// only the first 'Set' is effective
	atomic.CompareAndSwapInt64(&a.code, 0, int64(code))
}

// Get -
func (a *AtomicErrorCode) Get() int {
	return int(atomic.LoadInt64(&a.code))
}

// Reset -
func (a *AtomicErrorCode) Reset() {
	atomic.StoreInt64(&a.code, 0)
}

// Option -
type Option struct {
	f func(*CreateOptions)
}

// CreateOptions -
type CreateOptions struct {
	Conf              *config.Configuration
	UID               int
	GID               int
	Version           string
	DebugEnv          []string
	Name              string
	IsMcpFunction     bool
	NPMConfigRegistry string
	UVIndexURL        string
}

// WithVersion -
func WithVersion(version string) Option {
	return Option{f: func(op *CreateOptions) {
		op.Version = version
	}}
}

// WithDebugEnv -
func WithDebugEnv(debugEnv []string) Option {
	return Option{f: func(op *CreateOptions) {
		op.DebugEnv = debugEnv
	}}
}

// WithName -
func WithName(name string) Option {
	return Option{f: func(op *CreateOptions) {
		op.Name = name
	}}
}

// NewCreateOptions -
func NewCreateOptions(uid, gid int, conf *config.Configuration, ops ...Option) *CreateOptions {
	opt := &CreateOptions{
		UID:  uid,
		GID:  gid,
		Conf: conf,
	}
	for _, do := range ops {
		do.f(opt)
	}
	return opt
}

// WithIsMcpFunction -
func (c *CreateOptions) WithIsMcpFunction(isMcpFunction bool) *CreateOptions {
	c.IsMcpFunction = isMcpFunction
	return c
}

// WithNPMConfigRegistry -
func (c *CreateOptions) WithNPMConfigRegistry(npmConfigRegistry string) *CreateOptions {
	c.NPMConfigRegistry = npmConfigRegistry
	return c
}

// WithUVIndexURL -
func (c *CreateOptions) WithUVIndexURL(uvIndexURL string) *CreateOptions {
	c.UVIndexURL = uvIndexURL
	return c
}
