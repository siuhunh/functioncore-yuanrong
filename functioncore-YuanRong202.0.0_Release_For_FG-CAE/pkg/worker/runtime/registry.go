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
	"errors"
	"fmt"

	"functioncore/pkg/worker/config"
)

const (
	maxRuntimeNum = 20
)

// Registry is global singleton registry for worker's runtime
var Registry = Reg{
	creators: make(map[string]Creator, maxRuntimeNum),
}

// Creator create runtime instance
type Creator interface {
	NewRuntime(configuration *config.Configuration, handlers Handlers) (Runtime, error)
}

// PrestartableCreator defines a creator that needs to do something before NewRuntime. Prestart is called every time
// a new runtime process starts.
type PrestartableCreator interface {
	Creator
	Prestart(c *config.RuntimeConfig) error
}

// Reg -
type Reg struct {
	creators map[string]Creator
}

// Register register runtime type
func (r *Reg) Register(kind string, creator Creator) {
	Registry.creators[kind] = creator
}

// NewRuntime -
func (r *Reg) NewRuntime(kind string, configuration *config.Configuration, handlers Handlers) (Runtime, error) {
	creator, ok := r.creators[kind]
	if !ok || creator == nil {
		return nil, errors.New("unregister runtime type")
	}
	return creator.NewRuntime(configuration, handlers)
}

// Prestart -
func (r *Reg) Prestart(kind string, c *config.RuntimeConfig) error {
	creator, ok := r.creators[kind]
	if !ok || creator == nil {
		return fmt.Errorf("unregister runtime type: %s", kind)
	}
	prestartableCreator, ok := creator.(PrestartableCreator)
	if !ok {
		return nil
	}
	return prestartableCreator.Prestart(c)
}

// GetCreator -
func (r *Reg) GetCreator(kind string) Creator {
	return r.creators[kind]
}
