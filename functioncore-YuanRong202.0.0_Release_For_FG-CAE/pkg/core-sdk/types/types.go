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

// Package types contains struct of core sdk module
package types

import "functioncore/pkg/common/types"

// CreateStateConfig create state param
type CreateStateConfig struct {
	TenantID      string
	SnServiceID   string
	ProxyFunction string
	TraceID       string
	Creator       string
	BusinessID    string
	FuncName      string
	Version       string
	CreateOptions *types.StateCreateOptions
}

// StateRecoveryConfig state recovery param
type StateRecoveryConfig struct {
	CreateStateConfig
	FuncName         string
	StateID          string
	RealValue        string
	LocalExistInETCD bool
	RealExistInETCD  bool
}
