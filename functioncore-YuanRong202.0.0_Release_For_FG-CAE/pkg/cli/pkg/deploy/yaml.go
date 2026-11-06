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

package deploy

// FunctionConfig function config
type FunctionConfig struct {
	Function `yaml:"function"`
}

// Function function info
type Function struct {
	Name                string            `yaml:"name"`
	Service             string            `yaml:"service"`
	FunctionCodePath    string            `yaml:"functionCodePath,omitempty"`
	MinInstance         int               `yaml:"minInstance,omitempty"`
	MaxInstance         int               `yaml:"maxInstance,omitempty"`
	ConcurrentNum       int               `yaml:"concurrentNum,omitempty"`
	Handler             string            `yaml:"handler"`
	Description         string            `yaml:"description,omitempty"`
	UserData            map[string]string `yaml:"userData,omitempty"`
	EncryptedUserData   map[string]string `yaml:"encryptedUserData,omitempty"`
	Runtime             string            `yaml:"runtime,omitempty"`
	MemorySize          int               `yaml:"memorySize,omitempty"`
	EnableDynamicMemory bool              `yaml:"enableDynamicMemory,omitempty"`
	Timeout             int               `yaml:"timeout,omitempty"`
	Layers              []string          `yaml:"layers,omitempty"`
	InitializerTimeout  int               `yaml:"initializerTimeout,omitempty"`
	InitializerHandler  string            `yaml:"initializerHandler,omitempty"`
	IsStatefulFunction  bool              `yaml:"isStatefulFunction,omitempty"`
	Image               string            `yaml:"image,omitempty"`
	PreStopTimeout      int               `yaml:"preStopTimeout,omitempty"`
	PreStopHandler      string            `yaml:"preStopHandler,omitempty"`
	PreStopType         string            `yaml:"preStopType,omitempty"`
	EnableAuthInHeader  bool              `yaml:"enableAuthInHeader,omitempty"`
	AppXRole            string            `yaml:"appXRole,omitempty"`
	Agency              Agency            `yaml:"agency,omitempty"`
	DisableInsecureAkSk bool              `yaml:"disableInsecureAkSk,omitempty"`
	Version             string            `yaml:"-"`
	CPU                 int               `yaml:"-"`
	TenantID            string            `yaml:"-"`
}

// Agency -
type Agency struct {
	AccessKey     string `yaml:"accessKey"`
	SecretKey     string `yaml:"secretKey"`
	Token         string `yaml:"token"`
	SecurityAk    string `yaml:"securityAk"`
	SecuritySk    string `yaml:"securitySk"`
	SecurityToken string `yaml:"securityToken"`
}
