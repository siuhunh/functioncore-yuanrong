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

// Package utils get config
package utils

import (
	"strings"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/runtime-manager/config"
)

var (
	runtimeManagerCfgOnce sync.Once
	runtimeManagerCfg     *config.Config
)

// GetRuntimeManagerCfg get runtime manager config
func GetRuntimeManagerCfg() *config.Config {
	runtimeManagerCfgOnce.Do(func() {
		cfg, err := config.GetConfig()
		if err != nil {
			log.GetLogger().Errorf("cannot get runtime manager config: %s", err.Error())
			return
		}
		runtimeManagerCfg = cfg
	})
	return runtimeManagerCfg
}

// IsRegisterRuntimeType -
func IsRegisterRuntimeType(runtimeLanguage string) bool {
	// custom-image or custom image no need to pre start, so it is not in runtime manager config
	// but custom-image or custom image is register runtime type, so it will return true
	if strings.HasPrefix(runtimeLanguage, "custom") {
		return true
	}
	cfg := GetRuntimeManagerCfg()
	if cfg == nil {
		return false
	}
	if cfg.RuntimeConfigs == nil {
		return false
	}
	_, found := cfg.RuntimeConfigs[runtimeLanguage]
	return found
}
