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

// Package main is entry of ntp-check process
package main

import (
	"flag"
	"path/filepath"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/ntpcheck"
)

const cfgPath = "ntp_check_config.json"

var configPath string

func initCmd() {
	flag.StringVar(&configPath, "config", cfgPath, "specify the configuration directory")
	flag.Parse()
}

func main() {
	initCmd()
	var err error
	configPath, err = filepath.Abs(configPath)
	if err != nil {
		log.GetLogger().Errorf("failed to abs file.path: %s, error: %s", configPath, err.Error())
		return
	}
	err = ntpcheck.InitConfig(configPath)
	if err != nil {
		log.GetLogger().Errorf("failed to init config: %s", err.Error())
		return
	}

	err = ntpcheck.InitAlarmClient()
	if err != nil {
		log.GetLogger().Errorf("alarm client init error: %s", err.Error())
		return
	}

	ntpcheck.RegularNtpCheck()
}
