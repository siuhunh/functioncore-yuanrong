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

// Package main is entry of etcdalarm process
package main

import (
	"flag"
	"os"
	"path/filepath"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/tools/etcd-deploy/etcdalarm/alarm"
	"functioncore/tools/etcd-deploy/etcdalarm/config"
)

const (
	defaultConfigFilePath = "/home/etcd/alarm/config.json"
)

var (
	configFilePath string
)

var (
	alarmType string
	category  string
)

func initCmd() {
	flag.StringVar(&alarmType, "alarmType", "",
		"specify whether to report or clear alarms. Options: report or clear")
	flag.StringVar(&configFilePath, "config", defaultConfigFilePath,
		"specify the path of the alarm binary configuration file")
	flag.Parse()
	switch alarmType {
	case "report":
		category = "1"
	case "clear":
		category = "2"
	default:
		{
			flag.Usage()
			os.Exit(1)
		}
	}
}

func main() {
	initCmd()
	var (
		cfg *config.Config
		err error
	)
	configFilePath, err = filepath.Abs(configFilePath)
	if err != nil {
		log.GetLogger().Errorf("invalid path, err: %s", err.Error())
		os.Exit(1)
	}
	cfg, err = config.LoadConfig(configFilePath)
	if err != nil {
		log.GetLogger().Errorf("Init Alarm Config Err: %s", err.Error())
		os.Exit(1)
	}
	if err = config.SetTLSEnvValues(cfg.AuthConfig); err != nil {
		log.GetLogger().Errorf("set tls env err: %s", err.Error())
		os.Exit(1)
	}
	err = tls.InitTLSConfig(cfg.AuthConfig.TLSProtocol, cfg.AuthConfig.TLSCiphers)
	if err != nil {
		log.GetLogger().Errorf("init tls config err: %s", err.Error())
		os.Exit(1)
	}
	w, err := alarm.NewAlarmClient(tls.GetClientTLSConfig(), cfg)
	if err != nil {
		log.GetLogger().Errorf("Init Alarm Client err:%s", err.Error())
		os.Exit(1)
	}

	w.AlarmInfo = alarm.InitAlarmInfo(category, cfg.AlarmInfo, cfg.LocationInfo)
	req, err := w.GenAlarmRequest(cfg.AuthConfig.AKey, cfg.AuthConfig.SKey)
	if err != nil {
		log.GetLogger().Errorf("failed to gen alarm request, err: %s", err.Error())
		os.Exit(1)
	}
	w.NotifyAlarm(req)
}
