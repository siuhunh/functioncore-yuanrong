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

// Package main is entry of etcdalarm process
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/tools/autoscaleralarm/alarm"
	"functioncore/tools/autoscaleralarm/config"
)

const (
	defaultConfigFilePath = "/home/autoscaler/alarm/config.json"
)

var (
	configFilePath string
)

func initCmd() {

	flag.StringVar(&configFilePath, "config", defaultConfigFilePath,
		"specify the path of the alarm binary configuration file")
	flag.Parse()
}

func main() {
	initCmd()
	var (
		cfg *config.Config
		err error
	)
	stopCh := signals.WaitForSignal()
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
	if err = initETCDClient(cfg.RouteEtcd, stopCh); err != nil {
		fmt.Printf("init etcd client error, msg: %s\n", err.Error())
		os.Exit(1)
	}

	w, err := alarm.NewAlarmClient(tls.GetClientTLSConfig(), cfg)
	if err != nil {
		log.GetLogger().Errorf("Init Alarm Client err:%s", err.Error())
		os.Exit(1)
	}
	w.Witch(cfg, stopCh)
}

// InitETCDClient -
func initETCDClient(RouteEtcd *etcd3.ClientConfig, stopCh <-chan struct{}) error {
	// init cae meta etcd
	err := etcd3.InitParam().
		WithStopCh(stopCh).
		WithRouterEtcdConfig(RouteEtcd).
		InitClient()
	if err != nil {
		log.GetLogger().Errorf("failed to get cfg with error: %s", err.Error())
		return err
	}
	return nil
}
