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

// Package main contains scheduler main function
package main

import (
	"errors"
	"fmt"
	"os"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/autogc"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/etcdadapter"
	"functioncore/pkg/faas-scheduler/fasthttpserver"
	"functioncore/pkg/faas-scheduler/grpcserver"
	"functioncore/pkg/faas-scheduler/healthcheck"
	"functioncore/pkg/faas-scheduler/scheduler/service"
	"functioncore/pkg/faas-scheduler/servicerouter"
)

const (
	logFileName = "faas-scheduler"
)

var (
	schedulerService service.SchedulerService
)

const (
	errChanSize = 2
)

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	// init logger config
	err := log.InitRunLog(logFileName, true)
	if err != nil {
		fmt.Println("InitRunLog failed err: " + err.Error())
		return
	}

	autogc.InitAutoGOGC()
	stopCh := signals.WaitForSignal()
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}

	err = config.InitConfig()
	if err != nil {
		log.GetLogger().Errorf("InitConfig failed, err: %s", err.Error())
		return
	}

	// WatchConfig failed do not return, just config hot load not enable
	if err := config.WatchConfig(stopCh, updateConfig); err != nil {
		log.GetLogger().Warnf("WatchConfig %s failed, err %s", config.ConfigPath, err.Error())
	}

	if config.FConfig.HTTPSConfig.HTTPSEnable {
		err = tls.InitTLSConfig(config.FConfig.HTTPSConfig.TLSProtocol, config.FConfig.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return
		}
	}

	if err := alarm.NewClient(config.FConfig.AlarmServerAddress, constants.FaaSScheduler, stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth(config.FConfig.LocalAuth.AKey,
			config.FConfig.LocalAuth.SKey)); err != nil {
		log.GetLogger().Errorf("failed to init alarm client, error: %s", err.Error())
		return
	}

	err = etcdadapter.InitETCDClient(stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to InitETCDClient, err: %s", err.Error())
		return
	}

	schedulerService, err = service.CreateService(etcdadapter.FetchFuncMeta)
	if err != nil {
		log.GetLogger().Errorf("failed to CreateService, err: %s", err.Error())
		return
	}

	err = startEtcdObserver(stopCh)
	if err != nil {
		log.GetLogger().Errorf("StartEtcdObserver failed, err: %s", err.Error())
		return
	}

	errorChan := make(chan error, errChanSize)
	server, err := startHTTPServer(errorChan)
	if err != nil {
		log.GetLogger().Errorf("failed to startHttpServer, err: %s", err.Error())
		return
	}

	err = healthcheck.StartHealthCheck(errorChan)
	if err != nil {
		log.GetLogger().Errorf("failed to StartHealthCheck, err: %s", err.Error())
		return
	}

	servicerouter.RegisterAdapter = etcdadapter.CreateRegister()
	err = servicerouter.RegisterService(stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to InitETCDClient, err: %s", err.Error())
		return
	}

	err = waitShutDown(server, stopCh, errorChan)
	if err != nil {
		log.GetLogger().Errorf("failed to waitShutDown, err: %s", err.Error())
		return
	}
}

func startGrpcServer(errChan chan error) (*grpcserver.Server, error) {
	if errChan == nil {
		return nil, errors.New("errChan is nil")
	}
	podIP := os.Getenv("POD_IP")
	config := grpcserver.Config{
		IP:      podIP,
		Port:    config.FConfig.Runtime.ServicePort,
		Service: schedulerService,
	}
	server, err := grpcserver.NewServer(config)
	if err != nil {
		return nil, err
	}
	go func() {
		err := server.Start()
		errChan <- err
	}()
	return server, nil
}

func waitShutDown(server *fasthttpserver.Server, stopCh <-chan struct{}, errChan <-chan error) error {
	if stopCh == nil || errChan == nil {
		return errors.New("stopCh is nil or errChan is nil")
	}
	select {
	case <-stopCh:
		log.GetLogger().Infof("received termination signal")
		server.Stop()
		return nil
	case err := <-errChan:
		return err
	}
}

func startEtcdObserver(stopCh <-chan struct{}) error {
	notifier, err := etcdadapter.NewEtcdNotifier(stopCh)
	if err != nil {
		return err
	}

	funcMetaObserver := etcdadapter.CreateFuncMetaObserver(schedulerService)
	notifier.AddObservers(funcMetaObserver)

	if config.FConfig.CAEMetaETCD != nil && len(config.FConfig.CAEMetaETCD.ServerList) > 0 {
		caeFuncMetaObserver := etcdadapter.CreateCAEFuncMetaObserver()
		notifier.AddObservers(caeFuncMetaObserver)
	}

	funcInsObserver := etcdadapter.CreateFuncInsObserver(schedulerService)
	notifier.AddObservers(funcInsObserver)
	return nil
}

func startHTTPServer(errChan chan error) (*fasthttpserver.Server, error) {
	if errChan == nil {
		return nil, errors.New("errChan is nil")
	}
	podIP := os.Getenv("POD_IP")
	config := fasthttpserver.Config{
		IP:      podIP,
		Port:    config.FConfig.Runtime.ServicePort,
		Service: schedulerService,
	}
	server, err := fasthttpserver.NewServer(config)
	if err != nil {
		return nil, err
	}
	server.Start(errChan)
	return server, nil
}

func updateConfig() {
	servicerouter.RegisterAdapter.UpdateConfig(servicerouter.Config{
		LeaseTTL:      config.FConfig.EtcdLeaseConfig.LeaseTTL,
		LeaseInterval: config.FConfig.EtcdLeaseConfig.RenewTTL,
	})
	log.GetLogger().Infof("faas-scheduler updateConfig success")
}
