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

// Package etcd3 implements crud and watch operations based etcd clientv3
package etcd3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/utils"
)

var (
	routerEtcdClient       *EtcdClient
	metaEtcdClient         *EtcdClient
	caeMetaEtcdClient      *EtcdClient
	caeRouterEtcdClient    *EtcdClient
	metaEtcdProxyClient    *EtcdClient
	caeMetaEtcdProxyClient *EtcdClient
)

// EtcdType type of etcd
type EtcdType int

// enum of etcd type
const (
	Meta EtcdType = iota
	Router
	CAEMeta
	CAERouter
	MetaProxy
	CAEMetaProxy
	State
)

var etcdTypeName = [...]string{"meta", "router", "caeMeta", "caeRouter", "metaProxy", "caeMetaProxy", "state"}
var microServiceClusterEnable = os.Getenv("MICROSERVICE_CLUSTER_ENABLE")

// String get literal name for EtcdType
func (e EtcdType) String() string {
	if e < 0 || int(e) >= len(etcdTypeName) {
		return fmt.Sprintf("invalid etcd type %d", e)
	}
	return etcdTypeName[e]
}

var connectionAlarm = [...]alarm.Type{
	alarm.MetadataEtcdConnection,
	alarm.RouterEtcdConnection,
	alarm.CAEMetaEtcdConnection,
	alarm.CAERouterEtcdConnection,
	alarm.MetaEtcdProxyConnection,
	alarm.CAEMetaEtcdProxyConnection,
}

func (e EtcdType) getEtcdConnectionAlarm() alarm.Type {
	if e < 0 || int(e) >= len(connectionAlarm) {
		log.GetLogger().Warnf("invalid etcd type %d", e)
		return alarm.MetadataEtcdConnection
	}
	return connectionAlarm[e]
}

var connectionAbnormalContinuouslyAlarm = [...]alarm.Type{
	alarm.MetaETCDConnectionAbnormalContinuously,
	alarm.RouterETCDConnectionAbnormalContinuously,
	alarm.CAEMetaETCDConnectionAbnormalContinuously,
	alarm.CAERouterETCDConnectionAbnormalContinuously,
	alarm.MetaEtcdProxyConnectionAbnormalContinuously,
	alarm.CAEMetaEtcdProxyConnectionAbnormalContinuously,
}

func (e EtcdType) getEtcdConnectionAbnormalContinuouslyAlarm() alarm.Type {
	if e < 0 || int(e) >= len(connectionAbnormalContinuouslyAlarm) {
		log.GetLogger().Warnf("invalid etcd type %d", e)
		return alarm.MetaETCDConnectionAbnormalContinuously
	}
	return connectionAbnormalContinuouslyAlarm[e]
}

const (
	defaultEtcdLostContactTime               = 5 * time.Minute
	stateCheckFailedContinuouslyIntervalNums = 10
	durationTimeout                          = 3 * time.Second

	reportNormalAlarmTimes = 3
)

var (
	errInitRouterEtcd       = errors.New("failed to init router etcd client")
	errInitMetadataEtcd     = errors.New("failed to init metadata etcd client")
	errInitCaeMetaEtcd      = errors.New("failed to init cae metadata etcd client")
	errInitCaeRouterEtcd    = errors.New("failed to init cae router etcd client")
	errInitMetaEtcdProxy    = errors.New("failed to init metadata etcd proxy client")
	errInitCAEMetaEtcdProxy = errors.New("failed to init cae metadata etcd proxy client")
)

func getMetaEtcdProxyClient(t EtcdType) *EtcdClient {
	switch t {
	case MetaProxy:
		if metaEtcdProxyClient == nil {
			return nil
		}
		metaEtcdProxyClient.rwMutex.RLock()
		defer metaEtcdProxyClient.rwMutex.RUnlock()
		if metaEtcdProxyClient.DowngradesEtcdClient == nil {
			return metaEtcdProxyClient
		}
		return nil
	case CAEMetaProxy:
		if caeMetaEtcdProxyClient == nil {
			return nil
		}
		caeMetaEtcdProxyClient.rwMutex.RLock()
		defer caeMetaEtcdProxyClient.rwMutex.RUnlock()
		if caeMetaEtcdProxyClient.DowngradesEtcdClient == nil {
			return caeMetaEtcdProxyClient
		}
		return nil
	default:
		return nil
	}
}

// GetETCDClient get etcd client with given type
func GetETCDClient(t EtcdType) *EtcdClient {
	switch t {
	case Router:
		return routerEtcdClient
	case Meta:
		if proxyClient := getMetaEtcdProxyClient(MetaProxy); proxyClient != nil {
			return proxyClient
		}
		return metaEtcdClient
	case CAEMeta:
		if proxyClient := getMetaEtcdProxyClient(CAEMetaProxy); proxyClient != nil {
			return proxyClient
		}
		return caeMetaEtcdClient
	case CAERouter:
		return caeRouterEtcdClient
	case MetaProxy:
		return metaEtcdProxyClient
	case CAEMetaProxy:
		return caeMetaEtcdProxyClient
	case State:
		if microServiceClusterEnable == "true" {
			return caeRouterEtcdClient
		}
		if proxyClient := getMetaEtcdProxyClient(MetaProxy); proxyClient != nil {
			return proxyClient
		}
		return metaEtcdClient
	default:
		log.GetLogger().Errorf("invalid ETCD type %d", t)
		return nil
	}
}

// GetRouterEtcdClient -
func GetRouterEtcdClient() *EtcdClient {
	return routerEtcdClient
}

// GetMetaEtcdClient -
func GetMetaEtcdClient() *EtcdClient {
	if proxyClient := getMetaEtcdProxyClient(MetaProxy); proxyClient != nil {
		return proxyClient
	}
	return metaEtcdClient
}

// GetCAEMetaEtcdClient etcd client for CAE metadata etcd
func GetCAEMetaEtcdClient() *EtcdClient {
	if proxyClient := getMetaEtcdProxyClient(CAEMetaProxy); proxyClient != nil {
		return proxyClient
	}
	return caeMetaEtcdClient
}

// GetCaeRouterEtcdClient -
func GetCaeRouterEtcdClient() *EtcdClient {
	return caeRouterEtcdClient
}

// GetStateEtcdClient -
func GetStateEtcdClient() *EtcdClient {
	if microServiceClusterEnable == "true" {
		return caeRouterEtcdClient
	}
	if proxyClient := getMetaEtcdProxyClient(MetaProxy); proxyClient != nil {
		return proxyClient
	}
	return metaEtcdClient
}

// GetMetaEtcdProxyEtcdClient etcd client for etcd proxy
func GetMetaEtcdProxyEtcdClient() *EtcdClient {
	return metaEtcdProxyClient
}

// GetCAEMetaEtcdProxyEtcdClient etcd client for etcd proxy
func GetCAEMetaEtcdProxyEtcdClient() *EtcdClient {
	return caeMetaEtcdProxyClient
}

// InitParam -
func InitParam() *etcdInitParam {
	return new(etcdInitParam)
}

// WithMetaEtcdConfig -
func (param *etcdInitParam) WithMetaEtcdConfig(config *ClientConfig) *etcdInitParam {
	param.metaEtcdConfig = config
	return param
}

// WithRouterEtcdConfig -
func (param *etcdInitParam) WithRouterEtcdConfig(config *ClientConfig) *etcdInitParam {
	param.routerEtcdConfig = config
	return param
}

// WithCAEMetaEtcdConfig set config of cae metadata etcd
func (param *etcdInitParam) WithCAEMetaEtcdConfig(config *ClientConfig) *etcdInitParam {
	param.caeMetaEtcdConfig = config
	return param
}

// WithCaeRouterEtcdConfig -
func (param *etcdInitParam) WithCaeRouterEtcdConfig(config *ClientConfig) *etcdInitParam {
	param.caeRouterEtcdConfig = config
	return param
}

// WithMetaEtcdProxyConfig set config of etcd proxy
func (param *etcdInitParam) WithMetaEtcdProxyConfig(config *ClientConfig) *etcdInitParam {
	param.metaEtcdProxyConfig = config
	return param
}

// WithCAEMetaEtcdProxyConfig set config of cae etcd proxy
func (param *etcdInitParam) WithCAEMetaEtcdProxyConfig(config *ClientConfig) *etcdInitParam {
	param.caeMetaEtcdProxyConfig = config
	return param
}

// WithStopCh -
func (param *etcdInitParam) WithStopCh(ch <-chan struct{}) *etcdInitParam {
	param.stopCh = ch
	return param
}

// WithAlarmServerAddress -
func (param *etcdInitParam) WithAlarmSwitch() *etcdInitParam {
	param.enableAlarm = true
	return param
}

// InitClient initialize etcdClient based on initialization parameters.
func (param *etcdInitParam) InitClient() error {
	if param.routerEtcdConfig != nil && param.initRouterEtcdClient() != nil {
		return errInitRouterEtcd
	}
	if param.metaEtcdConfig != nil && param.initMetadataEtcdClient() != nil {
		return errInitMetadataEtcd
	}
	if param.caeMetaEtcdConfig != nil && param.initCAEMetadataEtcdClient() != nil {
		return errInitCaeMetaEtcd
	}
	if param.caeRouterEtcdConfig != nil && param.initCAERouterEtcdClient() != nil {
		return errInitCaeRouterEtcd
	}
	if param.metaEtcdProxyConfig != nil && param.initMetaEtcdProxyClient() != nil {
		return errInitMetaEtcdProxy
	}
	if param.caeMetaEtcdProxyConfig != nil && param.initCAEMetaEtcdProxyClient() != nil {
		return errInitCAEMetaEtcdProxy
	}
	return nil
}

func (param *etcdInitParam) initCAERouterEtcdClient() error {
	if param.enableAlarm {
		alarm.GetClient().RegisterAlarmType(alarm.CAERouterEtcdConnection)
		alarm.GetClient().RegisterAlarmType(alarm.CAERouterETCDConnectionAbnormalContinuously)
	}
	var err error
	caeRouterEtcdClient, err = newClient(param.caeRouterEtcdConfig, param.stopCh, param.enableAlarm, CAERouter)
	if err != nil {
		log.GetLogger().Errorf("failed to new global router etcd client with error: %s", err.Error())
		return err
	}
	return nil
}

func (param *etcdInitParam) initRouterEtcdClient() error {
	if param.enableAlarm {
		alarm.GetClient().RegisterAlarmType(alarm.RouterEtcdConnection)
		alarm.GetClient().RegisterAlarmType(alarm.RouterETCDConnectionAbnormalContinuously)
	}
	var err error
	routerEtcdClient, err = newClient(param.routerEtcdConfig, param.stopCh, param.enableAlarm, Router)
	if err != nil {
		log.GetLogger().Errorf("failed to new router etcd client with error: %s", err.Error())
		return err
	}
	return nil
}

func (param *etcdInitParam) initMetadataEtcdClient() error {
	if param.enableAlarm {
		alarm.GetClient().RegisterAlarmType(alarm.MetadataEtcdConnection)
		alarm.GetClient().RegisterAlarmType(alarm.MetaETCDConnectionAbnormalContinuously)
	}
	var err error
	metaEtcdClient, err = newClient(param.metaEtcdConfig, param.stopCh, param.enableAlarm, Meta)
	if err != nil {
		log.GetLogger().Errorf("failed to new metadata etcd client with error: %s", err.Error())
		return err
	}
	return nil
}

func (param *etcdInitParam) initCAEMetadataEtcdClient() error {
	if param.enableAlarm {
		alarm.GetClient().RegisterAlarmType(alarm.CAEMetaEtcdConnection)
		alarm.GetClient().RegisterAlarmType(alarm.CAEMetaETCDConnectionAbnormalContinuously)
	}
	var err error
	caeMetaEtcdClient, err = newClient(param.caeMetaEtcdConfig, param.stopCh, param.enableAlarm, CAEMeta)
	if err != nil {
		log.GetLogger().Errorf("failed to new cae metadata etcd client with error: %s", err.Error())
		return err
	}
	return nil
}

func (param *etcdInitParam) initMetaEtcdProxyClient() error {
	if param.enableAlarm {
		alarm.GetClient().RegisterAlarmType(alarm.MetaEtcdProxyConnection)
		alarm.GetClient().RegisterAlarmType(alarm.MetaEtcdProxyConnectionAbnormalContinuously)
	}
	var err error
	metaEtcdProxyClient, err = newClient(param.metaEtcdProxyConfig, param.stopCh, param.enableAlarm, MetaProxy)
	if err != nil {
		log.GetLogger().Errorf("failed to new meta etcd proxy client with error: %s", err.Error())
		return err
	}
	return nil
}

func (param *etcdInitParam) initCAEMetaEtcdProxyClient() error {
	if param.enableAlarm {
		alarm.GetClient().RegisterAlarmType(alarm.CAEMetaEtcdProxyConnection)
		alarm.GetClient().RegisterAlarmType(alarm.CAEMetaEtcdProxyConnectionAbnormalContinuously)
	}
	var err error
	caeMetaEtcdProxyClient, err = newClient(param.caeMetaEtcdProxyConfig, param.stopCh, param.enableAlarm, CAEMetaProxy)
	if err != nil {
		log.GetLogger().Errorf("failed to new cae meta etcd proxy client with error: %s", err.Error())
		return err
	}
	return nil
}

func newClient(config *ClientConfig, stopCh <-chan struct{}, enableAlarm bool,
	etcdType EtcdType) (*EtcdClient, error) {
	log.GetLogger().Infof("%s etcd cluster address is %s", etcdType.String(), config.ServerList)
	if stopCh == nil {
		return nil, errors.New("stopCh should not be nil")
	}
	client, err := buildClient(config)
	if err != nil && etcdType == Router {
		log.GetLogger().Errorf("failed to new %s etcd client, %s", etcdType.String(), err.Error())
		return nil, err
	}
	if err != nil {
		switch etcdType {
		case Meta, CAEMeta, CAERouter, MetaProxy, CAEMetaProxy:
			client = &EtcdClient{Client: nil, clientExitCh: make(chan struct{})}
			client.stopCh = stopCh
			client.config = config
			client.etcdType = etcdType
			client.isAlarmEnable = enableAlarm
			client.etcdStatusAfterLostContact = false
			client.etcdStatusNow = 0
			if etcdType == MetaProxy {
				client.DowngradesEtcdClient = metaEtcdClient
			} else if etcdType == CAEMetaProxy {
				client.DowngradesEtcdClient = caeMetaEtcdClient
			}
			go client.checkConnAlive(err, etcdType)
			log.GetLogger().Warnf("failed to new %s etcd client, %s", etcdType.String(), err.Error())
			return client, nil
		default:
		}
	}
	if client == nil {
		log.GetLogger().Errorf("failed to new %s etcd client, client is nil", etcdType.String())
		return nil, errors.New("client is nil")
	}
	client.stopCh = stopCh
	client.config = config
	client.etcdType = etcdType
	client.isAlarmEnable = enableAlarm
	client.etcdStatusAfterLostContact = true
	atomic.StoreUint32(&client.etcdStatusNow, 1)

	go client.keepConnAlive()

	return client, nil
}

func buildClient(config *ClientConfig) (*EtcdClient, error) {
	var tlsConfig *tls.Config
	var err error
	// CA
	// NOTE: Client relies on nil tlsConfig
	// for non-secure connections, update the implicit variable
	if !config.SslEnable {
		tlsConfig = nil
	} else {
		tlsConfig, err = buildCfg(config.CaFile, config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	cfg := clientv3.Config{
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
		Endpoints:            config.ServerList,
		TLS:                  tlsConfig,
	}

	pwd, err := localauth.Decrypt(config.Password)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt etcd config with error %s", err)
		return nil, err
	}
	if config.User != "" && pwd != nil {
		cfg.Username = config.User
		cfg.Password = string(pwd)
	}
	client, err := clientv3.New(cfg)
	utils.ClearByteMemory(pwd)
	if err != nil {
		utils.ClearStringMemory(cfg.Password)
		return nil, err
	}

	return &EtcdClient{Client: client, clientExitCh: make(chan struct{})}, nil
}

func (w *EtcdClient) keepConnAlive() {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			w.checkConnState()
		case _, ok := <-w.stopChan:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed and quits keep %s etcd conn alive task",
					w.etcdType.String())
			}
			ticker.Stop()
			return
		}
	}
}

func (w *EtcdClient) checkConnAlive(err error, etcdType EtcdType) {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			w.checkMetaConnState(err)
		case _, ok := <-w.stopChan:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed and quits keep etcd conn alive task, etcd type: %s",
					etcdType.String())
			}
			ticker.Stop()
			return
		}
	}
}

func (w *EtcdClient) checkConnState() {
	w.rwMutex.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), keepConnAliveTTL)
	_, err := w.Client.Get(ctx, "alive", clientv3.WithKeysOnly())
	w.rwMutex.RUnlock()
	cancel()
	if err != nil {
		w.abnormalContinuouslyTimes++
		if w.etcdTimer == nil {
			w.etcdTimer = time.AfterFunc(defaultEtcdLostContactTime, func() {
				w.etcdStatusAfterLostContact = false
				log.GetLogger().Warnf("etcd %s lost contact over %v, etcdStatusAfterLostContact is %v",
					w.etcdType, defaultEtcdLostContactTime, w.etcdStatusAfterLostContact)
			})
		}
		atomic.StoreUint32(&w.etcdStatusNow, 0)
		log.GetLogger().Warnf("failed to check etcd client conn, type: %s, err: %s",
			w.etcdType.String(), err.Error())
		if w.abnormalContinuouslyTimes >= reportNormalAlarmTimes {
			w.reportAlarm(err.Error())
		}
		w.reportCriticalAlarm(err.Error())
		w.stopMetaEtcdProxy()
		if err = w.restart(); err != nil {
			log.GetLogger().Errorf("failed to restart etcd client, type: %s, %s",
				w.etcdType.String(), err.Error())
		}
		return
	}
	w.abnormalContinuouslyTimes = 0
	if w.operateETCD != nil && w.redoFailedOperations != nil {
		go w.redoFailedOperations(w.operateETCD, w.stopCh)
	}
	if w.etcdTimer != nil {
		w.etcdTimer.Stop()
		w.etcdTimer = nil
		w.etcdStatusAfterLostContact = true
	}
	// if old etcdStatusNow is false and the new one will be true, call restartCallBack to sync workerIns
	if atomic.LoadUint32(&w.etcdStatusNow) == 0 {
		w.doRestartCallback()
	}
	atomic.StoreUint32(&w.etcdStatusNow, 1)
	w.clearAlarm()
	w.clearCriticalAlarm()
	w.startMetaEtcdProxy()
}

func (w *EtcdClient) doRestartCallback() {
	if w.restartCallback != nil {
		log.GetLogger().Infof("%s etcd client reconnected, do restartCallback", w.etcdType.String())
		w.restartCallback(w)
	}
}

func (w *EtcdClient) stopMetaEtcdProxy() {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	switch w.etcdType {
	case MetaProxy:
		w.DowngradesEtcdClient = metaEtcdClient
		log.GetLogger().Infof("stop using meta etcd proxy")
	case CAEMetaProxy:
		w.DowngradesEtcdClient = caeMetaEtcdClient
		log.GetLogger().Infof("stop using cae meta etcd proxy")
	default:
	}
}

func (w *EtcdClient) startMetaEtcdProxy() {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	switch w.etcdType {
	case MetaProxy:
		w.DowngradesEtcdClient = nil
		log.GetLogger().Infof("start using meta etcd proxy")
	case CAEMetaProxy:
		w.DowngradesEtcdClient = nil
		log.GetLogger().Infof("start using cae meta etcd proxy")
	default:
	}
}

func (w *EtcdClient) checkMetaConnState(err error) {
	if w.Client == nil {
		w.handleException(err)
		return
	}
	w.rwMutex.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), keepConnAliveTTL)
	_, errs := w.Client.Get(ctx, "alive", clientv3.WithKeysOnly())
	w.rwMutex.RUnlock()
	cancel()
	if errs != nil {
		w.handleException(errs)
		return
	}
	if w.operateETCD != nil && w.redoFailedOperations != nil {
		go w.redoFailedOperations(w.operateETCD, w.stopCh)
	}
	w.abnormalContinuouslyTimes = 0
	if w.etcdTimer != nil {
		w.etcdTimer.Stop()
		w.etcdTimer = nil
		w.etcdStatusAfterLostContact = true
	}
	if atomic.LoadUint32(&w.etcdStatusNow) == 0 {
		w.doRestartCallback()
	}
	atomic.StoreUint32(&w.etcdStatusNow, 1)
	w.clearAlarm()
	w.clearCriticalAlarm()
	w.startMetaEtcdProxy()
}

func (w *EtcdClient) handleException(err error) {
	w.abnormalContinuouslyTimes++
	if w.etcdTimer == nil {
		w.etcdTimer = time.AfterFunc(defaultEtcdLostContactTime, func() {
			log.GetLogger().Warnf("%s etcd lost contact over %v", defaultEtcdLostContactTime, w.etcdType.String())
			w.etcdStatusAfterLostContact = false
		})
	}
	log.GetLogger().Warnf("failed to check %s etcd client conn, err: %s",
		w.etcdType.String(), err.Error())
	atomic.StoreUint32(&w.etcdStatusNow, 0)
	w.reportAlarm(err.Error())
	w.reportCriticalAlarm(err.Error())
	w.stopMetaEtcdProxy()
	if errs := w.restart(); errs != nil {
		log.GetLogger().Warnf("failed to restart %s etcd client, err: %s",
			w.etcdType.String(), errs.Error())
	}
}

func (w *EtcdClient) reportCriticalAlarm(errMsg string) {
	if w.abnormalContinuouslyTimes%stateCheckFailedContinuouslyIntervalNums != 0 {
		return
	}
	if w.isAlarmEnable {
		alarmType := w.etcdType.getEtcdConnectionAbnormalContinuouslyAlarm()
		alarm.GetClient().ReportAlarm(alarmType, fmt.Sprintf("error msg=%s,", errMsg))
	}
}

func (w *EtcdClient) clearCriticalAlarm() {
	if w.isAlarmEnable {
		almType := w.etcdType.getEtcdConnectionAbnormalContinuouslyAlarm()
		alarm.GetClient().ClearAlarm(almType)
	}
}

func (w *EtcdClient) reportAlarm(errMsg string) {
	if w.isAlarmEnable {
		almType := w.etcdType.getEtcdConnectionAlarm()
		alarm.GetClient().ReportAlarm(almType, fmt.Sprintf("error msg=%s,", errMsg))
	}
}

func (w *EtcdClient) clearAlarm() {
	if w.isAlarmEnable {
		almType := w.etcdType.getEtcdConnectionAlarm()
		alarm.GetClient().ClearAlarm(almType)
	}
}

// CheckEtcdNodeStatus check etcd node status
func (w *EtcdClient) CheckEtcdNodeStatus() bool {
	etcdClusterAllNodesHealth := true
	endpoints := w.Client.Endpoints()
	for _, endpoint := range endpoints {
		ctx, cancel := context.WithTimeout(context.Background(), durationTimeout)
		_, err := w.Client.Status(ctx, endpoint)
		cancel()
		if err != nil {
			etcdClusterAllNodesHealth = false
			log.GetLogger().Errorf("failed to get status from endpoint: %s, err: %s", endpoint, err.Error())
		}
	}
	return etcdClusterAllNodesHealth
}

func (w *EtcdClient) restart() error {
	recreatedClient, err := buildClient(w.config)
	if err != nil {
		log.GetLogger().Errorf("failed to restart %s etcd client, %s", w.etcdType.String(), err.Error())
		return err
	}
	w.rwMutex.Lock()
	w.stop()
	w.Client = recreatedClient.Client
	close(w.clientExitCh)
	w.clientExitCh = make(chan struct{})
	w.rwMutex.Unlock()
	return nil
}

func (w *EtcdClient) stop() {
	if w.Client == nil {
		log.GetLogger().Warnf("failed to get %s etcd client, skip stop", w.etcdType.String())
		return
	}
	if err := w.Client.Close(); err != nil {
		log.GetLogger().Errorf("failed to close %s etcd client, %s", w.etcdType.String(), err.Error())
		return
	}
}

type operateETCDFunc func(ctx context.Context, path, value, operate string) error
type redoOperationsFunc func(operateFunc func(ctx context.Context, path, value, operate string) error,
	stopCh <-chan struct{})

// RegisterRedoFunc -
func (w *EtcdClient) RegisterRedoFunc(operateFunc operateETCDFunc, redoFunc redoOperationsFunc) {
	w.operateETCD = operateFunc
	w.redoFailedOperations = redoFunc
}

// GetEtcdStatusLostContact -
func (w *EtcdClient) GetEtcdStatusLostContact() bool {
	return w.etcdStatusAfterLostContact
}

// GetEtcdStatusNow -
func (w *EtcdClient) GetEtcdStatusNow() bool {
	return atomic.LoadUint32(&w.etcdStatusNow) != 0
}

// GetEtcdType return etcdType of the client
func (w *EtcdClient) GetEtcdType() EtcdType {
	return w.etcdType
}

func buildCfg(cafile string, certFile string, keyFile string) (*tls.Config, error) {
	var pool *x509.CertPool
	pool, err := commontls.GetX509CACertPool(cafile)
	if err != nil {
		log.GetLogger().Errorf("failed to getX509CACertPool: %s", err.Error())
		return nil, err
	}

	var certs []tls.Certificate
	if certs, err = commontls.LoadServerTLSCertificate(certFile, keyFile, "", false); err != nil {
		log.GetLogger().Errorf("failed to loadServerTLSCertificate: %s", err.Error())
		return nil, err
	}

	clientAuthMode := tls.NoClientCert
	tlsConfig := &tls.Config{
		RootCAs:      pool,
		Certificates: certs,
		ClientAuth:   clientAuthMode,
	}
	return tlsConfig, nil
}

// RegisterRouteEtcdRestartCallback register route etcd restart callback
func RegisterRouteEtcdRestartCallback(callback restartCallback) {
	if routerEtcdClient != nil {
		routerEtcdClient.restartCallback = callback
	}
	if caeRouterEtcdClient != nil {
		caeRouterEtcdClient.restartCallback = callback
	}
}
