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

// Package channelbusconn is used for routing messages between functiontask with bus proxy
package channelbusconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/utils"
)

const (
	// In the long-term stability test environment,
	// jitter may occur during etcd renewal.
	// the lease period should be twice as long as the renew period.
	defaultLeaseTTL     = 8
	defaultRenewTTL     = 4
	defaultRenewTimeOut = 4
)

var (
	leaseTTL                  int
	renewTTL                  int
	etcdLeaseTimeout          int
	microServiceClusterEnable = os.Getenv("MICROSERVICE_CLUSTER_ENABLE") == "true"
)

// RegisterInfo is registering info
type RegisterInfo struct {
	FuncName        string
	NodeID          string
	FunctionVersion string
	Tenant          string
	WorkerInstance
}

// WorkerInstance include instance info for registering
type WorkerInstance struct {
	IP       string `json:"ip"`
	Port     string `json:"port"`
	Status   string `json:"status"`
	P2pPort  string `json:"p2pPort"`
	NodeIP   string `json:"nodeIP"`
	NodePort string `json:"nodePort"`
}

func init() {
	leaseTTL = getValueFromEnv("leaseTTL", defaultLeaseTTL)
	renewTTL = getValueFromEnv("renewTTL", defaultRenewTTL)
	etcdLeaseTimeout = getValueFromEnv("ctxTimeout", defaultRenewTimeOut)
}

// Interface is Register interface
type Interface interface {
	Register() error
	Unregister() error
}

type etcdLeaser struct {
	monitor        *etcd3.LeaseMonitor
	client         *etcd3.EtcdClient
	renewTTLTicker *time.Ticker
	stop           <-chan struct{}
	leaseID        clientv3.LeaseID
	key            string
	value          string
	etcdType       etcd3.EtcdType
	alarmType      alarm.Type
}

func (e *etcdLeaser) checkKeyExist() error {
	if e.client == nil || e.client.Client == nil {
		return fmt.Errorf("client is nil, etcdType: %s", e.etcdType)
	}
	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), time.Duration(etcdLeaseTimeout)*time.Second)
	resp, err := e.client.GetResponseWithSerializable(ctx, e.key, clientv3.WithKeysOnly())
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return errors.New("task key not exists in " + e.etcdType.String())
	}
	return nil
}

func (e *etcdLeaser) putKeyWithLease() error {
	grantCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), time.Duration(etcdLeaseTimeout)*time.Second)
	id, err := e.client.Grant(grantCtx, int64(leaseTTL))
	if err != nil {
		log.GetLogger().Errorf("failed to grant lease with %s: %s", e.etcdType, err.Error())
		return err
	}

	backgroundCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(),
		time.Duration(etcdLeaseTimeout)*time.Second)
	err = e.client.Put(backgroundCtx, e.key, e.value, clientv3.WithLease(id))
	if err != nil {
		log.GetLogger().Errorf("failed to put function key to %s: %s", e.etcdType, err.Error())
		return err
	}
	e.leaseID = id
	if e.renewTTLTicker != nil {
		e.renewTTLTicker.Reset(time.Duration(renewTTL) * time.Second)
	}
	return nil
}

func (e *etcdLeaser) keepAliveOnce() bool {
	if e.client == nil || e.client.Client == nil {
		return false
	}
	leaseSucceed := true
	if err := e.checkKeyExist(); err != nil {
		log.GetLogger().Warnf("failed to check task key with %s: %s", e.etcdType, err.Error())
		err = e.putKeyWithLease()
		if err != nil {
			log.GetLogger().Errorf("failed to put register etcd to %s: %s", e.etcdType, err.Error())
			leaseSucceed = false
		}
		return leaseSucceed
	}

	keepAliveOnceCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(),
		time.Duration(etcdLeaseTimeout)*time.Second)
	err := e.client.KeepAliveOnce(keepAliveOnceCtx, e.leaseID)
	if err != nil {
		log.GetLogger().Warnf("failed to renew the lease to %s: %s and will callback the registered function",
			e.etcdType, err.Error())
		if err := e.putKeyWithLease(); err != nil {
			log.GetLogger().Errorf("failed to retry register etcd to %s: %s", e.etcdType, err.Error())
			leaseSucceed = false
		}
	}
	return leaseSucceed
}

func (e *etcdLeaser) register() {
	e.renewTTLTicker = time.NewTicker(time.Second * time.Duration(renewTTL))
	defer e.renewTTLTicker.Stop()
	for {
		select {
		case <-e.renewTTLTicker.C:
			succeed := e.keepAliveOnce()
			if succeed && e.monitor != nil {
				e.monitor.Update()
			}
		case _, ok := <-e.stop:
			if !ok {
				log.GetLogger().Warnf("stopChanel closed")
			}
			if err := e.revoke(); err != nil {
				log.GetLogger().Errorf("failed to revoke lease to %s: %s", e.etcdType, err.Error())
			}
			return
		}
	}
}

func (e *etcdLeaser) revoke() error {
	revokeCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err := e.client.Revoke(revokeCtx, e.leaseID)
	if err != nil {
		log.GetLogger().Errorf("failed to revoke key to %s: %s", e.etcdType, err.Error())
		return err
	}
	return nil
}

func (e *etcdLeaser) checkAlarm() {
	timeout := time.Duration(common.FConfig.AlarmConfig.RouterEtcdLeaseTimeout) * time.Second
	isTimeout, leaseFailedDuration := e.monitor.CheckLeaseTimeout(timeout)
	if isTimeout == true {
		log.GetLogger().Errorf("lease to %s etcd failed for %v, start alarm", e.etcdType,
			leaseFailedDuration)
		alarm.GetClient().ReportAlarm(e.alarmType,
			fmt.Sprintf("error msg=etcd lease failed for %v,", leaseFailedDuration))
	} else {
		alarm.GetClient().ClearAlarm(e.alarmType)
	}
}

func (e *etcdLeaser) monitorLease() {
	alarm.GetClient().RegisterAlarmType(e.alarmType)
	interval := common.FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval
	t := time.NewTicker(time.Second * time.Duration(interval))
	defer t.Stop()
	for {
		select {
		case <-t.C:
			e.checkAlarm()
			if interval != common.FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval {
				log.GetLogger().Infof("RouterEtcdLeaseDetectInterval updated, old: %d, new: %d, reset timer",
					interval, common.FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval)
				interval = common.FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval
				t.Reset(time.Duration(interval) * time.Second)
			}
		case <-e.stop:
			return
		}
	}
}

type etcdRegister struct {
	key                 string
	value               string
	routerEtcdClient    *etcd3.EtcdClient
	caeRouterEtcdClient *etcd3.EtcdClient
	stateClient         *etcd3.EtcdClient
	Info                RegisterInfo
	stop                <-chan struct{}
}

// NewRegistry creates a registry.
func NewRegistry(regInfo RegisterInfo, stopCh <-chan struct{}, handler common.ETCDConnectHandler) Interface {
	return &etcdRegister{
		Info:                regInfo,
		stop:                stopCh,
		routerEtcdClient:    etcd3.GetRouterEtcdClient(),
		stateClient:         etcd3.GetStateEtcdClient(),
		caeRouterEtcdClient: etcd3.GetCaeRouterEtcdClient(),
	}
}

// Register adds functionTask RegisterInfo to etcd
func (e *etcdRegister) Register() error {
	if e.routerEtcdClient == nil || e.routerEtcdClient.Client == nil {
		log.GetLogger().Errorf("router etcd client is nil")
		return errors.New("router etcd client is nil")
	}

	if e.stateClient == nil {
		log.GetLogger().Errorf("etcd stateClient is nil")
		return errors.New("etcd stateClient is nil")
	}

	if err := e.generateKV(); err != nil {
		log.GetLogger().Errorf("failed to generate kv: %s", err.Error())
		return err
	}

	routerLeaser := etcdLeaser{
		monitor:   etcd3.CreateLeaseMonitor(),
		client:    e.routerEtcdClient,
		stop:      e.stop,
		key:       e.key,
		value:     e.value,
		etcdType:  etcd3.Router,
		alarmType: alarm.RouterEtcdLease,
	}
	if err := routerLeaser.putKeyWithLease(); err != nil {
		log.GetLogger().Errorf("failed to register etcd: %s", err.Error())
		return err
	}

	stateLeaser := etcdLeaser{
		client:   e.stateClient,
		stop:     e.stop,
		key:      e.key,
		value:    e.value,
		etcdType: etcd3.State,
	}
	if e.stateClient.Client != nil {
		if err := stateLeaser.putKeyWithLease(); err != nil {
			log.GetLogger().Errorf("failed to register state etcd: %s", err.Error())
			return err
		}
	}

	e.registerCAERouterETCD()
	go routerLeaser.register()
	go routerLeaser.monitorLease()
	go stateLeaser.register()

	return nil
}

func (e *etcdRegister) registerCAERouterETCD() {
	// if microServiceClusterEnable, stateEtcd and caeRouterEtcd are same
	if common.IsCAERouterEtcdDefined() && !microServiceClusterEnable {
		caeRouterLeaser := etcdLeaser{
			client:   e.caeRouterEtcdClient,
			stop:     e.stop,
			key:      e.key,
			value:    e.value,
			etcdType: etcd3.CAERouter,
		}
		if e.caeRouterEtcdClient != nil && e.caeRouterEtcdClient.Client != nil {
			if err := caeRouterLeaser.putKeyWithLease(); err != nil {
				// if CAE Router failed, still try local router
				log.GetLogger().Errorf("failed to register cae etcd: %s", err.Error())
			}
		}
		go caeRouterLeaser.register()
	}
}

// Unregister remove functionTask RegisterInfo from etcd
func (e *etcdRegister) Unregister() error {
	key := e.getFunctionTaskRegisterInfoKey()
	if err := utils.DeleteValueInEtcd(key); err != nil {
		return err
	}
	return nil
}

func (e *etcdRegister) getFunctionTaskRegisterInfoKey() string {
	// /sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/node01
	path := fmt.Sprintf("/%s/function/%s/version/%s/%s/%s",
		e.Info.Tenant, e.Info.FuncName, e.Info.FunctionVersion, commonutils.AzEnv(), e.Info.NodeID)
	key := common.WatchPathPrefixWithWorkers + path
	return key
}

func (e *etcdRegister) generateKV() error {
	e.key = e.getFunctionTaskRegisterInfoKey()
	value, err := json.Marshal(e.Info.WorkerInstance)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal instance info: %s", err.Error())
		return err
	}
	e.value = string(value)
	return nil
}

func getValueFromEnv(key string, defaultValue int) (value int) {
	valueStr := os.Getenv(key)
	if valueStr != "" {
		convertValue, err := strconv.Atoi(valueStr)
		if err != nil {
			fmt.Printf("got %s from env failed, use default value %d, error %s \n",
				key, defaultValue, err.Error())
			value = defaultValue
			return
		}
		value = convertValue
	} else {
		value = defaultValue
	}
	fmt.Printf("set %s: %d\n", key, value)
	return
}

// UpdateLeaseInfo hot load lease config
func UpdateLeaseInfo(leaseTTLCfg, renewTTLCfg int) {
	if leaseTTLCfg > 0 {
		leaseTTL = leaseTTLCfg
	}
	if renewTTLCfg > 0 {
		renewTTL = renewTTLCfg
	}
}
