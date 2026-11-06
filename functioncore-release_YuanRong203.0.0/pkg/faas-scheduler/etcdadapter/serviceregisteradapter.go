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

// Package etcdadapter contains etcd interaction
package etcdadapter

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/faas-scheduler/servicerouter"
)

var (
	leaseDetectInterval = 60 * time.Second
	leaseLongTimeout    = 600 * time.Second
)

// ServiceRegisterAdapter etcd service register
type ServiceRegisterAdapter struct {
	etcdClient    *etcd3.EtcdClient
	leaseMonitor  *etcd3.LeaseMonitor
	config        servicerouter.Config
	leaseID       clientv3.LeaseID
	renewTTLTimer *time.Ticker
}

// CreateRegister -
func CreateRegister() servicerouter.ServiceRegister {
	return &ServiceRegisterAdapter{
		etcdClient:   etcd3.GetRouterEtcdClient(),
		leaseMonitor: etcd3.CreateLeaseMonitor(),
	}
}

// Register register service
func (register *ServiceRegisterAdapter) Register(config servicerouter.Config) error {
	register.config = config

	err := register.putInstanceInfoToEtcd()
	if err != nil {
		log.GetLogger().Errorf("failed to register instance to etcd when start, error:%s", err.Error())
		return err
	}

	register.leaseMonitor.Update()
	go register.startRefreshLeaseJob(config.StopCh)
	go register.monitorRouterEtcdLease(config.StopCh)
	return nil
}

// UpdateConfig update service register config
func (register *ServiceRegisterAdapter) UpdateConfig(config servicerouter.Config) {
	if config.LeaseTTL > 0 {
		log.GetLogger().Infof("update leaseTTL config old: %d, new: %d",
			register.config.LeaseTTL, config.LeaseTTL)
		register.config.LeaseTTL = config.LeaseTTL
	}
	if config.LeaseInterval > 0 {
		log.GetLogger().Infof("update leaseInterval config old: %d, new: %d",
			register.config.LeaseInterval, config.LeaseInterval)
		register.config.LeaseInterval = config.LeaseInterval
		register.renewTTLTimer = time.NewTicker(time.Duration(register.config.LeaseInterval) * time.Second)
	}
}

func (register *ServiceRegisterAdapter) putInstanceInfoToEtcd() error {
	if register.etcdClient == nil {
		log.GetLogger().Errorf("etcd client is nil")
		return fmt.Errorf("etcd client is nil")
	}

	grantCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	id, err := register.etcdClient.Grant(grantCtx, register.config.LeaseTTL)
	if err != nil {
		log.GetLogger().Errorf("failed to grant faas-scheduler instance lease: %s", err.Error())
		return err
	}
	register.leaseID = id

	if register.renewTTLTimer != nil {
		register.renewTTLTimer.Reset(time.Duration(register.config.LeaseInterval) * time.Second)
	}

	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err = register.etcdClient.Put(ctx, register.config.Key, register.config.Value, clientv3.WithLease(register.leaseID))
	if err != nil {
		log.GetLogger().Errorf("unable to put key:%s to etcd, err:%s", register.config.Key, err.Error())
		return err
	}
	log.GetLogger().Infof("register instance:%s to router etcd, leaseID: %d", register.config.Key, id)
	return nil
}

func (register *ServiceRegisterAdapter) isKeyExist() bool {
	if register.etcdClient == nil {
		log.GetLogger().Errorf("etcd client is nil")
		return false
	}

	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := register.etcdClient.GetResponseWithSerializable(ctx, register.config.Key,
		clientv3.WithKeysOnly())
	if err != nil {
		log.GetLogger().Errorf("failed to get etcd key:%s, err:%s", register.config.Key, err)
		return false
	}
	return len(resp.Kvs) > 0
}

func (register *ServiceRegisterAdapter) refreshLease() error {
	if !register.isKeyExist() {
		if err := register.putInstanceInfoToEtcd(); err != nil {
			return err
		}
	}

	keepAliveOnceCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err := register.etcdClient.KeepAliveOnce(keepAliveOnceCtx, register.leaseID)
	if err != nil {
		log.GetLogger().Errorf("unable to refresh lease:%s", err.Error())
		return err
	}
	return nil
}

func (register *ServiceRegisterAdapter) startRefreshLeaseJob(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil, will not refresh lease")
		return
	}
	register.renewTTLTimer = time.NewTicker(time.Duration(register.config.LeaseInterval) * time.Second)
	for {
		select {
		case <-register.renewTTLTimer.C:
			err := register.refreshLease()
			if err == nil {
				register.leaseMonitor.Update()
			}
		case <-stopCh:
			log.GetLogger().Infof("refresh lease job exit")
			register.renewTTLTimer.Stop()
			revokeCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err := register.etcdClient.Revoke(revokeCtx, register.leaseID)
			if err != nil {
				log.GetLogger().Warnf("revoke lease failed, err:%s", err.Error())
			}

			if register.etcdClient == nil {
				log.GetLogger().Errorf("etcd client is nil")
				return
			}

			ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err = register.etcdClient.Delete(ctx, register.config.Key)
			if err != nil {
				log.GetLogger().Errorf("delete key:%s failed, err:%s", register.config.Key, err.Error())
			}
			return
		}
	}
}

func (register *ServiceRegisterAdapter) monitorRouterEtcdLease(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}

	alarm.GetClient().RegisterAlarmType(alarm.RouterEtcdLease)
	t := time.NewTicker(leaseDetectInterval)
	for {
		select {
		case <-t.C:
			isTimeout, leaseFailedDuration := register.leaseMonitor.CheckLeaseTimeout(leaseLongTimeout)
			if isTimeout == true {
				log.GetLogger().Errorf("lease to router etcd failed for %v, start alarm",
					leaseFailedDuration)
				alarm.GetClient().ReportAlarm(alarm.RouterEtcdLease,
					fmt.Sprintf("error msg=etcd lease failed for %v,", leaseFailedDuration))
			} else {
				alarm.GetClient().ClearAlarm(alarm.RouterEtcdLease)
			}
		case <-stopCh:
			t.Stop()
			return
		}
	}
}
