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

// Package server define server
package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

var (
	leaseID                     clientv3.LeaseID
	autoscalerMasterLockKey     = "autoscalerMasterLock"
	refreshInterval             time.Duration
	leaseTTL                    int64
	leaseTicker                 *time.Ticker
	registerTicker              *time.Ticker
	podIP                       = os.Getenv("POD_IP")
	clusterID                   = os.Getenv("CLUSTER_ID")
	registerValue               = podIP + ":" + clusterID
	masterNodeJobStopCh         chan struct{}
	registerMasterNodeJobStopCh chan struct{}
)

func startSelecterMasterNode(stopCh <-chan struct{}) {
	log.GetLogger().Infof("begin to select master node")
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil, can not register master node")
		return
	}
	if err := initSelectMasterConfig(); err != nil {
		return
	}
	registerMasterNodeJobStopCh = make(chan struct{})
	startRegiterMasterNodeCircle(stopCh, registerMasterNodeJobStopCh)
}

func initSelectMasterConfig() error {
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get autoscaler config, error: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("initSelectMasterConfig, SelectMasterLeaseTTL: %d, SelectMasterRefreshInterval: %d",
		conf.SelectMasterLeaseTTL, conf.SelectMasterRefreshInterval)
	twiceRefreshInterval := conf.SelectMasterRefreshInterval * 2 // ensure renew lease time less than 1/2 of lease time.
	if conf.SelectMasterLeaseTTL <= 0 || conf.SelectMasterRefreshInterval <= 0 ||
		twiceRefreshInterval > conf.SelectMasterLeaseTTL {
		log.GetLogger().Errorf("SelectMasterLeaseTTL and SelectMasterRefreshInterval need bigger than zero and" +
			" SelectMasterRefreshInterval can not be too long")
		return errors.New("selectMasterLeaseTTL and SelectMasterRefreshInterval not legal")
	}
	refreshInterval = time.Duration(conf.SelectMasterRefreshInterval) * time.Second
	leaseTTL = conf.SelectMasterLeaseTTL
	log.GetLogger().Infof("autoscaler select master time refreshInterval: %s, leaseTTL: %d", refreshInterval, leaseTTL)

	return nil
}

func startRegiterMasterNodeCircle(stopCh <-chan struct{}, registerMasterNodeJobStopCh <-chan struct{}) {
	log.GetLogger().Infof("register master node circle service start!")
	if registerMasterNodeJobStopCh == nil {
		log.GetLogger().Errorf("startRegiterMasterNodeCircle fail! registerMasterNodeJobStopCh is nil")
		return
	}
	registerTicker = time.NewTicker(refreshInterval)
	for {
		select {
		case <-registerTicker.C:
			registerMasterNode(stopCh)
		case <-registerMasterNodeJobStopCh:
			registerTicker.Stop()
			return
		case <-stopCh:
			registerTicker.Stop()
			log.GetLogger().Infof("register master node circle service close!")
			return
		}
	}
}

func registerMasterNode(stopCh <-chan struct{}) {
	resp, err := getAutoScalerMasterLockKey()
	if resp == nil {
		log.GetLogger().Warnf("query route etcd info fail! reason: resp is empty")
		return
	}
	if err != nil {
		log.GetLogger().Warnf("query route etcd info fail! reason: %s", err.Error())
		return
	}
	if len(resp.Kvs) == 0 {
		err = putCurrentNodeAsMaster()
		if err != nil {
			log.GetLogger().Warnf("put current node as master fail! reason: %s", err.Error())
			return
		}
		upgradeToMaterNode(stopCh)

		return
	}
}

func getAutoScalerMasterLockKey() (*clientv3.GetResponse, error) {
	routerClient := etcd3.GetCaeRouterEtcdClient()
	if routerClient == nil || routerClient.Client == nil {
		log.GetLogger().Warnf("failed to init route ETCDclient")
		return nil, errors.New("failed to init route ETCDclient")
	}
	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := routerClient.GetResponse(ctx, autoscalerMasterLockKey)
	if resp == nil || err != nil {
		log.GetLogger().Warnf("failed to get autoscalerMasterLockKey key:%s, err:%s", autoscalerMasterLockKey, err)
		return nil, errors.New("failed to get autoscalerMasterLockKey")
	}
	return resp, nil
}

func putCurrentNodeAsMaster() error {
	routerClient := etcd3.GetCaeRouterEtcdClient()
	if routerClient.Client == nil {
		log.GetLogger().Warnf("failed to init route ETCDclient")
		return errors.New("failed to init route ETCDclient")
	}

	grantCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	id, err := routerClient.Grant(grantCtx, leaseTTL)
	if err != nil {
		log.GetLogger().Warnf("failed to grant autoscaler instance lease: %s", err.Error())
		return err
	}
	leaseID = id
	if leaseTicker != nil {
		leaseTicker.Reset(refreshInterval)
	}

	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err = routerClient.PutOnlyKeyMissing(ctx, autoscalerMasterLockKey, registerValue, clientv3.WithLease(leaseID))
	if err != nil {
		log.GetLogger().Warnf("unable to put key:%s to etcd, err:%s", autoscalerMasterLockKey, err.Error())
		return err
	}
	log.GetLogger().Infof("register pod:cluster %s as master node", registerValue)

	return nil
}

func startRefreshLeaseJob(stopMasterNodeCh <-chan struct{}, stopCh <-chan struct{}) {
	if stopMasterNodeCh == nil {
		log.GetLogger().Warnf("stopMasterNodeCh is nil, will not refresh lease")
		return
	}
	leaseTicker = time.NewTicker(refreshInterval)
	for {
		select {
		case <-leaseTicker.C:
			refreshLease(stopCh)
		case <-stopMasterNodeCh:
			log.GetLogger().Infof("node:cluster %s is not master node, refresh lease job need to stop", registerValue)
			if leaseTicker != nil {
				leaseTicker.Stop()
			}
			return
		case <-stopCh:
			log.GetLogger().Infof("master node:cluster %s shut down suddenly!", registerValue)
			downgradeToBakcupNode(stopCh)
			if leaseTicker != nil {
				leaseTicker.Stop()
			}
			routerClient := etcd3.GetCaeRouterEtcdClient()
			if routerClient == nil {
				log.GetLogger().Warnf("failed to init route ETCD client")
				return
			}
			if routerClient.Client == nil {
				log.GetLogger().Warnf("failed to init route ETCD client")
				return
			}
			revokeCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err := routerClient.Revoke(revokeCtx, leaseID)
			if err != nil {
				log.GetLogger().Warnf("revoke lease failed, err:%s", err.Error())
			}

			ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err = routerClient.Delete(ctx, autoscalerMasterLockKey)
			if err != nil {
				log.GetLogger().Warnf("delete key:%s failed, err:%s", autoscalerMasterLockKey, err.Error())
			}
			return
		}
	}
}

func refreshLease(stopCh <-chan struct{}) {
	if !isBeingMaser() {
		downgradeToBakcupNode(stopCh)
		return
	}
	routerClient := etcd3.GetCaeRouterEtcdClient()
	if routerClient.Client == nil {
		log.GetLogger().Warnf("failed to init route ETCDclient")
		return
	}
	keepAliveOnceCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err := routerClient.KeepAliveOnce(keepAliveOnceCtx, leaseID)
	if err != nil {
		log.GetLogger().Warnf("unable to refresh lease:%s", err.Error())
	}
}

func isBeingMaser() bool {
	resp, err := getAutoScalerMasterLockKey()
	if err != nil {
		log.GetLogger().Warnf("query route etcd info fail! reason: %s", err.Error())
		return false
	}
	if resp == nil || len(resp.Kvs) == 0 {
		log.GetLogger().Warnf("query route etcd info fail! reason: no value")
		return false
	}
	if string(resp.Kvs[0].Value) != registerValue {
		log.GetLogger().Warnf("current master node:cluster [%s] ip not match etcd master node:cluster",
			registerValue, resp.Kvs[0].Value)
		return false
	}
	return true
}

func upgradeToMaterNode(stopCh <-chan struct{}) {
	log.GetLogger().Infof("node:cluster %s become master node successful!", registerValue)
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get autoscaler config, error: %s", err.Error())
		return
	}
	masterNodeJobStopCh = make(chan struct{})
	fmt.Printf("node:cluster %s become master node successful!", registerValue)
	err = startMasterNodeJob(masterNodeJobStopCh, conf)
	if err != nil {
		log.GetLogger().Errorf("error happened when start master node job: %s", err.Error())
		downgradeToBakcupNode(stopCh)
		return
	}
	if registerMasterNodeJobStopCh != nil {
		close(registerMasterNodeJobStopCh)
	}
	go startRefreshLeaseJob(masterNodeJobStopCh, stopCh)
}

func downgradeToBakcupNode(stopCh <-chan struct{}) {
	log.GetLogger().Infof("node:cluster %s is not master node now", registerValue)
	if masterNodeJobStopCh != nil {
		close(masterNodeJobStopCh)
	}
	registerMasterNodeJobStopCh = make(chan struct{})
	startRegiterMasterNodeCircle(stopCh, registerMasterNodeJobStopCh)
}
