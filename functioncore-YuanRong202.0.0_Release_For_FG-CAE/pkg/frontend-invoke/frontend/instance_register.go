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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

var (
	leaseID         clientv3.LeaseID
	instanceKey     string
	refreshInterval       = 15 * time.Second
	leaseTTL        int64 = 30
	leaseTicker     *time.Ticker
)

func registerInstance(stopCh <-chan struct{}) {
	var err error
	instanceKey, err = getInstanceKey()
	if err != nil {
		return
	}

	err = putInstanceInfoToEtcd()
	if err != nil {
		log.GetLogger().Errorf("failed to register instance to etcd when start, error:%s", err.Error())
		return
	}

	go startRefreshLeaseJob(stopCh)
}

func startRefreshLeaseJob(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil, will not refresh lease")
		return
	}
	leaseTicker = time.NewTicker(refreshInterval)
	for {
		select {
		case <-leaseTicker.C:
			refreshLease()
		case <-stopCh:
			leaseTicker.Stop()
			metaClient := etcd3.GetMetaEtcdClient()
			if metaClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta ETCD client")
				return
			}
			revokeCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err := metaClient.Revoke(revokeCtx, leaseID)
			if err != nil {
				log.GetLogger().Warnf("revoke lease failed, err:%s", err.Error())
			}

			ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err = metaClient.Delete(ctx, instanceKey)
			if err != nil {
				log.GetLogger().Errorf("delete key:%s failed, err:%s", instanceKey, err.Error())
			}
			return
		}
	}
}

func refreshLease() {
	if !isKeyExist() {
		if err := putInstanceInfoToEtcd(); err != nil {
			return
		}
	}
	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient.Client == nil {
		log.GetLogger().Warnf("failed to init meta ETCD client")
		return
	}
	keepAliveOnceCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err := metaClient.KeepAliveOnce(keepAliveOnceCtx, leaseID)
	if err != nil {
		log.GetLogger().Errorf("unable to refresh lease:%s", err.Error())
		if err = putInstanceInfoToEtcd(); err != nil {
			log.GetLogger().Errorf("failed to retry register frontend instance, err: %s", err.Error())
		}
	}
}

func isKeyExist() bool {
	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient.Client == nil {
		log.GetLogger().Warnf("failed to init meta ETCD client")
		return false
	}
	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := metaClient.GetResponseWithSerializable(ctx, instanceKey,
		clientv3.WithKeysOnly())
	if err != nil {
		log.GetLogger().Errorf("failed to get etcd key:%s, err:%s", instanceKey, err)
		return false
	}
	return len(resp.Kvs) > 0
}

func putInstanceInfoToEtcd() error {
	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient.Client == nil {
		log.GetLogger().Warnf("failed to init meta ETCD client")
		return errors.New("failed to init meta ETCD client")
	}
	grantCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	id, err := metaClient.Grant(grantCtx, leaseTTL)
	if err != nil {
		log.GetLogger().Errorf("failed to grant frontend instance lease: %s", err.Error())
		return err
	}
	leaseID = id
	if leaseTicker != nil {
		leaseTicker.Reset(refreshInterval)
	}

	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err = metaClient.Put(ctx, instanceKey, "active", clientv3.WithLease(leaseID))
	if err != nil {
		log.GetLogger().Errorf("unable to put key:%s to etcd, err:%s", instanceKey, err.Error())
		return err
	}
	log.GetLogger().Infof("register instance:%s to etcd", instanceKey)
	return nil
}

func getInstanceKey() (string, error) {
	clusterID := os.Getenv("CLUSTER_ID")
	nodeIP := os.Getenv("NODE_IP")
	podName := os.Getenv("POD_NAME")
	err := validateEnvs(clusterID, nodeIP, podName)
	if err != nil {
		return "", err
	}

	key := fmt.Sprintf("/sn/frontend/instances/%s/%s/%s", clusterID, nodeIP, podName)
	return key, nil
}

func validateEnvs(clusterId, nodeIp, podName string) error {
	if clusterId == "" {
		log.GetLogger().Errorf("can not find CLUSTER_ID env, can not register frontend info to etcd")
		return fmt.Errorf("CLUSTER_ID env not found")
	}

	if nodeIp == "" {
		log.GetLogger().Errorf("can not find NODE_IP env, can not register frontend info to etcd")
		return fmt.Errorf("NODE_IP env not found")
	}

	if podName == "" {
		log.GetLogger().Errorf("can not find POD_NAME env, can not register frontend info to etcd")
		return fmt.Errorf("POD_NAME env not found")
	}

	return nil
}

// UpdateLeaseConfig update lease config
func UpdateLeaseConfig(leaseTTLCfg, renewTTLCfg int64) {
	if leaseTTLCfg > 0 {
		log.GetLogger().Infof("update leaseTTL config old: %d, new: %d", leaseTTL, leaseTTLCfg)
		leaseTTL = leaseTTLCfg
	}
	if renewTTLCfg > 0 {
		log.GetLogger().Infof("update renewTTL config old: %f, new: %d", refreshInterval.Seconds(), renewTTLCfg)
		refreshInterval = time.Duration(renewTTLCfg) * time.Second
	}
}
