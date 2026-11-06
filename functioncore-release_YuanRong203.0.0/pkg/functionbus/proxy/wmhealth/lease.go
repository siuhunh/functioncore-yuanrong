/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package wmhealth

import (
	"errors"
	"os"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/informers"
	informercdv1 "k8s.io/client-go/informers/coordination/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/k8sclient"
	"functioncore/pkg/common/logger/log"
)

const (
	kubeTimeout = 10 * time.Second
)

var (
	leaseInformer informercdv1.LeaseInformer
	wmAddr        string
	wmAddrMutex   sync.RWMutex
)

func getWmAddr() string {
	wmAddrMutex.RLock()
	ret := wmAddr
	wmAddrMutex.RUnlock()
	return ret
}

func updateWmAddr(addr string) {
	wmAddrMutex.Lock()
	wmAddr = addr
	if workerManagerHealthCtx != nil {
		workerManagerHealthCtx.CancelAndCreateNewContext()
	}
	wmAddrMutex.Unlock()
}

func initLeaseInformer(stopCh <-chan struct{}) error {
	kubeConfig, err := k8sclient.GetKubeConfig()
	if err != nil {
		log.GetLogger().Errorf("get k8s config error: %s when new resource lock cluster", err.Error())
		return err
	}

	kubeConfig.Timeout = kubeTimeout

	// creates the clientset
	kubeClientSet, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.GetLogger().Errorf("failed to create kubernetes clientset: %s", err)
		return err
	}

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClientSet, 0,
		informers.WithNamespace(os.Getenv(constants.SystemNameSpaceEnvKey)))
	leaseInformer = informerFactory.Coordination().V1().Leases()

	leaseInformer.Informer().AddEventHandler(generateInformerHandler())

	go leaseInformer.Informer().Run(stopCh)
	if ok := cache.WaitForCacheSync(stopCh, leaseInformer.Informer().HasSynced); !ok {
		return errors.New("failed to sync")
	}
	return nil
}

func generateInformerHandler() cache.FilteringResourceEventHandler {
	return cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			lease, ok := obj.(*coordinationv1.Lease)
			if !ok {
				return false
			}

			if lease.Name == "worker-manager" {
				return true
			}

			return false
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				lease, ok := obj.(*coordinationv1.Lease)
				if !ok {
					return
				}
				if lease.Spec.HolderIdentity == nil {
					return
				}
				updateWmAddr(*lease.Spec.HolderIdentity)
				log.GetLogger().Infof("ip of worker-manger leader is: %s", *lease.Spec.HolderIdentity)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				lease, ok := newObj.(*coordinationv1.Lease)
				if !ok {
					return
				}
				if lease.Spec.HolderIdentity == nil {
					return
				}
				oldAddr := getWmAddr()
				if oldAddr != *lease.Spec.HolderIdentity {
					updateWmAddr(*lease.Spec.HolderIdentity)
					log.GetLogger().Infof("ip of worker-manager leader update to %s", *lease.Spec.HolderIdentity)
				}
			},
			DeleteFunc: func(obj interface{}) {
				log.GetLogger().Errorf("leader of worker-manager lost")
				updateWmAddr("")
			},
		},
	}
}
