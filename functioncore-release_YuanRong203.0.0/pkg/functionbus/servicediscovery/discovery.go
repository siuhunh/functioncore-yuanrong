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

// Package servicediscovery is used for service discovery
package servicediscovery

var (
	// EtcdObserverList stores observer for etcd
	EtcdObserverList = make([]Observer, 0)
)

// StartServiceDiscovery starts service discovery
func StartServiceDiscovery(stopCh <-chan struct{}) error {
	etcdNotifier, err := NewEtcdNotifier(stopCh)
	if err != nil {
		return err
	}
	etcdNotifier.AddObservers(EtcdObserverList...)
	return nil
}
