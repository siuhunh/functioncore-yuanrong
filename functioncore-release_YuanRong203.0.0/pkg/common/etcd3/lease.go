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

// Package etcd3 implements crud and watch operations based etcd clientv3
package etcd3

import (
	"go.etcd.io/etcd/client/v3"
)

// Grant -
func (w *EtcdClient) Grant(ctxInfo EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	w.rwMutex.RLock()
	resp, err := w.Client.Grant(ctx, ttl)
	w.rwMutex.RUnlock()
	cancel()
	if err != nil {
		return 0, err
	}
	return resp.ID, nil
}

// KeepAliveOnce -
func (w *EtcdClient) KeepAliveOnce(ctxInfo EtcdCtxInfo, leaseID clientv3.LeaseID) error {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	w.rwMutex.RLock()
	_, err := w.Client.KeepAliveOnce(ctx, leaseID)
	w.rwMutex.RUnlock()
	cancel()
	return err
}

// Revoke -
func (w *EtcdClient) Revoke(ctxInfo EtcdCtxInfo, leaseID clientv3.LeaseID) error {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	w.rwMutex.RLock()
	_, err := w.Client.Revoke(ctx, leaseID)
	w.rwMutex.RUnlock()
	cancel()
	return err
}
