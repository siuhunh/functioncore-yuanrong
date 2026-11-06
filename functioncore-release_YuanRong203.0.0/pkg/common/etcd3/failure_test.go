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

package etcd3

import (
	"context"
	"sync"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestRedoFailedOperations(t *testing.T) {
	convey.Convey("Test Redo Failed Operations", t, func() {
		client := &EtcdClient{stopCh: make(chan struct{}, 1)}
		var operator operateETCDFunc
		client.RegisterRedoFunc(operator, RedoFailedOperations)
		client.redoFailedOperations(client.operateETCD, client.stopCh)
		convey.So(client, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Redo Failed Operations 1", t, func() {
		client := &EtcdClient{stopCh: make(chan struct{}, 1)}
		var operator operateETCDFunc
		operator = func(ctx context.Context, path, value, operate string) error {
			return nil
		}
		client.RegisterRedoFunc(operator, RedoFailedOperations)
		failedOperation = sync.Map{}
		failedOperation.Store("1", &ETCDOperation{
			Ctx:     context.Background(),
			Value:   "a",
			Operate: "as"})
		SaveFailedOperation(context.Background(), "1", "a", "b")
		client.redoFailedOperations(client.operateETCD, client.stopCh)
		convey.So(client, convey.ShouldNotBeNil)
		RemoveFailedOperation("1")
	})
	convey.Convey("Test Redo Failed Operations 2", t, func() {
		client := &EtcdClient{stopCh: make(chan struct{})}
		var operator operateETCDFunc
		operator = func(ctx context.Context, path, value, operate string) error {
			return nil
		}
		//<-client.stopCh
		client.RegisterRedoFunc(operator, RedoFailedOperations)
		failedOperation = sync.Map{}
		failedOperation.Store("1", &ETCDOperation{
			Ctx:     context.Background(),
			Value:   "a",
			Operate: "as"})
		SaveFailedOperation(context.Background(), "1", "a", "b")
		client.redoFailedOperations(client.operateETCD, nil)
		convey.So(client, convey.ShouldNotBeNil)
	})
}
