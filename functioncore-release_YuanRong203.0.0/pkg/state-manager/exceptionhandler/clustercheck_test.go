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

// Package exceptionhandler handling exceptions
package exceptionhandler

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/state-manager/state/instancemanager"
	"functioncore/test/utils"
)

func Test_WatchClusterStatus(t *testing.T) {
	test := []struct {
		name       string
		metaClient *etcd3.EtcdClient
		stopChan   chan struct{}
		patchFunc  utils.PatchesFunc
		extFunc    func(*etcd3.EtcdClient, chan struct{})
		wantErr    bool
	}{
		{
			"test result chan close",
			&etcd3.EtcdClient{
				Client: &clientv3.Client{},
			},
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
				})
				return patches
			},
			func(cl *etcd3.EtcdClient, c chan struct{}) {
			},
			false,
		},
		{
			"test process error",
			&etcd3.EtcdClient{
				Client: &clientv3.Client{},
			},
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
					gomonkey.ApplyFunc(processClusterStatusEvent, func(event *etcd3.Event) error {
						return errors.New("process bus proxy event error")
					}),
				})
				return patches
			},
			func(cl *etcd3.EtcdClient, c chan struct{}) {
			},
			false,
		},
		{
			"test process success",
			&etcd3.EtcdClient{
				Client: &clientv3.Client{},
			},
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
					gomonkey.ApplyFunc(processClusterStatusEvent, func(event *etcd3.Event) error {
						return nil
					}),
				})
				return patches
			},
			func(cl *etcd3.EtcdClient, c chan struct{}) {
			},
			false,
		},
	}

	for _, tt := range test {
		patches := tt.patchFunc()
		if tt.stopChan != nil {
			go func(stopChan chan struct{}) {
				<-time.After(time.Millisecond * 1)
				close(stopChan)
			}(tt.stopChan)
		}
		go tt.extFunc(tt.metaClient, tt.stopChan)
		// watchClusterStatus does not have return value, just make sure it doesn't run panic
		watchClusterStatus(tt.metaClient, tt.stopChan)
		patches.ResetAll()
	}
}

func Test_ProcessClusterStatusEvent(t *testing.T) {
	test := []struct {
		name        string
		event       *etcd3.Event
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			"test PUT error: parseClusterStatusInfo error",
			&etcd3.Event{
				Type: etcd3.PUT,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseClusterStatusInfo, func(event *etcd3.Event) (
						map[string]clusterstatusutil.ClusterFlowState, error) {
						return nil, errors.New("test error")
					}),
				})
				return patches
			},
			true,
		},
		{
			"test PUT success, state is not flowouting",
			&etcd3.Event{
				Type: etcd3.PUT,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseClusterStatusInfo, func(event *etcd3.Event) (
						map[string]clusterstatusutil.ClusterFlowState, error) {
						clusterStatusMap := make(map[string]clusterstatusutil.ClusterFlowState)
						clusterStatusMap["cluster001"] = clusterstatusutil.ClusterFlowState{State: clusterstatusutil.ClusterRunningStatus}
						return clusterStatusMap, nil
					}),
				})
				return patches
			},
			false,
		},
		{
			"test PUT success, state is flowouting",
			&etcd3.Event{
				Type: etcd3.PUT,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseClusterStatusInfo, func(event *etcd3.Event) (
						map[string]clusterstatusutil.ClusterFlowState, error) {
						clusterStatusMap := make(map[string]clusterstatusutil.ClusterFlowState)
						clusterStatusMap["cluster001"] = clusterstatusutil.ClusterFlowState{State: clusterstatusutil.ClusterFlowOutingStatus}
						return clusterStatusMap, nil
					}),
				})
				return patches
			},
			false,
		},
		{
			"test delete event",
			&etcd3.Event{
				Key:  busKey,
				Type: etcd3.DELETE,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			false,
		},
		{
			"test default",
			&etcd3.Event{
				Type: etcd3.ERROR,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
	}
	for _, tt := range test {
		patches := tt.patchesFunc()
		err := processClusterStatusEvent(tt.event)
		if (err != nil) != tt.wantErr {
			t.Errorf("process delete event %s error, err %v , wantErr %v", tt.name, err, tt.wantErr)
		}
		patches.ResetAll()
	}
}

func Test_ProcessClusterStatusEventConcurrent(t *testing.T) {
	count := 0
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyFunc(parseClusterStatusInfo, func(event *etcd3.Event) (
			map[string]clusterstatusutil.ClusterFlowState, error) {
			clusterStatusMap := make(map[string]clusterstatusutil.ClusterFlowState)
			clusterStatusMap["cluster001"] = clusterstatusutil.ClusterFlowState{State: clusterstatusutil.ClusterFlowOutingStatus}
			return clusterStatusMap, nil
		}),
		gomonkey.ApplyFunc(doReleaseStateIDInUpgradingCluster, func() {
			count++
		}),
	})
	defer patches.ResetAll()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		processClusterStatusEvent(&etcd3.Event{
			Type: etcd3.PUT,
		})
		wg.Done()
	}()
	go func() {
		processClusterStatusEvent(&etcd3.Event{
			Type: etcd3.PUT,
		})
		wg.Done()
	}()
	wg.Wait()
	convey.ShouldBeTrue(count == 1)
}

func Test_doReleaseStateIDInUpgradingCluster(t *testing.T) {
	callCount := 0
	test := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		callTimes     int
	}{
		{
			"status is not flowouting, will not call MigrateStateIDToOtherCluster",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusFlowouting, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(instancemanager.GetStateIDListOfCluster, func() []string {
						callCount++
						return []string{}
					}),
					gomonkey.ApplyFunc(instancemanager.MigrateStateIDToOtherCluster, func(string) bool {
						callCount++
						return true
					}),
				})
				return patches
			},
			0,
		},
		{
			"status is flowouting, GetStateIDListOfCluster is empty",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusFlowouting, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(instancemanager.GetStateIDListOfCluster, func() []string {
						callCount++
						return []string{}
					}),
					gomonkey.ApplyFunc(instancemanager.MigrateStateIDToOtherCluster, func(string) bool {
						callCount++
						return true
					}),
				})
				return patches
			},
			1,
		},
		{
			"status is flowouting, GetStateIDListOfCluster is not empty",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusFlowouting, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(instancemanager.GetStateIDListOfCluster, func() []string {
						callCount++
						return []string{"testStateID"}
					}),
					gomonkey.ApplyFunc(instancemanager.MigrateStateIDToOtherCluster, func(string) bool {
						callCount++
						return true
					}),
				})
				return patches
			},
			2,
		},
	}

	for _, tt := range test {
		patches := tt.patchesFunc()
		callCount = 0
		doReleaseStateIDInUpgradingCluster()
		if callCount != tt.callTimes {
			t.Errorf("process doReleaseStateIDInUpgradingCluster %s error, callCount is %d, but wanted is %d", tt.name, callCount, tt.callTimes)
		}
		patches.ResetAll()
	}
}