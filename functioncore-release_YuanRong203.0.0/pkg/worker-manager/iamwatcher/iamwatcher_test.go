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

// Package iamwatcher iam watcher
package iamwatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

func Test_RunIAMTokenWatcher(t *testing.T) {
	convey.Convey("test RunIAMTokenWatcher", t, func() {
		convey.Convey("case1: success", func() {
			iamWatchChan := &etcd3.EtcdWatchChan{ResultChan: make(chan *etcd3.Event, 1)}
			callParseIAMEventCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch,
					func(_ *etcd3.EtcdClient, _ context.Context, _ string, _ int64, _ bool) *etcd3.EtcdWatchChan {
						return iamWatchChan
					}),
				gomonkey.ApplyFunc(parseIAMEvent, func(_ *etcd3.Event) {
					callParseIAMEventCount++
				}),
			})
			defer patches.ResetAll()
			stopCh := make(chan struct{})
			go RunIAMTokenWatcher(stopCh)
			iamWatchChan.ResultChan <- &etcd3.Event{}
			time.Sleep(3100 * time.Millisecond)
			close(stopCh)
			convey.So(callParseIAMEventCount, convey.ShouldEqual, 1)
		})
		convey.Convey("case2: stopCh is nil", func() {
			callParseIAMEventCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(parseIAMEvent, func(_ *etcd3.Event) {
					callParseIAMEventCount++
				}),
			})
			defer patches.ResetAll()
			RunIAMTokenWatcher(nil)
			convey.So(callParseIAMEventCount, convey.ShouldEqual, 0)
		})
		convey.Convey("case3: fg meta client is nil", func() {
			callParseIAMEventCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return nil
				}),
				gomonkey.ApplyFunc(parseIAMEvent, func(_ *etcd3.Event) {
					callParseIAMEventCount++
				}),
			})
			defer patches.ResetAll()
			stopCh := make(chan struct{})
			RunIAMTokenWatcher(stopCh)
			close(stopCh)
			convey.So(callParseIAMEventCount, convey.ShouldEqual, 0)
		})
		convey.Convey("case4: iamWatchChan is nil", func() {
			callParseIAMEventCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch,
					func(_ *etcd3.EtcdClient, _ context.Context, _ string, _ int64, _ bool) *etcd3.EtcdWatchChan {
						return nil
					}),
				gomonkey.ApplyFunc(parseIAMEvent, func(_ *etcd3.Event) {
					callParseIAMEventCount++
				}),
			})
			defer patches.ResetAll()
			stopCh := make(chan struct{})
			RunIAMTokenWatcher(stopCh)
			close(stopCh)
			convey.So(callParseIAMEventCount, convey.ShouldEqual, 0)
		})
		convey.Convey("case5: iamWatchChan is closed", func() {
			iamWatchChan := &etcd3.EtcdWatchChan{}
			callParseIAMEventCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch,
					func(_ *etcd3.EtcdClient, _ context.Context, _ string, _ int64, _ bool) *etcd3.EtcdWatchChan {
						iamWatchChan.ResultChan = make(chan *etcd3.Event, 1)
						return iamWatchChan
					}),
				gomonkey.ApplyFunc(parseIAMEvent, func(_ *etcd3.Event) {
					callParseIAMEventCount++
				}),
			})
			defer patches.ResetAll()
			stopCh := make(chan struct{})
			go RunIAMTokenWatcher(stopCh)
			time.Sleep(3100 * time.Millisecond)
			close(iamWatchChan.ResultChan)
			time.Sleep(100 * time.Millisecond)
			close(stopCh)
			convey.So(callParseIAMEventCount, convey.ShouldEqual, 0)
		})
	})
}

func Test_parseIAMEvent(t *testing.T) {
	convey.Convey("test parseIAMEvent", t, func() {
		convey.Convey("case1: put event", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(updateWorkerAgency, func(_ string) {}),
				gomonkey.ApplyFunc(utils.ParseIAMToken, func(_, _ string) (string, bool) {
					return "token", true
				}),
			})
			defer patches.ResetAll()
			event := &etcd3.Event{
				Key:   "/yr/iam/token/new/cluster001/tenant001",
				Type:  etcd3.PUT,
				Value: []byte("{\"salt\": \"salt\", \"token\": \"token\"}"),
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt, convey.ShouldNotBeNil)
			convey.So(tokenSalt.Salt, convey.ShouldEqual, "salt")
			convey.So(tokenSalt.Token, convey.ShouldEqual, "token")
			cache.DeleteIAMToken("tenant001")
		})
		convey.Convey("case2: delete event", func() {
			tokenSalt := &cache.TokenSalt{
				Salt:  "salt",
				Token: "token",
			}
			err := cache.PutExpiringIAMTokenMap("tenant001", tokenSalt)
			convey.So(err, convey.ShouldBeNil)
			event := &etcd3.Event{
				Key:  "/yr/iam/token/new/cluster001/tenant001",
				Type: etcd3.DELETE,
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt2 := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt2, convey.ShouldBeNil)
		})
		convey.Convey("case3: unknown event", func() {
			event := &etcd3.Event{
				Key:  "/yr/iam/token/new/cluster001/tenant001",
				Type: etcd3.ERROR,
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt2 := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt2, convey.ShouldBeNil)
		})
		convey.Convey("case4: not iam event", func() {
			event := &etcd3.Event{
				Key:   "/xxx",
				Type:  etcd3.PUT,
				Value: []byte("{\"salt\": \"salt\", \"token\": \"token\"}"),
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt, convey.ShouldBeNil)
		})
		convey.Convey("case5: invalid iam token key", func() {
			event := &etcd3.Event{
				Key:   "/yr/iam/token/new/cluster001/",
				Type:  etcd3.PUT,
				Value: []byte("{\"salt\": \"salt\", \"token\": \"token\"}"),
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt, convey.ShouldBeNil)
		})
		convey.Convey("case6: invalid tokenSalt value", func() {
			event := &etcd3.Event{
				Key:   "/yr/iam/token/new/cluster001/tenant001",
				Type:  etcd3.PUT,
				Value: []byte("xxx"),
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt, convey.ShouldBeNil)
		})
		convey.Convey("case6: failed to put iam token into map", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(cache.PutExpiringIAMTokenMap, func(_ string, _ *cache.TokenSalt) error {
					return errors.New("error")
				}),
				gomonkey.ApplyFunc(utils.ParseIAMToken, func(_, _ string) (string, bool) {
					return "token", true
				}),
			})
			defer patches.ResetAll()
			event := &etcd3.Event{
				Key:   "/yr/iam/token/new/cluster001/tenant001",
				Type:  etcd3.PUT,
				Value: []byte("{\"salt\": \"salt\", \"token\": \"token\"}"),
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt, convey.ShouldBeNil)
		})
		convey.Convey("case7: failed to parse iam token", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(cache.PutExpiringIAMTokenMap, func(_ string, _ *cache.TokenSalt) error {
					return errors.New("error")
				}),
				gomonkey.ApplyFunc(utils.ParseIAMToken, func(_, _ string) (string, bool) {
					return "", false
				}),
			})
			defer patches.ResetAll()
			event := &etcd3.Event{
				Key:   "/yr/iam/token/new/cluster001/tenant001",
				Type:  etcd3.PUT,
				Value: []byte("{\"salt\": \"salt\", \"token\": \"token\"}"),
			}
			cache.DeleteIAMToken("tenant001")
			parseIAMEvent(event)
			tokenSalt := cache.GetFromExpiringIAMTokenMap("tenant001")
			convey.So(tokenSalt, convey.ShouldBeNil)
		})
	})
}

func Test_updateWorkerAgency(t *testing.T) {
	convey.Convey("test updateWorkerAgency", t, func() {
		convey.Convey("case1: success", func() {
			callSendCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
					return &podfetcher.PodFetcher{}
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).SendUpdateAgencyInfoRequestToWorker,
					func(_ *podfetcher.PodFetcher, _ v1alpha1.WorkerInstance, _ string, _ string, _ *faasfunction.FunctionService, _ string) {
						callSendCount++
					}),
				gomonkey.ApplyFunc(utils.GetFunctionPodsByInformer, func(_ *faasfunction.FunctionService) []*corev1.Pod {
					return []*corev1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: "pod",
								Labels: map[string]string{
									utils.LabelApplier: "worker-manager",
								},
							},
						},
					}
				}),
			})
			defer patches.ResetAll()
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("test", &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Runtime: faasfunction.RuntimeSpec{
							Language: "java8",
						},
						TenantID: "tenant001",
					},
				},
			})
			updateWorkerAgency("tenant001")
			convey.So(callSendCount, convey.ShouldEqual, 1)
			cache.WorkerCache.DeleteFunctionCache("test")
		})
		convey.Convey("case2: pod fetcher is nil", func() {
			callSendCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
					return nil
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).SendUpdateAgencyInfoRequestToWorker,
					func(_ *podfetcher.PodFetcher, _ v1alpha1.WorkerInstance, _ string, _ string, _ *faasfunction.FunctionService, _ string) {
						callSendCount++
					}),
			})
			defer patches.ResetAll()
			updateWorkerAgency("tenant001")
			convey.So(callSendCount, convey.ShouldEqual, 0)
			cache.WorkerCache.DeleteFunctionCache("test")
		})
		convey.Convey("case3: workerIns is empty", func() {
			callSendCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
					return &podfetcher.PodFetcher{}
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).SendUpdateAgencyInfoRequestToWorker,
					func(_ *podfetcher.PodFetcher, _ v1alpha1.WorkerInstance, _ string, _ string, _ *faasfunction.FunctionService, _ string) {
						callSendCount++
					}),
			})
			defer patches.ResetAll()
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("test", &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Runtime: faasfunction.RuntimeSpec{
							Language: "java8",
						},
						TenantID: "tenant001",
					},
				},
			})
			updateWorkerAgency("tenant001")
			convey.So(callSendCount, convey.ShouldEqual, 0)
			cache.WorkerCache.DeleteFunctionCache("test")
		})
		convey.Convey("case4: not iam token func", func() {
			callSendCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
					return &podfetcher.PodFetcher{}
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).SendUpdateAgencyInfoRequestToWorker,
					func(_ *podfetcher.PodFetcher, _ v1alpha1.WorkerInstance, _ string, _ string, _ *faasfunction.FunctionService, _ string) {
						callSendCount++
					}),
				gomonkey.ApplyFunc(utils.GetFunctionPodsByInformer, func(_ *faasfunction.FunctionService) []*corev1.Pod {
					return []*corev1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: "pod",
								Labels: map[string]string{
									utils.LabelApplier: "worker-manager",
								},
							},
						},
					}
				}),
			})
			defer patches.ResetAll()
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("test", &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Runtime: faasfunction.RuntimeSpec{
							Language: "java11",
						},
						TenantID: "tenant001",
					},
				},
			})
			updateWorkerAgency("tenant001")
			convey.So(callSendCount, convey.ShouldEqual, 0)
			cache.WorkerCache.DeleteFunctionCache("test")
		})
		convey.Convey("case5: not this tenant", func() {
			callSendCount := 0
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
					return &podfetcher.PodFetcher{}
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).SendUpdateAgencyInfoRequestToWorker,
					func(_ *podfetcher.PodFetcher, _ v1alpha1.WorkerInstance, _ string, _ string, _ *faasfunction.FunctionService, _ string) {
						callSendCount++
					}),
				gomonkey.ApplyFunc(utils.GetFunctionPodsByInformer, func(_ *faasfunction.FunctionService) []*corev1.Pod {
					return []*corev1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: "pod",
								Labels: map[string]string{
									utils.LabelApplier: "worker-manager",
								},
							},
						},
					}
				}),
			})
			defer patches.ResetAll()
			cache.WorkerCache.CreateFunctionCacheByFuncMeta("test", &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Runtime: faasfunction.RuntimeSpec{
							Language: "java8",
						},
						TenantID: "tenant002",
					},
				},
			})
			updateWorkerAgency("tenant001")
			convey.So(callSendCount, convey.ShouldEqual, 0)
			cache.WorkerCache.DeleteFunctionCache("test")
		})
	})
}
