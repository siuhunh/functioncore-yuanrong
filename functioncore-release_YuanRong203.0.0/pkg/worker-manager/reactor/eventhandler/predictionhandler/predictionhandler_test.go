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

package predictionhandler

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
	mockutils "functioncore/test/utils"
)

func Test_NewEventHandler(t *testing.T) {
	patch := gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
		return nil
	})
	handler := NewEventHandler()
	assert.Equal(t, false, handler != nil)
	patch.Reset()
}

func TestPredictionHandler_HandleEvent(t *testing.T) {
	cache.WorkerCache.StoreWorkerInstance("yyrk1234-0@default@nodejstest-latest-1955997107", v1alpha1.WorkerInstance{})
	kubeClient := &kubernetes.Clientset{}
	corev1Client := &typecorev1.CoreV1Client{}
	predictionNotify := &config.PredictionNotify{
		PredictionTime:  time.Now().Unix(),
		ValidTimeMinute: 1,
	}

	patch := gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {})
	defer func() {
		patch.Reset()
	}()

	eventByte, _ := json.Marshal(predictionNotify)
	utils.SetSmartWarmerEnable(true)
	type fields struct {
		podFetcher  *podfetcher.PodFetcher
		redisClient *redisclient.Client
	}
	type args struct {
		event eventsource.Event
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case invalid event kind",
			fields: fields{podFetcher: &podfetcher.PodFetcher{}},
			args: args{event: eventsource.Event{
				Kind: "unkown",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			wantErr: false,
		},
		{
			name:   "case illegal prediction value",
			fields: fields{podFetcher: &podfetcher.PodFetcher{}},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: []byte("{\"validTimeMinute\"}")},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			wantErr: false,
		},
		{
			name:   "case invalid prediction value",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: []byte("{\"validTimeMinute\":-1}")},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			wantErr: false,
		}, {
			name:   "case invalid value type",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind: utils.PredictionKind,
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case redis error",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: eventByte},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return " ", errors.New("failed to get from redis")
						}),
				})
				return patches
			},
			wantErr: true,
		}, {
			name:   "case illegal redis function key error",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: eventByte},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"yrk/12345678901234561234567890123456/0@default@nodejstest\":{\"num\":2}}", nil
						}),
				})
				return patches
			},
			wantErr: true,
		}, {
			name:   "case invalid past time",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: []byte(string("{\"predictionTime\":167048555400, \"validTimeMinute\":500}"))},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext, func(_ *podfetcher.PodFetcher,
						functionServiceID string) context.Context {
						return context.Background()
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"yrk/12345678901234561234567890123456/0@default@nodejstest/latest\":{\"num\":1}}", nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case prediction has expired",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: []byte(string("{\"predictionTime\":1670485554, \"validTimeMinute\":500}"))},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext, func(_ *podfetcher.PodFetcher,
						functionServiceID string) context.Context {
						return context.Background()
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"yrk/12345678901234561234567890123456/0@default@nodejstest/latest\":{\"num\":1}}", nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case first prediction",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: eventByte},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"yrk/12345678901234561234567890123456/0@default@nodejstest/latest\":{\"num\":2}}", nil
						}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).DoActions, func(_ *podfetcher.PodFetcher,
						funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
						return
					}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return corev1Client
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
						func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
							return mockutils.NewK8sPodImp(namespace)
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "List", func(_ *mockutils.K8sPodImp,
						ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{
							Items: []corev1.Pod{{}},
						}, nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case second prediction",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: eventByte},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext, func(_ *podfetcher.PodFetcher,
						functionServiceID string) context.Context {
						return context.Background()
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"yrk/12345678901234561234567890123456/0@default@nodejstest/latest\":{\"num\":1}}", nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case delete unPrediction pods",
			fields: fields{},
			args: args{event: eventsource.Event{
				Kind:   utils.PredictionKind,
				Object: &etcd3.Event{Value: eventByte},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext, func(_ *podfetcher.PodFetcher,
						functionServiceID string) context.Context {
						return context.Background()
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"yrk/12345678901234561234567890123456/0@default@pythontest/latest\":{\"num\":1}}", nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case smart warmer off",
			fields: fields{},
			args:   args{event: eventsource.Event{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetSmartWarmerEnable, func() bool {
						return false
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			handler := &PredictionHandler{
				redisClient: &redisclient.Client{},
				podFetcher:  &podfetcher.PodFetcher{},
			}
			err := handler.HandleEvent(tt.args.event)
			if err != nil {
				t.Log(err.Error())
			}
			if (err == nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
