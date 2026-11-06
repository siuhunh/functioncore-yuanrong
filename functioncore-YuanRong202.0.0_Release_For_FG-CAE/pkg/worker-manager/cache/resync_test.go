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

package cache

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8scache "k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/config"
)

func TestNewInstanceSelector(t *testing.T) {
	tests := []struct {
		name    string
		labels  map[string]string
		matched bool
	}{
		{
			name:    "label match",
			labels:  map[string]string{"isPoolPod": "false", "funcName": "bbb"},
			matched: true,
		},
		{
			name:   "no such label",
			labels: map[string]string{"funcName": "aaa", "version": "$latest"},
		},
		{
			name:   "pool pod",
			labels: map[string]string{"isPoolPod": "true", "app": "pool22-300-128"},
		},
	}

	selector := NewInstanceSelector()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.matched, selector.Matches(labels.Set(tt.labels)), tt.name)
		})
	}
}

func Test_hotLoadResync(t *testing.T) {
	defaultResyncer = &Resyncer{ticker: time.NewTicker(time.Hour)}
	oldConfig := &config.FaasConfig{}
	newConfig := &config.HotLoadConfig{Resync: config.ResyncConfig{
		SyncNow:  1,
		Disabled: 1,
	}}

	assert.NotEqual(t, newConfig.Resync.SyncNow, oldConfig.Resync.SyncNow)
	assert.NotEqual(t, newConfig.Resync.Disabled, oldConfig.Resync.Disabled)

	hotLoadResync(oldConfig, newConfig)

	assert.Equal(t, int64(defaultResyncTime), oldConfig.Resync.Interval)
	assert.Equal(t, newConfig.Resync.SyncNow, oldConfig.Resync.SyncNow)
	assert.Equal(t, newConfig.Resync.Disabled, oldConfig.Resync.Disabled)
	assert.Equal(t, newConfig.Resync.Disabled, defaultResyncer.disabled)
}

func TestResyncerWatch(t *testing.T) {
	stop := make(chan struct{})
	r := NewResyncer(time.Second, nil, stop)

	datas := []*etcd3.Event{
		{
			Type:  etcd3.PUT,
			Key:   "/sn/workers/business/yrk/tenant/123/function/0@default@test/version/latest/defaultaz/pod1",
			Value: []byte("{}"),
		},
		{
			Type:  etcd3.PUT,
			Key:   "/invalidkey",
			Value: []byte("{}"),
		},
		{
			Type:  etcd3.PUT,
			Key:   "/sn/workers/business/yrk/tenant/123/function/0@default@test/version/latest/defaultaz/pod2",
			Value: []byte("{}"),
		},
		{
			Type:  etcd3.PUT,
			Key:   "/sn/workers/business/yrk/tenant/123/function/0@default@test/version/latest/defaultaz/pod3",
			Value: []byte("invalid value"),
		},
		{
			Type:  etcd3.PUT,
			Key:   "/sn/workers/business/yrk/tenant/0/function/functiontask/version/latest/defaultaz/functiontask-1",
			Value: []byte("{}"),
		},
		{
			Type:  etcd3.PUT,
			Key:   "/sn/workers/business/yrk/tenant/0/function/functiontask/version/latest/defaultaz/functiontask-2",
			Value: []byte("{}"),
		},
		{
			Type:  etcd3.SYNCED,
			Key:   "/sn/workers/business/yrk/tenant/123/function/0@default@test/version/latest/defaultaz/pod3",
			Value: []byte("{}"),
		},
		{
			Type:      etcd3.DELETE,
			Key:       "/sn/workers/business/yrk/tenant/123/function/0@default@test/version/latest/defaultaz/pod2",
			PrevValue: []byte("{}"),
		},
		{
			Type:      etcd3.DELETE,
			Key:       "/sn/workers/business/yrk/tenant/0/function/functiontask/version/latest/defaultaz/functiontask-2",
			PrevValue: []byte("{}"),
		},
	}

	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(_ *etcd3.EtcdClient, ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			ret := &etcd3.EtcdWatchChan{
				ResultChan: make(chan *etcd3.Event, 100),
			}
			for i := range datas {
				ret.ResultChan <- datas[i]
			}
			return ret
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	go r.startWatch()
	time.Sleep(500 * time.Millisecond)
	close(stop)
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, len(r.etcdCache))
	_, ok := r.etcdCache["pod1"]
	assert.True(t, ok)
}

func TestResyncer_reSync(t *testing.T) {
	currentPods := []runtime.Object{
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1_valid",
			Namespace: "default",
			Labels:    map[string]string{"isPoolPod": "false", "funcName": "aaa"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "pod2_in_pool",
			Namespace: "default",
			Labels:    map[string]string{"isPoolPod": "true"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "pod3_prediction",
			Namespace: "default",
			Labels:    map[string]string{"isPoolPod": "false", "applier": constants.PredictionApplier, "funcName": "abc"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:              "pod4_deleted",
			Namespace:         "default",
			DeletionTimestamp: &metav1.Time{},
			Labels:            map[string]string{"isPoolPod": "false", "funcName": "xyz"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "pod5_valid",
			Namespace: "default",
			Labels:    map[string]string{"isPoolPod": "false", "funcName": "bbb"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "pod6_no_funcName",
			Namespace: "default",
			Labels:    map[string]string{"isPoolPod": "false"},
		}},
	}

	fakeK8sClient := fake.NewSimpleClientset(currentPods...)

	informerFakeFactory := informers.NewSharedInformerFactory(fakeK8sClient, 0)
	podInformers := informerFakeFactory.Core().V1().Pods().Informer()
	stop := make(chan struct{})
	informerFakeFactory.Start(stop)
	k8scache.WaitForCacheSync(nil, podInformers.HasSynced)

	r := NewResyncer(time.Second, informerFakeFactory.Core().V1().Pods(), stop)
	r.etcdCache = map[string]*syncCompare{
		"defaultaz-#-pod1_valid": {},
		"defaultaz-#-pod10":      {},
	}

	r.reSync()

	assert.Equal(t, 1, len(r.toAdd)) // need add pod5 to etcd
	assert.NotNil(t, r.toAdd["defaultaz-#-pod5_valid"])
	assert.Equal(t, 1, len(r.toDelete)) // need delete pod10 from etcd
	assert.NotNil(t, r.toDelete["defaultaz-#-pod10"])
}
