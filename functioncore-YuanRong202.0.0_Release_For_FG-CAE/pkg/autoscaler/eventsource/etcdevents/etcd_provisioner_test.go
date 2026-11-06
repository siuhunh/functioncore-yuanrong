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

package etcdevents

import (
	"context"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
)

func TestNewProvisioner(t *testing.T) {
	patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{}
	})
	patch.ApplyFunc(etcd3.GetCAEMetaEtcdClient, func() *etcd3.EtcdClient { return &etcd3.EtcdClient{} })
	defer patch.Reset()
	type args struct {
		sourcetype string
		kind       string
		path       string
		queue      workqueue.RateLimitingInterface
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Test with WorkerManagerInstancePrefix",
			args: args{
				sourcetype: "testSourceType",
				kind:       "testKind",
				path:       utils.WorkerManagerInstancePrefix,
				queue:      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
			},
		},
		{
			name: "Test with non-WorkerManagerInstancePrefix",
			args: args{
				sourcetype: "testSourceType",
				kind:       "testKind",
				path:       "non-WorkerManagerInstancePrefix",
				queue:      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewProvisioner(tt.args.sourcetype, tt.args.kind, tt.args.path, tt.args.queue)
			if got == nil {
				t.Errorf("NewProvisioner() = %v, want not nil", got)
			}
			if got.Path != tt.args.path {
				t.Errorf("NewProvisioner() Path = %v, want %v", got.Path, tt.args.path)
			}
			if got.SinkQueue != tt.args.queue {
				t.Errorf("NewProvisioner() SinkQueue = %v, want %v", got.SinkQueue, tt.args.queue)
			}
			if got.SourceType != tt.args.sourcetype {
				t.Errorf("NewProvisioner() SourceType = %v, want %v", got.SourceType, tt.args.sourcetype)
			}
			if got.Kind != tt.args.kind {
				t.Errorf("NewProvisioner() Kind = %v, want %v", got.Kind, tt.args.kind)
			}
			if got.EtcdClient == nil {
				t.Errorf("NewProvisioner() EtcdClient = %v, want not nil", got.EtcdClient)
			}
		})
	}
}

func TestStart(t *testing.T) {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
	err := p.Start(nil)
	assert.NoError(t, err)
}

func TestCreateWatcher(t *testing.T) {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{}}
	})
	patch.ApplyFunc((*etcd3.EtcdClient).Watch, func(_ *etcd3.EtcdClient, ctx context.Context,
		watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
		return nil
	})
	p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
	defer patch.Reset()
	watcher := p.createWatcher()
	assert.Nil(t, watcher)
}

func TestStartWatch(t *testing.T) {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	t.Run("stop is nil", func(t *testing.T) {
		p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
		p.StartWatch(nil)
	})
	t.Run("createWatcher return nil", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		patch.ApplyFunc((*etcd3.EtcdClient).Watch, func(_ *etcd3.EtcdClient, ctx context.Context,
			watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			return nil
		})
		defer patch.Reset()
		p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
		stop := make(chan struct{})
		p.StartWatch(stop)
	})
	t.Run("stop channel closed", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		patch.ApplyFunc((*etcd3.EtcdClient).Watch, func(_ *etcd3.EtcdClient, ctx context.Context,
			watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			return &etcd3.EtcdWatchChan{ResultChan: make(chan *etcd3.Event)}
		})
		defer patch.Reset()
		p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
		stop := make(chan struct{})
		done := make(chan bool)
		go func() {
			p.StartWatch(stop)
			done <- true
		}()
		close(stop)
		<-done
	})
	t.Run("resultChan closed", func(t *testing.T) {
		resultChan := make(chan *etcd3.Event)
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		patch.ApplyFunc((*etcd3.EtcdClient).Watch, func(_ *etcd3.EtcdClient, ctx context.Context,
			watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			return &etcd3.EtcdWatchChan{ResultChan: resultChan}
		})
		defer patch.Reset()
		p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
		stop := make(chan struct{})
		done := make(chan bool)
		go func() {
			p.StartWatch(stop)
			done <- true
		}()
		close(resultChan)
		close(stop)
		<-done
	})
	t.Run("received signal to quit", func(t *testing.T) {
		resultChan := make(chan *etcd3.Event)
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		patch.ApplyFunc((*etcd3.EtcdClient).Watch, func(_ *etcd3.EtcdClient, ctx context.Context,
			watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			return &etcd3.EtcdWatchChan{ResultChan: resultChan}
		})
		defer patch.Reset()
		p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
		stop := make(chan struct{})
		done := make(chan bool)
		go func() {
			p.StartWatch(stop)
			done <- true
		}()
		stop <- struct{}{}
		<-done
	})
}

func TestIsNeedWatchPath(t *testing.T) {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)

	if !p.isNeedWatchPath(utils.FunctionMetadataPrefix + "test") {
		t.Error("Expected true for path with FunctionMetadataPrefix")
	}

	if !p.isNeedWatchPath(utils.InstanceMetadataPrefix + "test") {
		t.Error("Expected true for path with InstanceMetadataPrefix")
	}

	if !p.isNeedWatchPath(utils.WorkerManagerInstancePrefix + "test") {
		t.Error("Expected true for path with WorkerManagerInstancePrefix")
	}

	if p.isNeedWatchPath("test") {
		t.Error("Expected false for path without any prefix")
	}
}

func TestGenerateEvents(t *testing.T) {
	p := &Provisioner{
		SourceType: "testSourceType",
		Kind:       "testKind",
	}

	testCases := []struct {
		name       string
		path       string
		sourcetype string
		value      interface{}
	}{
		{
			name:       "Test with string value",
			path:       "testPath",
			sourcetype: "testSourceType",
			value:      "testValue",
		},
		{
			name:       "Test with int value",
			path:       "testPath",
			sourcetype: "testSourceType",
			value:      123,
		},
		{
			name:       "Test with bool value",
			path:       "testPath",
			sourcetype: "testSourceType",
			value:      true,
		},
		{
			name:       "Test with struct value",
			path:       "testPath",
			sourcetype: "testSourceType",
			value:      struct{}{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := p.generateEvents(tc.path, tc.sourcetype, tc.value)
			assert.Equal(t, "testSourceType", event.SourceType)
			assert.Equal(t, tc.sourcetype, event.EventType)
			assert.Equal(t, "testKind", event.Kind)
			assert.Equal(t, tc.path, event.KeyRef)
			assert.Equal(t, tc.value, event.Object)
		})
	}
}

func TestProcessEvent(t *testing.T) {
	testCases := []struct {
		name      string
		Type      int
		key       string
		expectLen int
	}{
		{
			name:      "Test put event type",
			Type:      etcd3.PUT,
			key:       "/test/key",
			expectLen: 1,
		},
		{
			name:      "Test delete event type",
			Type:      etcd3.DELETE,
			key:       "/test/key",
			expectLen: 1,
		},
		{
			name:      "Test error event type",
			Type:      etcd3.ERROR,
			expectLen: 0,
		},
		{
			name:      "Test unknown event type",
			Type:      -1,
			expectLen: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
			p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
			event := &etcd3.Event{
				Type: tc.Type,
				Key:  "/test/key",
			}
			p.processEvent(event)
			assert.Equal(t, tc.expectLen, p.SinkQueue.Len())
		})
	}
}

func TestProcessWatchEvent(t *testing.T) {
	type args struct {
		event *etcd3.Event
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Test processWatchEvent when isNeedWatchPath is false",
			args: args{
				event: &etcd3.Event{
					Key: "/test/key",
				},
			},
		},
		{
			name: "Test processWatchEvent when isNeedWatchPath is true",
			args: args{
				event: &etcd3.Event{
					Key: utils.FunctionMetadataPrefix + "test",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
			p := NewProvisioner("testSourceType", "testKind", utils.WorkerManagerInstancePrefix, queue)
			p.processWatchEvent(tt.args.event)
		})
	}
}
