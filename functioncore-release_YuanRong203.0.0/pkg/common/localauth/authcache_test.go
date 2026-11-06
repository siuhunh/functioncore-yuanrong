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
package localauth

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/tools/cache"
)

func Test_receiverCacheKey(t *testing.T) {
	type args struct {
		obj interface{}
	}
	var a args
	a.obj = "aaa"
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"case1", a, "aaa", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := receiverCacheKey(tt.args.obj)
			if (err != nil) != tt.wantErr {
				t.Errorf("receiverCacheKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("receiverCacheKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_authCache_GetSignForSender(t *testing.T) {
	type fields struct {
		senderCache   *atomic.Value
		receiverCache cache.Store
		appID         string
		AuthConfig    AuthConfig
	}
	var f fields
	senderCache := &atomic.Value{}
	f.senderCache = senderCache

	var f2 fields
	senderCache2 := &atomic.Value{}
	senderCache2.Store(senderValue{
		time: "aaa",
		auth: "aaa",
	})
	f2.senderCache = senderCache2
	tests := []struct {
		name    string
		fields  fields
		wantS   string
		wantD   string
		wantErr bool
	}{
		{"case1", f, "", "", true},
		{"case2", f2, "aaa", "aaa", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &authCache{
				senderCache:   tt.fields.senderCache,
				receiverCache: tt.fields.receiverCache,
				appID:         tt.fields.appID,
				AuthConfig:    tt.fields.AuthConfig,
			}
			gotS, gotD, err := c.GetSignForSender()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSignForSender() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotS != tt.wantS {
				t.Errorf("GetSignForSender() gotS = %v, want %v", gotS, tt.wantS)
			}
			if gotD != tt.wantD {
				t.Errorf("GetSignForSender() gotD = %v, want %v", gotD, tt.wantD)
			}
		})
	}
}

func Test_authCache_updateReceiver(t *testing.T) {
	type fields struct {
		senderCache   *atomic.Value
		receiverCache cache.Store
		appID         string
		AuthConfig    AuthConfig
	}
	type args struct {
		sign string
	}
	var f fields
	var a args
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"case1", f, a, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &authCache{
				senderCache:   tt.fields.senderCache,
				receiverCache: tt.fields.receiverCache,
				appID:         tt.fields.appID,
				AuthConfig:    tt.fields.AuthConfig,
			}
			if err := c.updateReceiver(tt.args.sign); (err != nil) != tt.wantErr {
				t.Errorf("updateReceiver() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_authCache_createAndUpdateSender(t *testing.T) {
	receiverCache := cache.NewTTLStore(receiverCacheKey, time.Duration(5)*time.Second)
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(receiverCache), "ListKeys",
			func(_ *cache.ExpirationCache) []string {
				return []string{}
			}),
		gomonkey.ApplyFunc(DecryptKeys, func(inputAKey string, inputSKey string) ([]byte, []byte, error) {
			return []byte("aaa"), []byte("aaa"), nil
		}),
	}

	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type fields struct {
		senderCache   *atomic.Value
		receiverCache cache.Store
		appID         string
		AuthConfig    AuthConfig
	}
	var f fields
	f.AuthConfig.Duration = 1
	f.receiverCache = receiverCache
	f.senderCache = &atomic.Value{}
	f.senderCache.Store(senderValue{
		time: "aaa",
		auth: "aaa",
	})
	tests := []struct {
		name   string
		fields fields
	}{
		{"case1", f},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &authCache{
				senderCache:   tt.fields.senderCache,
				receiverCache: tt.fields.receiverCache,
				appID:         tt.fields.appID,
				AuthConfig:    tt.fields.AuthConfig,
			}
			c.createAndUpdateSender()
		})
	}
}

func TestWaitForDoneSignal(t *testing.T) {
	c := &authCache{
		senderCache: &atomic.Value{},
	}
	c.senderCache.Store(senderValue{
		time: "aaa",
		auth: "bbb",
	})
	c.waitForDoneSignal(nil)
	stopChan := make(chan struct{})
	go c.waitForDoneSignal(stopChan)
	close(stopChan)
	assert.NotEqual(t, c, nil)
}

func TestGetSignForReceiver(t *testing.T) {
	c := &authCache{
		senderCache:   &atomic.Value{},
		receiverCache: cache.NewTTLStore(receiverCacheKey, time.Duration(1)*time.Minute),
	}
	c.senderCache.Store(senderValue{
		time: "aaa",
		auth: "aaa",
	})
	c.updateReceiver("sign")
	_, _, err := c.GetSignForReceiver("auth")
	assert.Equal(t, err, nil)
}

func Test_authCache_startCleanExpiredKeysByTicker(t *testing.T) {
	type fields struct {
		senderCache   *atomic.Value
		receiverCache cache.Store
		appID         string
		AuthConfig    AuthConfig
	}
	type args struct {
		stopCh   chan struct{}
		duration time.Duration
	}
	receiverCache := cache.NewTTLStore(receiverCacheKey, 5*time.Second)
	tests := []struct {
		name        string
		fields      fields
		args        args
		operateFunc func(stopCh chan struct{})
		expectLen   int
	}{
		{
			name: "test success",
			fields: fields{
				senderCache:   nil,
				receiverCache: receiverCache,
				appID:         "",
				AuthConfig:    AuthConfig{},
			},
			args: args{
				stopCh:   make(chan struct{}),
				duration: 5 * time.Second,
			},
			operateFunc: func(stopCh chan struct{}) {
				receiverCache.Add("1")
				receiverCache.Add("2")
				receiverCache.Add("3")
				time.Sleep(3 * time.Second)
				receiverCache.Add("4")
				receiverCache.Add("5")
				time.Sleep(3 * time.Second)
				close(stopCh)
			},
			expectLen: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &authCache{
				senderCache:   tt.fields.senderCache,
				receiverCache: tt.fields.receiverCache,
				appID:         tt.fields.appID,
				AuthConfig:    tt.fields.AuthConfig,
			}

			go c.startCleanExpiredKeysByTicker(tt.args.stopCh, tt.args.duration)

			tt.operateFunc(tt.args.stopCh)

			keys := receiverCache.ListKeys()
			fmt.Println(keys)
			if len(keys) != tt.expectLen {
				t.Errorf("expect length %v but found %v", tt.expectLen, keys)
			}
		})
	}
}
