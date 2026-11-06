/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package options define tenantcommand
package tenantcommand

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/iam"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

func TestInitManager(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{}, nil
			}),
			gomonkey.ApplyFunc((*manager).processTenantCommandWatcher, func(m *manager, opt *watchOption) {
				return
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		workerClient := &fhttp.Client{}
		stopCh := make(chan struct{})

		err := InitManager(workerClient, stopCh)
		assert.Equal(t, err, nil)

		close(stopCh)
	})
	t.Run("get config fail", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return nil, errors.New("get config fail")
			}),
			gomonkey.ApplyFunc((*manager).processTenantCommandWatcher, func(m *manager, opt *watchOption) {
				return
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		workerClient := &fhttp.Client{}
		stopCh := make(chan struct{})

		err := InitManager(workerClient, stopCh)
		assert.Equal(t, err.Error(), "get config fail")

		close(stopCh)
	})
	t.Run("stopCh is nil", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{}, nil
			}),
			gomonkey.ApplyFunc((*manager).processTenantCommandWatcher, func(m *manager, opt *watchOption) {
				return
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		workerClient := &fhttp.Client{}

		err := InitManager(workerClient, nil)
		assert.Equal(t, err.Error(), "stopCh should not be nil")
	})
}

func TestProcessTenantCommandWatcher(t *testing.T) {
	t.Run("success update event", func(t *testing.T) {
		checkResult := false
		testResultChan := make(chan *etcd3.Event)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			}),
			gomonkey.ApplyFunc(getWatchChan, func(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
				return &etcd3.EtcdWatchChan{ResultChan: testResultChan}
			}),
			gomonkey.ApplyFunc((*manager).readEventChannelTask, func(m *manager, funcSvcID string,
				stopCh <-chan struct{}, eventChannel chan *Event) {
				checkResult = true
				return
			}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch, func(w *etcd3.EtcdClient, ctx context.Context,
				watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				return &etcd3.EtcdWatchChan{}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		stopCh := make(chan struct{})
		opt := &watchOption{StopCh: stopCh}
		m := &manager{
			workerClient:           &fhttp.Client{},
			cfg:                    &config.FaasConfig{},
			functionThreadMap:      &sync.Map{},
			functionTreadCrateLock: &sync.Mutex{},
			stopCh:                 stopCh,
		}

		go func() {
			time.Sleep(500 * time.Millisecond)
			event := &etcd3.Event{
				Type: etcd3.PUT,
				Key: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
					"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111",
				Value: []byte("{\"taskId\":\"task_id111\", \"createTime\":123456, \"mode\":\"function\"," +
					"\"funcUrn\":\"sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:" +
					"latest\",\"podName\":[\"podA\"], \"command\":[\"ls\"], \"timeout\": 300}"),
			}
			testResultChan <- event
			time.Sleep(1000 * time.Millisecond)
			close(testResultChan)
			time.Sleep(1500 * time.Millisecond)
			close(stopCh)
		}()

		m.processTenantCommandWatcher(opt)
		assert.Equal(t, checkResult, true)
	})
	t.Run("client is nil", func(t *testing.T) {
		checkResult := false
		testResultChan := make(chan *etcd3.Event)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
				return nil
			}),
			gomonkey.ApplyFunc(getWatchChan, func(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
				checkResult = true
				return &etcd3.EtcdWatchChan{ResultChan: testResultChan}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		stopCh := make(chan struct{})
		opt := &watchOption{StopCh: stopCh}
		m := &manager{
			workerClient:           &fhttp.Client{},
			cfg:                    &config.FaasConfig{},
			functionThreadMap:      &sync.Map{},
			functionTreadCrateLock: &sync.Mutex{},
			stopCh:                 stopCh,
		}

		m.processTenantCommandWatcher(opt)
		assert.Equal(t, checkResult, false)
	})
	t.Run("parseTenantCommandEvent fail", func(t *testing.T) {
		checkResult := false
		testResultChan := make(chan *etcd3.Event)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			}),
			gomonkey.ApplyFunc(getWatchChan, func(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
				return &etcd3.EtcdWatchChan{ResultChan: testResultChan}
			}),
			gomonkey.ApplyFunc((*manager).readEventChannelTask, func(m *manager, funcSvcID string,
				stopCh <-chan struct{}, eventChannel chan *Event) {
				checkResult = true
				return
			}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch, func(w *etcd3.EtcdClient, ctx context.Context,
				watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				return &etcd3.EtcdWatchChan{}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		stopCh := make(chan struct{})
		opt := &watchOption{StopCh: stopCh}
		m := &manager{
			workerClient:           &fhttp.Client{},
			cfg:                    &config.FaasConfig{},
			functionThreadMap:      &sync.Map{},
			functionTreadCrateLock: &sync.Mutex{},
			stopCh:                 stopCh,
		}

		go func() {
			time.Sleep(500 * time.Millisecond)
			event := &etcd3.Event{
				Type: etcd3.PUT,
				Key: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
					"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				Value: []byte("{\"taskId\":\"task_id111\", \"createTime\":123456, \"mode\":\"function\"," +
					"\"funcUrn\":\"sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:" +
					"latest\",\"podName\":[\"podA\"], \"command\":[\"ls\"], \"timeout\": 300}"),
			}
			testResultChan <- event
			time.Sleep(1000 * time.Millisecond)
			close(stopCh)
		}()

		m.processTenantCommandWatcher(opt)
		assert.Equal(t, checkResult, false)
	})
}

func TestParseTenantCommandEvent(t *testing.T) {
	t.Run("not get keyObj.funcURN", func(t *testing.T) {
		event := &etcd3.Event{
			Type: etcd3.PUT,
			Key: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
			Value: []byte("{\"taskId\":\"task_id111\", \"createTime\":123456, \"mode\":\"function\"," +
				"\"funcUrn\":\"sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:" +
				"latest\",\"podName\":[\"podA\"], \"command\":[\"ls\"], \"timeout\": 300}"),
		}

		_, err := parseTenantCommandEvent(event, etcd3.PUT)
		assert.Equal(t, err.Error(),
			"failed to parse from etcd event key: /sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78/"+
				"sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest")
	})
	t.Run("Unmarshal value fail", func(t *testing.T) {
		event := &etcd3.Event{
			Type: etcd3.PUT,
			Key: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111",
			Value: []byte("{\"taskId\":\"task_id111\", \"createTime\":\"123456\", \"mode\":\"function\"," +
				"\"funcUrn\":\"sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:" +
				"latest\",\"podName\":[\"podA\"], \"command\":[\"ls\"], \"timeout\": 300}"),
		}

		_, err := parseTenantCommandEvent(event, etcd3.PUT)
		assert.Equal(t, err.Error(), "failed to unmarshal the json, error: json: cannot unmarshal string into "+
			"Go struct field TenantCommandDeploymentSpec.createTime of type int64")
	})
	t.Run("unconcerned event type", func(t *testing.T) {
		event := &etcd3.Event{
			Type: etcd3.DELETE,
			Key: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111",
			Value: []byte("{\"taskId\":\"task_id111\", \"createTime\":\"123456\", \"mode\":\"function\"," +
				"\"funcUrn\":\"sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:" +
				"latest\",\"podName\":[\"podA\"], \"command\":[\"ls\"], \"timeout\": 300}"),
		}

		result, err := parseTenantCommandEvent(event, etcd3.DELETE)
		var expected *Event = nil
		assert.Equal(t, result, expected)
		assert.Equal(t, err, nil)
	})
}

func TestParseFromKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		key := "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
			"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111"

		result := parseFromKey(key)
		assert.Equal(t, result.projectID,
			"5cb29fa73b564c90b9878a6bf8e10d78")
		assert.Equal(t, result.funcURN,
			"sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest")
		assert.Equal(t, result.taskID,
			"task_id111")
		assert.Equal(t, result.businessID,
			"yrk")
		assert.Equal(t, result.tenantID,
			"5cb29fa73b564c90b9878a6bf8e10d78")
		assert.Equal(t, result.funcName,
			"test_java_log")
		assert.Equal(t, result.funcVersion,
			"latest")
		assert.Equal(t, result.fullFunctionName,
			"0@default@test_java_log")
	})
	t.Run("elements numbers not right", func(t *testing.T) {
		key := "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest"

		result := parseFromKey(key)
		assert.Equal(t, result.funcURN, "")
	})
	t.Run("functionUrn numbers not right", func(t *testing.T) {
		key := "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
			"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log/task_id111"

		result := parseFromKey(key)
		assert.Equal(t, result.funcURN, "")
	})
	t.Run("functionName numbers not right", func(t *testing.T) {
		key := "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
			"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@test_java_log/task_id111"

		result := parseFromKey(key)
		assert.Equal(t, result.funcURN, "")
	})
}

func TestGetWatchChan(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		checkResult := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch, func(w *etcd3.EtcdClient, ctx context.Context,
				watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				checkResult = true
				return &etcd3.EtcdWatchChan{}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		workerClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}

		getWatchChan(workerClient)
		assert.Equal(t, checkResult, true)
	})
}

func TestHandleTenantCommentEtcdEvent(t *testing.T) {
	t.Run("tow same function task handleTenantCommandEtcdEvent", func(t *testing.T) {
		startTime := time.Now()
		var executeTimes []time.Duration
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).handleTenantCommand, func(m *manager, funcSvcID string, event *Event) {
				time.Sleep(200 * time.Millisecond)
				executeTimes = append(executeTimes, time.Now().Sub(startTime))
				return
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		stopCh := make(chan struct{})
		m := &manager{
			workerClient:           &fhttp.Client{},
			cfg:                    &config.FaasConfig{},
			functionThreadMap:      &sync.Map{},
			functionTreadCrateLock: &sync.Mutex{},
			stopCh:                 stopCh,
		}

		tenantCommandEvent := &Event{
			tenantCommandEtcdKey: tenantCommandEtcdKey{
				projectID:   "5cb29fa73b564c90b9878a6bf8e10d78",
				funcURN:     "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				taskID:      "task_id111",
				businessID:  "yrk",
				tenantID:    "5cb29fa73b564c90b9878a6bf8e10d78",
				funcName:    "test_java_log",
				funcVersion: "latest",
			},
			resultKey: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111",
			etcdType: etcd3.PUT,
			tenantCommandResult: &types.TenantCommandDeploymentSpec{
				TaskID:     "task_id111",
				CreateTime: 123456,
				Mode:       "function",
				FuncUrn:    "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				PodName:    []string{},
				Path:       "",
				Command:    []string{"ls"},
				Timeout:    300,
			},
		}

		tenantCommandEvent2 := &Event{
			tenantCommandEtcdKey: tenantCommandEtcdKey{
				projectID:   "5cb29fa73b564c90b9878a6bf8e10d78",
				funcURN:     "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				taskID:      "task_id222",
				businessID:  "yrk",
				tenantID:    "5cb29fa73b564c90b9878a6bf8e10d78",
				funcName:    "test_java_log",
				funcVersion: "latest",
			},
			resultKey: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111",
			etcdType: etcd3.PUT,
			tenantCommandResult: &types.TenantCommandDeploymentSpec{
				TaskID:     "task_id111",
				CreateTime: 123456,
				Mode:       "function",
				FuncUrn:    "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				PodName:    []string{},
				Path:       "",
				Command:    []string{"ls"},
				Timeout:    300,
			},
		}

		m.handleTenantCommandEtcdEvent(tenantCommandEvent)
		m.handleTenantCommandEtcdEvent(tenantCommandEvent2)
		time.Sleep(500 * time.Millisecond)
		assert.Equal(t, 2, len(executeTimes))
		assert.Equal(t, true, (executeTimes[1]-executeTimes[0]) > 200*time.Millisecond)
	})
	t.Run("tow different function task handleTenantCommandEtcdEvent", func(t *testing.T) {
		startTime := time.Now()
		var executeTimes []time.Duration
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*manager).handleTenantCommand, func(m *manager, funcSvcID string, event *Event) {
				time.Sleep(200 * time.Millisecond)
				executeTimes = append(executeTimes, time.Now().Sub(startTime))
				return
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		stopCh := make(chan struct{})
		m := &manager{
			workerClient:           &fhttp.Client{},
			cfg:                    &config.FaasConfig{},
			functionThreadMap:      &sync.Map{},
			functionTreadCrateLock: &sync.Mutex{},
			stopCh:                 stopCh,
		}

		tenantCommandEvent := &Event{
			tenantCommandEtcdKey: tenantCommandEtcdKey{
				projectID:        "5cb29fa73b564c90b9878a6bf8e10d78",
				funcURN:          "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				taskID:           "task_id111",
				businessID:       "yrk",
				tenantID:         "5cb29fa73b564c90b9878a6bf8e10d78",
				funcName:         "test_java_log",
				funcVersion:      "latest",
				fullFunctionName: "0@default@test_java_log",
			},
			resultKey: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111",
			etcdType: etcd3.PUT,
			tenantCommandResult: &types.TenantCommandDeploymentSpec{
				TaskID:     "task_id111",
				CreateTime: 123456,
				Mode:       "function",
				FuncUrn:    "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				PodName:    []string{},
				Path:       "",
				Command:    []string{"ls"},
				Timeout:    300,
			},
		}

		tenantCommandEvent2 := &Event{
			tenantCommandEtcdKey: tenantCommandEtcdKey{
				projectID:        "5cb29fa73b564c90b9878a6bf8e10d78",
				funcURN:          "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log2:latest",
				taskID:           "task_id222",
				businessID:       "yrk",
				tenantID:         "5cb29fa73b564c90b9878a6bf8e10d78",
				funcName:         "test_java_log2",
				funcVersion:      "latest",
				fullFunctionName: "0@default@test_java_log2",
			},
			resultKey: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log2:latest/task_id111",
			etcdType: etcd3.PUT,
			tenantCommandResult: &types.TenantCommandDeploymentSpec{
				TaskID:     "task_id111",
				CreateTime: 123456,
				Mode:       "function",
				FuncUrn:    "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log2:latest",
				PodName:    []string{},
				Path:       "",
				Command:    []string{"ls"},
				Timeout:    300,
			},
		}

		m.handleTenantCommandEtcdEvent(tenantCommandEvent)
		m.handleTenantCommandEtcdEvent(tenantCommandEvent2)
		time.Sleep(500 * time.Millisecond)
		assert.Equal(t, 2, len(executeTimes))
		assert.Equal(t, true, (executeTimes[1]-executeTimes[0]) < 200*time.Millisecond)
	})
}

func TestHandleTenantCommand(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		resultCnt := 0
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(iam.SetIAMTokenRequestHeader, func(req *fhttp.Request, tenantID string, urn string) {
				return
			}),
			gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "test", "test"
			}),
			gomonkey.ApplyFunc((*fhttp.Client).DoTimeout, func(c *fhttp.Client, req *fhttp.Request,
				resp *fhttp.Response, timeout time.Duration) error {
				if strings.Contains(string(req.Header.RequestURI()), "127.0.0.2") {
					// Ensure that return fhttp.StatusOK are properly handled.
					resp.Header.SetStatusCode(fhttp.StatusOK)
				}
				if strings.Contains(string(req.Header.RequestURI()), "127.0.0.3") {
					resultCnt++
					// Ensure that request timeouts are properly handled.
					return errors.New("query timeout")
				}
				if strings.Contains(string(req.Header.RequestURI()), "127.0.0.5") {
					// Ensure that return fhttp.StatusInternalServerError but body not nil are properly handled.
					resp.Header.SetStatusCode(fhttp.StatusInternalServerError)
					resp.SetBody([]byte("command error in runtime-manager"))
				}
				if strings.Contains(string(req.Header.RequestURI()), "127.0.0.6") {
					// Ensure that return fhttp.StatusInternalServerError but body nil are properly handled.
					resp.Header.SetStatusCode(fhttp.StatusInternalServerError)
					resp.SetBody(nil)
				}
				if strings.Contains(string(req.Header.RequestURI()), "127.0.0.7") {
					// Ensure that request 404 but body nil are properly handled.
					resp.Header.SetStatusCode(fhttp.StatusNotFound)
				}
				if strings.Contains(string(req.Header.RequestURI()), "127.0.0.8") {
					// Ensure that all child processes have finished before closing resultChan.
					time.Sleep(2 * time.Second)
				}
				resultCnt++
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		stopCh := make(chan struct{})
		m := &manager{
			workerClient:           &fhttp.Client{},
			cfg:                    &config.FaasConfig{LocalAuth: &localauth.AuthConfig{AKey: "asas", SKey: "dsaf", Duration: 300}},
			functionThreadMap:      &sync.Map{},
			functionTreadCrateLock: &sync.Mutex{},
			stopCh:                 stopCh,
		}

		tenantCommandEvent := &Event{
			tenantCommandEtcdKey: tenantCommandEtcdKey{
				projectID:   "5cb29fa73b564c90b9878a6bf8e10d78",
				funcURN:     "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				taskID:      "task_id111",
				businessID:  "yrk",
				tenantID:    "5cb29fa73b564c90b9878a6bf8e10d78",
				funcName:    "test_java_log",
				funcVersion: "latest",
			},
			resultKey: "/sn/tenantcommand/5cb29fa73b564c90b9878a6bf8e10d78" +
				"/sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest/task_id111",
			etcdType: etcd3.PUT,
			tenantCommandResult: &types.TenantCommandDeploymentSpec{
				TaskID:     "task_id111",
				CreateTime: 123456,
				Mode:       "pod",
				FuncUrn:    "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest",
				PodName: []string{"mock_pod_name2", "mock_pod_name3", "mock_pod_name4", "mock_pod_name5",
					"mock_pod_name6", "mock_pod_name7", "mock_pod_name8"},
				Path:    "",
				Command: []string{"ls"},
				Timeout: 1,
			},
		}
		tenantCommandKey := tenantCommandEvent.tenantCommandEtcdKey
		funcSvcID := utils.CrName(tenantCommandKey.businessID, tenantCommandKey.tenantID,
			tenantCommandKey.funcName, tenantCommandKey.funcVersion)

		fs := &faasfunction.FunctionService{}
		cache.WorkerCache.CreateFunctionCacheByFuncMeta(funcSvcID, fs)
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name1",
			Applier: "", IP: "127.0.0.1", IsRunning: true})
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name2",
			Applier: "", IP: "127.0.0.2", IsRunning: true})
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name3",
			Applier: constants.WorkerManagerApplier, IP: "127.0.0.3", IsRunning: true})
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name4",
			Applier: constants.WorkerManagerApplier, IP: "127.0.0.4", IsRunning: false})
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name5",
			Applier: constants.WorkerManagerApplier, IP: "127.0.0.5", IsRunning: true})
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name6",
			Applier: constants.WorkerManagerApplier, IP: "127.0.0.6", IsRunning: true})
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name7",
			Applier: constants.WorkerManagerApplier, IP: "127.0.0.7", IsRunning: true})
		cache.WorkerCache.StoreWorkerInstance(funcSvcID, v1alpha1.WorkerInstance{PodName: "mock_pod_name8",
			Applier: constants.WorkerManagerApplier, IP: "127.0.0.8", IsRunning: true})
		defer cache.WorkerCache.DeleteFunctionCache(funcSvcID)

		m.handleTenantCommand(funcSvcID, tenantCommandEvent)
		// Two pod (totally eight) not handle: mock_pod_name1(not in pod list)、mock_pod_name4(not running)
		assert.Equal(t, 6, resultCnt)
	})
}
