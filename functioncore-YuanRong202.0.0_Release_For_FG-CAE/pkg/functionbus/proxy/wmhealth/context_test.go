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

package wmhealth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"functioncore/pkg/common/healthcheck"
	"functioncore/pkg/common/k8sclient"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/functionbus/common"
)

func Test_hotLoadWorkerManagerHealthCheckConfig(t *testing.T) {
	hotLoadWorkerManagerHealthCheckConfig(&common.Configs{})
	assert.Equal(t, common.WorkerManagerHealthCheck{
		Timeout:   defaultHealthCheckTimeout,
		Interval:  defaultHealthCheckInterval,
		Threshold: defaultHealthCheckThreshold,
		Disable:   false,
	}, common.FConfig.WMHealthCheck)

	defer func() {
		workerManagerHealthCtx = nil
	}()
	workerManagerHealthCtx = healthcheck.NewContext(healthcheck.Config{}, func(ctx context.Context) error {
		return nil
	})
	hotLoadWorkerManagerHealthCheckConfig(&common.Configs{
		WMHealthCheck: common.WorkerManagerHealthCheck{
			Timeout:   5,
			Interval:  6,
			Threshold: 7,
			Disable:   true,
		},
	})
	assert.Equal(t, common.WorkerManagerHealthCheck{
		Timeout:   5,
		Interval:  6,
		Threshold: 7,
		Disable:   true,
	}, common.FConfig.WMHealthCheck)
}

func Test_checkWorkerManagerHealth(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "", ""
		}),
	}

	r := gin.New()
	ts := httptest.NewServer(r)
	defer func() {
		ts.Close()
		updateWmAddr("")
		for i := range patches {
			patches[i].Reset()
		}
	}()
	common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: false}
	common.FConfig.LocalAuth = &localauth.AuthConfig{}

	statusCode := http.StatusOK

	r.GET("/componenthealth", func(c *gin.Context) {
		c.Status(statusCode)
	})

	tests := []struct {
		name       string
		wmAddr     string
		statusCode int
		hasError   bool
	}{
		{
			name:       "no worker-manager-addr",
			statusCode: http.StatusOK,
			hasError:   true,
		},
		{
			name:       "health check ok",
			wmAddr:     strings.Split(ts.URL, "//")[1],
			statusCode: http.StatusOK,
			hasError:   false,
		},
		{
			name:       "health check failed",
			wmAddr:     strings.Split(ts.URL, "//")[1],
			statusCode: http.StatusBadRequest,
			hasError:   true,
		},
		{
			name:       "invalid addr",
			wmAddr:     "127.0.0.2:3333",
			statusCode: http.StatusOK,
			hasError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateWmAddr(tt.wmAddr)
			statusCode = tt.statusCode
			err := checkWorkerManagerHealth(context.Background())
			if tt.hasError {
				assert.Error(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
			}
		})
	}
}

func TestStartWorkerManagerHealthCheck(t *testing.T) {
	addr1 := "10.0.0.1:58866"
	leases := []*v1.Lease{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "state-manager"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-manager"},
			Spec:       v1.LeaseSpec{HolderIdentity: &addr1},
		},
	}

	fakeClient := fake.NewSimpleClientset(leases[0], leases[1])
	fakeInformer := informers.NewSharedInformerFactory(fakeClient, 0)

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
			return &rest.Config{}, nil
		}),
		gomonkey.ApplyFunc(kubernetes.NewForConfig, func(*rest.Config) (*kubernetes.Clientset, error) {
			return &kubernetes.Clientset{}, nil
		}),
		gomonkey.ApplyFunc(informers.NewSharedInformerFactoryWithOptions, func(client kubernetes.Interface, defaultResync time.Duration, options ...informers.SharedInformerOption) informers.SharedInformerFactory {
			return fakeInformer
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	common.FConfig.WMHealthCheck.Interval = 3

	stopCh := make(chan struct{})
	err := StartWorkerManagerHealthCheck(stopCh)
	assert.Nil(t, err)
	close(stopCh)
}

func TestGetContext(t *testing.T) {
	r := gin.New()
	ts := httptest.NewServer(r)
	r.GET("/test", func(c *gin.Context) {
		time.Sleep(500 * time.Millisecond)
		c.Status(http.StatusOK)
	})

	defer func() {
		ts.Close()
		updateWmAddr("")
		workerManagerHealthCtx = nil
	}()
	workerManagerHealthCtx = healthcheck.NewContext(healthcheck.Config{}, func(ctx context.Context) error {
		return nil
	})

	req, _ := http.NewRequestWithContext(GetContext(), http.MethodGet, ts.URL+"/test", nil)
	ch := make(chan struct{})
	timer := time.NewTimer(600 * time.Millisecond)
	go func() {
		http.DefaultClient.Do(req)
		close(ch)
	}()
	select {
	case <-ch:
	case <-timer.C:
		assert.Fail(t, "request should finish in 500ms")
	}

	req, _ = http.NewRequestWithContext(GetContext(), http.MethodGet, ts.URL+"/test", nil)
	ch = make(chan struct{})
	timer = time.NewTimer(100 * time.Millisecond)
	go func() {
		http.DefaultClient.Do(req)
		close(ch)
	}()
	updateWmAddr("")
	select {
	case <-ch:
	case <-timer.C:
		assert.Fail(t, "request should done immediately as context is canceled")
	}
}
