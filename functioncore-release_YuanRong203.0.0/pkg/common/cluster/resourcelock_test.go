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

package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/k8sclient"
	"functioncore/pkg/common/logger/log"
	"functioncore/test/utils"
)

// This test case is used to simulate the active node selection process. The K8S connection is required.
// If this case is executed on Windows, download .kube/config from the Rancher and save it to the ~/.kube/config file.
// Use TestCon() to test whether the connection is successful.
// Disable your httpproxy.

// test connect to kubernetes cluster
func TestCon(t *testing.T) {
	_, err := k8sclient.GetKubeConfig()
	assert.Equal(t, err, nil)
}

func TestGetResourceLockConfig(t *testing.T) {
	logger = log.GetLogger()
	config, err := GetResourceLockConfig()
	assert.Equal(t, err, nil)
	assert.Equal(t, config.LeaseDuration, defaultLeaseDuration)
	assert.Equal(t, config.ResourceLockType, defaultResourceLockType)

	value := "{\"leaseDuration\":1,\"renewDeadline\":6,\"retryPeriod\":5,\"resourceLockType\":\"leases\"," +
		"\"maxIdleConnsPerHost\":10000}"
	os.Setenv("RESOURCELOCK_CONFIG", value)

	config, err = GetResourceLockConfig()
	assert.Equal(t, err, nil)
	assert.Equal(t, config.LeaseDuration, 1)
	assert.Equal(t, config.RenewDeadline, 6)
	assert.Equal(t, config.RetryPeriod, 5)
	assert.Equal(t, config.ResourceLockType, LeasesResourceLock)

	value = "{\"leaseDuration\":1,\"renewDeadline\":6,\"retryPeriod\":5,\"resourceLockType\":\"configmaps\"," +
		"\"maxIdleConnsPerHost\":10000}"
	os.Setenv("RESOURCELOCK_CONFIG", value)
	config, err = GetResourceLockConfig()
	assert.Equal(t, config.ResourceLockType, ConfigMapsResourceLock)

	value = "{\"leaseDuration\":1,\"renewDeadline\":6,\"retryPeriod\":5,\"resourceLockType\":\"endpoints\"," +
		"\"maxIdleConnsPerHost\":10000}"
	os.Setenv("RESOURCELOCK_CONFIG", value)
	config, err = GetResourceLockConfig()
	assert.Equal(t, config.ResourceLockType, EndpointsResourceLock)

	value = "{\"leaseDuration\":1,\"renewDeadline\":6,\"retryPeriod\":5}"
	os.Setenv("RESOURCELOCK_CONFIG", value)
	config, err = GetResourceLockConfig()
	assert.NotEqual(t, err, nil)

	value = "{\"leaseDuration\":1,\"renewDeadline\":6}"
	os.Setenv("RESOURCELOCK_CONFIG", value)
	config, err = GetResourceLockConfig()
	logger.Infof("failed to get resource lock config: %v", err)
	assert.NotEqual(t, err, nil)
}

// Test leader elect
func TestLeaderElect(t *testing.T) {
	os.Setenv(constants.PodIPEnvKey, "127.0.0.1")
	os.Setenv(constants.PodNamespaceEnvKey, "default")
	value := "{\"leaseDuration\":5,\"renewDeadline\":4,\"retryPeriod\":3,\"maxIdleConnsPerHost\":10000," +
		"\"resourceLockType\":\"leases\"}"
	os.Setenv("RESOURCELOCK_CONFIG", value)

	leaseCluster, _ := NewResourceLockCluster(":8087", nil, "mytest", nil)
	run := func(stopCh <-chan struct{}) error {
		t.Logf("this is callback")
		<-stopCh
		return nil
	}

	leaseCluster.SetCallback(run)
	leaseCluster.Discovery()
	time.Sleep(1 * time.Second)
	assert.Equal(t, false, leaseCluster.leaderElector.IsLeader())
	assert.Equal(t, "", leaseCluster.Role())
	assert.Equal(t, "", leaseCluster.Leader().Host)

	namespace := leaseCluster.namespace
	leaseCluster.k8sClient.CoordinationV1().Leases(namespace).Delete(context.Background(), "mytest", metav1.DeleteOptions{})
}

func Test_initLeaderElector(t *testing.T) {
	logger = log.GetLogger()
	value := "{\"leaseDuration\":1,\"renewDeadline\":6,\"retryPeriod\":5,\"resourceLockType\":\"leases\"," +
		"\"maxIdleConnsPerHost\":10000}"
	oldEnv := os.Getenv("RESOURCELOCK_CONFIG")
	os.Setenv("RESOURCELOCK_CONFIG", value)
	defer func() {
		os.Setenv("RESOURCELOCK_CONFIG", oldEnv)
	}()
	cfg, err := GetResourceLockConfig()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)

	c := ResourceLockCluster{
		leaseConfig: cfg,
		k8sClient:   &kubernetes.Clientset{},
	}

	_, err = c.initLeaderElector()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "leaseDuration must be greater than renewDeadline"))
}

// Test other node
func TestLeaderContend1(t *testing.T) {
	os.Setenv(constants.PodIPEnvKey, "127.0.0.1")
	os.Setenv(constants.PodNamespaceEnvKey, "default")
	value := "{\"leaseDuration\":3,\"renewDeadline\":2,\"retryPeriod\":1," +
		"\"maxIdleConnsPerHost\":10000,\"resourceLockType\":\"leases\"}"
	os.Setenv("RESOURCELOCK_CONFIG", value)

	stopCh1 := make(chan struct{})
	leaseCluster1, _ := NewResourceLockCluster(":8087", stopCh1, "mytest", nil)
	run := func(stopCh <-chan struct{}) error {
		logger.Info("this is callback")
		<-stopCh1
		return nil
	}

	leaseCluster1.SetCallback(run)
	leaseCluster1.Discovery()

	time.Sleep(1 * time.Second)
	logger.Infof("cluster1 success to run for leader")
	assert.Equal(t, false, leaseCluster1.leaderElector.IsLeader())

	stopCh2 := make(chan struct{})
	leaseCluster2, _ := NewResourceLockCluster(":8088", stopCh2, "mytest", nil)
	leaseCluster2.SetCallback(run)
	leaseCluster2.Discovery()

	time.Sleep(1 * time.Second)
	logger.Infof("cluster2 run for standby")
	assert.Equal(t, "", leaseCluster2.role)

	namespace := leaseCluster2.namespace
	leaseCluster1.k8sClient.CoordinationV1().Leases(namespace).Delete(context.Background(),
		"mytest", metav1.DeleteOptions{})

	stopCh3 := make(chan struct{})
	leaseCluster3, _ := NewResourceLockCluster(":8089", stopCh3, "mytest", nil)
	leaseCluster3.SetCallback(run)
	leaseCluster3.Discovery()

	time.Sleep(1 * time.Second)
	logger.Infof("cluster3 run for leader")
	assert.Equal(t, false, leaseCluster3.leaderElector.IsLeader())

	namespace = leaseCluster3.namespace
	leaseCluster3.k8sClient.CoordinationV1().Leases(namespace).Delete(context.Background(), "mytest", metav1.DeleteOptions{})
}

func TestNewResourceLockCluster(t *testing.T) {
	convey.Convey("Test New Resource Lock Cluster Error 1", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(k8sclient.GetKubeConfig,
			func() (*rest.Config, error) {
				return nil, errors.New("err")
			})})
		defer patches.ResetAll()
		leaseCluster, err := NewResourceLockCluster(":8087", nil, "mytest", nil)
		convey.So(leaseCluster, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Resource Lock Cluster Error 2", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(k8sclient.GetKubeConfig,
			func() (*rest.Config, error) {
				return nil, nil
			})})
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(GetResourceLockConfig,
			func() (*ResourceLockConfig, error) {
				return nil, errors.New("err")
			})})
		defer patches.ResetAll()
		leaseCluster, err := NewResourceLockCluster(":8087", nil, "mytest", nil)
		convey.So(leaseCluster, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Resource Lock Cluster Error 3", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(k8sclient.GetKubeConfig,
			func() (*rest.Config, error) {
				return &rest.Config{Timeout: defaultDailTimeout}, nil
			})})
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(GetResourceLockConfig,
			func() (*ResourceLockConfig, error) {
				return &ResourceLockConfig{RenewDeadline: 1}, nil
			})})
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(kubernetes.NewForConfig,
			func(c *rest.Config) (*kubernetes.Clientset, error) {
				return nil, errors.New("err")
			})})

		defer patches.ResetAll()
		leaseCluster, err := NewResourceLockCluster(":8087", nil, "mytest", nil)
		convey.So(leaseCluster, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Resource Lock Cluster Error 3", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(k8sclient.GetKubeConfig,
			func() (*rest.Config, error) {
				return &rest.Config{Timeout: defaultDailTimeout}, nil
			})})
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(GetResourceLockConfig,
			func() (*ResourceLockConfig, error) {
				return &ResourceLockConfig{RenewDeadline: 1}, nil
			})})
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(kubernetes.NewForConfig,
			func(c *rest.Config) (*kubernetes.Clientset, error) {
				return nil, nil
			})})
		patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv,
			func(key string) string {
				return ""
			})})
		defer patches.ResetAll()
		leaseCluster, err := NewResourceLockCluster(":8087", nil, "mytest", nil)
		convey.So(leaseCluster, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestInitReverseProxy(t *testing.T) {
	discovery := &ResourceLockCluster{leaseConfig: &ResourceLockConfig{
		MaxIdleConnsPerHost: 1,
	},
		tlsConfig: &tls.Config{}}
	discovery.initReverseProxy("test")
	assert.NotEmpty(t, discovery)
}

type fakeResponseWriter struct {
	close      bool
	header     http.Header
	body       []byte
	statusCode int
}

func (f *fakeResponseWriter) Header() http.Header {
	return f.header
}

func (f *fakeResponseWriter) Write(bytes []byte) (int, error) {
	if f.close {
		return 0, errors.New("fakeResponseWriter closed")
	}
	f.body = append(f.body, bytes...)
	return len(bytes), nil
}

func (f *fakeResponseWriter) WriteHeader(statusCode int) {
	f.statusCode = statusCode
}

func TestResourceLockCluster_ForwardRequest(t *testing.T) {
	convey.Convey("TestResourceLockCluster_ForwardRequest", t, func() {
		convey.Convey("succeed to forwardRequest", func() {
			c := &ResourceLockCluster{}
			f := &fakeResponseWriter{
				header: map[string][]string{},
			}
			c.ForwardRequest(f, &http.Request{})
			convey.So(len(f.body), convey.ShouldNotEqual, 0)
		})
		convey.Convey("failed to forwardRequest", func() {
			c := &ResourceLockCluster{}
			f := &fakeResponseWriter{
				header: map[string][]string{},
			}
			f.close = true
			c.ForwardRequest(f, &http.Request{})
			convey.So(len(f.body), convey.ShouldEqual, 0)
		})
	})
}

func TestRegisterOnStoppedLeadingCallback(t *testing.T) {
	convey.Convey("TestRegisterOnStoppedLeadingCallback", t, func() {
		flag := false
		callback := func() {
			flag = true
		}
		onStoppedLeadingCallback()
		convey.So(flag, convey.ShouldBeFalse)
		RegisterOnStoppedLeadingCallback(callback)
		onStoppedLeadingCallback()
		convey.So(flag, convey.ShouldBeTrue)
	})
}
