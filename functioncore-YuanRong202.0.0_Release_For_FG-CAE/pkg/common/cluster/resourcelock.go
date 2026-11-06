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

// Package cluster use resourcelosk of client-go to implement leader elect.
package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"github.com/asaskevich/govalidator"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/k8sclient"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commonTLS "functioncore/pkg/common/tls"
)

const (
	// defaultResourceLockType indicate default use lease resource lock
	defaultResourceLockType      = LeasesResourceLock
	defaultLeaseDuration         = 15
	defaultRenewDeadline         = 10
	defaultRetryPeriod           = 3
	defaultMaxIdleConnsPerHost   = 10000
	defaultHostNum               = 3
	defaultDailTimeout           = 10 * time.Second
	defaultDailKeepAlive         = 30 * time.Second
	defaultIdleConnTimeout       = 90 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultExpectContinueTimeout = 1 * time.Second
	// leaderNotSelectedError if this exception occurs, the active node is not selected.
	leaderNotSelectedError = 230001
)

var (
	logger                   log.FormatLogger
	onStoppedLeadingCallback = func() {}
)

// ResourceLockConfig is the configure of the leader elect.
type ResourceLockConfig struct {
	LeaseDuration       int    `json:"leaseDuration"       valid:",required,int"`
	RenewDeadline       int    `json:"renewDeadline"       valid:",required,int"`
	RetryPeriod         int    `json:"retryPeriod"         valid:",required,int"`
	ClientTimeout       int    `json:"clientTimeout"`
	ResourceLockType    string `json:"resourceLockType"    valid:",required,in(endpoints|configmaps|leases)"`
	MaxIdleConnsPerHost int    `json:"maxIdleConnsPerHost" valid:",required,int"`
}

// ResourceLockCluster define a single node cluster
type ResourceLockCluster struct {
	leaseConfig   *ResourceLockConfig
	k8sClient     *kubernetes.Clientset
	leaderElector *leaderelection.LeaderElector
	leaderProxy   *httputil.ReverseProxy
	namespace     string
	lockName      string
	identity      string
	callback      DiscoverCallback
	leader        LeaderInfo
	role          string
	stopCh        <-chan struct{}
	tlsConfig     *tls.Config
}

// RegisterOnStoppedLeadingCallback register on stopped leading callback
func RegisterOnStoppedLeadingCallback(callback func()) {
	onStoppedLeadingCallback = callback
}

// NewResourceLockCluster is used to return a LeaseCluster based active/standby scheme.
// user can provide a null-nil tlsConfig to use HTTPS.
func NewResourceLockCluster(listenAddress string, stopCh <-chan struct{},
	resourceLockName string, tlsConfig *tls.Config) (*ResourceLockCluster, error) {
	logger = log.GetLogger()
	k8sConfig, err := k8sclient.GetKubeConfig()
	if err != nil {
		logger.Errorf("get k8s config error: %s when new resource lock cluster", err.Error())
		return nil, err
	}

	resourceLockConfig, err := GetResourceLockConfig()
	if err != nil {
		logger.Errorf("failed to get the leader election config :%s", err.Error())
		return nil, err
	}

	k8sConfig.Timeout = time.Duration(resourceLockConfig.ClientTimeout) * time.Second
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		logger.Errorf("new k8s client set error : %s", err.Error())
		return nil, err
	}

	namespace := os.Getenv(constants.PodNamespaceEnvKey)
	if namespace == "" {
		logger.Errorf("failed to get leader election namespace")
		return nil, errors.New("no environment 'POD_NAMESPACE'")
	}

	// ipAndPort is the identity of the current node and also the ID of the current node.
	ipAndPort := getAdvertiseURL(listenAddress)

	return &ResourceLockCluster{
		identity:      ipAndPort,
		callback:      nil,
		leaderElector: nil,
		lockName:      resourceLockName,
		k8sClient:     k8sClient,
		stopCh:        stopCh,
		leaderProxy:   nil,
		leaseConfig:   resourceLockConfig,
		namespace:     namespace,
		tlsConfig:     tlsConfig,
	}, nil
}

// Discovery set leader
func (c *ResourceLockCluster) Discovery() error {
	go c.doDiscovery()
	return nil
}

// ForwardRequest Forward Request
func (c *ResourceLockCluster) ForwardRequest(w http.ResponseWriter, r *http.Request) {
	if c.leaderProxy == nil {
		logger.Errorf("the active node was not selected")
		w.WriteHeader(http.StatusInternalServerError)
		badResponse := snerror.BadResponse{
			Code:    leaderNotSelectedError,
			Message: "the active node was not selected. Please try again later",
		}
		badByte, err := json.Marshal(badResponse)
		if err != nil {
			badByte = []byte("failed to marshal badResponse")
		}
		_, err = w.Write(badByte)
		if err != nil {
			logger.Errorf("failed to write response")
		}
		return
	}
	c.leaderProxy.ServeHTTP(w, r)
}

// Leader returns the leader information of the current node.
func (c *ResourceLockCluster) Leader() *LeaderInfo {
	// if the node is leader, return advertiseURL
	if c.role == ClusterRoleLeader {
		return &LeaderInfo{
			Host: c.identity,
		}
	}
	return &LeaderInfo{
		Host: c.leaderElector.GetLeader(),
	}
}

// Role return get Role
func (c *ResourceLockCluster) Role() string {
	return c.role
}

// SetCallback Set Call back
// The callback function must be configured before the Discovery. Otherwise, the callback function is useless.
func (c *ResourceLockCluster) SetCallback(f DiscoverCallback) {
	c.callback = f
}

// electing
func (c *ResourceLockCluster) doDiscovery() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error
	c.leaderElector, err = c.initLeaderElector()
	if err != nil {
		logger.Errorf("Failed to initialize the lease.The cluster does not have a master node.%s", err)
		return err
	}
	c.leaderElector.Run(ctx)
	return nil
}

func (c *ResourceLockCluster) initResourceLock() (resourcelock.Interface, error) {
	rl, err := resourcelock.New(c.leaseConfig.ResourceLockType,
		c.namespace,
		c.lockName,
		c.k8sClient.CoreV1(),
		c.k8sClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity: c.identity,
		})

	return rl, err
}

func (c *ResourceLockCluster) initLeaderElector() (*leaderelection.LeaderElector, error) {
	rl, err := c.initResourceLock()
	if err != nil {
		return nil, err
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: time.Duration(c.leaseConfig.LeaseDuration) * time.Second,
		RenewDeadline: time.Duration(c.leaseConfig.RenewDeadline) * time.Second,
		RetryPeriod:   time.Duration(c.leaseConfig.RetryPeriod) * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				c.role = ClusterRoleLeader
				if c.callback != nil {
					if err := c.callback(c.stopCh); err != nil {
						fmt.Printf("failed to exec callback func with error: %s and exit now\n", err.Error())
						log.GetLogger().Errorf("failed to exec callback func with error: %s and exit now", err.Error())
						log.GetLogger().Sync()
						os.Exit(0)
					}
				}
			},
			OnStoppedLeading: func() {
				onStoppedLeadingCallback()
				c.role = ClusterRoleNode
				logger.Errorf("failed to renew the lease with LeaseDuration:%d seconds,"+
					"RenewDeadline:%d seconds,RetryPeriod:%d seconds"+
					"Stop and exit now", c.leaseConfig.LeaseDuration, c.leaseConfig.RenewDeadline, c.leaseConfig.RetryPeriod)
				log.GetLogger().Sync()
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				// If the returned identity is the current node, the current node is the leader node.
				if identity == c.identity {
					c.role = ClusterRoleLeader
				} else {
					c.role = ClusterRoleNode
					c.initReverseProxy(identity)
				}
			},
		},
		ReleaseOnCancel: true,
		Name:            c.lockName,
	}

	leaderElector, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		logger.Errorf("new leaderElector error: %v", err)
		return nil, err
	}
	logger.Infof("succeed to new leaderElector, LeaseDuration: %d, RenewDeadline: %d, RetryPeriod: %d, timeout: %d",
		c.leaseConfig.LeaseDuration, c.leaseConfig.RenewDeadline, c.leaseConfig.RetryPeriod, c.leaseConfig.ClientTimeout)

	return leaderElector, nil
}

func (c *ResourceLockCluster) initReverseProxy(identity string) {
	scheme := commonTLS.GetURLScheme(c.tlsConfig != nil)
	leaderURL, err := url.Parse(scheme + "://" + identity)
	if err != nil {
		logger.Errorf("new leader url error: %s", err)
		return
	}
	c.leaderProxy = httputil.NewSingleHostReverseProxy(leaderURL)
	c.leaderProxy.Transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   defaultDailTimeout,
			KeepAlive: defaultDailKeepAlive,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   c.leaseConfig.MaxIdleConnsPerHost,
		MaxIdleConns:          c.leaseConfig.MaxIdleConnsPerHost * defaultHostNum,
		TLSClientConfig:       c.tlsConfig,
		IdleConnTimeout:       defaultIdleConnTimeout,
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		ExpectContinueTimeout: defaultExpectContinueTimeout,
		DisableKeepAlives:     false,
	}
}

// GetResourceLockConfig get resource lock config
func GetResourceLockConfig() (*ResourceLockConfig, error) {
	config := os.Getenv("RESOURCELOCK_CONFIG")
	sysConfig := &ResourceLockConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), sysConfig)
		if err != nil {
			return nil, errors.New("environment RESOURCELOCK_CONFIG is not json string")
		}

		if _, err := govalidator.ValidateStruct(sysConfig); err != nil {
			return nil, fmt.Errorf("failed to validate the resource lock config: %s", err.Error())
		}
	} else {
		sysConfig.LeaseDuration = defaultLeaseDuration
		sysConfig.RenewDeadline = defaultRenewDeadline
		sysConfig.RetryPeriod = defaultRetryPeriod
		sysConfig.ResourceLockType = defaultResourceLockType
		sysConfig.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}
	// if client timeout not config, use RenewDeadline / 2 as default
	if sysConfig.ClientTimeout <= 0 {
		sysConfig.ClientTimeout = sysConfig.RenewDeadline >> 1
	}
	// client timeout should be at least 1 second
	if sysConfig.ClientTimeout < 1 {
		sysConfig.ClientTimeout = 1
	}

	logger.Infof("resource lock config is %v", *sysConfig)
	return sysConfig, nil
}
