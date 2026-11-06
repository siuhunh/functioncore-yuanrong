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

// Package clustermgr -
package clustermgr

import (
	"bytes"
	"errors"
	"functioncore/pkg/autoscaler/cache"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/workermanagerutils"
	mockutils "functioncore/test/utils"
)

var reset = false

func TestClusterManager_checkClustersStatus(t *testing.T) {
	type fields struct {
		strategy           Strategy
		stopCh             <-chan struct{}
		clusterMetricCache map[string]config.ClusterUsage
		lock               sync.RWMutex
	}

	tests := []struct {
		name          string
		fields        fields
		patchesFunc   mockutils.PatchesFunc
		wantAvailable func() int
		wantAll       int
	}{
		{
			"checkClustersStatus with all healthy",
			fields{
				strategy: GetStrategy(strategyDefault),
				stopCh:   make(<-chan struct{}),
				clusterMetricCache: map[string]config.ClusterUsage{
					"cluster001": {
						CPUUsed:  50,
						CPUTotal: 100,
					},
					"cluster002": {
						CPUUsed:  60,
						CPUTotal: 100,
					},
				},
				lock: sync.RWMutex{},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							HTTPSConfig: HTTPSConfig,
							LocalAuth:   &localauth.AuthConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
						return "asdfsdffsdafasdf", time.Now().String()
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
						error) {
						return &http.Response{StatusCode: http.StatusOK, Body: ioutil.NopCloser(bytes.NewBuffer([]byte{}))}, nil
					}),
				})
				return patches
			},
			func() int {
				return 2
			},
			2,
		},
		{
			"checkClustersStatus with unhealthy cluster",
			fields{
				strategy: GetStrategy(strategyDefault),
				stopCh:   make(<-chan struct{}),
				clusterMetricCache: map[string]config.ClusterUsage{
					"cluster001": {
						CPUUsed:  50,
						CPUTotal: 100,
					},
					"cluster002": {
						CPUUsed:  60,
						CPUTotal: 100,
					},
				},
				lock: sync.RWMutex{},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							HTTPSConfig: HTTPSConfig,
							LocalAuth:   &localauth.AuthConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
						return "asdfsdffsdafasdf", time.Now().String()
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
						error) {
						if strings.Contains(req.URL.String(), "10.2.1.1") && !reset {
							return &http.Response{StatusCode: http.StatusBadRequest, Body: ioutil.NopCloser(bytes.NewBuffer([]byte{}))}, nil
						} else {
							return &http.Response{StatusCode: http.StatusOK, Body: ioutil.NopCloser(bytes.NewBuffer([]byte{}))}, nil
						}
					}),
				})
				return patches
			},
			func() int {
				if reset {
					return 2
				} else {
					return 1
				}
			},
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			stopCh := make(chan struct{})
			cache.GetInstance().UpdateCaeAvailableCluster("funcSvc", []string{"cluster001", "cluster002"})
			c := &ClusterManager{
				strategy:           tt.fields.strategy,
				stopCh:             stopCh,
				clusterMetricCache: tt.fields.clusterMetricCache,
				lock:               tt.fields.lock,
			}
			c.strategy.UpdateClusterPool(map[string]Cluster{
				"cluster001": {
					ClusterID: "cluster001",
					ActiveNodes: NodeStatus{
						IP: "10.2.1.1",
					},
					State: utils.Healthy,
				},
				"cluster002": {
					ClusterID: "cluster002",
					ActiveNodes: NodeStatus{
						IP: "10.2.1.2",
					},
					State: utils.Healthy,
				},
			})
			go c.checkClustersStatus()
			time.Sleep(healthCheckInterval)
			wantAvailable := tt.wantAvailable()
			if len(tt.fields.strategy.GetFuncAvailablePool("funcSvc")) != wantAvailable {
				t.Errorf("expect avaliable length %v but found %v", wantAvailable, len(tt.fields.strategy.GetFuncAvailablePool("funcSvc")))
			}
			if len(tt.fields.strategy.GetClusterPool()) != tt.wantAll {
				t.Errorf("expect all length %v but found %v", tt.wantAll, len(tt.fields.strategy.GetClusterPool()))
			}
			reset = true
			wantAvailableAfterReset := tt.wantAvailable()
			if len(tt.fields.strategy.GetFuncAvailablePool("funcSvc")) != wantAvailableAfterReset {
				t.Errorf("expect avaliable length %v but found %v", wantAvailable, len(tt.fields.strategy.GetFuncAvailablePool("funcSvc")))
			}
			close(stopCh)
			time.Sleep(healthCheckInterval)
			cache.GetInstance().RemoveCaeAvailableCluster("funcSvc")
			patches.ResetAll()
		})
	}
}

func TestRemoveCluster(t *testing.T) {
	// 测试没有策略的情况
	c := &ClusterManager{
		strategy: NewLBStrategy(),
		lock:     sync.RWMutex{},
	}
	c.AddCluster("cluster001", Cluster{State: utils.Healthy})
	c.RemoveCluster(&utils.WorkerMgrInstance{ClusterID: "cluster001"})
	clusters := c.GetAllCluster()
	assert.Equal(t, 1, len(clusters))
	assert.Equal(t, utils.UnHealthy, clusters["cluster001"].State)
	assert.Equal(t, false, clusters["cluster001"].offlineTime.IsZero())
}

func TestHealthCheck(t *testing.T) {
	c := &ClusterManager{
		strategy: NewLBStrategy(),
		lock:     sync.RWMutex{},
	}
	t.Run("no need to check offline cluster", func(t *testing.T) {
		c.AddCluster("cluster001", Cluster{State: utils.Healthy})
		c.RemoveCluster(&utils.WorkerMgrInstance{ClusterID: "cluster001"})
		c.healthCheck()
		clusters := c.GetAllCluster()
		assert.Equal(t, 1, len(clusters))
		assert.Equal(t, utils.UnHealthy, clusters["cluster001"].State)
	})
	t.Run("remove offline cluster", func(t *testing.T) {
		c.AddCluster("cluster001", Cluster{State: utils.Healthy})
		c.RemoveCluster(&utils.WorkerMgrInstance{ClusterID: "cluster001"})
		removeOfflineClusterPeriod = 1 * time.Second
		time.Sleep(2 * time.Second)
		c.healthCheck()
		clusters := c.GetAllCluster()
		assert.Equal(t, 0, len(clusters))
	})
}

func TestUpdateAlarmFlag(t *testing.T) {
	clusterID := "cluster001"
	state := utils.UnHealthy
	currentUnHealthyClusters := make(map[string]Cluster)
	cluster := Cluster{ClusterID: "cluster001"}

	t.Run("currentUnHealthyClusters is nil", func(t *testing.T) {
		result := updateAlarmFlag(clusterID, state, nil, cluster)
		assert.Equal(t, false, result)
	})
	t.Run("update unhealthy cluster", func(t *testing.T) {
		historyAlarmUnHealthyClusters = make(map[string]Cluster)
		result := updateAlarmFlag(clusterID, utils.UnHealthy, currentUnHealthyClusters, cluster)
		assert.Equal(t, true, result)
	})
	t.Run("update unhealthy cluster change to healthy", func(t *testing.T) {
		historyAlarmUnHealthyClusters[clusterID] = cluster
		result := updateAlarmFlag(clusterID, utils.Healthy, currentUnHealthyClusters, cluster)
		assert.Equal(t, true, result)
	})
	t.Run("no update situation", func(t *testing.T) {
		historyAlarmUnHealthyClusters = make(map[string]Cluster)
		result := updateAlarmFlag(clusterID, utils.Healthy, currentUnHealthyClusters, cluster)
		assert.Equal(t, false, result)
	})
	historyAlarmUnHealthyClusters = make(map[string]Cluster)
}

func TestUpdateAlarmInfo(t *testing.T) {
	needUpdateAlarmInfo := true
	currentUnHealthyClusters := make(map[string]Cluster)

	t.Run("currentUnHealthyClusters is nil", func(t *testing.T) {
		updateAlarmInfo(needUpdateAlarmInfo, currentUnHealthyClusters)
	})
	t.Run("update unhealthy cluster", func(t *testing.T) {
		currentUnHealthyClusters["cluster001"] = Cluster{ClusterID: "cluster001"}
		updateAlarmInfo(needUpdateAlarmInfo, currentUnHealthyClusters)
	})
}

func TestCreateClusterMgr(t *testing.T) {
	type args struct {
		stopCh <-chan struct{}
		conf   *config.Config
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"case test CreateClusterMgr",
			args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stopCh := make(chan struct{})
			conf := &config.Config{
				RemoveOfflineClusterPeriod: 1,
			}
			CreateClusterMgr(stopCh, conf)
			GetClusterMgr().strategy.UpdateClusterPool(map[string]Cluster{
				"cluster001": {
					ClusterID: "cluster001",
					ActiveNodes: NodeStatus{
						IP: "10.2.1.1",
					},
					State: utils.Healthy,
				},
				"cluster002": {
					ClusterID: "cluster002",
					ActiveNodes: NodeStatus{
						IP: "10.2.1.2",
					},
					State: utils.Healthy,
				},
			})
			clusterMetricCache := map[string]config.ClusterUsage{
				"cluster001": {
					CPUUsed:  20,
					CPUTotal: 30,
				},
			}
			GetClusterMgr().RefreshClusterMetricCache(clusterMetricCache)
			cache.GetInstance().UpdateCaeAvailableCluster("funcSvc", []string{"cluster001", "cluster002"})
			got := GetClusterMgr().GetAvailableClusterForScaleUp("funcSvc")
			t.Logf("got = %v", got)
			if got.ClusterID != "cluster001" {
				t.Errorf("expect got.ClusterID cluster001 but found %v", got.ClusterID)
			}

			got = GetClusterMgr().GetAvailableClusterForScaleDown("funcSvc", map[string]int64{"cluster001": 10})
			t.Logf("got = %v", got)
			if got.ClusterID != "cluster001" {
				t.Errorf("expect got.ClusterID cluster001 but found %v", got.ClusterID)
			}

			got2 := GetClusterMgr().GetAllAvailableCluster()
			if len(got2) != 2 {
				t.Errorf("expect got 2 but found %v", len(got2))
			}
			cache.GetInstance().UpdateCaeAvailableCluster("funcSvc", []string{"cluster001"})
			got3 := GetClusterMgr().GetFuncAllAvailableCluster("funcSvc")
			if len(got3) != 1 {
				t.Errorf("expect got 1 but found %v", len(got2))
			}

			cache.GetInstance().RemoveCaeAvailableCluster("funcSvc")
			close(stopCh)
		})
	}
}

func Test_doWorkerManagerRequest(t *testing.T) {
	type args struct {
		req *http.Request
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case1 test doWorkerManagerRequest",
			args{
				req: &http.Request{},
			},
			utils.UnHealthy,
			true,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							HTTPSConfig: HTTPSConfig,
							LocalAuth:   &localauth.AuthConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc(workermanagerutils.FillInWorkerManagerRequestHeaders,
						func(request *http.Request, auth *localauth.AuthConfig) { return }),
					gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
						error) {
						return &http.Response{StatusCode: http.StatusOK,
							Body: ioutil.NopCloser(bytes.NewBuffer([]byte{}))}, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"case2 test doWorkerManagerRequest",
			args{
				req: &http.Request{},
			},
			utils.UnHealthy,
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							HTTPSConfig: HTTPSConfig,
							LocalAuth:   &localauth.AuthConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc(workermanagerutils.FillInWorkerManagerRequestHeaders,
						func(request *http.Request, auth *localauth.AuthConfig) { return }),
					gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
						error) {
						return &http.Response{StatusCode: http.StatusBadRequest,
							Body: ioutil.NopCloser(bytes.NewBuffer([]byte{}))}, nil
					}),
				})
				return patches
			},
		},
		{
			"case3 test doWorkerManagerRequest",
			args{
				req: &http.Request{},
			},
			utils.UnHealthy,
			true,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							HTTPSConfig: HTTPSConfig,
							LocalAuth:   &localauth.AuthConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc(workermanagerutils.FillInWorkerManagerRequestHeaders,
						func(request *http.Request, auth *localauth.AuthConfig) { return }),
					gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
						error) {
						return &http.Response{StatusCode: http.StatusInternalServerError,
							Body: ioutil.NopCloser(bytes.NewBuffer([]byte{}))}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := doWorkerManagerRequest(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("doWorkerManagerRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("doWorkerManagerRequest() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestClusterManager_ClusterFlowOut(t *testing.T) {
	c := &ClusterManager{
		strategy: NewLBStrategy(),
		lock:     sync.RWMutex{},
	}
	c.AddCluster("cluster001_flow_out", Cluster{State: utils.Healthy})
	clusters := c.GetAllCluster()
	assert.Equal(t, 1, len(clusters))
	assert.Equal(t, utils.Healthy, clusters["cluster001_flow_out"].State)
	assert.Equal(t, true, clusters["cluster001_flow_out"].offlineTime.IsZero())

	c.ClusterFlowOut("cluster001_flow_out")
	availableClusters := c.GetAllAvailableCluster()
	assert.Equal(t, 0, len(availableClusters))
	clusters = c.GetAllCluster()
	assert.Equal(t, 1, len(clusters))
	assert.Equal(t, utils.UnHealthy, clusters["cluster001_flow_out"].State)
	assert.Equal(t, false, clusters["cluster001_flow_out"].offlineTime.IsZero())

	cache.GetInstance().UpdateCaeAvailableCluster("funcSvc", []string{"cluster001_flow_out"})
	availableScaleUpClusters := c.GetAvailableClusterForScaleUp("funcSvc")
	assert.Equal(t, "", availableScaleUpClusters.ClusterID)

	availableScaleDownClusters := c.GetAvailableClusterForScaleDown("funcSvc", map[string]int64{})
	assert.Equal(t, "", availableScaleDownClusters.ClusterID)
	cache.GetInstance().RemoveCaeAvailableCluster("funcSvc")
}

func TestClusterManager_ClusterFlowIn(t *testing.T) {
	c := &ClusterManager{
		strategy: NewLBStrategy(),
		lock:     sync.RWMutex{},
	}
	c.AddCluster("cluster001_flow_in", Cluster{State: utils.Healthy, ClusterID: "cluster001_flow_in"})
	clusters := c.GetAllCluster()
	assert.Equal(t, 1, len(clusters))
	assert.Equal(t, utils.Healthy, clusters["cluster001_flow_in"].State)
	assert.Equal(t, true, clusters["cluster001_flow_in"].offlineTime.IsZero())

	c.ClusterFlowOut("cluster001_flow_in")
	availableClusters := c.GetAllAvailableCluster()
	assert.Equal(t, 0, len(availableClusters))
	clusters = c.GetAllCluster()
	assert.Equal(t, 1, len(clusters))
	assert.Equal(t, utils.UnHealthy, clusters["cluster001_flow_in"].State)
	assert.Equal(t, false, clusters["cluster001_flow_in"].offlineTime.IsZero())

	cache.GetInstance().UpdateCaeAvailableCluster("funcSvc", []string{"cluster001_flow_in"})
	availableScaleUpClusters := c.GetAvailableClusterForScaleUp("funcSvc")
	assert.Equal(t, "", availableScaleUpClusters.ClusterID)

	availableScaleDownClusters := c.GetAvailableClusterForScaleDown("funcSvc", map[string]int64{})
	assert.Equal(t, "", availableScaleDownClusters.ClusterID)

	c.ClusterFlowIn("cluster001_flow_in")
	clusters = c.GetAllCluster()
	assert.Equal(t, 1, len(clusters))
	assert.Equal(t, utils.Healthy, clusters["cluster001_flow_in"].State)
	assert.Equal(t, true, clusters["cluster001_flow_in"].offlineTime.IsZero())

	cache.GetInstance().RemoveCaeAvailableCluster("funcSvc")
	availableClusters = c.GetFuncAllAvailableCluster("funcSvc")
	assert.Equal(t, 0, len(availableClusters))

	availableClusters = c.GetAllAvailableCluster()
	assert.Equal(t, 1, len(availableClusters))
	assert.Equal(t, utils.Healthy, availableClusters["cluster001_flow_in"].State)
	assert.Equal(t, true, availableClusters["cluster001_flow_in"].offlineTime.IsZero())
}

func TestClusterManager_GetClusterInfo(t *testing.T) {
	c := &ClusterManager{
		strategy: NewLBStrategy(),
		lock:     sync.RWMutex{},
	}
	c.AddCluster("cluster001_flow_in", Cluster{State: utils.Healthy, ClusterID: "cluster001_flow_in"})
	cluster := c.GetClusterInfo("cluster001_flow_in")
	assert.Equal(t, utils.Healthy, cluster.State)
	cluster2 := c.GetClusterInfo("cluster002_flow_in")
	assert.NotEqual(t, utils.Healthy, cluster2.State)
}

func TestClusterPoolPanic(t *testing.T) {
	c := ClusterManager{
		strategy: CreateStrategy(strategyDefault),
	}

	c.strategy.AddCluster("cluster01", Cluster{ClusterID: "cluster01", State: utils.Healthy})
	c.strategy.AddCluster("cluster02", Cluster{ClusterID: "cluster02", State: utils.Healthy})

	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(checkWorkerManagerHealth, func(url string) (string, error) {
			return "", nil
		}),
		gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
			return false
		}),
	})

	var wg sync.WaitGroup

	wg.Add(20000)

	go func() {
		for i := 0; i < 10000; i++ {
			c.healthCheck()
			wg.Done()
		}
	}()

	go func() {
		for i := 0; i < 10000; i++ {
			c.strategy.GetAvailablePool()
			wg.Done()
		}
	}()

	wg.Wait()

	patches.ResetAll()
}

func TestHealthCheckRight(t *testing.T) {
	c := ClusterManager{
		strategy: &LoadBalanceStrategy{
			clusterPool:            make(map[string]Cluster),
			clusterSwitchFlowState: make(map[string]bool),
			poolLock:               sync.RWMutex{},
		},
	}

	c.strategy.AddCluster("cluster01", Cluster{ClusterID: "cluster01", State: utils.Healthy})
	c.strategy.AddCluster("cluster02", Cluster{ClusterID: "cluster02", State: utils.Healthy})

	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(checkWorkerManagerHealth, func(url string) (string, error) {
			return "", nil
		}),
		gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
			return false
		}),
	})

	var wg sync.WaitGroup

	wg.Add(20000)

	go func() {
		for i := 0; i < 10000; i++ {
			c.healthCheck()
			wg.Done()
		}
	}()

	go func() {
		for i := 0; i < 10000; i++ {
			c.strategy.RemoveCluster("cluster02")
			if len(c.strategy.GetClusterPool()) != 1 {
				t.Errorf("failed to get cluster pool,len is %d", len(c.strategy.GetClusterPool()))
			}
			c.strategy.AddCluster("cluster02", Cluster{ClusterID: "cluster02", State: utils.Healthy})
			if len(c.strategy.GetClusterPool()) != 2 {
				t.Errorf("failed to get cluster pool,len is %d", len(c.strategy.GetClusterPool()))
			}
			wg.Done()
		}
	}()

	wg.Wait()

	patches.ResetAll()
}

func TestClusterManager_RemoveCluster(t *testing.T) {
	type fields struct {
		strategy           Strategy
		stopCh             <-chan struct{}
		clusterMetricCache map[string]config.ClusterUsage
		lock               sync.RWMutex
	}
	type args struct {
		cluster *utils.WorkerMgrInstance
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantLen int
	}{
		{
			"",
			fields{
				strategy: CreateStrategy(strategyDefault),
			},
			args{
				cluster: &utils.WorkerMgrInstance{
					ClusterID: "cluster001",
				},
			},
			0,
		},
		{
			"",
			fields{
				strategy: CreateStrategy(strategyDefault),
			},
			args{
				cluster: &utils.WorkerMgrInstance{
					ClusterID: "cluster001",
					NodeIP:    "111.0.0.1",
				},
			},
			1,
		},
		{
			"",
			fields{
				strategy: CreateStrategy(strategyDefault),
			},
			args{
				cluster: &utils.WorkerMgrInstance{
					ClusterID: "cluster001",
					NodeIP:    "101.0.0.1",
				},
			},
			0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClusterManager{
				strategy:           tt.fields.strategy,
				stopCh:             tt.fields.stopCh,
				clusterMetricCache: tt.fields.clusterMetricCache,
				lock:               tt.fields.lock,
			}
			c.AddCluster("cluster001", Cluster{ClusterID: "cluster001",
				State: utils.Healthy, ActiveNodes: NodeStatus{IP: "101.0.0.1"}})
			c.RemoveCluster(tt.args.cluster)
			if len(c.strategy.GetAvailablePool()) != tt.wantLen {
				t.Errorf("failed to TestClusterManager_RemoveCluster, cluster: %+v", c.strategy.GetAvailablePool())
			}
		})
	}
}
