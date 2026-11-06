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

// Package scaler -
package scaler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/functiontypes"
	mockutils "functioncore/test/utils"
)

func TestDoActions(t *testing.T) {
	type args struct {
		actions map[string]Action
	}
	clustermanager := &clustermgr.ClusterManager{}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case1 test scale action",
			args{
				actions: map[string]Action{
					"functionSvcA": {
						Type:          ActionPull,
						NeedPull:      6,
						TotalExpected: 10,
					},
					"functionB": {
						Type:          ActionReduce,
						NeedReduce:    2,
						TotalExpected: 5,
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(clustermgr.GetClusterMgr, func() *clustermgr.ClusterManager {
						return clustermanager
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(clustermanager), "GetAvailableClusterForScaleUp",
						func(_ *clustermgr.ClusterManager) clustermgr.Cluster {
							return clustermgr.Cluster{
								ClusterID: "cluster001",
								ActiveNodes: clustermgr.NodeStatus{
									IP: "10.2.3.1",
								},
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(clustermanager), "GetAvailableClusterForScaleDown",
						func(_ *clustermgr.ClusterManager) clustermgr.Cluster {
							return clustermgr.Cluster{
								ClusterID: "cluster002",
								ActiveNodes: clustermgr.NodeStatus{
									IP: "10.2.3.2",
								},
							}
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							DownscaleStabilization: 0,
						}, nil
					}),
				})
				return patches
			},
		},
		{
			"case2 test scale action",
			args{
				actions: map[string]Action{
					"functionSvcA": {
						Type:          ActionPull,
						NeedPull:      6,
						TotalExpected: 10,
					},
					"functionB": {
						Type:          ActionReduce,
						NeedReduce:    2,
						TotalExpected: 5,
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(clustermgr.GetClusterMgr, func() *clustermgr.ClusterManager {
						return clustermanager
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(clustermanager), "GetAvailableClusterForScaleUp",
						func(_ *clustermgr.ClusterManager) clustermgr.Cluster {
							return clustermgr.Cluster{
								ClusterID: "cluster001",
								ActiveNodes: clustermgr.NodeStatus{
									IP: "10.2.3.1",
								},
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(clustermanager), "GetAvailableClusterForScaleDown",
						func(_ *clustermgr.ClusterManager) clustermgr.Cluster {
							return clustermgr.Cluster{
								ClusterID: "cluster002",
								ActiveNodes: clustermgr.NodeStatus{
									IP: "10.2.3.2",
								},
							}
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							DownscaleStabilization: 0,
						}, nil
					}),
					gomonkey.ApplyFunc(ScaleUpInstance, func(functionSvcID string, expectedNum int64,
						workerManagerIP string) (*types.Instance, error) {
						return &types.Instance{}, nil
					}),
					gomonkey.ApplyFunc(ScaleDownInstance, func(functionSvcID string,
						expected int64, workerManagerIP string) (bool, error) {
						return false, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			DoActions(tt.args.actions)
			time.Sleep(10 * time.Second)
			patches.ResetAll()
		})
	}
}

func TestScaleDownInstance(t *testing.T) {
	type args struct {
		functionSvcID   string
		expected        int64
		workerManagerIP string
	}
	testFunctionID := "testFunctionID"
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case test succeed to scale down instance",
			args{
				functionSvcID:   testFunctionID,
				expected:        6,
				workerManagerIP: "10.2.2.1",
			},
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
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
						return &http.Response{StatusCode: http.StatusOK,
							Body: ioutil.NopCloser(bytes.NewBuffer(buildValidScaleDownBody()))}, nil
					}),
				})
				return patches
			},
		},
		{
			"case1 test failed to scale down instance",
			args{
				functionSvcID:   testFunctionID,
				expected:        6,
				workerManagerIP: "10.2.2.1",
			},
			true,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
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
						return &http.Response{StatusCode: http.StatusInternalServerError,
							Body: ioutil.NopCloser(bytes.NewBuffer(buildInValidScaleDownBody()))}, nil
					}),
				})
				return patches
			},
		},
		{
			"case2 test failed to scale down instance",
			args{
				functionSvcID:   testFunctionID,
				expected:        6,
				workerManagerIP: "10.2.2.1",
			},
			true,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
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
						return &http.Response{StatusCode: http.StatusOK,
							Body: ioutil.NopCloser(bytes.NewBuffer(buildInValidScaleUpBody()))}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if _, err := ScaleDownInstance(tt.args.functionSvcID, tt.args.expected, tt.args.workerManagerIP); (err != nil) != tt.wantErr {
				t.Errorf("ScaleDownInstance() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func buildValidScaleUpBody() []byte {
	rsp := config.GetWorkerSuccessResponse{
		Code:     statusOKCode,
		Worker:   &config.Worker{},
		Instance: &types.Instance{},
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		return nil
	}
	return body
}

func buildInValidHttpBody() []byte {
	rsp := config.GetWorkerSuccessResponse{
		Code:     statusOKCode,
		Worker:   &config.Worker{},
		Instance: &types.Instance{},
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		return nil
	}
	return body
}

func buildInValidScaleUpBody() []byte {
	rsp := config.GetWorkerSuccessResponse{
		Code:    http.StatusInternalServerError,
		Message: "failed to request",
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		return nil
	}
	return body
}

func buildValidScaleDownBody() []byte {
	rsp := DeleteWorkerResponse{
		Code: http.StatusOK,
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		return nil
	}
	return body
}

func buildInValidScaleDownBody() []byte {
	rsp := DeleteWorkerResponse{
		Code:    http.StatusInternalServerError,
		Message: "failed to request",
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		return nil
	}
	return body
}

func TestScaleUpInstance(t *testing.T) {
	type args struct {
		functionSvcID   string
		expectedNum     int64
		workerManagerIP string
	}
	wantInstance := &types.Instance{}
	tests := []struct {
		name        string
		args        args
		want        *types.Instance
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case succeed to scale up instance",
			args: args{
				functionSvcID:   "testFunctionID",
				expectedNum:     6,
				workerManagerIP: "10.2.2.1",
			},
			want:    wantInstance,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				HTTPSConfig := &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "",
					TLSCiphers:  "",
				}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							HTTPSConfig: HTTPSConfig,
							LocalAuth:   &localauth.AuthConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
					gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
						error) {
						return &http.Response{StatusCode: http.StatusOK,
							Body: ioutil.NopCloser(bytes.NewBuffer(buildValidScaleUpBody()))}, nil
					}),
				})
				return patches
			},
		},
		{
			name: "case failed to scale up instance 1",
			args: args{
				functionSvcID:   "testFunctionID",
				expectedNum:     6,
				workerManagerIP: "10.2.2.1",
			},
			want:    nil,
			wantErr: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
					gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			name: "case failed to scale up instance 2",
			args: args{
				functionSvcID:   "testFunctionID",
				expectedNum:     6,
				workerManagerIP: "10.2.2.1",
			},
			want:    nil,
			wantErr: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
					gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{Timeout: 600000}
					}),
				})
				return patches
			},
		},
		{
			name: "case failed to scale up instance 3",
			args: args{
				functionSvcID:   "testFunctionID",
				expectedNum:     6,
				workerManagerIP: "10.2.2.1",
			},
			want:    nil,
			wantErr: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							HTTPSConfig: &tls.InternalHTTPSConfig{
								HTTPSEnable: false,
								TLSProtocol: "",
								TLSCiphers:  "",
							},
							LocalAuth: &localauth.AuthConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
					gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
						error) {
						return &http.Response{StatusCode: http.StatusBadRequest,
							Body: ioutil.NopCloser(bytes.NewBuffer(buildValidScaleUpBody()))}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := ScaleUpInstance(tt.args.functionSvcID, tt.args.expectedNum, tt.args.workerManagerIP)
			if (err != nil) != tt.wantErr {
				t.Errorf("ScaleUpInstance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ScaleUpInstance() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_handleScaleUpRetry(t *testing.T) {
	type args struct {
		workerManagerIP      string
		err                  error
		functionSvcID        string
		expectedNumByCluster int64
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case test handle scale up retry",
			args{
				"10.2.2.1",
				context.DeadlineExceeded,
				"testFunctionSvcID",
				6,
			},
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
						func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
							return map[string]clustermgr.Cluster{
								"cluster1": {
									ClusterID: "cluster1",
									ActiveNodes: clustermgr.NodeStatus{
										IP: "10.2.2.2",
									},
								},
							}
						}),
					gomonkey.ApplyFunc(ScaleUpInstance, func(functionSvcID string, expectedNum int64,
						workerManagerIP string) (*types.Instance, error) {
						return &types.Instance{}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := handleScaleUpRetry(tt.args.workerManagerIP, tt.args.err, tt.args.functionSvcID, tt.args.expectedNumByCluster); (err != nil) != tt.wantErr {
				t.Errorf("handleScaleUpRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_needRetryToOtherCluter(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"case 1 err nil",
			args{
				err: nil,
			},
			false,
		},
		{
			"case 2 err context.DeadlineExceeded",
			args{
				err: context.DeadlineExceeded,
			},
			true,
		},
		{
			"case 3 err reachMaxInstanceErr",
			args{
				err: errReachMaxInstance,
			},
			true,
		},
		{
			"case 4 err new",
			args{
				err: errors.New("error"),
			},
			false,
		},
		{
			"case 5 err reachOnDemandMaxInstanceErr",
			args{
				err: errReachMaxOnDemandInstance,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needRetryToOtherCluter(tt.args.err); got != tt.want {
				t.Errorf("needRetryToOtherCluter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_handleScaleDownRetry(t *testing.T) {
	stopCh := make(chan struct{})
	close(stopCh)

	clustermgr.CreateClusterMgr(stopCh, &config.Config{})
	clustermgr.GetClusterMgr().AddCluster("cluster1", clustermgr.Cluster{
		ClusterID: "cluster1",
		State:     utils.Healthy,
		ActiveNodes: clustermgr.NodeStatus{
			IP: "0.0.0.0",
		},
	})

	clustermgr.GetClusterMgr().AddCluster("cluster2", clustermgr.Cluster{
		ClusterID: "cluster2",
		State:     utils.Healthy,
		ActiveNodes: clustermgr.NodeStatus{
			IP: "1.1.1.1",
		},
	})

	cache.GetInstance().FunctionInstanceCache.Store("func1", &config.InstanceCache{
		ClusterInsCache: []config.ClusterInsCache{{"cluster1", 1}, {"cluster2", 2}},
	})

	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(ScaleDownInstance, func(functionSvcID string, expected int64, workerManagerIP string) (bool, error) {
			return false, nil
		}),
	})
	isSuccess := handleScaleDownRetry("0.0.0.0", "func1", 0)
	if !isSuccess {
		t.Errorf("failed to Test_handleScaleDownRetry ")
	}
	patches.ResetAll()

	patches = mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(ScaleDownInstance, func(functionSvcID string, expected int64, workerManagerIP string) (bool, error) {
			return true, nil
		}),
	})
	isSuccess = handleScaleDownRetry("0.0.0.0", "func1", 0)
	if isSuccess {
		t.Errorf("failed to Test_handleScaleDownRetry ")
	}
	patches.ResetAll()

}
