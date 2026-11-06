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

package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/functiontypes"
	mockutils "functioncore/test/utils"
)

func Test_ASPersistence_UpdateFuncInsCacheFromRedis(t *testing.T) {
	clusterInsCaches := make([]config.ClusterInsCache, 0)
	clusterInsCaches = append(clusterInsCaches, config.ClusterInsCache{
		Count:     1,
		ClusterID: "cluster",
	})
	instances := make(map[string]*config.InstanceCache)
	instancesCache := &config.InstanceCache{
		TotalCurrentCount: 1,
		ClusterInsCache:   clusterInsCaches,
	}
	instances["mockPodSvc"] = instancesCache
	tests := []struct {
		name string
		asp  *ASPersistence
		want *config.InstanceCache
	}{
		{
			"test update func instance success",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: instances,
			},
			instancesCache,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				GetInstance().FunctionInstanceCache = sync.Map{}
			}()
			tt.asp.updateFuncInsCacheFromRedis()
			cache, ok := GetInstance().FunctionInstanceCache.Load("mockPodSvc")
			got := cache.(*config.InstanceCache)
			if !ok || got != tt.want {
				t.Errorf("GetInstance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ASPersistence_setInstances(t *testing.T) {
	clusterInsCaches := make([]config.ClusterInsCache, 0)
	clusterInsCaches = append(clusterInsCaches, config.ClusterInsCache{
		Count:     1,
		ClusterID: "cluster",
	})
	instances := make(map[string]*config.InstanceCache)
	instancesCache := &config.InstanceCache{
		TotalCurrentCount: 1,
		ClusterInsCache:   clusterInsCaches,
	}
	instances["mockPodSvc"] = instancesCache
	tests := []struct {
		name   string
		asp    *ASPersistence
		want   map[string]*config.InstanceCache
		doFunc func()
	}{
		{
			"test ASPersistence set instances key failed",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: make(map[string]*config.InstanceCache),
			},
			make(map[string]*config.InstanceCache),
			func() {
				GetInstance().FunctionInstanceCache.Store(123, instancesCache)
			},
		},
		{
			"test ASPersistence set instances value failed",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: make(map[string]*config.InstanceCache),
			},
			make(map[string]*config.InstanceCache),
			func() {
				GetInstance().FunctionInstanceCache.Store("mockPodSvc", 123)
			},
		},
		{
			"test ASPersistence set instances success",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: make(map[string]*config.InstanceCache),
			},
			instances,
			func() {
				GetInstance().FunctionInstanceCache.Store("mockPodSvc", instancesCache)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.doFunc()
			tt.asp.setInstances()
			defer func() {
				GetInstance().FunctionInstanceCache = sync.Map{}
			}()
			if !reflect.DeepEqual(tt.asp.Instances, tt.want) {
				t.Errorf("GetInstance() = %v, want %v", tt.asp.Instances, tt.want)
			}
		})
	}
}

func TestSaveInsCacheToRedis(t *testing.T) {
	convey.Convey("test save instance cache to redis", t, func() {
		convey.Convey("test OK", func() {
			redisAlarmCount = 1
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					HTTPSConfig: &tls.InternalHTTPSConfig{
						HTTPSEnable: false,
						TLSProtocol: "",
						TLSCiphers:  "",
					},
					LocalAuth: &localauth.AuthConfig{},
				}, nil
			})
			patches = gomonkey.ApplyFunc(redisclient.SetWithRetry, func(redisKey string, redisValue []byte, expireTime time.Duration) bool {
				return true
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
			})
			defer patches.Reset()
			SaveInsCacheToRedis()
			convey.So(redisAlarmCount, convey.ShouldEqual, 0)
		})

		convey.Convey("test error", func() {
			redisAlarmCount = 2
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return nil, errors.New("error")
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc(redisclient.SetWithRetry, func(redisKey string, redisValue []byte, expireTime time.Duration) bool {
				return false
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, almType alarm.Type, specAddition string) {
				redisAlarmCount++
			})
			defer patches.Reset()
			SaveInsCacheToRedis()
			convey.So(redisAlarmCount, convey.ShouldEqual, 4)
		})
	})
}

func TestRecoverPersistence(t *testing.T) {
	insMap := make(map[string]*config.InstanceCache)
	insMap["func11"] = &config.InstanceCache{TotalCurrentCount: 4}
	asp := &ASPersistence{
		Version:   defaultVersion,
		Timestamp: time.Now().Unix(),
		Instances: insMap,
	}
	aspBytes, _ := json.Marshal(asp)
	convey.Convey("test recover persistence", t, func() {
		convey.Convey("test success", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return &redisclient.Client{}
				}),
				gomonkey.ApplyFunc((*redisclient.Client).Get,
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
						return &redis.StringCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
					"Result", func(_ *redis.StringCmd) (string, error) {
						return string(aspBytes), nil
					}),
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
					a, ok := v.(*ASPersistence)
					if ok {
						a.Version = defaultVersion
						a.Instances = insMap
					}
					return nil
				}),
				gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
					func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
						return map[string]clustermgr.Cluster{"cluster1": {}}
					}),
			})
			defer patches.ResetAll()
			RecoverPersistence()
			_, ok := GetInstance().FunctionInstanceCache.Load("func11")
			convey.So(ok, convey.ShouldBeTrue)
		})
	})
}

func TestQueryInstancesFromWmByIP(t *testing.T) {
	queryInsResp := types.QueryInstanceResponse{
		Code:      200,
		Message:   "success",
		Instances: types.QueryInstance{},
	}
	byteResp, _ := json.Marshal(queryInsResp)
	type args struct {
		wantResp   types.QueryInstanceResponse
		wantErr    bool
		wantErrMsg string
	}
	urlValue := &url.URL{
		Scheme:   "https",
		User:     url.UserPassword("me", "pass"),
		Host:     "example.com",
		Path:     "foo/bar",
		RawQuery: "x=1&y=2",
		Fragment: "anchor"}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"url parse failed",
			args{
				types.QueryInstanceResponse{},
				true,
				"failed to parse url",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return nil, fmt.Errorf("failed to parse url")
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURLByIP, func(targetIP string) string {
						return "ip"
					}),
				})
				return patches
			},
		},
		{
			"create quest failed",
			args{
				types.QueryInstanceResponse{},
				true,
				"failed to create request",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURLByIP, func(targetIP string) string {
						return "ip"
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("http.NewRequestWithContext")
					}),
				})
				return patches
			},
		},
		{
			name: "do quest failed",
			args: args{
				types.QueryInstanceResponse{},
				true,
				"failed to send request to worker-manager",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{
							Scheme:   "https",
							User:     url.UserPassword("me", "pass"),
							Host:     "example.com",
							Path:     "foo/bar",
							RawQuery: "x=1&y=2",
							Fragment: "anchor"}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURLByIP, func(targetIP string) string {
						return "ip"
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{}
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("do request error")
					}),
				})
				return patches
			},
		},
		{
			"response code not OK",
			args{
				types.QueryInstanceResponse{},
				true,
				"resp code",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURLByIP, func(targetIP string) string {
						return "ip"
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{}
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 500,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("{}"))),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), nil
					}),
				})
				return patches
			},
		},
		{
			"read resp body failed",
			args{
				types.QueryInstanceResponse{},
				true,
				"failed to read resp body",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURLByIP, func(targetIP string) string {
						return "ip"
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{}
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("{}"))),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("err")
					}),
				})
				return patches
			},
		},
		{
			"query success",
			args{
				queryInsResp,
				false,
				"",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURLByIP, func(targetIP string) string {
						return "ip"
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{}
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("{}"))),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return byteResp, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			got, err := queryInstancesFromWmByIP("func1", "192.168.1.1")
			if (err != nil) != tt.args.wantErr {
				t.Errorf("queryInstancesFromWmByIP() = %v, want %v, wantErr %v", got, tt.args.wantResp, tt.args.wantErr)
			}
			if tt.args.wantErrMsg != "" {
				assert.Contains(t, err.Error(), tt.args.wantErrMsg)
			}
		})
	}
}

func TestUpdateFuncInsCache(t *testing.T) {
	convey.Convey("test update funcInsCache", t, func() {
		var metaCache sync.Map
		updateFlag := false
		metaCache.Store("func1", &functiontypes.FunctionEtcdInfo{})
		patch := gomonkey.ApplyFunc(GetInstance,
			func() *AutoScalerCache {
				return &AutoScalerCache{FunctionMetaCache: metaCache}
			})
		defer patch.Reset()
		patch = gomonkey.ApplyFunc(updateFuncInsCacheBySvc, func(funcSvc string) {
			updateFlag = true
		})
		defer patch.Reset()
		UpdateFuncInsCache()
		convey.So(updateFlag, convey.ShouldBeTrue)
	})
}

func TestUpdateFuncInsCacheBySvc(t *testing.T) {
	convey.Convey("test update funcInsCache by funcSvc", t, func() {
		convey.Convey("test update success", func() {
			patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
				func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
					return map[string]clustermgr.Cluster{"cluster1": {}}
				})
			defer patch.Reset()
			patch = gomonkey.ApplyFunc(queryInstancesFromWmByIP,
				func(funcSvc string, IP string) (types.QueryInstanceResponse, error) {
					return types.QueryInstanceResponse{
						Code:    200,
						Message: "ok",
						Instances: types.QueryInstance{
							ClusterID: "cluster1",
							ExpectNum: 2,
							PodsName:  []string{"pod1", "pod2"},
						},
					}, nil
				})
			defer patch.Reset()
			updateFuncInsCacheBySvc("func1")
			value, ok := GetInstance().FunctionInstanceCache.Load("func1")
			convey.So(ok, convey.ShouldBeTrue)
			insCache := value.(*config.InstanceCache)
			convey.So(insCache.TotalCurrentCount, convey.ShouldEqual, 2)
		})
		convey.Convey("test query instance not OK", func() {
			patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
				func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
					return map[string]clustermgr.Cluster{"cluster1": {}}
				})
			defer patch.Reset()
			patch = gomonkey.ApplyFunc(queryInstancesFromWmByIP,
				func(funcSvc string, IP string) (types.QueryInstanceResponse, error) {
					return types.QueryInstanceResponse{
						Code:    200,
						Message: "ok",
						Instances: types.QueryInstance{
							ClusterID: "cluster1",
							ExpectNum: 2,
							PodsName:  []string{"pod1", "pod2"},
						},
					}, errors.New("error")
				})
			defer patch.Reset()
			updateFuncInsCacheBySvc("func2")
			value, ok := GetInstance().FunctionInstanceCache.Load("func2")
			convey.So(ok, convey.ShouldBeTrue)
			insCache := value.(*config.InstanceCache)
			convey.So(insCache.TotalCurrentCount, convey.ShouldEqual, 0)
		})
		convey.Convey("test response not OK", func() {
			patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
				func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
					return map[string]clustermgr.Cluster{"cluster1": {}}
				})
			defer patch.Reset()
			patch = gomonkey.ApplyFunc(queryInstancesFromWmByIP,
				func(funcSvc string, IP string) (types.QueryInstanceResponse, error) {
					return types.QueryInstanceResponse{
						Code:    500,
						Message: "ok",
						Instances: types.QueryInstance{
							ClusterID: "cluster1",
							ExpectNum: 2,
							PodsName:  []string{"pod1", "pod2"},
						},
					}, nil
				})
			defer patch.Reset()
			updateFuncInsCacheBySvc("func3")
			value, ok := GetInstance().FunctionInstanceCache.Load("func3")
			convey.So(ok, convey.ShouldBeTrue)
			insCache := value.(*config.InstanceCache)
			convey.So(insCache.TotalCurrentCount, convey.ShouldEqual, 0)
		})
	})
}
