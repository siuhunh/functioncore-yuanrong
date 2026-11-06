/*
 * Copyright (c) 2021-2024 Huawei Technologies Co., Ltd
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

package faasfunction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	informercorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	k8cache "k8s.io/client-go/tools/cache"

	apiServerOptions "functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/clusterstatusutil"
	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/cache/willdeleteworker"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource/etcdevents"
	interfacelog "functioncore/pkg/worker-manager/log"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	vpaPod "functioncore/pkg/worker-manager/vpa"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockUtils "functioncore/test/utils"
)

type MockResponseWriter struct{}

func (m MockResponseWriter) Header() http.Header {
	return nil
}

func (m MockResponseWriter) Write([]byte) (int, error) {
	return 0, nil
}

func (m MockResponseWriter) WriteHeader(int) {
	return
}

func TestHandleComponentHealthCheck(t *testing.T) {
	convey.Convey("Test Handle Component HealthCheck", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		defer func() {
			patch.Reset()
		}()
		req := httptest.NewRequest("GET", "/componenthealth", nil)
		w := httptest.NewRecorder()
		handleComponentHealthCheck(w, req)
		resp := w.Result()
		convey.So(resp.StatusCode, convey.ShouldEqual, 500)
	})

	convey.Convey("InitEtcdClient", t, func() {
		convey.Convey("InitClientOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(clientv3.New, func(cfg clientv3.Config) (*clientv3.Client, error) {
					return &clientv3.Client{}, nil
				}),
				gomonkey.ApplyFunc(concurrency.NewSession, func(client *clientv3.Client,
					opts ...concurrency.SessionOption) (*concurrency.Session, error) {
					return &concurrency.Session{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			stopCh := make(<-chan struct{})
			_ = etcd3.InitParam().
				WithStopCh(stopCh).
				WithMetaEtcdConfig(&etcd3.ClientConfig{}).
				WithRouterEtcdConfig(&etcd3.ClientConfig{}).
				InitClient()
		})
	})

	convey.Convey("Test Handle Component HealthCheck", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		patch.ApplyFunc((*etcd3.EtcdClient).GetEtcdStatusLostContact, func(_ *etcd3.EtcdClient) bool {
			return true
		})
		defer func() {
			patch.Reset()
		}()
		req := httptest.NewRequest("GET", "/componenthealth", nil)
		w := httptest.NewRecorder()
		handleComponentHealthCheck(w, req)
		resp := w.Result()
		convey.So(resp.StatusCode, convey.ShouldEqual, 200)
	})
}

func TestDeleteFunctionInstance(t *testing.T) {
	convey.Convey("Test Delete Function Instance", t, func() {
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		req := httptest.NewRequest("POST", "/worker/restore", nil)
		w := httptest.NewRecorder()
		patches := [...]*Patches{
			ApplyMethod(reflect.TypeOf(m.podFetcher), "GetPodByName",
				func(p *podfetcher.PodFetcher, podName, namespace string) (*corev1.Pod, error) {
					return &corev1.Pod{}, nil
				}),
			ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
				instance v1alpha1.WorkerInstance) {
				return
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		m.deleteFunctionInstance(w, req)
		resp := w.Result()
		convey.So(resp.StatusCode, convey.ShouldNotBeNil)
	})

	m := &FunctionAPI{
		podFetcher: &podfetcher.PodFetcher{
			KubeClient: &kubernetes.Clientset{},
		},
	}
	w := httptest.NewRecorder()
	tests := []struct {
		name        string
		req         *http.Request
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 delete instance but instance pod is already deleted",
			httptest.NewRequest("POST", "/worker/delete", nil),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*FunctionAPI).parseDeleteRequest,
						func(*FunctionAPI, http.ResponseWriter, *http.Request) (*DeleteWorkerRequest, error) {
							return &DeleteWorkerRequest{}, nil
						}),
					ApplyFunc((*podfetcher.PodFetcher).DeletePodByWorkerInstance,
						func(p *podfetcher.PodFetcher, funcSvcID, namespace, traceID string,
							instance v1alpha1.WorkerInstance) {
							return
						}),
					ApplyFunc((*podfetcher.PodFetcher).GetPodByName,
						func(p *podfetcher.PodFetcher, podName, namespace string) (*corev1.Pod, error) {
							return nil, fmt.Errorf("not found")
						}),
					ApplyFunc(k8serror.IsNotFound,
						func(err error) bool {
							if err.Error() == "not found" {
								return true
							}
							return false
						}),
					ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
						instance v1alpha1.WorkerInstance) {
						return
					}),
				})
				return patches
			}},

		{"case2 delete instance but GetPodByName error",
			httptest.NewRequest("POST", "/worker/delete", nil),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*FunctionAPI).parseDeleteRequest,
						func(*FunctionAPI, http.ResponseWriter, *http.Request) (*DeleteWorkerRequest, error) {
							return &DeleteWorkerRequest{}, nil
						}),
					ApplyFunc((*podfetcher.PodFetcher).DeletePodByWorkerInstance,
						func(p *podfetcher.PodFetcher, funcSvcID, namespace, traceID string,
							instance v1alpha1.WorkerInstance) {
							return
						}),
					ApplyFunc((*podfetcher.PodFetcher).GetPodByName,
						func(p *podfetcher.PodFetcher, podName, namespace string) (*corev1.Pod, error) {
							return nil, fmt.Errorf("not found")
						}),
					ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
						instance v1alpha1.WorkerInstance) {
						return
					}),
				})
				return patches
			}},

		{"case3 delete reserved  instance and isBrokenConnection is true",
			httptest.NewRequest("POST", "/worker/delete", bytes.NewReader([]byte("{\"instance_id\":\"test\",\"function_name\":\"test\",\"function_version\":\"test\",\"tenant_id\":\"test\",\"business_id\":\"test\",\"applier\":\"worker-manager\",\"broken_connection_status\":true}"))),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*podfetcher.PodFetcher).GetPodByName,
						func(p *podfetcher.PodFetcher, podName, namespace string) (*corev1.Pod, error) {
							pod := &corev1.Pod{
								TypeMeta: metav1.TypeMeta{},
								ObjectMeta: metav1.ObjectMeta{
									Name: "podName2",
									Annotations: map[string]string{
										constants.OndemandKey: "true",
									},
									Labels: map[string]string{
										"applier": "worker-manager",
									},
								},
								Spec: corev1.PodSpec{
									NodeName: "nodeName2",
								},
								Status: corev1.PodStatus{},
							}
							return pod, nil
						}),
					ApplyFunc((*podfetcher.PodFetcher).DeletePodByWorkerInstance,
						func(p *podfetcher.PodFetcher, funcSvcID, namespace, traceID string,
							instance v1alpha1.WorkerInstance) {
							return
						}),
					ApplyFunc((*FunctionAPI).handleUpdateAgencyInfoToWorker,
						func(f *FunctionAPI, funcSvcID string, podName string,
							workerInstance v1alpha1.WorkerInstance, traceID string) {
							return
						}),
					ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							return nil
						}),
				})
				return patches
			}},
		{"case4 delete reserved instance and not found pod",
			httptest.NewRequest("POST", "/worker/delete", bytes.NewReader([]byte("{\"instance_id\":\"\",\"function_name\":\"test\",\"function_version\":\"test\",\"tenant_id\":\"test\",\"business_id\":\"test\",\"applier\":\"ASBRes\",\"broken_connection_status\":true}"))),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*FunctionAPI).parseDeleteRequest,
						func(*FunctionAPI, http.ResponseWriter, *http.Request) (*DeleteWorkerRequest, error) {
							return &DeleteWorkerRequest{
								Applier:    constants.ASBResApplier,
								InstanceID: "",
							}, nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			m.deleteFunctionInstance(w, tt.req)
			patches.ResetAll()
		})
	}
}

func TestScaleUpFunction(t *testing.T) {
	jsonBytes, _ := json.Marshal(&config.RequestInfo{})
	type args struct {
		c *gin.Context
	}
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("POST", "/worker", bytes.NewReader(jsonBytes))
	ctx2, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx2.Request = httptest.NewRequest("POST", "/worker", bytes.NewReader(jsonBytes))

	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 calltype active", &FunctionAPI{}, args{
			c: ctx2,
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(utils.GetMetaName, func(c *gin.Context) (string, error) {
					c.Request.Header.Add(constants.HeaderCallType, utils.RequestCallType)
					return "test", nil
				}),
				ApplyFunc((*FunctionAPI).parseHTTPRequest,
					func(_ *FunctionAPI, c *gin.Context) *config.RequestInfo {
						return &config.RequestInfo{
							CallType: utils.RequestCallType,
						}
					}),
				ApplyFunc((*FunctionAPI).describeFunctionInternal,
					func(_ *FunctionAPI, w http.ResponseWriter, requestInfo *config.RequestInfo) {
						return
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.m.scaleUpFunction(tt.args.c)
			resp := tt.args.c.Writer.Status()
			if resp != tt.statusCode {
				t.Errorf("resp %v", tt.args.c.Writer.Status())
			}
			patches.ResetAll()
		})
	}
}

func TestDescribeFunction(t *testing.T) {
	convey.Convey("Test Describe Function", t, func() {
		patches := [...]*Patches{
			ApplyFunc(utils.GetMetaName, func(c *gin.Context) (string, error) {
				return "test", nil
			}),
			ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx.Request = httptest.NewRequest("POST", "/worker", nil)
		m := &FunctionAPI{}
		m.describeFunction(ctx)
		resp := ctx.Writer.Status()
		convey.So(resp, convey.ShouldNotBeNil)
	})

	jsonBytes, _ := json.Marshal(&config.RequestInfo{})
	type args struct {
		c *gin.Context
	}
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("POST", "/worker", bytes.NewReader(jsonBytes))
	ctx2, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx2.Request = httptest.NewRequest("POST", "/worker", bytes.NewReader(jsonBytes))

	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 requestinfo nil", &FunctionAPI{}, args{
			c: ctx,
		}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(utils.GetMetaName, func(c *gin.Context) (string, error) {
					return "test", nil
				}),
				ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					return errors.New("test")
				}),
			})
			return patches
		}},

		{"case2 calltype active", &FunctionAPI{}, args{
			c: ctx2,
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(utils.GetMetaName, func(c *gin.Context) (string, error) {
					c.Request.Header.Add(constants.HeaderCallType, utils.RequestCallType)
					return "test", nil
				}),
				ApplyFunc((*FunctionAPI).describeFunctionInternal,
					func(_ *FunctionAPI, w http.ResponseWriter, requestInfo *config.RequestInfo) {
						return
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.m.describeFunction(tt.args.c)
			resp := tt.args.c.Writer.Status()
			if resp != tt.statusCode {
				t.Errorf("resp %v", tt.args.c.Writer.Status())
			}
			patches.ResetAll()
		})
	}
}

func TestRegisterAPI(t *testing.T) {
	convey.Convey("Test Register API", t, func() {
		m := &FunctionAPI{
			Path: "123",
		}
		router := gin.New()
		m.RegisterAPI(router)
	})
}

func TestNewFaaSFunctionAPI(t *testing.T) {
	convey.Convey("Test New FaaS FunctionAPI", t, func() {
		patches := [...]*Patches{
			ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
				return &podfetcher.PodFetcher{}
			}),
			ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
				return &vpcmanager.VpcHandler{}, nil
			}),
			ApplyFunc(utils.WatcherUserAgency, func(stopCh <-chan struct{}) error {
				return nil
			}),
			ApplyFunc(clusterstatusutil.InitClusterStatusCache, func() {
				return
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
				func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		apiVersionPath := "test"
		options := &apiServerOptions.APIServerOptions{}
		stopCh := make(chan struct{})
		FaaSFunctionApi, _ := NewFaaSFunctionAPI(apiVersionPath, options, stopCh)
		convey.So(FaaSFunctionApi, convey.ShouldNotBeNil)
	})

	convey.Convey("Test New FaaS FunctionAPI, GetVPCController error", t, func() {
		patches := [...]*Patches{
			ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
				return &podfetcher.PodFetcher{}
			}),
			ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
				return &vpcmanager.VpcHandler{}, errors.New("test")
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
				func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
			ApplyFunc(clusterstatusutil.InitClusterStatusCache, func() {
				return
			}),
			ApplyFunc(utils.WatcherUserAgency, func(stopCh <-chan struct{}) error {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		apiVersionPath := "test"
		options := &apiServerOptions.APIServerOptions{}
		stopCh := make(chan struct{})
		FaaSFunctionApi, _ := NewFaaSFunctionAPI(apiVersionPath, options, stopCh)
		convey.So(FaaSFunctionApi, convey.ShouldBeNil)
	})
}

func TestExecutePod(t *testing.T) {
	convey.Convey("Test Execute Pod", t, func() {
		w := httptest.NewRecorder()
		r := &config.RequestInfo{}
		functionService := &faasfunction.FunctionService{}
		m := &FunctionAPI{
			kubeClient: &kubernetes.Clientset{},
			podFetcher: &podfetcher.PodFetcher{},
		}
		RequestInfo, pod, _ := m.executePod(r, functionService, w)
		convey.So(RequestInfo, convey.ShouldBeNil)
		convey.So(pod, convey.ShouldBeNil)
	})

	convey.Convey("Test GetContext Not ok", t, func() {
		w := httptest.NewRecorder()
		r := &config.RequestInfo{}
		functionService := &faasfunction.FunctionService{}
		m := &FunctionAPI{
			kubeClient: &kubernetes.Clientset{},
			podFetcher: &podfetcher.PodFetcher{},
		}
		RequestInfo, pod, _ := m.executePod(r, functionService, w)
		convey.So(RequestInfo, convey.ShouldBeNil)
		convey.So(pod, convey.ShouldBeNil)
		convey.So(w.Body.String(), convey.ShouldEqual,
			"{\"code\":150424,\"message\":\"function metadata not found\"}")
	})

	type args struct {
		requestInfo     *config.RequestInfo
		functionService *faasfunction.FunctionService
		w               http.ResponseWriter
	}
	w := httptest.NewRecorder()
	r := &config.RequestInfo{}
	functionService := &faasfunction.FunctionService{}
	m := &FunctionAPI{
		kubeClient: &kubernetes.Clientset{},
		podFetcher: &podfetcher.PodFetcher{},
	}
	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        *config.RequestInfo
		want1       *commontypes.Instance
		wantErr     bool
	}{
		{"case1 handle vpc function, no permissions", m, args{
			requestInfo:     r,
			functionService: functionService,
			w:               w,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}, nil, nil,
			true},

		{"case2 handle vpc function, no available vpc pat instance", m, args{
			requestInfo:     r,
			functionService: functionService,
			w:               w,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}, nil, nil,
			true},

		{"case3 WithContext error", m, args{
			requestInfo:     &config.RequestInfo{StateID: "a"},
			functionService: functionService,
			w:               w,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(isDefaultResource,
						func(requestInfo *config.RequestInfo,
							functionService *faasfunction.FunctionService) bool {
							return false
						}),
				})
				return patches
			}, nil, nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, got1, err := m.executePod(tt.args.requestInfo, tt.args.functionService, tt.args.w)
			if (err != nil) != tt.wantErr {
				t.Errorf("executePod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("executePod() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("executePod() got1 = %v, want %v", got1, tt.want1)
			}
			time.Sleep(100 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestSuccessResponse(t *testing.T) {
	convey.Convey("Test Success Response", t, func() {
		w := httptest.NewRecorder()
		r := &config.RequestInfo{}
		functionService := &faasfunction.FunctionService{}
		pod := &corev1.Pod{}
		m := &FunctionAPI{
			kubeClient: &kubernetes.Clientset{},
			podFetcher: &podfetcher.PodFetcher{},
		}
		m.successResponse(w, r, functionService, utils.BuildInstanceByPod(pod, r, ""))
	})
}

func TestFunctionAPI_reportMetrics(t *testing.T) {
	m := &FunctionAPI{
		kubeClient: &kubernetes.Clientset{},
		podFetcher: &podfetcher.PodFetcher{},
	}

	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		requestBody interface{}
		statusCode  int
	}{
		{"case1 report work metrics successfully",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(podfetcher.AddAutoScalerMetrics,
						func(ctx context.Context, view workermanager.WorkerView) {
							return
						}),
				})
				return patches
			},
			workermanager.View{
				Type: workermanager.WorkerMetric,
				Object: []workermanager.WorkerView{
					{InstanceID: "11111"},
				},
			},
			http.StatusOK},
		{"case2 report pending request metrics successfully",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(podfetcher.AddPendingRequestMetrics,
						func(view workermanager.PendingRequestView) {
							return
						}),
				})
				return patches
			},
			workermanager.View{
				Type: workermanager.PendingRequestMetric,
				Object: []workermanager.PendingRequestView{
					{FuncName: "aaaa"},
				},
			},
			http.StatusOK},
		{"case3 read requestBody failed",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return nil, fmt.Errorf("read body failed")
						}),
				})
				return patches
			},
			"nil",
			http.StatusInternalServerError},

		{"case4 Unmarshal failed",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			},
			"nil",
			http.StatusInternalServerError},

		{"case5 report disk usage request metrics successfully",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			workermanager.View{
				Type: workermanager.DiskUsageRequestMetric,
				Object: workermanager.MirroredDiskSpaceView{
					NodeIP:        "127.0.0.1",
					FreeDiskSpace: "10G",
				},
			},
			http.StatusOK},

		{"case6 report disk usage request metrics failed",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			workermanager.View{
				Type:   workermanager.DiskUsageRequestMetric,
				Object: 5,
			},
			http.StatusInternalServerError},
		{
			name: "case7 no view type",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			requestBody: map[string]string{},
			statusCode:  http.StatusInternalServerError,
		},
		{
			name: "case8 invalid view type",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			requestBody: map[string]interface{}{
				workermanager.ViewType: 123,
			},
			statusCode: http.StatusInternalServerError,
		},
		{
			name: "case9 no view obj",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			requestBody: map[string]interface{}{
				workermanager.ViewType: "123",
			},
			statusCode: http.StatusInternalServerError,
		},
		{
			name: "case10 invalid view type",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			requestBody: map[string]interface{}{
				workermanager.ViewType:   "123",
				workermanager.ViewObject: "123",
			},
			statusCode: http.StatusOK,
		},
		{
			name: "invalid worker metrics",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			requestBody: map[string]interface{}{
				workermanager.ViewType:   workermanager.WorkerMetric,
				workermanager.ViewObject: "",
			},
			statusCode: http.StatusInternalServerError,
		},
		{
			name: "invalid pending request metrics",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			requestBody: map[string]interface{}{
				workermanager.ViewType:   workermanager.PendingRequestMetric,
				workermanager.ViewObject: "",
			},
			statusCode: http.StatusInternalServerError,
		},
		{
			name: "invalid disk usage metrics",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			requestBody: map[string]interface{}{
				workermanager.ViewType:   workermanager.DiskUsageRequestMetric,
				workermanager.ViewObject: "",
			},
			statusCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			jsonBytes, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest("POST", "/worker/metrics", bytes.NewReader(jsonBytes))
			w := httptest.NewRecorder()
			m.reportMetrics(w, req)
			resp := w.Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("Response code is %v, response body is %v", resp.StatusCode, resp)
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionAPI_scaleLimitResponse(t *testing.T) {
	w := httptest.NewRecorder()
	r := &config.RequestInfo{}
	functionService := &faasfunction.FunctionService{}
	m := &FunctionAPI{
		kubeClient: &kubernetes.Clientset{},
		podFetcher: &podfetcher.PodFetcher{},
	}

	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		statusCode  int
	}{
		{"case1 succeed to scaleLimitResponse",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			m.scaleLimitResponse(r, w, functionService, "", 150429)
			resp := w.Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("Response code is %v, response body is %v", resp.StatusCode, resp)
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionAPI_describeFunctionInternal(t *testing.T) {
	funcSvnID := utils.CrName("business", "tenantID", "functionName",
		"functionVersion")
	willdeleteworker.GetCache().AddWillDeleteWorkerInstances(funcSvnID, []v1alpha1.WorkerInstance{
		{InstanceID: "11111111111111"},
		{InstanceID: "22222222222222"},
	})

	type args struct {
		w           http.ResponseWriter
		requestInfo *config.RequestInfo
	}
	var callUpdateFuncCache bool
	tests := []struct {
		name      string
		m         *FunctionAPI
		args      args
		patchFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to get function meta from cache", &FunctionAPI{}, args{
			w: httptest.NewRecorder(),
			requestInfo: &config.RequestInfo{
				BusinessID:      "business",
				TenantID:        "tenantID",
				FunctionName:    "functionName",
				FunctionVersion: "functionVersion",
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(refreshSilentFunc, func(requestInfo *config.RequestInfo, funcSvcID string) error {
					return errors.New("test err")
				}),
				gomonkey.ApplyFunc(cache.UpdateFuncCache, func(funcName string, businessId string,
					tenantId string, version string) {
					callUpdateFuncCache = true
				}),
				gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext,
					func(_ *podfetcher.PodFetcher, _ string) context.Context {
						return context.TODO()
					}),
			})
			return patches
		}},
	}

	for _, tt := range tests {
		patches := tt.patchFunc()
		t.Run(tt.name, func(t *testing.T) {
			tt.m.describeFunctionInternal(tt.args.w, tt.args.requestInfo)
			_, ok := tt.args.w.Header()[constants.HeaderStartDuration]
			if !ok {
				t.Errorf("ResponseWriter is %v", tt.args.w)
			}
		})
		if tt.name == "case1 failed to get function meta from cache" {
			assert.True(t, callUpdateFuncCache)
		}
		patches.ResetAll()
	}
}

func TestFunctionAPI_refreshFuncMetaData(t *testing.T) {
	type args struct {
		requestInfo *config.RequestInfo
		service     *faasfunction.FunctionService
	}

	tests := []struct {
		name      string
		m         *FunctionAPI
		args      args
		patchFunc mockUtils.PatchesFunc
	}{
		{"case1 refresh function meta data when update or rollback", &FunctionAPI{}, args{

			requestInfo: &config.RequestInfo{
				BusinessID:      "business",
				TenantID:        "tenantID",
				FunctionName:    "functionName",
				FunctionVersion: "functionVersion",
				TraceID:         "traceID",
			},
			service: &faasfunction.FunctionService{
				Version: "Version",
				Spec: faasfunction.FunctionServiceSpec{
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						BucketID:       "BucketID",
						ObjectID:       "ObjectID",
						SnapshotSha256: "SnapshotSha256",
						SnapshotSize:   0,
						SnapshotVersions: []commontypes.SnapshotVersion{
							{
								ObjectID:         "UpdateObjectID",
								SnapshotSha256:   "UpdateSnapshotSha256",
								SnapshotSize:     1,
								SnapshotImageTag: "TAG",
							},
							{
								ObjectID:         "ObjectID",
								SnapshotSha256:   "UpdateSnapshotSha256",
								SnapshotSize:     1,
								SnapshotImageTag: "OLD_TAG",
							},
						},
					},
				},
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "RUNTIME_MANAGER_TAG"
				}),
			})
			return patches
		}},
		{"case2 do not refresh function meta data when normal create snapshot.", &FunctionAPI{}, args{

			requestInfo: &config.RequestInfo{
				BusinessID:      "business",
				TenantID:        "tenantID",
				FunctionName:    "functionName",
				FunctionVersion: "functionVersion",
				TraceID:         "traceID",
			},
			service: &faasfunction.FunctionService{
				Version: "Version",
				Spec: faasfunction.FunctionServiceSpec{
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						BucketID:       "BucketID",
						ObjectID:       "ObjectID",
						SnapshotSha256: "SnapshotSha256",
						SnapshotSize:   0,
					},
				},
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "RUNTIME_MANAGER_TAG"
				}),
			})
			return patches
		}},
		{"case3 do not refresh function meta data when checkpoint failed.", &FunctionAPI{}, args{

			requestInfo: &config.RequestInfo{
				BusinessID:      "business",
				TenantID:        "tenantID",
				FunctionName:    "functionName",
				FunctionVersion: "functionVersion",
				TraceID:         "traceID",
			},
			service: &faasfunction.FunctionService{
				Version: "Version",
				Spec: faasfunction.FunctionServiceSpec{
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						BucketID:       "BucketID",
						ObjectID:       "ObjectID",
						SnapshotSha256: "SnapshotSha256",
						SnapshotSize:   0,
						SnapshotVersions: []commontypes.SnapshotVersion{
							{
								ObjectID:         "FailObjectID",
								SnapshotSha256:   "UpdateSnapshotSha256",
								SnapshotSize:     1,
								SnapshotImageTag: "TAG",
								ErrCode:          utils.StatusInternalServerError,
								ErrMessage:       "CheckPoint Fail",
							},
							{
								ObjectID:         "ObjectID",
								SnapshotSha256:   "UpdateSnapshotSha256",
								SnapshotSize:     1,
								SnapshotImageTag: "OLD_TAG",
							},
						},
					},
				},
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "RUNTIME_MANAGER_TAG"
				}),
			})
			return patches
		}},
	}

	for _, tt := range tests {
		patches := tt.patchFunc()
		t.Run(tt.name, func(t *testing.T) {
			tt.m.refreshFuncMetaData(tt.args.requestInfo, tt.args.service)
			var objectID = tt.args.service.Spec.SnapshotDeployment.ObjectID
			if len(tt.args.service.Spec.SnapshotDeployment.SnapshotVersions) != 0 {
				var errCode = tt.args.service.Spec.SnapshotDeployment.SnapshotVersions[0].ErrCode
				if errCode == 0 && objectID != "UpdateObjectID" {
					t.Errorf("when update or rollback, refreshFuncMetaData failed, ObjectID is %s", objectID)
				}
				if errCode == utils.StatusInternalServerError && objectID != "ObjectID" {
					t.Errorf("when update or rollback, refreshFuncMetaData failed, ObjectID is %s", objectID)
				}
			} else {
				if objectID != "ObjectID" {
					t.Errorf("when normal create snapshot, refreshFuncMetaData error, ObjectID is %s", objectID)
				}

			}
		})
		patches.ResetAll()
	}
}

// Test_getSilentFunc -
func Test_getSilentFunc(t *testing.T) {
	convey.Convey("test: refresh silent function", t, func() {
		testKey1 := fmt.Sprintf(constants.SilentFuncKey, "testerr", "testerr", "testerr")
		testKey2 := fmt.Sprintf(constants.SilentFuncKey, "testnotfound", "testnotfound", "testnotfound")
		testKey3 := fmt.Sprintf(constants.SilentFuncKey, "testvalueinvalid", "testvalueinvalid", "testvalueinvalid")
		etcdevents.ProcessSilentEvent = func(event *etcd3.Event) {}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				if key == testKey1 {
					return nil, snerror.New(utils.StatusInternalServerError, "failed to query from etcd")
				}
				if key == testKey2 {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{},
					}, nil
				}
				if key == testKey3 {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Value:       []byte("invalid"),
								ModRevision: 0,
							},
						},
					}, nil
				}
				return &clientv3.GetResponse{
					Kvs: []*mvccpb.KeyValue{
						{
							Value:       []byte("test"),
							ModRevision: 0,
						},
					},
				}, nil
			}),
			gomonkey.ApplyFunc(utils.CheckMetaInfoValid, func(value []byte) error {
				if strings.Contains(string(value), "invalid") {
					return errors.New("function meta is invalid")
				}
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
			etcdevents.ProcessSilentEvent = nil
		}()
		err := refreshSilentFunc(&config.RequestInfo{
			TenantID:        "testerr",
			FunctionName:    "testerr",
			FunctionVersion: "testerr",
		}, "test1").(snerror.SNError)
		convey.So(err.Code(), convey.ShouldEqual, utils.StatusQueryETCDError)
		err = refreshSilentFunc(&config.RequestInfo{
			TenantID:        "testnotfound",
			FunctionName:    "testnotfound",
			FunctionVersion: "testnotfound",
		}, "test2").(snerror.SNError)
		convey.So(err.Code(), convey.ShouldEqual, snerror.FuncMetaNotFound)
		err = refreshSilentFunc(&config.RequestInfo{
			TenantID:        "testSilent",
			FunctionName:    "testSilent",
			FunctionVersion: "testSilent",
		}, "test3").(snerror.SNError)
		convey.So(err.Code(), convey.ShouldEqual, snerror.RefreshSilentFunc)
		err = refreshSilentFunc(&config.RequestInfo{
			TenantID:        "testvalueinvalid",
			FunctionName:    "testvalueinvalid",
			FunctionVersion: "testvalueinvalid",
		}, "test4").(snerror.SNError)
		convey.So(err.Code(), convey.ShouldEqual, snerror.FuncMetaNotFound)
	})
}

func TestFunctionAPI_dealErrorWithType(t *testing.T) {
	type args struct {
		err             error
		requestInfo     *config.RequestInfo
		functionService *faasfunction.FunctionService
		w               http.ResponseWriter
	}
	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 reach max instance num error", &FunctionAPI{}, args{
			err:             utils.ErrReachMaxInstanceNum,
			requestInfo:     &config.RequestInfo{},
			functionService: &faasfunction.FunctionService{},
			w:               httptest.NewRecorder(),
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},

		{"case2 the VIP node is overload and unavailable now", &FunctionAPI{}, args{
			err:             utils.ErrVIPOverload,
			requestInfo:     &config.RequestInfo{},
			functionService: &faasfunction.FunctionService{},
			w:               httptest.NewRecorder(),
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
		{"case3 reach max on-demand instance num per tenant error", &FunctionAPI{}, args{
			err:             utils.ErrReachMaxOnDemandInstanceNumPerTenant,
			requestInfo:     &config.RequestInfo{},
			functionService: &faasfunction.FunctionService{},
			w:               httptest.NewRecorder(),
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
		{"case4 cancel generalize pod error", &FunctionAPI{}, args{
			err:             utils.ErrCancelGeneralizePod,
			requestInfo:     &config.RequestInfo{},
			functionService: &faasfunction.FunctionService{},
			w:               httptest.NewRecorder(),
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
		{"case5 ErrNoNodesCanScheduler", &FunctionAPI{}, args{
			err:             utils.ErrNoNodesCanScheduler,
			requestInfo:     &config.RequestInfo{},
			functionService: &faasfunction.FunctionService{},
			w:               httptest.NewRecorder(),
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := tt.m.dealErrorWithType(tt.args.err, tt.args.requestInfo, tt.args.functionService, tt.args.w); (err != nil) != tt.wantErr {
				t.Errorf("dealErrorWithType() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionAPI_parseHTTPRequest(t *testing.T) {
	jsonBytes, _ := json.Marshal("abc")
	type args struct {
		c *gin.Context
	}
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = &http.Request{}
	ctx1, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx1.Request = httptest.NewRequest("POST", "/worker", bytes.NewReader(jsonBytes))

	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		want        *config.RequestInfo
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 GetMetaName error", &FunctionAPI{interfaceLog: &interfacelog.InterfaceLog{}}, args{
			c: ctx,
		}, nil, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(utils.GetMetaName,
					func(c *gin.Context) (string, error) {
						return "", utils.ErrFunctionNameNotFound
					}),
			})
			return patches
		}},

		{"case2 read r.Body error", &FunctionAPI{interfaceLog: &interfacelog.InterfaceLog{}}, args{
			c: ctx,
		}, nil, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(utils.GetMetaName,
					func(c *gin.Context) (string, error) {
						return "test", nil
					}),
				ApplyFunc(ioutil.ReadAll,
					func(r io.Reader) ([]byte, error) {
						return nil, errors.New("test")
					}),
			})
			return patches
		}},

		{"case3 unmarshal error", &FunctionAPI{interfaceLog: &interfacelog.InterfaceLog{}}, args{
			c: ctx1,
		}, nil, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(utils.GetMetaName,
					func(c *gin.Context) (string, error) {
						return "test", nil
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := tt.m.parseHTTPRequest(tt.args.c); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseHTTPRequest() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()

		})
	}
}

func TestFunctionAPI_setMetricsResponse(t *testing.T) {
	type args struct {
		statusCode int
		msg        string
		w          http.ResponseWriter
	}
	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 json Marshal error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			msg:        "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test")
				}),
			})
			return patches
		}},

		{"case2 write error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			msg:        "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.m.setMetricsResponse(tt.args.statusCode, tt.args.msg, tt.args.w)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionAPI_setDeleteResponse(t *testing.T) {
	type args struct {
		statusCode int
		reserved   bool
		message    string
		w          http.ResponseWriter
	}
	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 Marshal error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			reserved:   false,
			message:    "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test")
				}),
			})
			return patches
		}},

		{"case2 Marshal error ,Write error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			reserved:   false,
			message:    "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test")
				}),
				ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},

		{"case3 Marshal true ,Write error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			reserved:   false,
			message:    "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.m.setDeleteResponse(tt.args.statusCode, tt.args.reserved, tt.args.message, tt.args.w)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionAPI_errorResponse(t *testing.T) {
	type args struct {
		r        *config.RequestInfo
		w        http.ResponseWriter
		err      error
		describe string
	}
	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 Marshal error", &FunctionAPI{}, args{
			r:        &config.RequestInfo{},
			w:        httptest.NewRecorder(),
			err:      errors.New("test"),
			describe: "abc",
		}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test")
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.m.errorResponse(tt.args.r, tt.args.w, tt.args.err, tt.args.describe)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionAPI_scaleLimitResponse1(t *testing.T) {
	type args struct {
		r               *config.RequestInfo
		w               http.ResponseWriter
		functionService *faasfunction.FunctionService
		message         string
		innercode       int
	}
	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 Marshal error", &FunctionAPI{}, args{
			r:               &config.RequestInfo{},
			w:               httptest.NewRecorder(),
			functionService: &faasfunction.FunctionService{},
			message:         "test",
			innercode:       150429,
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test")
				}),
			})
			return patches
		}},

		{"case2 Write error", &FunctionAPI{}, args{
			r:               &config.RequestInfo{},
			w:               httptest.NewRecorder(),
			functionService: &faasfunction.FunctionService{},
			message:         "test",
			innercode:       150429,
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.m.scaleLimitResponse(tt.args.r, tt.args.w, tt.args.functionService, tt.args.message, tt.args.innercode)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionAPI_queryTerminatingLongRunByHostIP(t *testing.T) {
	api := &FunctionAPI{}
	r := gin.New()
	r.GET("/:hostIP", api.queryTerminatingLongRunByHostIP)

	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc

		exceptedCode int
	}{
		{
			name: "pool manager not init",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
						return nil
					}),
				})
				return patches
			},
			exceptedCode: http.StatusInternalServerError,
		},
		{
			name: "pool manager is init",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
						return &poolmanager.PoolMgr{}
					}),
					ApplyMethod(reflect.TypeOf(&poolmanager.PoolMgr{}), "QueryTerminatingLongRunByHostIP", func(_ *poolmanager.PoolMgr, _ string) map[string]commontypes.InstanceRouter {
						return nil
					}),
				})
				return patches
			},
			exceptedCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(http.MethodGet, "/127.0.0.1", nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tt.exceptedCode, w.Code)
			patches.ResetAll()
		})
	}
}

func TestGetMultiSnapshotConfig(t *testing.T) {
	m := &FunctionAPI{
		podFetcher: &podfetcher.PodFetcher{
			KubeClient: &kubernetes.Clientset{},
		},
	}
	_, err := m.getMultiSnapshotConfig()
	assert.NotNil(t, err)

	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{
				PoolMgrConfig: config.PoolMgrConfig{
					CheckpointPodConfig: config.CheckpointPodConfig{
						CRIUCheckpointOpts: "test CRIUCheckpointOpts",
					},
				},
			}, nil
		}),
	})
	defer patches.ResetAll()
	_, err = m.getMultiSnapshotConfig()
	assert.Nil(t, err)
}

func TestHandleUpdateAgencyInfoToWorker(t *testing.T) {
	m := &FunctionAPI{
		podFetcher: &podfetcher.PodFetcher{
			KubeClient: &kubernetes.Clientset{},
		},
	}
	m.handleUpdateAgencyInfoToWorker("funcSvcID", "podName", v1alpha1.WorkerInstance{}, "traceID")
}

func TestUpdateSnapshotCriuOpts(t *testing.T) {
	m := &FunctionAPI{
		podFetcher: &podfetcher.PodFetcher{
			KubeClient: &kubernetes.Clientset{},
		},
	}
	service := &faasfunction.FunctionService{}
	m.updateSnapshotCriuOpts(service)
	if service.Spec.SnapshotDeployment.SnapshotCriuOpts != "" {
		t.Errorf("service.Spec.SnapshotDeployment.SnapshotCriuOpts is not empty")
	}

	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{
				PoolMgrConfig: config.PoolMgrConfig{
					CheckpointPodConfig: config.CheckpointPodConfig{
						CRIUCheckpointOpts: "test CRIUCheckpointOpts",
					},
				},
			}, nil
		}),
	})
	defer patches.ResetAll()
	m.updateSnapshotCriuOpts(service)
	if service.Spec.SnapshotDeployment.SnapshotCriuOpts != "test CRIUCheckpointOpts" {
		t.Errorf("service.Spec.SnapshotDeployment.SnapshotCriuOpts is %s", service.Spec.SnapshotDeployment.SnapshotCriuOpts)
	}
}

func TestSetInstancesResponse(t *testing.T) {
	type args struct {
		statusCode int
		instance   commontypes.QueryInstance
		message    string
		w          http.ResponseWriter
	}
	tests := []struct {
		name        string
		m           *FunctionAPI
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 Marshal error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			instance:   commontypes.QueryInstance{},
			message:    "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test")
				}),
			})
			return patches
		}},

		{"case2 Marshal error ,Write error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			instance:   commontypes.QueryInstance{},
			message:    "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test")
				}),
				ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},

		{"case3 Marshal true ,Write error", &FunctionAPI{}, args{
			statusCode: http.StatusOK,
			instance:   commontypes.QueryInstance{},
			message:    "test",
			w:          httptest.NewRecorder(),
		}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.m.setInstancesResponse(tt.args.statusCode, tt.args.instance, tt.args.message, tt.args.w)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func TestQueryInstances(t *testing.T) {
	convey.Convey("Test query Instance", t, func() {
		convey.Convey("test failed", func() {
			m := &FunctionAPI{
				podFetcher: &podfetcher.PodFetcher{
					KubeClient: &kubernetes.Clientset{},
				},
			}
			w := httptest.NewRecorder()

			r, _ := http.NewRequest("GET", "/worker-manager/v1/functions/instances?funcSvcID=not_exist_instance", nil)
			m.queryInstances(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})
		convey.Convey("test success, not bridge function", func() {
			m := &FunctionAPI{
				podFetcher: &podfetcher.PodFetcher{
					KubeClient: &kubernetes.Clientset{},
				},
			}
			w := httptest.NewRecorder()
			urn, fsID := "urn", "mock_reserved_instance"
			fs := &faasfunction.FunctionService{}
			fs.Spec.Configuration.FunctionVersionURN = urn
			cache.WorkerCache.CreateFunctionCacheByFuncMeta(fsID, fs)
			cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name",
				Applier: constants.WorkerManagerApplier})
			defer cache.WorkerCache.DeleteFunctionCache(fsID)
			r, _ := http.NewRequest("GET", "/worker-manager/v1/functions/instances?funcSvcID=mock_reserved_instance", nil)
			m.queryInstances(w, r)

			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			convey.So(w.Body.String(), convey.ShouldContainSubstring, "mock_pod_name")
			convey.So(w.Body.String(), convey.ShouldContainSubstring, "mock_pod_name")
			convey.So(w.Body.String(), convey.ShouldNotContainSubstring, "bridgePodsInfo")
		})
		convey.Convey("test success, bridge function", func() {
			m := &FunctionAPI{
				podFetcher: &podfetcher.PodFetcher{
					KubeClient: &kubernetes.Clientset{},
				},
			}
			w := httptest.NewRecorder()
			urn, fsID := "urn", "mock_reserved_instance"
			fs := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						IsBridgeFunction: true,
					},
				},
			}
			fs.Spec.Configuration.FunctionVersionURN = urn
			cache.WorkerCache.CreateFunctionCacheByFuncMeta(fsID, fs)
			cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name",
				Applier: constants.WorkerManagerApplier, StateID: "state"})
			defer cache.WorkerCache.DeleteFunctionCache(fsID)
			r, _ := http.NewRequest("GET", "/worker-manager/v1/functions/instances?funcSvcID=mock_reserved_instance", nil)
			m.queryInstances(w, r)

			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			convey.So(w.Body.String(), convey.ShouldContainSubstring, "mock_pod_name")
			convey.So(w.Body.String(), convey.ShouldContainSubstring, "bridgePodsInfo")
		})
	})
}

func TestFunctionAPI_executePod(t *testing.T) {
	type fields struct {
		Options         *apiServerOptions.APIServerOptions
		podFetcher      *podfetcher.PodFetcher
		interfaceLog    *interfacelog.InterfaceLog
		VpcHandler      *vpcmanager.VpcHandler
		kubeClient      kubernetes.Interface
		Name            string
		Path            string
		delInsPendingCh map[string]chan struct{}
	}
	type args struct {
		requestInfo     *config.RequestInfo
		functionService *faasfunction.FunctionService
		w               http.ResponseWriter
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		needInit bool
		instance v1alpha1.WorkerInstance
		want     *commontypes.Instance
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name: "test return a reserved instance",
			fields: fields{
				podFetcher: &podfetcher.PodFetcher{},
			},
			args: args{
				requestInfo: &config.RequestInfo{
					FunctionName: "mockFuncName",
					TraceID:      "mockTraceID",
				},
				functionService: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mockFuncSvcID",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							EnableRemoteDebug: true,
						},
					},
				},
				w: httptest.NewRecorder(),
			},
			needInit: true,
			instance: v1alpha1.WorkerInstance{
				PodName: "mockPodName",
				Applier: constants.WorkerManagerApplier,
			},
			want: utils.BuildInstanceByWorkerIns(&v1alpha1.WorkerInstance{
				PodName: "mockPodName",
				Applier: constants.WorkerManagerApplier,
			}),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		},
		{
			name: "test instance is not in fc cache",
			fields: fields{
				podFetcher: &podfetcher.PodFetcher{},
			},
			args: args{
				requestInfo: &config.RequestInfo{
					FunctionName: "mockFuncName",
					TraceID:      "mockTraceID",
				},
				functionService: &faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{
						ID: "mockFuncSvcID",
					},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							EnableRemoteDebug: true,
						},
					},
				},
				w: httptest.NewRecorder(),
			},
			needInit: false,
			want:     nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &FunctionAPI{
				Options:         tt.fields.Options,
				podFetcher:      tt.fields.podFetcher,
				interfaceLog:    tt.fields.interfaceLog,
				VpcHandler:      tt.fields.VpcHandler,
				kubeClient:      tt.fields.kubeClient,
				Name:            tt.fields.Name,
				Path:            tt.fields.Path,
				delInsPendingCh: tt.fields.delInsPendingCh,
			}
			if tt.needInit {
				cache.WorkerCache.StoreWorkerInstance(tt.args.functionService.ID, tt.instance)
			}
			_, got, err := m.executePod(tt.args.requestInfo, tt.args.functionService, tt.args.w)
			if !tt.wantErr(t, err, fmt.Sprintf("executePod(%v, %v, %v)", tt.args.requestInfo, tt.args.functionService, tt.args.w)) {
				return
			}
			assert.Equalf(t, tt.want, got, "executePod(%v, %v, %v)", tt.args.requestInfo, tt.args.functionService, tt.args.w)
		})
	}
}

func Test_isNoSpaceErr(t *testing.T) {
	type args struct {
		err             error
		functionService *faasfunction.FunctionService
	}
	tests := []struct {
		name      string
		args      args
		isNoSpace bool
	}{
		{
			"case 1, is space error",
			args{
				err: snerror.NewSNError(snerror.ClusterOverloadCode,
					fmt.Sprintf("%s:the node is: %s", common.FailedToPullUpImageOfNoSpace, "node1")),
				functionService: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							BusinessType: constants.BusinessTypeCAE,
						},
					},
				},
			},
			true,
		}, {
			"case 2, not space error",
			args{
				err: snerror.NewSNError(snerror.GettingPodErrorCode,
					fmt.Sprintf("%s:the node is: %s", common.FailedToPullUpImageOfNoSpace, "node1")),
				functionService: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							BusinessType: constants.BusinessTypeCAE,
						},
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.isNoSpace != isNoSpaceErr(tt.args.err, tt.args.functionService) {
				t.Errorf("failed to test no space error")
			}
		})
	}
}

func TestFunctionAPI_handlerNoSpaceOfCustomContainers(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{
				RetryTimeWhenNoSpace: 0,
			}, nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&podfetcher.PodFetcher{}), "GeneralizePod",
			func(_ *podfetcher.PodFetcher, ctx context.Context, requestInfo *config.RequestInfo,
				functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
				return &corev1.Pod{}, nil
			}),
	})

	m := FunctionAPI{
		podFetcher: &podfetcher.PodFetcher{},
	}
	m.handlerNoSpaceOfCustomContainers(errors.New("error"), &config.RequestInfo{}, &faasfunction.FunctionService{}, mockUtils.MockResponseWriter{})

	patches.ResetAll()
}

func TestFunctionAPI_revisePodResource(t *testing.T) {
	const vpaPath = "/worker-manager/v1/instanceview/resourceview"
	var (
		patch   = gomonkey.NewPatches()
		reqBody = &config.VPARequest{
			InstanceList: vpa.FuncInsVpaView{VpaInsView: []vpa.FuncInstanceVpaResp{{
				FuncInstanceInfo: vpa.FuncInstanceInfo{
					Action:       "create",
					FuncName:     "test-func",
					BusinessType: "CAE",
					PodName:      "pause-pod-node1",
					ContainerIDs: &vpa.ContainerInfo{},
					Resource:     &vpa.ResourceConfig{},
				},
				VpaResource: vpa.FuncInsVpaResource{
					CPU: vpa.CPU{
						Quota: 100000, // 1cpu
						Share: 512,    // 0.5cpu
					},
					Memory: vpa.Memory{
						Limit: 2 << 27}, // 128Mi
				},
			}}},
			Node: config.Node{},
		}
		pausePod = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pause-pod-node1"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:      "pause-pod",
						Resources: getResourceByCPUAndMem(100, 100, 128),
					},
				},
			},
			Status: corev1.PodStatus{},
		}
	)
	convey.Convey("test revisePodResource", t, func() {
		convey.Convey("failed to read request body", func() {
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest("POST", vpaPath, nil)
			mockIOReadAll(patch, nil, errors.New("read body error"))
			m := &FunctionAPI{}
			m.revisePodResource(ctx)
			resp := ctx.Writer.Status()
			respCode := ctx.Writer.Header().Get(constants.HeaderInnerCode)
			convey.So(resp, convey.ShouldEqual, http.StatusInternalServerError)
			convey.So(respCode, convey.ShouldEqual, strconv.Itoa(snerror.VPAResourceMsgTransferError))
		})

		convey.Convey("failed to json unmarshal", func() {
			reqBody := &config.VPARequest{}
			jsonBytes, _ := json.Marshal(reqBody)
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest("POST", vpaPath, bytes.NewReader(jsonBytes))
			mockJsonUnmarshal(patch, errors.New("json unmarshal error"))
			m := &FunctionAPI{}
			m.revisePodResource(ctx)
			resp := ctx.Writer.Status()
			respCode := ctx.Writer.Header().Get(constants.HeaderInnerCode)
			convey.So(resp, convey.ShouldEqual, http.StatusInternalServerError)
			convey.So(respCode, convey.ShouldEqual, strconv.Itoa(snerror.VPAResourceMsgTransferError))
		})
		convey.Convey("empty VpaInsView", func() {
			reqBody := &config.VPARequest{}
			jsonBytes, _ := json.Marshal(reqBody)
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest("POST", vpaPath, bytes.NewReader(jsonBytes))
			m := &FunctionAPI{}
			mockPodFetcherGetPodByName(patch, pausePod, nil)
			m.revisePodResource(ctx)
			resp := ctx.Writer.Status()
			convey.So(resp, convey.ShouldEqual, http.StatusOK)
		})
		convey.Convey("calculateVPAAlignResourceValue is negative", func() {
			jsonBytes, _ := json.Marshal(reqBody)
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest("POST", vpaPath, bytes.NewReader(jsonBytes))
			patch.ApplyFunc(calculateVPAAlignResourceValue, func(requestInfo *config.VPARequest, traceID string) (*vpaAlignResourceValue, error) {
				return &vpaAlignResourceValue{requestCPU: -1. - 1. - 1}, nil
			})
			m := &FunctionAPI{}
			m.revisePodResource(ctx)
			resp := ctx.Writer.Status()
			respCode := ctx.Writer.Header().Get(constants.HeaderInnerCode)
			convey.So(resp, convey.ShouldEqual, http.StatusInternalServerError)
			convey.So(respCode, convey.ShouldEqual, strconv.Itoa(snerror.VPAResourceMsgTransferError))
		})
		convey.Convey("test update pause pod resource success", func() {
			jsonBytes, _ := json.Marshal(reqBody)
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest("POST", vpaPath, bytes.NewReader(jsonBytes))
			mockPodFetcherGetPodByName(patch, pausePod, nil)
			m := &FunctionAPI{}
			patch.ApplyGlobalVar(&utils.IsSupportUpdateVpaResource, true)
			m.kubeClient = &kubernetes.Clientset{}
			mockGetPod(patch, &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Resources: getResourceByCPUAndMem(100, 100, 128)},
					},
				},
			}, nil)
			mockDeleteVpaPod(patch, nil)
			patch.ApplyFunc(checkIsNeedUpdatePausePodResource, func(v *vpaAlignResourceValue,
				pausePod *corev1.Pod, traceID string) bool {
				return true
			})
			m.revisePodResource(ctx)
			resp := ctx.Writer.Status()
			convey.So(resp, convey.ShouldEqual, http.StatusOK)
		})
		convey.Convey("CAE vpa align limit cpu < request cpu", func() {
			jsonBytes, _ := json.Marshal(reqBody)
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest("POST", vpaPath, bytes.NewReader(jsonBytes))
			patch.ApplyFunc(calculateVPAAlignResourceValue, func(requestInfo *config.VPARequest,
				traceID string) (*vpaAlignResourceValue, error) {
				return &vpaAlignResourceValue{requestCPU: 1500, limitCPU: 500, limitMemory: 1000}, nil
			})
			pausePod.Name = "cae-pause"
			mockPodFetcherGetPodByName(patch, pausePod, nil)
			mockDeleteVpaPod(patch, nil)
			patch.ApplyFunc(checkIsNeedUpdatePausePodResource, func(v *vpaAlignResourceValue, pausePod *corev1.Pod,
				traceID string) bool {
				return true
			})
			m := &FunctionAPI{podFetcher: &podfetcher.PodFetcher{}}
			m.revisePodResource(ctx)
			resp := ctx.Writer.Status()
			convey.So(resp, convey.ShouldEqual, http.StatusOK)
			actual := vpaPod.GetPausePodResourceRequirements(pausePod.Name)
			convey.So(actual.Limits.Cpu().MilliValue(), convey.ShouldEqual, 1500)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func mockIOReadAll(patch *gomonkey.Patches, body []byte, err error) {
	patch.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
		return body, err
	})
}

func mockJsonUnmarshal(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
		return err
	})
}

func mockPodFetcherGetPodByName(patch *gomonkey.Patches, pausePod *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&podfetcher.PodFetcher{}), "GetPodByName",
		func(_ *podfetcher.PodFetcher, podName, namespace string) (*corev1.Pod, error) {
			return pausePod, err
		})
}

func mockDeleteVpaPod(patch *gomonkey.Patches, err error) {
	patch.ApplyMethod(reflect.TypeOf(&vpaPod.VerticalPausePod{}), "DeleteVpaPod",
		func(_ *vpaPod.VerticalPausePod, ctx context.Context, pod *corev1.Pod) error {
			return err
		})
}

func mockGetPod(patch *gomonkey.Patches, pod *corev1.Pod, err error) {
	patch.ApplyFunc(utils.GetPodInformer, func() informercorev1.PodInformer {
		return fakePodInformer{}
	})
	gomonkey.ApplyMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "Get", func(_ *fakePodNamespaceLister,
		podName string) (*corev1.Pod, error) {
		return pod, err
	})
}

var podName = ""

type fakePodLister struct{}

func (s *fakePodLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (s *fakePodLister) Pods(namespace string) listerscorev1.PodNamespaceLister {
	return fakePodNamespaceLister{}
}

type fakePodNamespaceLister struct{}

func (s fakePodNamespaceLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (s fakePodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	if podName == "" {
		return &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}},
			{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}},
		}}}, nil
	} else {
		return &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Terminated: nil}},
			{State: corev1.ContainerState{Terminated: nil}},
		}}}, nil
	}
}

type fakePodInformer struct {
}

func (f fakePodInformer) Informer() k8cache.SharedIndexInformer {
	return nil
}

func (f fakePodInformer) Lister() listerscorev1.PodLister {
	return &fakePodLister{}
}

func mockGetInitVpaPodSpec(patch *gomonkey.Patches, cpu, mem int64) {
	patch.ApplyFunc(config.GetInitVpaPodSpec, func() *config.Resource {
		return &config.Resource{CPU: cpu, Memory: mem}
	})
}

func TestCheckIsNeedUpdatePausePodResource(t *testing.T) {
	pausePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pause-pod-node1"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:      "pause-pod",
					Resources: getResourceByCPUAndMem(100, 100, 128),
				},
			},
		},
		Status: corev1.PodStatus{},
	}
	patch := gomonkey.NewPatches()
	mockGetInitVpaPodSpec(patch, 100, 128)
	t.Run("need to update > threshold(1000)", func(t *testing.T) {
		v := &vpaAlignResourceValue{
			requestCPU:  1001,
			limitCPU:    1001,
			limitMemory: 1001,
		}
		result := checkIsNeedUpdatePausePodResource(v, pausePod, "xxxx")
		assert.True(t, result)
	})
	t.Run("need to update <= threshold(1000)", func(t *testing.T) {
		v := &vpaAlignResourceValue{
			requestCPU:  1000,
			limitCPU:    1000,
			limitMemory: 1000,
		}
		result := checkIsNeedUpdatePausePodResource(v, pausePod, "xxxx")
		assert.False(t, result)
	})
	t.Run("need to update > threshold(-1000)", func(t *testing.T) {
		pausePod.Spec.Containers = []corev1.Container{
			{
				Name:      "pause-pod",
				Resources: getResourceByCPUAndMem(2100, 2100, 2128),
			},
		}
		v := &vpaAlignResourceValue{
			requestCPU:  999,
			limitCPU:    999,
			limitMemory: 999,
		}
		result := checkIsNeedUpdatePausePodResource(v, pausePod, "xxxx")
		assert.True(t, result)
	})
}

func TestFunctionAPI_describeFunctionInternal1(t *testing.T) {
	isMocked := false
	type fields struct {
		Options         *apiServerOptions.APIServerOptions
		podFetcher      *podfetcher.PodFetcher
		interfaceLog    *interfacelog.InterfaceLog
		VpcHandler      *vpcmanager.VpcHandler
		kubeClient      kubernetes.Interface
		Name            string
		Path            string
		delInsPendingCh map[string]chan struct{}
	}
	type args struct {
		w           http.ResponseWriter
		requestInfo *config.RequestInfo
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		isMocked    bool
	}{
		{
			name:   "test with unsupported runtime type python3",
			fields: fields{},
			args: args{
				requestInfo: &config.RequestInfo{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*FunctionAPI).getFunctionService,
						func(_ *FunctionAPI, funcSvcID string, requestInfo *config.RequestInfo) *faasfunction.FunctionService {
							return &faasfunction.FunctionService{
								Spec: faasfunction.FunctionServiceSpec{
									Configuration: faasfunction.ConfigurationSpec{
										Runtime: faasfunction.RuntimeSpec{
											Language: "python3",
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*FunctionAPI).errorResponse,
						func(_ *FunctionAPI, r *config.RequestInfo, w http.ResponseWriter, err error, describe string) {
							isMocked = true
							return
						}),
					gomonkey.ApplyFunc(utils.GetRuntimeManagerCfg, func() *rmconfig.Config {
						return &rmconfig.Config{
							RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
								"python3.6": {},
							},
						}
					}),
				})
				return patches
			},
			isMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMocked = false
			patches := tt.patchesFunc()
			m := &FunctionAPI{
				Options:         tt.fields.Options,
				podFetcher:      tt.fields.podFetcher,
				interfaceLog:    tt.fields.interfaceLog,
				VpcHandler:      tt.fields.VpcHandler,
				kubeClient:      tt.fields.kubeClient,
				Name:            tt.fields.Name,
				Path:            tt.fields.Path,
				delInsPendingCh: tt.fields.delInsPendingCh,
			}
			m.describeFunctionInternal(tt.args.w, tt.args.requestInfo)
			patches.ResetAll()
			if isMocked != tt.isMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
			}
		})
	}
}

func TestCalculateVPAAlignResourceValue(t *testing.T) {
	t.Run("empty VpaInsView", func(t *testing.T) {
		requestInfo := &config.VPARequest{}
		v, err := calculateVPAAlignResourceValue(requestInfo, "xxx")
		assert.NoError(t, err)
		assert.Equal(t, int64(0), v.limitCPU)
		assert.Equal(t, int64(0), v.limitMemory)
		assert.Equal(t, int64(0), v.requestCPU)
	})
}

func TestMarkWorkerInstanceWillDelete(t *testing.T) {
	convey.Convey("TestMarkWorkerInstanceWillDelete ok", t, func() {
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID",
		}
		flag := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				jsonBytes, _ := json.Marshal(workerIns)
				return string(jsonBytes), nil
			}),
			gomonkey.ApplyFunc(etcdhandler.PutInstanceToETCDWithErr,
				func(ctx context.Context, instance v1alpha1.WorkerInstance, operation string) error {
					flag = instance.WillDelete
					return nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		_ = m.markWorkerInstanceWillDelete(MockResponseWriter{}, workerIns)
		convey.So(flag, convey.ShouldBeTrue)
	})
	convey.Convey("TestMarkWorkerInstanceWillDelete QueryETCDWithRetry err", t, func() {
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID",
		}
		msg := ""
		errMsg := "QueryETCDWithRetry error happened"
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				return "", errors.New(errMsg)
			}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(m), "setDeleteResponse",
				func(m *FunctionAPI, statusCode int, reserved bool, message string, w http.ResponseWriter) {
					msg = message
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		_ = m.markWorkerInstanceWillDelete(MockResponseWriter{}, workerIns)
		convey.So(msg, convey.ShouldEqual, errMsg)
	})
	convey.Convey("TestMarkWorkerInstanceWillDelete WorkerInstance unmarshal err", t, func() {
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID",
		}
		msg := ""
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				return "{", nil
			}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(m), "setDeleteResponse",
				func(m *FunctionAPI, statusCode int, reserved bool, message string, w http.ResponseWriter) {
					msg = message
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		_ = m.markWorkerInstanceWillDelete(MockResponseWriter{}, workerIns)
		convey.So(strings.Contains(msg, "failed to unmarshal"), convey.ShouldBeTrue)
	})
	convey.Convey("TestMarkWorkerInstanceWillDelete ok", t, func() {
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		workerIns := v1alpha1.WorkerInstance{
			BusinessID: "businessID",
			TenantID:   "tenantID",
			FuncName:   "funcName",
			Version:    "version",
			InstanceID: "instanceID",
		}
		msg := ""
		errMsg := "QueryETCDWithRetry error happened"
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcdhandler.QueryETCDWithRetry, func(ctx context.Context, path string) (string, error) {
				jsonBytes, _ := json.Marshal(workerIns)
				return string(jsonBytes), nil
			}),
			gomonkey.ApplyFunc(etcdhandler.PutInstanceToETCDWithErr,
				func(ctx context.Context, instance v1alpha1.WorkerInstance, operation string) error {
					return errors.New(errMsg)
				}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(m), "setDeleteResponse",
				func(m *FunctionAPI, statusCode int, reserved bool, message string, w http.ResponseWriter) {
					msg = message
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		_ = m.markWorkerInstanceWillDelete(MockResponseWriter{}, workerIns)
		convey.So(strings.Contains(msg, errMsg), convey.ShouldBeTrue)
	})
}

func TestQueryETCDWithRetry(t *testing.T) {
	convey.Convey("TestQueryETCDWithRetry ok", t, func() {
		etcdClient := &etcd3.EtcdClient{}
		expRes := "res"
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return etcdClient
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetValues",
				func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					return []string{expRes}, nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		res, _ := etcdhandler.QueryETCDWithRetry(context.Background(), "/sn/worker/xxx")
		convey.So(res, convey.ShouldEqual, expRes)
	})
}

func TestRunWillDeleteTask(t *testing.T) {
	convey.Convey("TestRunWillDeleteTask assert nok", t, func() {
		funcApi := &FunctionAPI{}
		ins := v1alpha1.WorkerInstance{
			PodName: "podName",
		}
		svcID := "funcSvcID"
		willDeleteTasks.Store("podName", "xxx")
		runWillDeleteTask(ins, funcApi, svcID, "funcName")
	})
}

func TestCancelWillDeleteTask(t *testing.T) {
	convey.Convey("TestRunWillDeleteTask assert nok", t, func() {
		funcApi := &FunctionAPI{}
		ins := v1alpha1.WorkerInstance{
			PodName: "podName",
		}
		svcID := "funcSvcID"
		runWillDeleteTask(ins, funcApi, svcID, "funcName")
		cancelWillDeleteTask("podName")
	})
}

func TestRunWillDeleteCheckTask(t *testing.T) {
	convey.Convey("TestRunWillDeleteCheckTask closeCh closed", t, func() {
		task := &willDeleteCheckTask{
			closeCh: make(chan interface{}),
		}
		close(task.closeCh)
		task.run()
	})
	convey.Convey("TestRunWillDeleteCheckTask timeout", t, func() {
		timer := time.NewTimer(10 * time.Millisecond)
		pf := &podfetcher.PodFetcher{}
		vpcHandler := &vpcmanager.VpcHandler{}
		task := &willDeleteCheckTask{
			funcAPI: &FunctionAPI{
				podFetcher: pf,
			},
			closeCh: make(chan interface{}),
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return timer
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(pf), "DeletePodByWorkerInstance",
				func(p *podfetcher.PodFetcher, funcSvcID, namespace, traceID string, instance v1alpha1.WorkerInstance) {
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(vpcHandler), "IdleDeletePatServiceList",
				func(v *vpcmanager.VpcHandler, instanceName string) error {
					return nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		task.run()
	})
}

func Test_getFuncMetaInfo(t *testing.T) {
	convey.Convey("TestGetFuncMetaInfo from etcd", t, func() {
		router := gin.New()
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		router.POST("/funcmeta", func(c *gin.Context) {
			m.getFuncMetaInfo(c)
		})

		reqBytes, _ := json.Marshal(&workermanager.FuncMetaBaseInfo{
			FuncName:   "gotest",
			TenantID:   "580943",
			BusinessID: "yrk",
			Version:    "latest",
			ServiceID:  "0@base",
		})
		req, _ := http.NewRequest(http.MethodPost, "/funcmeta", strings.NewReader(string(reqBytes)))
		w := httptest.NewRecorder()
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService,
				commontypes.ProfileMetaData, etcd3.EtcdType, error) {
				return &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							BusinessType: "CAE",
						},
					},
				}, commontypes.ProfileMetaData{}, etcd3.Meta, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		router.ServeHTTP(w, req)
		respBody, _ := ioutil.ReadAll(w.Result().Body)
		funcMetaInfoResp := &workermanager.FuncMetaBaseInfoResp{}
		_ = json.Unmarshal(respBody, funcMetaInfoResp)
		convey.So(funcMetaInfoResp.FuncMetaInfo.BusinessType == "CAE", convey.ShouldBeTrue)
	})
	convey.Convey("TestGetFuncMetaInfo from local cache", t, func() {
		router := gin.New()
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		router.POST("/funcmeta", func(c *gin.Context) {
			m.getFuncMetaInfo(c)
		})

		reqBytes, _ := json.Marshal(&workermanager.FuncMetaBaseInfo{
			FuncName:   "gotest",
			TenantID:   "580943",
			BusinessID: "yrk",
			Version:    "latest",
			ServiceID:  "0@base",
		})
		req, _ := http.NewRequest(http.MethodPost, "/funcmeta", strings.NewReader(string(reqBytes)))
		w := httptest.NewRecorder()
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService,
				commontypes.ProfileMetaData, etcd3.EtcdType, error) {
				return &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							BusinessType: "CAE",
						},
					},
				}, commontypes.ProfileMetaData{}, etcd3.Meta, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		router.ServeHTTP(w, req)
		respBody, _ := ioutil.ReadAll(w.Result().Body)
		funcMetaInfoResp := &workermanager.FuncMetaBaseInfoResp{}
		_ = json.Unmarshal(respBody, funcMetaInfoResp)
		convey.So(funcMetaInfoResp.FuncMetaInfo.BusinessType == "CAE", convey.ShouldBeTrue)
	})
	convey.Convey("TestGetFuncMetaInfo from etcd with FuncMetaNotFound", t, func() {
		router := gin.New()
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		router.POST("/funcmeta", func(c *gin.Context) {
			m.getFuncMetaInfo(c)
		})
		reqBytes, _ := json.Marshal(&workermanager.FuncMetaBaseInfo{
			FuncName:   "gotest1",
			TenantID:   "580943",
			BusinessID: "yrk",
			Version:    "latest",
			ServiceID:  "0@base",
		})
		req, _ := http.NewRequest(http.MethodPost, "/funcmeta", strings.NewReader(string(reqBytes)))
		w := httptest.NewRecorder()
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService,
				commontypes.ProfileMetaData, etcd3.EtcdType, error) {
				return nil, commontypes.ProfileMetaData{}, etcd3.Meta,
					snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		router.ServeHTTP(w, req)
		respBody, _ := ioutil.ReadAll(w.Result().Body)
		funcMetaInfoResp := &workermanager.FuncMetaBaseInfoResp{}
		_ = json.Unmarshal(respBody, funcMetaInfoResp)
		convey.So(strings.Contains(funcMetaInfoResp.ErrMsg, snerror.ErrText(snerror.FuncMetaNotFound)), convey.ShouldBeTrue)
	})
	convey.Convey("TestGetFuncMetaInfo from etcd with other error", t, func() {
		router := gin.New()
		m := &FunctionAPI{
			podFetcher: &podfetcher.PodFetcher{
				KubeClient: &kubernetes.Clientset{},
			},
		}
		router.POST("/funcmeta", func(c *gin.Context) {
			m.getFuncMetaInfo(c)
		})
		reqBytes, _ := json.Marshal(&workermanager.FuncMetaBaseInfo{
			FuncName:   "gotest1",
			TenantID:   "580943",
			BusinessID: "yrk",
			Version:    "latest",
			ServiceID:  "0@base",
		})
		req, _ := http.NewRequest(http.MethodPost, "/funcmeta", strings.NewReader(string(reqBytes)))
		w := httptest.NewRecorder()
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetMetaInfoFromEtcd, func(functionKey string) (*faasfunction.FunctionService,
				commontypes.ProfileMetaData, etcd3.EtcdType, error) {
				return nil, commontypes.ProfileMetaData{}, etcd3.Meta,
					snerror.New(snerror.ClusterOverloadCode, snerror.ErrText(snerror.ClusterOverloadCode))
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		router.ServeHTTP(w, req)
		respBody, _ := ioutil.ReadAll(w.Result().Body)
		funcMetaInfoResp := &workermanager.FuncMetaBaseInfoResp{}
		_ = json.Unmarshal(respBody, funcMetaInfoResp)
		convey.So(!strings.Contains(funcMetaInfoResp.ErrMsg, snerror.ErrText(snerror.FuncMetaNotFound)), convey.ShouldBeTrue)
	})
}
