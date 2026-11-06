/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package poolmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fhttp "github.com/valyala/fasthttp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"
	"k8s.io/client-go/rest"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/poderrormessage"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	tools "functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func TestImageVerifyFlight(t *testing.T) {
	flight := newImageVerifyFlight()
	e := errors.New("my error")
	err := flight.do("abc", func() error {
		return e
	})
	require.Equal(t, e, err)
	err = flight.do("abc", func() error {
		return errors.New("another error")
	})
	require.Equal(t, e, err)

	e2 := errors.New("def error")
	err = flight.do("def", func() error {
		return e2
	})
	require.Equal(t, e2, err)
}

func Test_swrHandler_doRequest(t *testing.T) {
	type fields struct {
		spec            *faasfunction.FunctionServiceSpec
		errRespHandlers utils.SWRErrRespHandlers
		traceID         string
	}
	type args struct {
		ctx    context.Context
		method string
		path   string
		extra  utils.SWRErrRespHandlers
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		// test Decrypt error
		{
			"case1",
			fields{
				spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{
							XRole: "test",
						},
					},
				},
			},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), errors.New("decrypt error")
					}),
				})
				return patches
			},
			true,
		},
		// test http error
		{
			"case2",
			fields{
				spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{
							XRole: "test",
						},
					},
				},
			},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{}, errors.New("error")
						}),
				})
				return patches
			},
			true,
		},
		// test http success
		{
			"case3",
			fields{
				spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{
							XRole: "test",
						},
					},
				},
			},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{
								Header: http.Header{},
							}, nil
						}),
				})
				return patches
			},
			true,
		},
		// test http success
		{
			"case4",
			fields{
				spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{
							XRole: "test",
						},
					},
				},
			},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{
								Header: http.Header{},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do",
						func(_ *http.Client, req *http.Request) (*http.Response, error) {
							return &http.Response{StatusCode: http.StatusOK,
								Body: ioutil.NopCloser(bytes.NewReader([]byte("abc")))}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), errors.New("read all error")
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case5",
			fields{
				spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{
							XRole: "test",
						},
					},
				},
			},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext,
						func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
							return &http.Request{
								Header: http.Header{},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do",
						func(_ *http.Client, req *http.Request) (*http.Response, error) {
							return &http.Response{
								Body: mockUtils.ReadCloserImp{},
							}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), nil
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &swrHandler{
				spec:            tt.fields.spec,
				errRespHandlers: tt.fields.errRespHandlers,
				traceID:         tt.fields.traceID,
			}
			patches := tt.patchesFunc()
			_, err := s.doRequest(tt.args.ctx, tt.args.method, tt.args.path, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("doRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_customContainerMGR_waitWorkerReady(t *testing.T) {
	type fields struct {
		workerClient    *fhttp.Client
		flight          *imageVerifyFlight
		nameSpace       string
		kubeClient      kubernetes.Interface
		config          config.PoolMgrConfig
		onDemandWaitMap sync.Map
	}
	type args struct {
		ctx context.Context
		pod *corev1.Pod
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1",
			fields{
				workerClient: &fhttp.Client{},
			},
			args{
				ctx: context.Background(),
				pod: &corev1.Pod{
					Status: corev1.PodStatus{
						PodIP: "127.0.0.1",
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return nil
						}),
					gomonkey.ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						return &fhttp.Response{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fhttp.Response{}), "StatusCode",
						func(_ *fhttp.Response) int {
							return fhttp.StatusOK
						}),
				})
				return patches
			},
			false,
		},
		{
			"case2 timeout",
			fields{
				workerClient: &fhttp.Client{},
				kubeClient:   mockUtils.K8sFakeClient,
			},
			args{
				ctx: context.Background(),
				pod: &corev1.Pod{
					Status: corev1.PodStatus{
						PodIP: "127.0.0.1",
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return errors.New("timeout")
						}),
					gomonkey.ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						return &fhttp.Response{}
					}),
					gomonkey.ApplyFunc(context.WithTimeout, func(ctx context.Context, t time.Duration) (context.Context, context.CancelFunc) {
						c, cancel := context.WithCancel(ctx)
						go func() {
							<-time.After(100 * time.Millisecond)
							cancel()
						}()
						return c, cancel
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(utils.RetryPodOperationList), "AddPodOperation", func(_ *utils.PodOperationRetryList, _ *utils.PodOperationEle) {}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &customContainerMGR{
				workerClient:    tt.fields.workerClient,
				flight:          tt.fields.flight,
				nameSpace:       tt.fields.nameSpace,
				kubeClient:      tt.fields.kubeClient,
				config:          tt.fields.config,
				onDemandWaitMap: tt.fields.onDemandWaitMap,
			}
			patches := tt.patchesFunc()
			if err := c.waitWorkerReady(tt.args.ctx, tt.args.pod); (err != nil) != tt.wantErr {
				t.Errorf("waitWorkerReady() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_customContainerMGR_forceDeleteTerminatedPod(t *testing.T) {
	type fields struct {
		kubeClient kubernetes.Interface
	}
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        bool
	}{
		{
			name: "case_01 DeletionTimestamp is nil",
			fields: fields{
				kubeClient: &kubernetes.Clientset{},
			},
			args: args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: nil}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			want: false,
		}, {
			name: "case_02 wait for worker to finish",
			fields: fields{
				kubeClient: &kubernetes.Clientset{},
			},
			args: args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{}},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: tools.WorkerContainerName, Ready: true}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			want: false,
		}, {
			name: "case_03 normal scenario",
			fields: fields{
				kubeClient: &kubernetes.Clientset{},
			},
			args: args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{}},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: tools.WorkerContainerName, Ready: false}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
						func(_ *fake.FakePods, ctx context.Context,
							name string, opts metav1.DeleteOptions) error {
							return nil
						}),
				})
				return patches
			},
			want: true,
		}, {
			name: "case_04 normal scenario, but failed to force delete pod",
			fields: fields{
				kubeClient: &kubernetes.Clientset{},
			},
			args: args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{}},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: tools.WorkerContainerName, Ready: false}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
						func(_ *fake.FakePods, ctx context.Context,
							name string, opts metav1.DeleteOptions) error {
							return fmt.Errorf("delete timeout")
						}),
				})
				return patches
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerMGR{
				kubeClient: tt.fields.kubeClient,
			}
			if got := c.deleteTerminatedPod(tt.args.pod); got != tt.want {
				t.Errorf("deleteTerminatedPod() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_parseImageName(t *testing.T) {
	type args struct {
		image string
	}
	tests := []struct {
		name  string
		args  args
		want  imageName
		want1 bool
	}{
		{
			name:  "case_01 image is short",
			args:  args{image: "dockerhub.io/project"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_02 image with no tag",
			args:  args{image: "dockerhub.io/project/"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_03 image with no namespace",
			args:  args{image: "dockerhub.io//image:1.0"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_04 image with tags",
			args:  args{image: "dockerhub.io/project/image:a:1.0"},
			want:  imageName{},
			want1: false,
		}, {
			name: "case_05 image tag is latest",
			args: args{image: "dockerhub.io/project/image"},
			want: imageName{
				nameSpace:  "project",
				repository: "image",
				tag:        "latest",
			},
			want1: true,
		}, {
			name: "case_06 image repository contains slash",
			args: args{image: "dockerhub.io/project/image/abc/def:latest"},
			want: imageName{
				nameSpace:  "project",
				repository: "image$abc$def",
				tag:        "latest",
			},
			want1: true,
		}, {
			name:  "case_07 image repository contains empty slash",
			args:  args{image: "dockerhub.io/project/image//def:latest"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_08 image tag contains slash",
			args:  args{image: "dockerhub.io/project/image/def:latest/abc"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_09 image tag with spaces",
			args:  args{image: "xx dockerhub.io/project/image/def:latest/abc"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_10 image tag with invalid characters",
			args:  args{image: "dockerhub@#.io/project/image/def:latest/abc"},
			want:  imageName{},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := parseImageName(tt.args.image)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseImageName() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("parseImageName() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_getTimeoutDuration(t *testing.T) {
	var s = int64(10)
	type args struct {
		seconds         *int64
		defaultDuration time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "case_01 seconds not nil",
			args: args{seconds: &s, defaultDuration: time.Duration(2)},
			want: time.Duration(10) * time.Second,
		}, {
			name: "case_02 seconds nil",
			args: args{seconds: nil, defaultDuration: time.Duration(2)},
			want: time.Duration(2),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getTimeoutDuration(tt.args.seconds, tt.args.defaultDuration); got != tt.want {
				t.Errorf("getTimeoutDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sanitizeFuncMountUser(t *testing.T) {
	type args struct {
		uid int64
		gid int64
	}
	tests := []struct {
		name  string
		args  args
		want  int64
		want1 int64
	}{
		{
			name:  "case_01 uid is zero",
			args:  args{0, 1},
			want:  constants.DefaultRuntimeUID,
			want1: 1,
		}, {
			name:  "case_02 gid is zero",
			args:  args{1, 0},
			want:  1,
			want1: constants.DefaultRuntimeGID,
		}, {
			name:  "case_03 uid, gid not zero",
			args:  args{1, 1},
			want:  1,
			want1: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := sanitizeFuncMountUser(tt.args.uid, tt.args.gid)
			if got != tt.want {
				t.Errorf("sanitizeFuncMountUser() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("sanitizeFuncMountUser() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_parseJSONEnv(t *testing.T) {
	var res []corev1.EnvVar
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []corev1.EnvVar
		wantErr bool
	}{
		{
			name:    "case_01 data is nil",
			args:    args{data: nil},
			want:    res,
			wantErr: false,
		}, {
			name:    "case_02 unmashal env error",
			args:    args{data: []byte(`def{`)},
			want:    res,
			wantErr: true,
		}, {
			name:    "case_03 normal scenario",
			args:    args{data: []byte(`{"name": "test"}`)},
			want:    []corev1.EnvVar{{Name: "name", Value: "test"}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJSONEnv(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJSONEnv() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseJSONEnv() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_configDemandNodes(t *testing.T) {
	type args struct {
		pod          *corev1.Pod
		nodeAffinity *config.NodeAffinity
		demandNodes  []string
		forceDeploy  bool
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case_01 forceDeploy is true",
			args: args{&corev1.Pod{},
				&config.NodeAffinity{NodeAffinityKey: "k", NodeAffinityValue: []string{"v"}},
				[]string{"node"},
				true},
		}, {
			name: "case_02 forceDeploy is false",
			args: args{&corev1.Pod{},
				&config.NodeAffinity{NodeAffinityKey: "k", NodeAffinityValue: []string{"v"}},
				[]string{"node"},
				false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configDemandNodes(tt.args.pod, tt.args.nodeAffinity, tt.args.demandNodes, tt.args.forceDeploy)
		})
	}
}

func Test_isPodUnschedulable(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "case_01 pod status is nil",
			args: args{&corev1.Pod{Status: corev1.PodStatus{}}},
			want: false,
		}, {
			name: "case_01 normal scenario",
			args: args{&corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: "PodScheduled", Status: "False", Reason: "Unschedulable"}}}}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPodUnschedulable(tt.args.pod); got != tt.want {
				t.Errorf("isPodUnschedulable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getCrashPodLogs(t *testing.T) {
	convey.Convey("getCrashPodLogs", t, func() {
		defer gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
			func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
				return &fake.FakeCoreV1{}
			}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
			func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
				return &fake.FakePods{}
			}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "GetLogs",
			func(_ *fake.FakePods, name string, opts *corev1.PodLogOptions) *rest.Request {
				return &rest.Request{}
			}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&rest.Request{}), "Stream",
			func(_ *rest.Request, ctx context.Context) (io.ReadCloser, error) {
				return ioutil.NopCloser(bytes.NewReader([]byte("logs error"))), nil
			}).Reset()
		convey.Convey("case 1", func() {
			patch1 := gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			})
			str := getCrashPodLogs(&corev1.Pod{})
			convey.So(str, convey.ShouldEqual, "logs error")
			patch1.Reset()
		})
		convey.Convey("case 2", func() {
			defer gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return nil
			}).Reset()
			str := getCrashPodLogs(&corev1.Pod{})
			convey.So(str, convey.ShouldEqual, "")
		})
	})
}

func Test_isPodReadyOrError(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        error
		want1       bool
	}{
		{
			name: "case_01 pod is ready",
			args: args{&corev1.Pod{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
			want:  nil,
			want1: true,
		}, {
			name: "case_02 Waiting is nil",
			args: args{&corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{Name: "x"}, {Name: tools.RuntimeContainerName, State: corev1.ContainerState{Waiting: nil}}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
			want:  nil,
			want1: false,
		}, {
			name: "case_03 Waiting reason CreateContainerError or ImagePullBackOff",
			args: args{&corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{Name: "x"},
					{Name: tools.RuntimeContainerName, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{"CreateContainerError", ""}}}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
			want:  snerror.NewSNError(snerror.FunctionExceptionCode, "CreateContainerError: "),
			want1: true,
		}, {
			name: "case_04 Waiting reason CrashLoopBackOff",
			args: args{&corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{Name: "x"},
					{Name: tools.RuntimeContainerName, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{"CrashLoopBackOff", ""}}}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc(getCrashPodLogs, func(pod *corev1.Pod) string {
						return "123"
					}),
				})
				return patches
			},
			want: snerror.NewSNError(snerror.RuntimeProcessExit,
				"The application inside the container keeps crashing\n123"),
			want1: true,
		}, {
			name: "case_05 Waiting for other reason",
			args: args{&corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{Name: "x"},
					{Name: tools.RuntimeContainerName, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{"xx", ""}}}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
			want:  nil,
			want1: false,
		}, {
			name: "case_06 other scenario",
			args: args{&corev1.Pod{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
			want:  nil,
			want1: false,
		},
		{
			name: "gpu pod start error",
			args: args{
				pod: &corev1.Pod{
					Status: corev1.PodStatus{
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name: "runtime",
								State: corev1.ContainerState{
									Terminated: &corev1.ContainerStateTerminated{
										Reason:  "StartError",
										Message: `failed to create containerd task: failed to create shim task: Others("other error: container_linux.go:340: starting container process caused \"process_linux.go:380: container init caused \\\"process_linux.go:363: running prestart hook 0:/usr/bin/nvidia-container-runtime-hook,/usr/bin/nvidia-container-runtime-hook,prestart, ContainerID: 116d2bb9094ba9efcc866c6b652d93db24b0b3fe61b8037d35a1cc57cf85a72a caused \\\\\\\"error running hook: exit status 1, stdout: , stderr: Auto-detected mode as ''legacy''\\\\\\\\nnvidia-container-cli: create xgpu container error: create container failed, errno=-1\\\\\\\\n\\\\\\\"\\\"\""): unknown`,
									},
								},
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			want: snerror.New(snerror.GettingPodErrorCode,
				`StartError: failed to create containerd task: failed to create shim task: Others("other error: container_linux.go:340: starting container process caused \"process_linux.go:380: container init caused \\\"process_linux.go:363: running prestart hook 0:/usr/bin/nvidia-container-runtime-hook,/usr/bin/nvidia-container-runtime-hook,prestart, ContainerID: 116d2bb9094ba9efcc866c6b652d93db24b0b3fe61b8037d35a1cc57cf85a72a caused \\\\\\\"error running hook: exit status 1, stdout: , stderr: Auto-detected mode as ''legacy''\\\\\\\\nnvidia-container-cli: create xgpu container error: create container failed, errno=-1\\\\\\\\n\\\\\\\"\\\"\""): unknown`),
			want1: true,
		},
		{
			name: "use runtime exit with restartPolicyNever",
			args: args{&corev1.Pod{
				Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{Name: "x"},
						{Name: tools.RuntimeContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}}}}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc(getCrashPodLogs, func(pod *corev1.Pod) string {
						return "123"
					}),
				})
				return patches
			},
			want: snerror.NewSNError(snerror.RuntimeProcessExit,
				"Error: \n123"),
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, got1 := isPodReadyOrError(tt.args.pod)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("isPodReadyOrError() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("isPodReadyOrError() got1 = %v, want %v", got1, tt.want1)
			}
			patches.ResetAll()
		})
	}
}

func Test_isXGPUStartError(t *testing.T) {
	xstartErr, _ := isPodReadyOrError(&corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "runtime",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:  "StartError",
							Message: `failed to create containerd task: failed to create shim task: Others("other error: container_linux.go:340: starting container process caused \"process_linux.go:380: container init caused \\\"process_linux.go:363: running prestart hook 0:/usr/bin/nvidia-container-runtime-hook,/usr/bin/nvidia-container-runtime-hook,prestart, ContainerID: 116d2bb9094ba9efcc866c6b652d93db24b0b3fe61b8037d35a1cc57cf85a72a caused \\\\\\\"error running hook: exit status 1, stdout: , stderr: Auto-detected mode as ''legacy''\\\\\\\\nnvidia-container-cli: create xgpu container error: create container failed, errno=-1\\\\\\\\n\\\\\\\"\\\"\""): unknown`,
						},
					},
				},
			},
		},
	})

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "getting pod error",
			err:      snerror.New(snerror.GettingPodErrorCode, "getting pod error"),
			expected: false,
		},
		{
			name:     "not sn error",
			err:      errors.New("context timeout"),
			expected: false,
		},
		{
			name:     "xgpu start err return by pod ready check",
			err:      xstartErr,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, utils.IsXGPUStartError(tt.err))
		})
	}
}

func Test_retryCreatePodWithSpec(t *testing.T) {
	rolledback := uint32(0)
	postcreated := uint32(0)
	createCount := uint32(0)
	deleteCount := uint32(0)
	getcount := uint32(0)
	xstartErr, _ := isPodReadyOrError(&corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "runtime",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:  "StartError",
							Message: `failed to create containerd task: failed to create shim task: Others("other error: container_linux.go:340: starting container process caused \"process_linux.go:380: container init caused \\\"process_linux.go:363: running prestart hook 0:/usr/bin/nvidia-container-runtime-hook,/usr/bin/nvidia-container-runtime-hook,prestart, ContainerID: 116d2bb9094ba9efcc866c6b652d93db24b0b3fe61b8037d35a1cc57cf85a72a caused \\\\\\\"error running hook: exit status 1, stdout: , stderr: Auto-detected mode as ''legacy''\\\\\\\\nnvidia-container-cli: create xgpu container error: create container failed, errno=-1\\\\\\\\n\\\\\\\"\\\"\""): unknown`,
						},
					},
				},
			},
		},
	})

	c := &customContainerMGR{
		kubeClient: &kubernetes.Clientset{},
	}
	addOn := &simpleAddOn{
		postCreateFn: func(pod *corev1.Pod) {
			atomic.AddUint32(&postcreated, 1)
		},
		rollbackFn: func() {
			atomic.AddUint32(&rolledback, 1)
		},
	}
	ctx, cancel := context.WithCancel(context.Background())

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
		gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1", func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
			return &fake.FakeCoreV1{}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods", func(_ *fake.FakeCoreV1, _ string) typecorev1.PodInterface {
			return &fake.FakePods{}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Get", func(_ *fake.FakePods, _ context.Context, _ string, _ metav1.GetOptions) (*corev1.Pod, error) {
			atomic.AddUint32(&getcount, 1)
			return &corev1.Pod{}, nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete", func(_ *fake.FakePods, _ context.Context, _ string, _ metav1.DeleteOptions) error {
			atomic.AddUint32(&deleteCount, 1)
			return nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create", func(_ *fake.FakePods, ctx context.Context, pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
			go func() {
				for {
					<-time.After(50 * time.Millisecond)
					waiterIf, ok := c.onDemandWaitMap.Load(pod.Name)
					if !ok {
						continue
					}
					if atomic.AddUint32(&createCount, 1) < 3 {
						waiterIf.(*utils.PodWaiter).NotifyStatus(utils.PodStatus{Pod: pod.DeepCopy(), Err: xstartErr})
						return
					}
					cancel()
					return
				}
			}()
			return pod.DeepCopy(), nil
		}),
		gomonkey.ApplyFunc((*customContainerMGR).waitWorkerReady, func(_ *customContainerMGR, _ context.Context, _ *corev1.Pod) error {
			return nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	_, _ = c.retryCreatePodWithSpec(createCustomContainerPodParam{
		ctx:  ctx,
		spec: &faasfunction.FunctionServiceSpec{},
	}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test_pod_1"}}, addOn)
	assert.Equal(t, uint32(1), atomic.LoadUint32(&rolledback))
	assert.Equal(t, uint32(3), atomic.LoadUint32(&createCount))
	assert.Equal(t, uint32(3), atomic.LoadUint32(&deleteCount))
	assert.Equal(t, uint32(3), atomic.LoadUint32(&getcount))
	assert.Equal(t, uint32(0), atomic.LoadUint32(&postcreated))
}

func Test_customContainerRegcred_setOwner(t *testing.T) {
	type fields struct {
		secret     *corev1.Secret
		nameSpace  string
		kubeClient kubernetes.Interface
	}
	type args struct {
		ctx context.Context
		pod *corev1.Pod
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{secret: &corev1.Secret{}, nameSpace: "default", kubeClient: &kubernetes.Clientset{}},
			args:   args{context.TODO(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "1001"}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Secrets",
						func(_ *fake.FakeCoreV1, namespace string) v1.SecretInterface {
							return &fake.FakeSecrets{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Update",
						func(_ *fake.FakeSecrets, ctx context.Context, secret *corev1.Secret, opts metav1.UpdateOptions) (*corev1.Secret, error) {
							return &corev1.Secret{}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerRegcred{
				secret:     tt.fields.secret,
				nameSpace:  tt.fields.nameSpace,
				kubeClient: tt.fields.kubeClient,
			}
			if err := c.setOwner(tt.args.ctx, tt.args.pod); (err != nil) != tt.wantErr {
				t.Errorf("setOwner() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_customContainerRegcred_delete(t *testing.T) {
	type fields struct {
		name       string
		nameSpace  string
		kubeClient kubernetes.Interface
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{name: "test", nameSpace: "default", kubeClient: &kubernetes.Clientset{}},
			args:   args{context.TODO()},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Secrets",
						func(_ *fake.FakeCoreV1, namespace string) v1.SecretInterface {
							return &fake.FakeSecrets{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Delete",
						func(_ *fake.FakeSecrets, ctx context.Context, name string, opts metav1.DeleteOptions) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerRegcred{
				name:       tt.fields.name,
				nameSpace:  tt.fields.nameSpace,
				kubeClient: tt.fields.kubeClient,
			}
			if err := c.delete(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_checkMountFile(t *testing.T) {
	type fields struct {
		FileMountConfig config.FileMountConfig
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case_01 check mountFile s3 config is nil ",
			fields: fields{config.FileMountConfig{
				S3Config: nil,
				MountFiles: []*config.MountFile{
					{"", "bucket-test-log1", "go.mod", "", "/opt/go.mod"},
				},
			}},
			args:    args{context.TODO()},
			wantErr: true,
		},
		{
			name: "case_02 check mountFile is  nil",
			fields: fields{config.FileMountConfig{
				S3Config: &config.S3Config{
					UseSSL:    false,
					AccessKey: "xxxxxx",
					SecretKey: "xxxxxx",
					Endpoint:  "1.0.0.0:9000",
					CaFile:    "",
					TrustedCA: false,
				},
				MountFiles: nil,
			}},
			args:    args{context.TODO()},
			wantErr: true,
		},
		{
			name: "case_03 check mountFile length ",
			fields: fields{config.FileMountConfig{
				S3Config: &config.S3Config{
					UseSSL:    false,
					AccessKey: "xxxxxx",
					SecretKey: "xxxxxx",
					Endpoint:  "1.0.0.0:9000",
					CaFile:    "",
					TrustedCA: false,
				},
				MountFiles: []*config.MountFile{},
			}},
			args:    args{context.TODO()},
			wantErr: true,
		},
		{
			name: "case_04 check mountFile ok",
			fields: fields{config.FileMountConfig{
				S3Config: &config.S3Config{
					UseSSL:    false,
					AccessKey: "xxxxxx",
					SecretKey: "xxxxxx",
					Endpoint:  "1.0.0.0:9000",
					CaFile:    "",
					TrustedCA: false,
				},
				MountFiles: []*config.MountFile{
					{"", "bucket-test-log1", "go.mod", "", "/opt/go.mod"},
				},
			}},
			args:    args{context.TODO()},
			wantErr: false,
		},
		{
			name: "case_05 check s3 ak or sk is nil ",
			fields: fields{config.FileMountConfig{
				S3Config: &config.S3Config{
					UseSSL:    false,
					AccessKey: "xxxxxx",
					SecretKey: "xxxxxx",
					Endpoint:  "1.0.0.0:9000",
					CaFile:    "",
					TrustedCA: false,
				},
				MountFiles: []*config.MountFile{
					{"", "", "go.mod", "", "/opt/go.mod"},
				},
			}},
			args:    args{context.TODO()},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkMountFile(tt.fields.FileMountConfig); (err != nil) != tt.wantErr {
				t.Errorf("checkMountFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_customContainerRegcred_create(t *testing.T) {
	type fields struct {
		secret     *corev1.Secret
		nameSpace  string
		kubeClient kubernetes.Interface
		spec       *faasfunction.FunctionServiceSpec
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case_01 create spec failed",
			fields: fields{&corev1.Secret{}, "default", &kubernetes.Clientset{}, &faasfunction.FunctionServiceSpec{}},
			args:   args{context.TODO()},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerRegcred).createSpec,
						func(_ *customContainerRegcred, ctx context.Context) (*corev1.Secret, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			wantErr: true,
		}, {
			name:   "case_02 create secret failed",
			fields: fields{&corev1.Secret{}, "default", &kubernetes.Clientset{}, &faasfunction.FunctionServiceSpec{}},
			args:   args{context.TODO()},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerRegcred).createSpec,
						func(_ *customContainerRegcred, ctx context.Context) (*corev1.Secret, error) {
							return &corev1.Secret{}, nil
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Secrets",
						func(_ *fake.FakeCoreV1, namespace string) v1.SecretInterface {
							return &fake.FakeSecrets{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Create",
						func(_ *fake.FakeSecrets, ctx context.Context, secret *corev1.Secret, opts metav1.CreateOptions) (*corev1.Secret, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case_03 normal scenario",
			fields: fields{&corev1.Secret{}, "default", &kubernetes.Clientset{}, &faasfunction.FunctionServiceSpec{}},
			args:   args{context.TODO()},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerRegcred).createSpec,
						func(_ *customContainerRegcred, ctx context.Context) (*corev1.Secret, error) {
							return &corev1.Secret{}, nil
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Secrets",
						func(_ *fake.FakeCoreV1, namespace string) v1.SecretInterface {
							return &fake.FakeSecrets{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Create",
						func(_ *fake.FakeSecrets, ctx context.Context, secret *corev1.Secret, opts metav1.CreateOptions) (*corev1.Secret, error) {
							return &corev1.Secret{}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test succeed to decrypt file content",
			fields: fields{
				kubeClient: mockUtils.K8sFakeClient,
				spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FileMountConfig: config.FileMountConfig{
							SecretFiles: []*config.SecretFileMount{
								{
									// 123456
									EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
									FileMountPath:        "/tmp/abc.txt",
								},
								{
									// abcdefg
									EncryptedFileContent: "00000001000000017AB1F29D2E38971544E0DF52C595E7276D1A8E980F53A641DF9666D3DB85AFDF",
									FileMountPath:        "/abc/def.txt",
								},
							},
						},
					},
				},
			},
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerRegcred).createSpec,
						func(_ *customContainerRegcred, ctx context.Context) (*corev1.Secret, error) {
							return &corev1.Secret{
								Data: map[string][]byte{},
							}, nil
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Secrets",
						func(_ *fake.FakeCoreV1, namespace string) v1.SecretInterface {
							return &fake.FakeSecrets{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Create",
						func(_ *fake.FakeSecrets, ctx context.Context, secret *corev1.Secret, opts metav1.CreateOptions) (*corev1.Secret, error) {
							return &corev1.Secret{}, nil
						}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte("123456"), nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerRegcred{
				secret:     tt.fields.secret,
				nameSpace:  tt.fields.nameSpace,
				kubeClient: tt.fields.kubeClient,
				spec:       tt.fields.spec,
			}
			if err := c.create(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("create() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_newCustomContainerRegcred(t *testing.T) {
	type args struct {
		c           *customContainerMGR
		regcredName string
		spec        *faasfunction.FunctionServiceSpec
		swrHandler  *swrHandler
		traceID     string
	}
	tests := []struct {
		name string
		args args
		want *customContainerRegcred
	}{
		{
			name: "case_01 normal scenario",
			args: args{&customContainerMGR{nameSpace: "default", kubeClient: nil}, "test", &faasfunction.FunctionServiceSpec{}, &swrHandler{}, "123"},
			want: &customContainerRegcred{
				spec:       &faasfunction.FunctionServiceSpec{},
				swrHandler: &swrHandler{},
				name:       "test",
				nameSpace:  "default",
				kubeClient: nil,
				traceID:    "123",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newCustomContainerRegcred(tt.args.c, tt.args.regcredName, tt.args.spec, tt.args.swrHandler, tt.args.traceID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newCustomContainerRegcred() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_customContainerMGR_updatePod(t *testing.T) {
	var m = sync.Map{}
	m.Store("test", &tools.PodWaiter{})
	type fields struct {
		onDemandWaitMap sync.Map
	}
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 force delete terminated pod success",
			fields: fields{sync.Map{}},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
		}, {
			name:   "case_02 pod without used label",
			fields: fields{sync.Map{}},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name:   "case_03 pod is in use",
			fields: fields{sync.Map{}},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Annotations: map[string]string{"used": "true"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name:   "case_04 unknown pod",
			fields: fields{sync.Map{}},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Annotations: map[string]string{"used": "false"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name:   "case_05 pod unschedulable status changes",
			fields: fields{m},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Annotations: map[string]string{"used": "false"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc(isPodUnschedulable,
						func(pod *corev1.Pod) bool {
							return true
						}),
					gomonkey.ApplyFunc((*tools.PodWaiter).SetUnschedulable,
						func(_ *tools.PodWaiter, unschedulable bool) bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name:   "case_06 pod is unschedulable",
			fields: fields{m},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Annotations: map[string]string{"used": "false"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc(isPodUnschedulable,
						func(pod *corev1.Pod) bool {
							return true
						}),
					gomonkey.ApplyFunc((*tools.PodWaiter).SetUnschedulable,
						func(_ *tools.PodWaiter, unschedulable bool) bool {
							return true
						}),
				})
				return patches
			},
		}, {
			name:   "case_07 pod is not ready",
			fields: fields{m},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Annotations: map[string]string{"used": "false"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc(isPodUnschedulable,
						func(pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc((*tools.PodWaiter).SetUnschedulable,
						func(_ *tools.PodWaiter, unschedulable bool) bool {
							return false
						}),
					gomonkey.ApplyFunc(isPodReadyOrError,
						func(pod *corev1.Pod) (snerror.SNError, bool) {
							return snerror.New(123, "abc"), false
						}),
				})
				return patches
			},
		}, {
			name:   "case_07 pod is ready",
			fields: fields{m},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Annotations: map[string]string{"used": "false"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).deleteTerminatedPod,
						func(_ *customContainerMGR, pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc(isPodUnschedulable,
						func(pod *corev1.Pod) bool {
							return false
						}),
					gomonkey.ApplyFunc((*tools.PodWaiter).SetUnschedulable,
						func(_ *tools.PodWaiter, unschedulable bool) bool {
							return false
						}),
					gomonkey.ApplyFunc(isPodReadyOrError,
						func(pod *corev1.Pod) (snerror.SNError, bool) {
							return snerror.New(123, "abc"), true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerMGR{
				onDemandWaitMap: tt.fields.onDemandWaitMap,
			}
			c.updatePod(tt.args.pod)
			patches.ResetAll()
		})
	}
}

func Test_customContainerMGR_getRuntimeSecurityContext(t *testing.T) {
	var runAsNonRoot = true
	var uid = int64(1001)
	var gid = int64(1001)
	type args struct {
		spec *faasfunction.FunctionServiceSpec
	}
	tests := []struct {
		name string
		args args
		want *corev1.SecurityContext
	}{
		{
			name: "case_01 normal scenario",
			args: args{&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{CustomContainerConfig: types.CustomContainerConfig{UID: int(uid), GID: int(gid)}}}},
			want: &corev1.SecurityContext{
				RunAsUser:    &uid,
				RunAsGroup:   &gid,
				RunAsNonRoot: &runAsNonRoot,
				Capabilities: &corev1.Capabilities{
					Add:  []corev1.Capability{"KILL"},
					Drop: []corev1.Capability{"ALL"},
				},
			},
		},
		{
			name: "case_02 cae scenario",
			args: args{&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{CustomContainerConfig: types.CustomContainerConfig{UID: int(uid), GID: int(gid)}, BusinessType: constants.BusinessTypeCAE}}},
			want: &corev1.SecurityContext{
				Capabilities: &corev1.Capabilities{
					Add:  []corev1.Capability{"ALL"},
					Drop: []corev1.Capability{"ALL"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &customContainerMGR{}
			c.config.RuntimeCaps.Add = []string{"ALL"}
			c.config.RuntimeCaps.Drop = []string{"ALL"}
			if got := c.getRuntimeSecurityContext(tt.args.spec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRuntimeSecurityContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_customContainerMGR_getRuntimeEnv(t *testing.T) {
	type args struct {
		spec *faasfunction.FunctionServiceSpec
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case_01 decrypt user's env error",
			args: args{&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
				Env: []corev1.EnvVar{{Name: "WORKER_ENV", Value: "123"}},
			}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt,
						func(src string) ([]byte, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			wantErr: true,
		}, {
			name: "case_02 decrypt user's data error",
			args: args{&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
				Env: []corev1.EnvVar{{Name: "USER_DATA", Value: "123"}},
			}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt,
						func(src string) ([]byte, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			wantErr: true,
		}, {
			name: "case_03 all env is nil",
			args: args{&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
				Env: []corev1.EnvVar{},
			}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		}, {
			name: "case_04 parse user's env error",
			args: args{&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
				Env: []corev1.EnvVar{{Name: "WORKER_ENV", Value: "123"}},
			}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt,
						func(src string) ([]byte, error) {
							return []byte(`def{`), nil
						}),
				})
				return patches
			},
			wantErr: true,
		}, {
			name: "case_05 parse user's data error",
			args: args{&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
				Env: []corev1.EnvVar{{Name: "USER_DATA", Value: "123"}},
			}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt,
						func(src string) ([]byte, error) {
							return []byte(`def{`), nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerMGR{}
			_, err := c.getRuntimeEnv(tt.args.spec)
			if (err != nil) != tt.wantErr {
				t.Errorf("getRuntimeEnv() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_customContainerMGR_describeContainers(t *testing.T) {
	pool := &Pool{
		RequestCPU:       2,
		LimitCPU:         3,
		RequestMemory:    4,
		LimitMemory:      5,
		EphemeralStorage: 512,
	}
	req := common.PodRequest{}
	req.PublicNetworkPatPodInfo = append([]types.NATConfigure{}, types.NATConfigure{ContainerCidr: "ContainerCidr"})
	var privileged = true

	runtimeResource := getRuntimeResource(pool)
	workerResource := getWorkerResource(pool)
	workerLimitMemory := workerResource.Limits.Memory().Value() >> mbShift

	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		cs   *componentSet
		spec *faasfunction.FunctionServiceSpec
		pool *Pool
		req  common.PodRequest
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        []corev1.Container
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case_01 failed to get runtime env",
			fields: fields{config.PoolMgrConfig{PoolContainerTags: config.PoolContainerImageTags{WorkerImageTag: "1.0"}}},
			args:   args{&componentSet{}, &faasfunction.FunctionServiceSpec{}, pool, common.PodRequest{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).getRuntimeEnv,
						func(_ *customContainerMGR, spec *faasfunction.FunctionServiceSpec) ([]corev1.EnvVar, error) {
							return []corev1.EnvVar{}, fmt.Errorf("error")
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		}, {
			name:   "case_02 normal scenario",
			fields: fields{config.PoolMgrConfig{PoolContainerTags: config.PoolContainerImageTags{WorkerImageTag: "1.0"}}},
			args: args{&componentSet{argsBuilder: &argsBuilder{map[container][]string{tools.WorkerContainerName: {"on"}}},
				envBuilder:    &envBuilder{map[container][]corev1.EnvVar{tools.WorkerContainerName: {}}},
				volumeBuilder: &volumeBuilder{mounts: map[container][]corev1.VolumeMount{tools.RuntimeContainerName: {}, tools.WorkerContainerName: {}}}},
				&faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{CustomContainerConfig: types.CustomContainerConfig{Image: "test_image", Command: []string{}, Args: []string{}, WorkingDir: "/opt"}}},
				pool, req},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).getRuntimeEnv,
						func(_ *customContainerMGR, spec *faasfunction.FunctionServiceSpec) ([]corev1.EnvVar, error) {
							return []corev1.EnvVar{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).getRuntimeSecurityContext,
						func(_ *customContainerMGR, spec *faasfunction.FunctionServiceSpec) *corev1.SecurityContext {
							return &corev1.SecurityContext{}
						}),
					gomonkey.ApplyFunc(sanitizeCAP,
						func(caps []string) []corev1.Capability {
							return []corev1.Capability{}
						}),
					gomonkey.ApplyFunc(config.GetK8sCidr,
						func() *config.K8sCidr {
							return &config.K8sCidr{}
						}),
				})
				return patches
			},
			want: []corev1.Container{
				{
					Name:            tools.RuntimeContainerName,
					Image:           strings.TrimSpace("test_image"),
					ImagePullPolicy: corev1.PullAlways,
					Env:             []corev1.EnvVar{},
					Resources:       runtimeResource,
					VolumeMounts:    []corev1.VolumeMount{},
					Command:         []string{},
					Args:            []string{},
					WorkingDir:      "/opt",
					SecurityContext: &corev1.SecurityContext{},
				},
				{
					Name:  tools.WorkerContainerName,
					Image: "1.0",
					Ports: []corev1.ContainerPort{
						{
							Name:          "func",
							Protocol:      corev1.ProtocolTCP,
							ContainerPort: workerFuncPort,
						},
						{
							Name:          "admin",
							Protocol:      corev1.ProtocolTCP,
							ContainerPort: workerAdminPort,
						},
					},
					Args:         []string{"on"},
					Env:          addMockBusinessTypeToWorkerEnv(append(getWorkerEnv(workerLimitMemory), getMockCustomWorkerEnv(req)...)),
					Resources:    workerResource,
					VolumeMounts: []corev1.VolumeMount{},
					ReadinessProbe: getWorkerLivenessProbe(defaultLivenessProbeFailureThreshold,
						defaultLivenessProbePeriodSeconds),
					LivenessProbe: getCustomWorkerLivenessProbe(defaultCustomLivenessProbeFailureThreshold,
						defaultCustomLivenessProbeFailureSeconds),
					SecurityContext: &corev1.SecurityContext{
						Capabilities: &corev1.Capabilities{
							Add:  []corev1.Capability{},
							Drop: []corev1.Capability{},
						},
						Privileged: &privileged,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerMGR{
				config: tt.fields.config,
			}
			got, err := c.describeContainers(tt.args.cs, tt.args.spec, tt.args.pool, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("describeContainers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("describeContainers() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func getMockCustomWorkerEnv(req common.PodRequest) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name:  constants.SvcCidr,
			Value: "",
		},
	}
	publicPatPodInfo := append([]types.NATConfigure{}, types.NATConfigure{ContainerCidr: "ContainerCidr"})
	publicPatInfo, _ := json.Marshal(publicPatPodInfo)
	envs = append(envs, corev1.EnvVar{
		Name:  constants.PublicNetworkVpcNATConfigure,
		Value: string(publicPatInfo),
	})
	routeInfo := types.RouteInfoData{
		ApigRouteEnable: req.RouteInfo.ApigRouteEnable,
		ApigRoutes:      req.RouteInfo.ApigRoutes,
	}
	routeInfoResult, _ := json.Marshal(routeInfo)
	envs = append(envs, corev1.EnvVar{
		Name:  constants.RouteInfo,
		Value: string(routeInfoResult),
	})
	return envs
}

func addMockBusinessTypeToWorkerEnv(env []corev1.EnvVar) []corev1.EnvVar {
	return append(env, []corev1.EnvVar{
		{
			Name:  constants.BusinessType,
			Value: "",
		},
	}...)
}

func Test_getWorkerResourceRequestCPU(t *testing.T) {
	type args struct {
		pool *Pool
	}
	tests := []struct {
		name    string
		args    args
		wantCpu int
	}{
		{
			name: "test memory 128",
			args: args{
				pool: &Pool{LimitMemory: 128},
			},
			wantCpu: 50,
		},
		{
			name: "test memory other",
			args: args{
				pool: &Pool{LimitMemory: 256},
			},
			wantCpu: 100,
		},
		{
			name: "test memory other",
			args: args{
				pool: &Pool{LimitMemory: 2048},
			},
			wantCpu: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getWorkerResource(tt.args.pool); got.Requests["cpu"] != resource.MustParse(fmt.Sprintf("%dm", tt.wantCpu)) {
				t.Errorf("request cpu = %v, want %v", got, tt.wantCpu)
			}
		})
	}
}

func Test_getWorkerResourceLimitMemory(t *testing.T) {
	type args struct {
		pool *Pool
	}
	tests := []struct {
		name    string
		args    args
		wantMem int
	}{
		{
			name: "test memory 128",
			args: args{
				pool: &Pool{LimitMemory: 128},
			},
			wantMem: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getWorkerResource(tt.args.pool); got.Limits["memory"] != resource.MustParse(fmt.Sprintf("%dMi", tt.wantMem)) {
				fmt.Println("limit memory: ", got.Limits["memory"])
				t.Errorf("limit memory = %v, want %v", got, tt.wantMem)
			}
		})
	}
}

func Test_customContainerMGR_describeInitContainers(t *testing.T) {
	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		cs   *componentSet
		spec *faasfunction.FunctionServiceSpec
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []corev1.Container
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{config.PoolMgrConfig{PoolContainerTags: config.PoolContainerImageTags{WorkerInitImageTag: "1.0"}}},
			args: args{&componentSet{envBuilder: &envBuilder{map[container][]corev1.EnvVar{utils.WorkerInitContainerName: {}}},
				volumeBuilder: &volumeBuilder{mounts: map[container][]corev1.VolumeMount{utils.WorkerInitContainerName: {}}}},
				&faasfunction.FunctionServiceSpec{},
			},
			want: []corev1.Container{
				{
					Name:  utils.WorkerInitContainerName,
					Image: "1.0",
					Env:   []corev1.EnvVar{},
					SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{
						Add:  []corev1.Capability{"NET_ADMIN", "NET_RAW"},
						Drop: []corev1.Capability{"ALL"},
					}},
					VolumeMounts: []corev1.VolumeMount{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &customContainerMGR{
				config: tt.fields.config,
			}
			if got := c.describeInitContainers(tt.args.cs, tt.args.spec); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("describeInitContainers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_customContainerMGR_describeComponentSet(t *testing.T) {
	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		spec *faasfunction.FunctionServiceSpec
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{config.PoolMgrConfig{VolumeConfig: config.VolumeConfig{LogHostPath: config.LogHostPath{UserLog: "/tmp/user"}, UserLogMountPath: "/var/log"}}},
			args:   args{&faasfunction.FunctionServiceSpec{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetK8sCidr,
						func() *config.K8sCidr {
							return &config.K8sCidr{SvcCidr: "192.168.1.0/24", PodCidr: "192.168.2.0/24", HostCidr: "192.168.3.0/24"}
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerMGR{
				config: tt.fields.config,
			}
			_, err := c.describeComponentSet(tt.args.spec, &Pool{})
			if (err != nil) != tt.wantErr {
				t.Errorf("describeComponentSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_customContainerMGR_describeCAEComponentSet(t *testing.T) {
	type fields struct {
		config config.PoolMgrConfig
	}
	type args struct {
		spec *faasfunction.FunctionServiceSpec
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		// test failed to get config
		{
			name:   "case_01 normal scenario",
			fields: fields{config.PoolMgrConfig{VolumeConfig: config.VolumeConfig{LogHostPath: config.LogHostPath{UserLog: "/tmp/user"}, UserLogMountPath: "/var/log"}}},
			args:   args{&faasfunction.FunctionServiceSpec{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{SvcCidr: "192.168.1.0/24", PodCidr: "192.168.2.0/24", HostCidr: "192.168.3.0/24"}
					}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case_02 add user log config",
			fields: fields{config.PoolMgrConfig{VolumeConfig: config.VolumeConfig{LogHostPath: config.LogHostPath{UserLog: "/tmp/user"}, UserLogMountPath: "/var/log"}}},
			args: args{&faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					LogPolicy: types.LogPolicy{
						LogPaths:           []string{"/tmp"},
						HostPath:           "/host",
						EnableMultiCollect: true,
						MultiPattern:       "xxx",
						LogGroupID:         "xxx",
						LogStreamID:        "xxx",
					},
				},
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{SvcCidr: "192.168.1.0/24", PodCidr: "192.168.2.0/24", HostCidr: "192.168.3.0/24"}
					}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case_03 enable new case base image",
			fields: fields{config.PoolMgrConfig{VolumeConfig: config.VolumeConfig{LogHostPath: config.LogHostPath{UserLog: "/tmp/user"}, UserLogMountPath: "/var/log"}}},
			args:   args{&faasfunction.FunctionServiceSpec{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr {
						return &config.K8sCidr{SvcCidr: "192.168.1.0/24", PodCidr: "192.168.2.0/24", HostCidr: "192.168.3.0/24"}
					}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
					gomonkey.ApplyFunc(utils.EnableNewCaeBaseImage, func() bool {
						return true
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerMGR{
				config: tt.fields.config,
			}
			_, err := c.describeCAEComponentSet(tt.args.spec, &Pool{}, tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("describeCAEComponentSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_newCustomContainerMGR(t *testing.T) {
	type args struct {
		workerClient *fhttp.Client
		kubeClient   kubernetes.Interface
		cfg          config.PoolMgrConfig
	}
	tests := []struct {
		name string
		args args
		want *customContainerMGR
	}{
		{
			name: "case_01 normal scenario",
			args: args{&fhttp.Client{}, &kubernetes.Clientset{}, config.PoolMgrConfig{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newCustomContainerMGR(tt.args.workerClient, tt.args.kubeClient, tt.args.cfg); got == nil {
				t.Errorf("newCustomContainerMGR() failed")
			}
		})
	}
}

func Test_customContainerMGR_createPod(t *testing.T) {
	type fields struct {
		nameSpace  string
		kubeClient kubernetes.Interface
		config     config.PoolMgrConfig
	}
	type args struct {
		ctx            context.Context
		spec           *faasfunction.FunctionServiceSpec
		req            common.PodRequest
		customResource map[string]int64
		demandNodes    []string
	}
	mockPod := &corev1.Pod{}
	mockPod.Name = "mock-custom-container-pod"
	mockPod.Spec.Containers = []corev1.Container{
		{
			Name: tools.RuntimeContainerName,
		},
		{
			Name: tools.WorkerContainerName,
		},
		{
			Name: "worker-init",
		},
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        *corev1.Pod
		wantErr     bool
	}{
		{
			name:   "case_01 prepare pod spec and addOn failed",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{}},
			args:   args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{}, req: common.PodRequest{}, customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return nil, nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		}, {
			name:   "case_02 failed to wait pod ready",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: nil}}},
			args:   args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{}, req: common.PodRequest{ForceDeploy: true}, customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return &corev1.Pod{}, combinedAddOn{}, nil
						}),
					gomonkey.ApplyFunc(configDemandNodes,
						func(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
						}),
					gomonkey.ApplyFunc((*customContainerMGR).configCAENodeAffinity,
						func(_ *customContainerMGR, businessType string, pod *corev1.Pod) error {
							return nil
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
						func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
							addOn customContainerAddOn) (*corev1.Pod, error) {
							return &corev1.Pod{}, errors.New("error")
						}),
					gomonkey.ApplyFunc(configPodSchedulePolicyWithLabels,
						func(pod *corev1.Pod, FunSvcID string) {
							return
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		}, {
			name:   "case_03 failed to wait worker ready",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: nil}}},
			args:   args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{}, req: common.PodRequest{ForceDeploy: true}, customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return mockPod, combinedAddOn{}, nil
						}),
					gomonkey.ApplyFunc(configDemandNodes,
						func(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
						func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
							addOn customContainerAddOn) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitWorkerReady,
						func(_ *customContainerMGR, ctx context.Context, pod *corev1.Pod) error {
							return errors.New("error")
						}),
					gomonkey.ApplyFunc(configPodSchedulePolicyWithLabels,
						func(pod *corev1.Pod, FunSvcID string) {
							return
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		}, {
			name:   "case_04 normal scenario",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: nil}}},
			args:   args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{}, req: common.PodRequest{ForceDeploy: true}, customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return mockPod, combinedAddOn{}, nil
						}),
					gomonkey.ApplyFunc(configDemandNodes,
						func(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
						func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
							addOn customContainerAddOn) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitWorkerReady,
						func(_ *customContainerMGR, ctx context.Context, pod *corev1.Pod) error {
							return nil
						}),
					gomonkey.ApplyFunc(configPodSchedulePolicyWithLabels,
						func(pod *corev1.Pod, FunSvcID string) {
							return
						}),
				})
				return patches
			},
			want:    &corev1.Pod{},
			wantErr: false,
		}, {
			name: "case_05 gpu scenario",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: &config.NodeAffinity{}},
				GPUPodConfig: config.GPUPodConfig{GPUMemoryCapacity: make(map[string]int64, 0)}}},
			args: args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{}, req: common.PodRequest{ForceDeploy: true, GpuMemory: 1024}, customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return &corev1.Pod{Spec: corev1.PodSpec{
								Affinity: &corev1.Affinity{NodeAffinity: nil},
								Containers: []corev1.Container{corev1.Container{Name: tools.RuntimeContainerName, Resources: corev1.ResourceRequirements{
									Limits:   make(map[corev1.ResourceName]resource.Quantity, 0),
									Requests: make(map[corev1.ResourceName]resource.Quantity, 0)}}}},
							}, combinedAddOn{}, nil
						}),
					gomonkey.ApplyFunc(configDemandNodes,
						func(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
						func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
							addOn customContainerAddOn) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitWorkerReady,
						func(_ *customContainerMGR, ctx context.Context, pod *corev1.Pod) error {
							return nil
						}),
					gomonkey.ApplyFunc(configPodSchedulePolicyWithLabels,
						func(pod *corev1.Pod, FunSvcID string) {
							return
						}),
				})
				return patches
			},
			want:    &corev1.Pod{},
			wantErr: false,
		},
		{
			name: "case_06 gpu memory exceeded",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: &config.NodeAffinity{}},
				GPUPodConfig: config.GPUPodConfig{GPUMemoryCapacity: map[string]int64{"nvidia-a30": 24}}}},
			args: args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{}, req: common.PodRequest{ForceDeploy: true, GpuMemory: 25000, GpuType: "nvidia-a30"},
				customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return &corev1.Pod{Spec: corev1.PodSpec{
								Affinity: &corev1.Affinity{NodeAffinity: nil},
								Containers: []corev1.Container{corev1.Container{Name: tools.RuntimeContainerName, Resources: corev1.ResourceRequirements{
									Limits:   make(map[corev1.ResourceName]resource.Quantity, 0),
									Requests: make(map[corev1.ResourceName]resource.Quantity, 0)}}}},
							}, combinedAddOn{}, nil
						}),
					gomonkey.ApplyFunc(configDemandNodes,
						func(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
						func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
							addOn customContainerAddOn) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitWorkerReady,
						func(_ *customContainerMGR, ctx context.Context, pod *corev1.Pod) error {
							return nil
						}),
					gomonkey.ApplyFunc(configPodSchedulePolicyWithLabels,
						func(pod *corev1.Pod, FunSvcID string) {
							return
						}),
				})
				return patches
			},
			want:    &corev1.Pod{},
			wantErr: false,
		},
		{
			name:   "case_07 vpc container",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: nil}}},
			args: args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{DNSDomainCfg: []types.DNSDomainInfo{{DomainName: "AAA"}}}}, req: common.PodRequest{ForceDeploy: true}, customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return mockPod, combinedAddOn{}, nil
						}),
					gomonkey.ApplyFunc(configDemandNodes,
						func(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
						func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
							addOn customContainerAddOn) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitWorkerReady,
						func(_ *customContainerMGR, ctx context.Context, pod *corev1.Pod) error {
							return nil
						}),
					gomonkey.ApplyFunc(configPodSchedulePolicyWithLabels,
						func(pod *corev1.Pod, FunSvcID string) {
							return
						}),
				})
				return patches
			},
			want:    &corev1.Pod{},
			wantErr: true,
		},
		{
			name:   "case_08 create pod failed, check mount file error",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: nil}}},
			args: args{ctx: context.TODO(), spec: &faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					FileMountConfig: config.FileMountConfig{
						S3Config: &config.S3Config{
							UseSSL:    false,
							AccessKey: "xxxxxx",
							SecretKey: "",
							Endpoint:  "1.0.0.0:9000",
							CaFile:    "",
							TrustedCA: false,
						},
						MountFiles: []*config.MountFile{
							{"", "bucket-test-log1", "go.mod", "", "/opt/go.mod"},
						},
					},
				},
			}, req: common.PodRequest{ForceDeploy: true}, customResource: map[string]int64{}, demandNodes: []string{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).preparePodSpecAndAddOn,
						func(_ *customContainerMGR, ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest, pool *Pool) (*corev1.Pod, customContainerAddOn, error) {
							return mockPod, combinedAddOn{}, nil
						}),
					gomonkey.ApplyFunc(configDemandNodes,
						func(pod *corev1.Pod, nodeAffinity *config.NodeAffinity, demandNodes []string, forceDeploy bool) {
						}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
						func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
							addOn customContainerAddOn) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*customContainerMGR).waitWorkerReady,
						func(_ *customContainerMGR, ctx context.Context, pod *corev1.Pod) error {
							return nil
						}),
				})
				return patches
			},
			want:    &corev1.Pod{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customContainerMGR{
				nameSpace:  tt.fields.nameSpace,
				kubeClient: tt.fields.kubeClient,
				config:     tt.fields.config,
			}
			_, err := c.createPod(
				createCustomContainerPodParam{&Pool{}, tt.args.spec, tt.args.ctx, tt.args.req, tt.args.customResource, tt.args.demandNodes})
			if (err != nil) != tt.wantErr {
				t.Errorf("createPod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_configCAENodeAffinity(t *testing.T) {
	convey.Convey("Test_configCAENodeAffinity normal", t, func() {
		c := &customContainerMGR{}
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{},
				},
			},
		}
		err := c.configCAENodeAffinity("CAE", pod)
		expectedNodeAffinity := &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
				Weight: 100,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "node-type",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"fixed"},
					}},
				},
			}},
		}
		convey.ShouldEqual(pod.Spec.Affinity.NodeAffinity, expectedNodeAffinity)
		convey.ShouldBeNil(err)
	})
	convey.Convey("Test_configCAENodeAffinity not CAE", t, func() {
		c := &customContainerMGR{}
		pod := &corev1.Pod{}
		err := c.configCAENodeAffinity("cae", pod)
		convey.ShouldBeNil(err)
		convey.ShouldEqual(pod, &corev1.Pod{})
	})
	convey.Convey("Test_configCAENodeAffinity double config", t, func() {
		c := &customContainerMGR{}
		prefer := corev1.PreferredSchedulingTerm{}
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{prefer},
					},
				},
			},
		}
		err := c.configCAENodeAffinity("CAE", pod)

		convey.ShouldEqual(len(pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution), 2)
		convey.ShouldBeNil(err)
	})
}

func Test_verify(t *testing.T) {
	type args struct {
		image string
	}
	tests := []struct {
		name  string
		args  args
		want  imageName
		want1 bool
	}{
		{
			name:  "case_01 image is short",
			args:  args{image: "dockerhub.io/project"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_02 image with no tag",
			args:  args{image: "dockerhub.io/project/"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_03 image with no namespace",
			args:  args{image: "dockerhub.io//image:1.0"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_04 image with tags",
			args:  args{image: "dockerhub.io/project/image:a:1.0"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_07 image repository contains empty slash",
			args:  args{image: "dockerhub.io/project/image//def:latest"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_08 image tag contains slash",
			args:  args{image: "dockerhub.io/project/image/def:latest/abc"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_09 image tag with spaces",
			args:  args{image: "xx dockerhub.io/project/image/def:latest/abc"},
			want:  imageName{},
			want1: false,
		}, {
			name:  "case_10 image tag with invalid characters",
			args:  args{image: "dockerhub@#.io/project/image/def:latest/abc"},
			want:  imageName{},
			want1: false,
		},
	}
	imageVerifier := newImageVerifier(nil, "traceid001")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := imageVerifier.verify(context.TODO(), tt.args.image)
			snErr1 := err.(snerror.SNError)
			require.Equal(t, snErr1.Code(), snerror.FunctionExceptionCode)
		})
	}
}

func Test_preparePodSpecAndAddOn(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return []byte("abc"), nil })
	patches.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.String(), "repos") {
			// image verify request
			data := []swrImageInfo{{Size: 1234}}
			encoded, err := json.Marshal(data)
			if err != nil {
				return nil, err
			}
			return &http.Response{
				Status:     http.StatusText(http.StatusOK),
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBuffer(encoded)),
			}, nil
		}
		if strings.Contains(req.URL.String(), "secret") {
			// regcred request
			return &http.Response{
				Status:     http.StatusText(http.StatusOK),
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString("dummy secret")),
			}, nil
		}

		return &http.Response{
			Status:     http.StatusText(http.StatusNotFound),
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewBufferString("404 page not found")),
		}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
		func(_ *kubernetes.Clientset) v1.CoreV1Interface {
			return &fake.FakeCoreV1{}
		})
	patches.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Secrets",
		func(_ *fake.FakeCoreV1, namespace string) v1.SecretInterface {
			return &fake.FakeSecrets{}
		})
	patches.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Create",
		func(_ *fake.FakeSecrets, ctx context.Context, secret *corev1.Secret, opts metav1.CreateOptions) (*corev1.Secret, error) {
			return &corev1.Secret{}, nil
		})
	patches.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Update",
		func(_ *fake.FakeSecrets, ctx context.Context, secret *corev1.Secret, opts metav1.UpdateOptions) (*corev1.Secret, error) {
			return &corev1.Secret{}, nil
		})
	patches.ApplyMethod(reflect.TypeOf(&fake.FakeSecrets{}), "Delete",
		func(_ *fake.FakeSecrets, ctx context.Context, name string, opts metav1.DeleteOptions) error {
			return nil
		})
	patches.ApplyFunc(config.GetK8sCidr, func() *config.K8sCidr { return &config.K8sCidr{} })
	defer patches.Reset()

	c := customContainerMGR{
		kubeClient: &kubernetes.Clientset{},
		flight:     newImageVerifyFlight(),
		dataFlight: newSwrDataFlight(),
	}
	spec := &faasfunction.FunctionServiceSpec{
		Configuration: faasfunction.ConfigurationSpec{
			CustomContainerConfig: types.CustomContainerConfig{Image: "dockerhub.io/project/image:1.0"},
		},
	}
	pod, addOn, err := c.preparePodSpecAndAddOn(context.Background(), spec, common.PodRequest{}, &Pool{})
	assert.NotNil(t, pod)
	assert.NotNil(t, addOn)
	assert.NoError(t, err)
	assert.Equal(t, pod.Spec.RestartPolicy, corev1.RestartPolicyNever)

	addOn.preCreate(pod)
	addOn.postCreate(pod)
	addOn.rollback()
}

func Test_getRuntimeContainerID(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	var a args
	containerStatuses := corev1.ContainerStatus{
		Name:        utils.RuntimeContainerName,
		ContainerID: "docker://aaa",
	}
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{containerStatuses},
		},
	}
	a.pod = pod
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "aaa"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getRuntimeContainerID(tt.args.pod), "getRuntimeContainerID(%v)", tt.args.pod)
		})
	}
}

func Test_customContainerMGR_notifyWorker(t *testing.T) {
	type fields struct {
		workerClient    *fhttp.Client
		flight          *imageVerifyFlight
		nameSpace       string
		kubeClient      kubernetes.Interface
		config          config.PoolMgrConfig
		onDemandWaitMap sync.Map
	}
	type args struct {
		ctx context.Context
		pod *corev1.Pod
	}
	ctx, _ := context.WithTimeout(context.TODO(), 5*time.Second)
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case1",
			fields{
				workerClient: &fhttp.Client{},
				kubeClient:   &kubernetes.Clientset{},
			},
			args{
				pod: &corev1.Pod{
					Status: corev1.PodStatus{
						PodIP: "127.0.0.1",
					},
				},
				ctx: ctx,
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
						func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
							return nil
						}),
					gomonkey.ApplyFunc(fhttp.AcquireResponse, func() *fhttp.Response {
						return &fhttp.Response{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fhttp.Response{}), "StatusCode",
						func(_ *fhttp.Response) int {
							return fhttp.StatusOK
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Get",
						func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error) {
							return &corev1.Pod{Status: corev1.PodStatus{PodIP: "mock_IP", ContainerStatuses: []corev1.ContainerStatus{{Name: utils.RuntimeContainerName, ContainerID: "docker://aaa"}}}}, nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &customContainerMGR{
				workerClient:    tt.fields.workerClient,
				flight:          tt.fields.flight,
				nameSpace:       tt.fields.nameSpace,
				kubeClient:      tt.fields.kubeClient,
				config:          tt.fields.config,
				onDemandWaitMap: tt.fields.onDemandWaitMap,
			}
			patches := tt.patchesFunc()
			c.notifyWorker(tt.args.ctx, tt.args.pod)
			patches.ResetAll()
		})
	}
}

func Test_generateWorkerEnv(t *testing.T) {
	type args struct {
		workerEnv    []corev1.EnvVar
		spec         *faasfunction.FunctionServiceSpec
		isCheckpoint bool
	}
	tests := []struct {
		name string
		args args
		want []corev1.EnvVar
	}{
		{
			"case test generate worker env",
			args{
				[]corev1.EnvVar{},
				&faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						BusinessType: "CAE",
					},
				},
				true,
			},
			[]corev1.EnvVar{
				{
					Name:  constants.BusinessType,
					Value: "CAE",
				},
				{
					Name:  "RUNNING_MODE",
					Value: "CHECKPOINT",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateWorkerEnv(tt.args.workerEnv, tt.args.spec, tt.args.isCheckpoint); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generateWorkerEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_customContainerMGR_generateRuntimeEnv(t *testing.T) {
	type fields struct {
		workerClient    *fhttp.Client
		flight          *imageVerifyFlight
		nameSpace       string
		kubeClient      kubernetes.Interface
		config          config.PoolMgrConfig
		onDemandWaitMap sync.Map
	}
	type args struct {
		spec         *faasfunction.FunctionServiceSpec
		isCheckpoint bool
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        []corev1.EnvVar
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case test customContainerMGR generateRuntimeEnv",
			args: args{
				spec:         &faasfunction.FunctionServiceSpec{},
				isCheckpoint: true,
			},
			want: []corev1.EnvVar{
				corev1.EnvVar{
					Name:  "RUNNING_MODE",
					Value: "CHECKPOINT",
				}, corev1.EnvVar{
					Name:  constants.CheckpointRestoreOpts,
					Value: "-v",
				},
			},
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*customContainerMGR).getRuntimeEnv,
						func(_ *customContainerMGR, spec *faasfunction.FunctionServiceSpec) ([]corev1.EnvVar, error) {
							return []corev1.EnvVar{}, nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &customContainerMGR{
				workerClient:    tt.fields.workerClient,
				flight:          tt.fields.flight,
				nameSpace:       tt.fields.nameSpace,
				kubeClient:      tt.fields.kubeClient,
				config:          tt.fields.config,
				onDemandWaitMap: tt.fields.onDemandWaitMap,
			}
			_, err := c.generateRuntimeEnv(tt.args.spec, tt.args.isCheckpoint)
			if (err != nil) != tt.wantErr {
				t.Errorf("generateRuntimeEnv() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_customContainerRegcred_createSpecWithUserAuth(t *testing.T) {
	patches := gomonkey.ApplyFunc(localauth.Decrypt, func(s string) ([]byte, error) {
		if s == "" {
			return nil, errors.New("empty string input")
		}
		return []byte(s), nil
	})
	defer patches.Reset()

	tests := []struct {
		name   string
		config types.CustomContainerConfig

		expectedErr  bool
		expectedData map[string][]byte
	}{
		{
			name:   "invalid image addr",
			config: types.CustomContainerConfig{Image: "example.com", User: "some_user", Pwd: "some_pwd"},

			expectedErr: true,
		},
		{
			name:   "empty user",
			config: types.CustomContainerConfig{Image: "example.com/aaa/bbb:latest", User: "", Pwd: "some_pwd"},

			expectedErr: true,
		},
		{
			name:   "empty pwd",
			config: types.CustomContainerConfig{Image: "example.com/aaa/bbb:latest", User: "some_user", Pwd: ""},

			expectedErr: true,
		},
		{
			name:   "public image with empty user and pwd",
			config: types.CustomContainerConfig{Image: "example.com/aaa/bbb:latest", User: "", Pwd: ""},

			expectedErr:  false,
			expectedData: map[string][]byte{},
		},
		{
			name:   "valid config",
			config: types.CustomContainerConfig{Image: "example.com/aaa/bbb:latest", User: "some_user", Pwd: "some_pwd"},

			expectedErr: false,
			expectedData: map[string][]byte{
				corev1.DockerConfigJsonKey: []byte(`{"auths": {"example.com": {"auth": "c29tZV91c2VyOnNvbWVfcHdk"}}}`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := customContainerRegcred{spec: &faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{CustomContainerConfig: tt.config}}}
			secret, err := c.createSpecWithUserAuth()
			if tt.expectedErr {
				assert.NotNil(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
				assert.Equal(t, tt.expectedData, secret.Data)
			}
		})
	}
}

func TestCustomContainerMGR_createPodWithSpec(t *testing.T) {
	var errStr string
	patches := [...]*gomonkey.Patches{

		gomonkey.ApplyFunc(poderrormessage.ReportPodErrMsgToRedis, func(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
			podErrInfo poderrormessage.PodErrorInfo) {
			errStr = podErrInfo.ErrorMessage
			return
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
			func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
				return &fake.FakeCoreV1{}
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
			func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
				return &fake.FakePods{}
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
			func(_ *fake.FakePods, ctx context.Context,
				pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
				return &corev1.Pod{}, nil
			}),
		gomonkey.ApplyFunc((*customContainerMGR).waitPodReady,
			func(_ *customContainerMGR, parent context.Context, curPod *corev1.Pod,
				addOn customContainerAddOn) (*corev1.Pod, error) {
				return &corev1.Pod{}, errors.New("waitPodReady error")
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	customContainerMGR := &customContainerMGR{
		kubeClient: mockUtils.K8sFakeClient,
	}
	_, err := customContainerMGR.createPodWithSpec(createCustomContainerPodParam{
		ctx: context.Background(),
		spec: &faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"},
		},
	}, &corev1.Pod{}, combinedAddOn{})
	assert.Equal(t, err != nil, true)
	assert.Equal(t, errStr, "waitPodReady error")
}

func Test_printPodNotReadyStatus(t *testing.T) {
	podStatus := corev1.PodStatus{
		Phase:   corev1.PodPending,
		Reason:  "pending reason",
		Message: "pending message",
		Conditions: []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionTrue,
			},
		},
		InitContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "worker-init",
				Ready:        false,
				RestartCount: 0,
				State:        corev1.ContainerState{},
			}},
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "runtime",
				Ready:        false,
				RestartCount: 1,
				State:        corev1.ContainerState{},
			},
			{
				Name:         "worker",
				Ready:        true,
				RestartCount: 0,
				State:        corev1.ContainerState{},
			},
		},
	}

	wantStatus := corev1.PodStatus{
		Phase:   corev1.PodPending,
		Reason:  "pending reason",
		Message: "pending message",
		Conditions: []corev1.PodCondition{
			{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionTrue,
			},
		},
		InitContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "worker-init",
				Ready:        false,
				RestartCount: 0,
				State:        corev1.ContainerState{},
			}},
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "runtime",
				Ready:        false,
				RestartCount: 1,
				State:        corev1.ContainerState{},
			},
		},
	}
	wantBytes, _ := json.Marshal(wantStatus)
	type args struct {
		pod corev1.PodStatus
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "case_01 success",
			args: args{pod: podStatus},
			want: wantBytes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := printPodNotReadyStatus(tt.args.pod); !bytes.Equal(got, tt.want) {
				t.Errorf("printPodNotReadyStatus() = %s, want %s", got, tt.want)
			}
		})
	}
}

func Test_customContainerMGR_configNodeAffinityOfDisk(t *testing.T) {
	nodeMirroredVal1 := sync.Map{}
	nodeMirroredVal1.Store("node1", "100B")

	type fields struct {
		workerClient    *fhttp.Client
		flight          *imageVerifyFlight
		nameSpace       string
		kubeClient      kubernetes.Interface
		config          config.PoolMgrConfig
		onDemandWaitMap sync.Map
	}
	type args struct {
		funcSvcID string
		pod       *corev1.Pod
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		patchesFunc     mockUtils.PatchesFunc
		nodeMirroredVal sync.Map
		imageSize       int64
		wantErr         error
	}{
		{
			"test use default scheduler",
			fields{},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			},
			sync.Map{},
			0,
			nil,
		},
		{
			"test use default scheduler",
			fields{},
			args{
				funcSvcID: "funcSvcID1",
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetSchedulerWithDisk, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(cache.GetNodeNameByNodeIP, func(nodeIP string) string {
						return "node1"
					}),
				})
				return patches
			},
			nodeMirroredVal1,
			1000,
			utils.ErrNoNodesCanScheduler,
		},
		{
			"test success1",
			fields{},
			args{
				funcSvcID: "funcSvcID1",
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetSchedulerWithDisk, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(cache.GetNodeNameByNodeIP, func(nodeIP string) string {
						return "node1"
					}),
				})
				return patches
			},
			nodeMirroredVal1,
			10,
			nil,
		},
		{
			"test success2",
			fields{},
			args{
				funcSvcID: "funcSvcID1",
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetSchedulerWithDisk, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(cache.GetNodeNameByNodeIP, func(nodeIP string) string {
						return "node1"
					}),
				})
				return patches
			},
			nodeMirroredVal1,
			10,
			nil,
		},
		{
			"test success3",
			fields{},
			args{
				funcSvcID: "funcSvcID1",
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Affinity: &corev1.Affinity{},
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetSchedulerWithDisk, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(cache.GetNodeNameByNodeIP, func(nodeIP string) string {
						return "node1"
					}),
				})
				return patches
			},
			nodeMirroredVal1,
			10,
			nil,
		},
		{
			"test success4",
			fields{},
			args{
				funcSvcID: "funcSvcID1",
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Affinity: &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{},
						},
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetSchedulerWithDisk, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(cache.GetNodeNameByNodeIP, func(nodeIP string) string {
						return "node1"
					}),
				})
				return patches
			},
			nodeMirroredVal1,
			10,
			nil,
		},
		{
			"test success5",
			fields{},
			args{
				funcSvcID: "funcSvcID1",
				pod: &corev1.Pod{
					Spec: corev1.PodSpec{
						Affinity: &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{},
							},
						},
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetSchedulerWithDisk, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(cache.GetNodeNameByNodeIP, func(nodeIP string) string {
						return "node1"
					}),
				})
				return patches
			},
			nodeMirroredVal1,
			10,
			nil,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()

		tempDiskCache := cache.NodeMirroredDiskCache
		cache.NodeMirroredDiskCache = tt.nodeMirroredVal

		functionCache := cache.WorkerCache.CreateFunctionCacheByFuncMeta(tt.args.funcSvcID, &faasfunction.FunctionService{})

		functionCache.CustomImgSize = tt.imageSize

		c := &customContainerMGR{
			workerClient:    tt.fields.workerClient,
			flight:          tt.fields.flight,
			nameSpace:       tt.fields.nameSpace,
			kubeClient:      tt.fields.kubeClient,
			config:          tt.fields.config,
			onDemandWaitMap: tt.fields.onDemandWaitMap,
		}
		err := c.configNodeAffinityOfDisk(tt.args.funcSvcID, tt.args.pod)
		if err != tt.wantErr {
			t.Errorf("failed to test, want %v, get %v", tt.wantErr, err)
		}

		cache.WorkerCache.DeleteFunctionCache(tt.args.funcSvcID)

		cache.NodeMirroredDiskCache = tempDiskCache
		patches.ResetAll()
	}
}

func Test_customContainerMGR_configNodeAffinityOfImageCache(t *testing.T) {
	type fields struct {
		workerClient    *fhttp.Client
		flight          *imageVerifyFlight
		nameSpace       string
		kubeClient      kubernetes.Interface
		config          config.PoolMgrConfig
		onDemandWaitMap sync.Map
	}
	var f fields
	type args struct {
		funcURN string
		pod     *corev1.Pod
	}
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(cache.GetNodeNameByNodeIP, func(nodeIP string) string {
			return "node1"
		}),
	})
	cache.ImageNodeCache.Store("fakeURN1", []string{"127.0.0.1"})
	cache.ImageNodeCache.Store("fakeURN2", 5)

	var a1 args
	a1.funcURN = "fakeURN"
	var a2 args
	a2.funcURN = "fakeURN2"

	mockPod1 := &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}}}
	mockScheduler := corev1.PreferredSchedulingTerm{
		Weight: 100,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      corev1.LabelHostname,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"node1"},
			}},
		},
	}
	mockPod2 := &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{mockScheduler}}}}}

	var a3 args
	a3.funcURN = "fakeURN1"
	a3.pod = mockPod1
	var a4 args
	a4.funcURN = "fakeURN1"
	a4.pod = mockPod2
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"do not get configure affinity policies", f, a1},
		{"do not need to configure affinity policies", f, a2},
		{"configure affinity policies1", f, a3},
		{"configure affinity policies2", f, a4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &customContainerMGR{
				workerClient:    tt.fields.workerClient,
				flight:          tt.fields.flight,
				nameSpace:       tt.fields.nameSpace,
				kubeClient:      tt.fields.kubeClient,
				config:          tt.fields.config,
				onDemandWaitMap: tt.fields.onDemandWaitMap,
			}
			c.configNodeAffinityOfImageCache(tt.args.funcURN, tt.args.pod)
		})
	}
	patches.ResetAll()
}

func Test_getCustomJavaArgsWhiteList(t *testing.T) {
	convey.Convey("get custom white list", t, func() {
		convey.Convey("test empty white list", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{}, nil
			})
			defer patch.Reset()
			whitelist := getCustomJavaArgsWhiteList()
			convey.So(whitelist, convey.ShouldEqual, "")
		})
		convey.Convey("test exist white list", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{CustomJavaArgsWhiteList: []string{"-XX:CompressedClassSpaceSize", "-XX:ErrorFile"}}, nil
			})
			defer patch.Reset()
			whitelist := getCustomJavaArgsWhiteList()
			convey.So(whitelist, convey.ShouldEqual, "{\"-XX:CompressedClassSpaceSize\":true,\"-XX:ErrorFile\":true}")
		})
	})
}

func Test_customContainerMGR_waitPodReady(t *testing.T) {
	timeout := int64(1)

	addOn := &simpleAddOn{
		preCreateFn: func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = append(p.Spec.ImagePullSecrets, corev1.LocalObjectReference{
				Name: "",
			})
		},
	}

	c := &customContainerMGR{
		workerClient: nil,
		flight:       &imageVerifyFlight{},
		nameSpace:    "default",
		kubeClient:   mockUtils.K8sFakeClient,
		config: config.PoolMgrConfig{
			CustomContainerConfig: config.CustomContainerConfig{
				WaitReadyTimeout:     &timeout,
				UnschedulableTimeout: &timeout,
			},
		},
		onDemandWaitMap: sync.Map{},
	}

	waiter := utils.NewPodWaiter()

	waiter.NotifyStatus(utils.PodStatus{Pod: &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
	}, Err: nil})

	_, err := c.waitPodReady(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
	}, addOn, waiter)

	if err != nil {
		t.Errorf("failed to Test_customContainerMGR_waitPodReady")
	}
}

func Test_configAntiAffinityForReservedIns(t *testing.T) {
	tests := []struct {
		name string
		urn  string
		pod  *corev1.Pod

		expected       *corev1.Affinity
		expectedLabels map[string]string
	}{
		{
			name: "test1",
			urn:  "sn:cn:yrk:123456789:function:0@default@test:latest",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},

			expected: &corev1.Affinity{
				PodAntiAffinity: &corev1.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
						{
							Weight: 20,
							PodAffinityTerm: corev1.PodAffinityTerm{
								TopologyKey: k8sHostNameKey,
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"urnHash": utils.FnvHash("sn:cn:yrk:123456789:function:0@default@test:latest"),
									},
								},
							},
						},
					},
				},
			},
			expectedLabels: map[string]string{
				"urnHash": utils.FnvHash("sn:cn:yrk:123456789:function:0@default@test:latest"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configAntiAffinityForReservedIns(createCustomContainerPodParam{
				spec: &faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{FunctionVersionURN: tt.urn}},
			}, tt.pod)
			assert.Equal(t, tt.expected, tt.pod.Spec.Affinity, tt.name)
			assert.Equal(t, tt.expectedLabels, tt.pod.Labels, tt.name)
		})
	}
}

func Test_configPodRequiredSchedulePolicyWithLabels(t *testing.T) {
	tests := []struct {
		name            string
		urn             string
		pod             *corev1.Pod
		funSvcID        string
		expected        *corev1.NodeAffinity
		expectedLabels  map[string]string
		storedMetaData  *faasfunction.FunctionService
		podScheduleInfo cache.PodScheduleInfo
	}{
		{
			name:     "podScheduleInfo.TaggedScheduleLabel is true",
			funSvcID: "test-1",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{},
				},
			},
			podScheduleInfo: cache.PodScheduleInfo{
				TaggedScheduleLabel:    true,
				AffinityNodes:          []string{"node3"},
				PreferredAffinityNodes: []string{"node4"},
				AntiAffinityNodes:      []string{"node2"},
			},

			expected: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      k8sHostNameKey,
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"node3"},
								},
								{
									Key:      k8sHostNameKey,
									Operator: corev1.NodeSelectorOpNotIn,
									Values:   []string{"node2"},
								},
							},
							MatchFields: nil,
						},
					},
				},
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
					{
						Weight: 60,
						Preference: corev1.NodeSelectorTerm{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key:      k8sHostNameKey,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"node4"},
							}},
							MatchFields: nil,
						},
					},
				},
			},
		},

		{
			name:     "podScheduleInfo.TaggedScheduleLabel is false",
			funSvcID: "test-1",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{},
				},
			},
			podScheduleInfo: cache.PodScheduleInfo{
				TaggedScheduleLabel:    false,
				AffinityNodes:          []string{"node3"},
				PreferredAffinityNodes: []string{"node4"},
				AntiAffinityNodes:      []string{"node2"},
			},

			expected: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      k8sHostNameKey,
									Operator: corev1.NodeSelectorOpNotIn,
									Values:   []string{"node2"},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPodSchedulePolicyWithTaggedLabels(tt.pod, tt.podScheduleInfo)
			assert.Equal(t, *tt.expected.RequiredDuringSchedulingIgnoredDuringExecution, *tt.pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution, tt.name)
			assert.Equal(t, tt.expected.PreferredDuringSchedulingIgnoredDuringExecution, tt.pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, tt.name)
		})
	}
}

func Test_swrDataFlight_do(t *testing.T) {
	type fields struct {
		m   map[string]*swrDataFlightItem
		mux sync.Mutex
	}
	type args struct {
		secretPath string
		fn         func() ([]byte, error)
	}
	var f fields
	var a args
	f.m = make(map[string]*swrDataFlightItem)
	f.m["secretPath"] = &swrDataFlightItem{data: []byte{1}, result: nil}
	a.secretPath = "secretPath"
	a.fn = func() ([]byte, error) {
		return []byte{1}, nil
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{"test get secretPath", f, a, []byte{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &swrDataFlight{
				m:   tt.fields.m,
				mux: tt.fields.mux,
			}
			got, _ := f.do(tt.args.secretPath, tt.args.fn)
			assert.Equalf(t, tt.want, got, "do(%v, %v)", tt.args.secretPath, tt.args.fn)
		})
	}
}

func TestCustomContainerMGR_waitPodReadyRollback(t *testing.T) {
	addOn := &simpleAddOn{
		preCreateFn: func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = append(p.Spec.ImagePullSecrets, corev1.LocalObjectReference{
				Name: "",
			})
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
	}
	timeout := int64(1)
	c := &customContainerMGR{
		workerClient: nil,
		flight:       &imageVerifyFlight{},
		nameSpace:    "default",
		kubeClient:   mockUtils.K8sFakeClient,
		config: config.PoolMgrConfig{
			CustomContainerConfig: config.CustomContainerConfig{
				WaitReadyTimeout:     &timeout,
				UnschedulableTimeout: &timeout,
			},
		},
		onDemandWaitMap: sync.Map{},
	}

	t.Run("success: not force delete", func(t *testing.T) {
		var deleteOption metav1.DeleteOptions

		patches := gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Get",
			func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error) {
				return &corev1.Pod{Status: corev1.PodStatus{PodIP: "mock_IP", ContainerStatuses: []corev1.ContainerStatus{{Name: utils.RuntimeContainerName, ContainerID: "docker://aaa"}}}}, nil
			})
		patches.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete", func(_ *fake.FakePods, _ context.Context, _ string, option metav1.DeleteOptions) error {
			deleteOption = option
			return nil
		})
		defer patches.Reset()

		c.waitPodReadyRollback(pod, addOn, nil, false)
		assert.Nil(t, deleteOption.GracePeriodSeconds)
	})

	t.Run("success: force delete", func(t *testing.T) {
		var deleteOption metav1.DeleteOptions

		patches := gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Get",
			func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error) {
				return &corev1.Pod{Status: corev1.PodStatus{PodIP: "mock_IP", ContainerStatuses: []corev1.ContainerStatus{{Name: utils.RuntimeContainerName, ContainerID: "docker://aaa"}}}}, nil
			})
		patches.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete", func(_ *fake.FakePods, _ context.Context, _ string, option metav1.DeleteOptions) error {
			deleteOption = option
			return nil
		})
		defer patches.Reset()

		c.waitPodReadyRollback(pod, addOn, nil, true)
		assert.NotNil(t, deleteOption.GracePeriodSeconds)
		assert.Equal(t, *deleteOption.GracePeriodSeconds, int64(0))
	})
}
