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

package utils

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	kmsModel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/kms/v2/model"
	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"
	. "github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gotest.tools/assert"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	informerv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	appsv1fake "k8s.io/client-go/kubernetes/typed/apps/v1/fake"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/kms"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/config"
	interfacelog "functioncore/pkg/worker-manager/log"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	mockutils "functioncore/test/utils"
)

func TestDeepCopy(t *testing.T) {
	type args struct {
		src interface{}
		dst interface{}
	}
	var a args
	a.dst = nil
	var b args
	b.dst = "1"
	b.src = nil
	var c args
	c.src = map[int]string{}
	c.dst = "1"
	var d args
	d.src = "1"
	d.dst = map[int]string{}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
		{"case2", b, true},
		{"case3", c, true},
		{"case4", d, true},
	}
	fmt.Print("2")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := DeepCopy(tt.args.src, tt.args.dst); (err != nil) != tt.wantErr {
				t.Errorf("DeepCopy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFnvHash(t *testing.T) {
	type args struct {
		s string
	}
	var a args
	a.s = ""
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "2166136261"},
	}
	fmt.Print("4")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FnvHash(tt.args.s); got != tt.want {
				t.Errorf("FnvHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTransferFunctionInfoMeta(t *testing.T) {
	source := config.FunctionMetaInfo{}
	target := faasfunction.FunctionService{}

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(GetFunctionInfoByURN, func(urn string) FunctionInfo {
			return FunctionInfo{}
		}),
		gomonkey.ApplyFunc(CrNameByURN, func(args ...string) string {
			return ""
		}),
		gomonkey.ApplyFunc(FunctionNameSpace, func() string {
			return ""
		}),
		gomonkey.ApplyFunc(getFuncMountConfig, func(source *config.FunctionMetaInfo) faasfunction.FuncMountConfig {
			return faasfunction.FuncMountConfig{}
		}),
	}

	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()

	transferFunctionInfoMeta(&source, &target)
}

func TestGetKubernetesClient(t *testing.T) {
	convey.Convey("TestTransferRepoMetaToFunctionService", t, func() {
		convey.Convey("success", func() {
			cfg := &config.FaasConfig{}
			patches := ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return cfg, nil
			})
			defer patches.Reset()
			interfacelog.CreateInterLogger("/home/sn/log")
			tests := []struct {
				name  string
				want  string
				want1 string
			}{
				{"case1", "", ""},
			}
			fmt.Print("5")
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					fmt.Print("============tag1==============\n")
					got := GetKubernetesClient()
					fmt.Print("============tag2==============\n")
					if !reflect.DeepEqual("", tt.want1) {
						t.Errorf("GetKubernetesClient() got = %v, want %v", got, tt.want1)
					}
				})
			}
		})
	})

}

func TestGetMd5String(t *testing.T) {
	type args struct {
		s string
	}
	var a args
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "d41d8cd98f00b204e9800998ecf8427e"},
	}
	fmt.Print("6")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetMd5String(tt.args.s); got != tt.want {
				t.Errorf("GetMd5String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHomeJudge(t *testing.T) {
	type args struct {
		home *string
	}
	var a args
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	fmt.Print("9")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
}

func TestSetTraceID(t *testing.T) {
	type args struct {
		target  *faasfunction.FunctionService
		traceID string
	}
	var a args
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	fmt.Print("12")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
}

func TestTransferRepoMetaToFunctionService(t *testing.T) {
	convey.Convey("TestTransferRepoMetaToFunctionService", t, func() {
		convey.Convey("success", func() {
			cfg := &config.FaasConfig{}
			cfg.BusProtocolType = "tcp"
			patches2 := ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return cfg, nil
			})
			defer patches2.Reset()
			type args struct {
				source *config.FunctionMetaInfo
				target *faasfunction.FunctionService
			}
			var a args
			a.source = &config.FunctionMetaInfo{}
			a.target = &faasfunction.FunctionService{}

			tests := []struct {
				name    string
				args    args
				wantErr bool
			}{
				{"case1", a, false},
			}
			fmt.Print("13")
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					if err := TransferRepoMetaToFunctionService(tt.args.source, tt.args.target); (err != nil) != tt.wantErr {
						t.Errorf("TransferRepoMetaToFunctionService() error = %v, wantErr %v", err, tt.wantErr)
					}
				})
			}
		})
	})
}

func TestUniqueID(t *testing.T) {
	type args struct {
		len int
	}
	var a args
	a.len = 45
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, ""},
	}
	fmt.Print("14")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UniqueID(tt.args.len); "" != tt.want {
				t.Errorf("UniqueID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetCRSignature_ignore_AutoScalerConfig(t *testing.T) {
	funcMetaA := faasfunction.FunctionService{
		Spec: faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{
				FunctionName: "aaaa",
				TenantID:     "bbbbb",
				AutoScalerConfig: faasfunction.AutoScalerConfig{
					Name:             "some name",
					Type:             "some type",
					IsScaleUp:        true,
					Duration:         "some duration",
					RepeatInterval:   "some interval",
					Tolerance:        100,
					IdleMode:         true,
					MinimumScale:     20,
					MaximumScale:     40,
					InstanceType:     "some instance type",
					MetricsCondition: "some metrics condition",
					MetricsType:      "some metrics type",
					ProjectID:        "project id",
					ScaleStep:        10,
					WorkloadID:       "worker load id",
				},
			},
		},
	}
	funcMetaB := faasfunction.FunctionService{
		Spec: faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{
				FunctionName: "aaaa",
				TenantID:     "bbbbb",
			},
		},
	}
	Equal(t, GetCRSignature(&funcMetaA), GetCRSignature(&funcMetaB))
}

func Test_GetCRSignature(t *testing.T) {
	convey.Convey("Test_GetCRSignature", t, func() {
		convey.Convey("success", func() {
			var env []corev1.EnvVar
			env1 := corev1.EnvVar{
				Name:  "ENVIRONMENT_KEY",
				Value: "aaaaaaaa",
			}
			env2 := corev1.EnvVar{
				Name:  "BUCKET_KEY",
				Value: "bbbbbb",
			}
			env = append(env, env1)
			env = append(env, env2)
			var functionMeta = faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Env:          env,
						FunctionName: "aaaa",
						TenantID:     "bbbbb",
					},
				},
			}
			crSignature1 := GetCRSignature(&functionMeta)
			crSignature2 := GetCRSignature(&functionMeta)
			log.Printf("%s %s", crSignature1, crSignature2)
			convey.So(crSignature1, convey.ShouldEqual, crSignature2)
		})
	})
}

// GetMd5String Get Md5 String
func GetMd5String(s string) string {
	h := md5.New()
	_, err := h.Write([]byte(s))
	if err != nil {
		return err.Error()
	}
	return hex.EncodeToString(h.Sum(nil))
}

// UniqueID generate Unique ID
func UniqueID(len int) string {
	const size = 32
	b := make([]byte, size, size)

	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}

	// 32 is the length of Unique ID
	if len > 32 {
		len = 32
	}

	return GetMd5String(base64.URLEncoding.EncodeToString(b))[:len]
}

func TestGetUserAgencyFromCacheMap(t *testing.T) {
	agency := `{"accessKey": "ak","secretKey": "sk","token": "token","akSkExpireTime": "2021-06-11T23:00:00.101073Z","tokenExpireTime": "2021-06-12T13:23:00.483000Z"}`
	tenant := "tenant"
	domainID := "domainID"
	agencyID := "agencyID"
	key := WatchPathPrefixWithAgency + tenant + "/domain/" + domainID + "/agency/" + agencyID
	updateUserAgencyCacheMap(key, []byte(agency), etcd3.Meta)
	convey.Convey("TestGetUserAgencyFromCacheMap", t, func() {
		convey.Convey("success", func() {
			userAgency := GetUserAgencyFromCacheMap(tenant, domainID, agencyID)
			convey.So(userAgency.SecretKey, convey.ShouldEqual, "sk")
		})
	})
}

func TestParseUserAgencyEvent(t *testing.T) {
	agency := `{"accessKey": "ak","secretKey": "sk","token": "token","akSkExpireTime": "2021-06-11T23:00:00.101073Z","tokenExpireTime": "2021-06-12T13:23:00.483000Z"}`
	tenant := "tenant"
	domainID := "domainID"
	agencyID := "agencyID"
	key := WatchPathPrefixWithAgency + tenant + "/domain/" + domainID + "/agency/" + agencyID
	convey.Convey("TestParseUserAgencyEvent", t, func() {
		convey.Convey("put", func() {
			event := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   key,
				Value: []byte(agency),
			}
			parseUserAgencyEvent(event, etcd3.Meta)
			userAgency := GetUserAgencyFromCacheMap(tenant, domainID, agencyID)
			convey.So(userAgency.SecretKey, convey.ShouldEqual, "sk")
		})
		convey.Convey("delete", func() {
			event := &etcd3.Event{
				Type: etcd3.DELETE,
				Key:  key,
			}
			parseUserAgencyEvent(event, etcd3.Meta)
			userAgency := GetUserAgencyFromCacheMap(tenant, domainID, agencyID)
			convey.So(userAgency.SecretKey, convey.ShouldEqual, "")
		})
	})
}

func TestWatcherUserAgency(t *testing.T) {
	convey.Convey("TestWatcherUserAgency", t, func() {
		convey.Convey("TestWatcherUserAgency", func() {
			etcdclient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
			patches := [...]*Patches{
				ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return etcdclient
				}),
				ApplyMethod(reflect.TypeOf(etcdclient), "GetResponse",
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{}, nil
					}),
				ApplyMethod(reflect.TypeOf(etcdclient), "Watch",
					func(_ *etcd3.EtcdClient, ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
						ch := make(chan *etcd3.Event)
						return &etcd3.EtcdWatchChan{ResultChan: ch}
					}),
				ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
					ch := make(chan time.Time, 1)
					ch <- time.Time{}
					return &time.Ticker{C: ch}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			stopChan := make(chan struct{}, 1)
			close(stopChan)
			err := WatcherUserAgency(stopChan)
			convey.So(err, convey.ShouldBeNil)
			time.Sleep(50 * time.Millisecond)
		})
	})
}

func TestIsWorkerPod(t *testing.T) {
	pod := &corev1.Pod{}
	label := map[string]string{"isPoolPod": "true"}
	pod.Labels = label
	assert.Equal(t, IsWorkerPod(pod), true)
}

func TestIsPoolPod(t *testing.T) {
	pod := &corev1.Pod{}
	label := map[string]string{"isPoolPod": "true"}
	pod.Labels = label
	assert.Equal(t, IsPoolPod(pod), true)
}

func TestGetNodeIPAddress(t *testing.T) {
	node := &corev1.Node{}
	nodeInternalAddress := corev1.NodeAddress{Address: "1.1.1.1", Type: corev1.NodeInternalIP}
	node.Status.Addresses = append(node.Status.Addresses, nodeInternalAddress)
	assert.Equal(t, GetNodeIPAddress(node), "1.1.1.1")
}

func TestTimeoutRetry(t *testing.T) {
	tests := []struct {
		errorStr string
		expect   bool
	}{
		{
			"timeout",
			true,
		},
		{
			"test",
			false,
		},
	}

	for _, test := range tests {
		assert.Equal(t, TimeoutRetry(test.errorStr), test.expect)
	}
}

func TestGetRandomIndex(t *testing.T) {
	for n := 1; n < 100; n++ {
		if GetRandomIndex(n) > n {
			t.Errorf("Random number exceeds maximum %d", n)
		}
	}
}

func TestMakeWorkerInsInfo(t *testing.T) {
	pod := &corev1.Pod{}
	pod.Name = "pod-1"
	pod.Labels = make(map[string]string)
	pod.Labels[LabelBusinessID] = "businessID"
	pod.Labels[LabelTenantID] = "tenantID"
	pod.Labels[LabelFuncNamePrefix] = "0"
	pod.Labels[LabelServiceID] = "serviceID"
	pod.Labels[LabelShortFuncName] = "shortFuncName"
	pod.Labels[LabelFunctionVersion] = "version"
	pod.Labels[LabelCreatorFuncName] = "creatorFuncName"
	pod.Labels[LabelCreatorFunctionVersion] = "creatorFuncVersion"
	pod.Labels[LabelCreatorStateID] = "creatorStateID"
	pod.Annotations = make(map[string]string)
	pod.Annotations[OwnerIPKey] = "1.2.3.4"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.HostIP = "1.2.3.4"

	expectedInstance := v1alpha1.WorkerInstance{
		InstanceID: utils.AzEnv() + "-#-" + pod.Name,
		PodName:    pod.Name,
		IP:         pod.Status.PodIP,
		Port:       "8080",
		Status:     "ready",
		P2pPort:    P2pPort,
		NodeIP:     pod.Status.HostIP,
		NodePort:   NodePort,
		NodeName:   pod.Spec.NodeName,
		Applier:    pod.Labels["applier"],
		OwnerIP:    "1.2.3.4",
		BusinessID: "businessID",
		TenantID:   "tenantID",
		FuncName:   "0@serviceID@shortFuncName",
		Version:    "version",
		Creator: pod.Labels[LabelCreatorFuncName] + ":" + pod.Labels[LabelCreatorFunctionVersion] + ":" +
			pod.Labels[LabelCreatorStateID],
		IsRunning: true,
	}
	got := MakeWorkerInsInfo(pod)
	if !reflect.DeepEqual(got, expectedInstance) {
		t.Errorf("MakeWorkerInsInfo() = %#v, want %#v", got, expectedInstance)
	}
}

func TestIsNodeReady(t *testing.T) {
	node := &corev1.Node{}
	assert.Equal(t, IsNodeReady(node), false)
	node.Status.Conditions = make([]corev1.NodeCondition, 4)
	node.Status.Conditions[3].Status = "True"
	node.Status.Conditions[3].Type = "Ready"
	assert.Equal(t, IsNodeReady(node), true)
	node.Status.Conditions[3].Status = "False"
	node.Status.Conditions[3].Type = "Ready"
	assert.Equal(t, IsNodeReady(node), false)
	node.Status.Conditions[3].Status = "True"
	node.Status.Conditions[3].Type = "NotReady"
	assert.Equal(t, IsNodeReady(node), false)
}

func TestGetWorkerManagerDeployUID(t *testing.T) {
	mockDeploy := &v1.Deployment{}
	mockDeploy.UID = "mock-uid"
	tests := []struct {
		name   string
		deploy *v1.Deployment
		want   types.UID
	}{
		{
			name: "test001",
			want: "",
		},
		{
			name:   "test002",
			deploy: mockDeploy,
			want:   "mock-uid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workerManagerDeploy = tt.deploy
			if got := GetWorkerManagerDeployUID(); got != tt.want {
				t.Errorf("GetWorkerManagerDeployUID() = %v, want %v", got, tt.want)
			}
			workerManagerDeploy = nil
		})
	}
}

func TestOwnedByFunctionService(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	mockPod1 := &corev1.Pod{}
	mockPod1.ObjectMeta = metav1.ObjectMeta{}
	mockPod1.Annotations = map[string]string{
		WorkerManagerDeployName: "mock",
	}
	mockPod1.Labels = map[string]string{
		LabelBusinessID:      "yrk",
		LabelTenantID:        "fb6a00ff7ae54a5fbb8ff855d0841d00",
		LabelFuncNamePrefix:  "0",
		LabelServiceID:       "default",
		LabelShortFuncName:   "puresleep-java-10ms",
		LabelFunctionVersion: "latest",
	}
	tests := []struct {
		name      string
		args      args
		patchFunc mockutils.PatchesFunc
		want      string
		want1     bool
	}{
		{
			name: "test001",
			args: args{
				pod: &corev1.Pod{},
			},
			patchFunc: func() mockutils.PatchSlice {
				return mockutils.InitPatchSlice()
			},
			want:  "",
			want1: false,
		},
		{
			name: "test002",
			args: args{
				pod: mockPod1,
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&mockPod1.ObjectMeta), "GetOwnerReferences",
						func(_ *metav1.ObjectMeta) []metav1.OwnerReference {
							return []metav1.OwnerReference{
								{Name: WorkerManagerDeployName},
							}
						}),
				})
				return patches
			},
			want:  CrName("yrk", "fb6a00ff7ae54a5fbb8ff855d0841d00", "0@default@puresleep-java-10ms", "latest"),
			want1: true,
		},
		{
			name: "test003",
			args: args{
				pod: mockPod1,
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&mockPod1.ObjectMeta), "GetOwnerReferences",
						func(_ *metav1.ObjectMeta) []metav1.OwnerReference {
							return []metav1.OwnerReference{
								{Name: "mockName", APIVersion: FaasAPIVersion, Kind: FaasAPIName},
							}
						}),
				})
				return patches
			},
			want:  "mockName",
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			got, got1 := OwnedByFunctionService(tt.args.pod)
			if got != tt.want {
				t.Errorf("OwnedByFunctionService() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("OwnedByFunctionService() got1 = %v, want %v", got1, tt.want1)
			}
			fmt.Println(tt.want)
			patches.ResetAll()
		})
	}
}

func TestSplitFuncName(t *testing.T) {
	type args struct {
		funcName string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
		want2 string
	}{
		{
			name: "test001",
			args: args{
				funcName: "",
			},
			want:  "",
			want1: "",
			want2: "",
		},
		{
			name: "test002",
			args: args{
				funcName: "0@123@456",
			},
			want:  "0",
			want1: "123",
			want2: "456",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := SplitFuncName(tt.args.funcName)
			if got != tt.want {
				t.Errorf("SplitFuncName() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("SplitFuncName() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("SplitFuncName() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func TestIsPodReady(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test001",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: &metav1.Time{},
					},
				},
			},
			want: false,
		},
		{
			name: "test002",
			args: args{
				pod: &corev1.Pod{
					Status: corev1.PodStatus{
						Conditions: []corev1.PodCondition{
							{
								Type:   corev1.PodReady,
								Status: corev1.ConditionTrue,
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "test003",
			args: args{
				pod: &corev1.Pod{
					Status: corev1.PodStatus{
						Conditions: []corev1.PodCondition{
							{
								Type:   corev1.PodReady,
								Status: corev1.ConditionFalse,
							},
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPodReady(tt.args.pod); got != tt.want {
				t.Errorf("IsPodReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetPodNameFromInstanceID(t *testing.T) {
	type args struct {
		instanceID string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				instanceID: "defaultaz-#-pool22-300-128-fusion-5cb9d5b664-2b942",
			},
			want: "pool22-300-128-fusion-5cb9d5b664-2b942",
		},
		{
			name: "test002",
			args: args{
				instanceID: "",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPodNameFromInstanceID(tt.args.instanceID); got != tt.want {
				t.Errorf("GetPodNameFromInstanceID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsReservedInstance(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test001",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"applier": constants.WorkerManagerApplier,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "test002",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"applier": "functiontask",
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsReservedInstance(tt.args.pod); got != tt.want {
				t.Errorf("IsReservedInstance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetNodeIPAddress1(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				node: &corev1.Node{
					Status: corev1.NodeStatus{
						Addresses: []corev1.NodeAddress{
							{
								Type:    corev1.NodeInternalIP,
								Address: "mock-address",
							},
						},
					},
				},
			},
			want: "mock-address",
		},
		{
			name: "test001",
			args: args{
				node: &corev1.Node{
					Status: corev1.NodeStatus{
						Addresses: []corev1.NodeAddress{
							{
								Type:    corev1.NodeHostName,
								Address: "mock-address",
							},
						},
					},
				},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetNodeIPAddress(tt.args.node); got != tt.want {
				t.Errorf("GetNodeIPAddress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitETCDClient(t *testing.T) {
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name      string
		args      args
		patchFunc mockutils.PatchesFunc
		wantErr   bool
	}{
		{
			name: "test001",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return nil, errors.New("get config error")
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test003",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			if err := InitETCDClient(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("InitETCDClient() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestMakeWorkerInsWithRequest(t *testing.T) {
	type args struct {
		pod          *corev1.Pod
		requestInfo  *config.RequestInfo
		crSignature  string
		businessType string
	}
	var a args
	mockPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1-500-500",
		},
	}
	requestInfo := &config.RequestInfo{
		CreationTime: 100,
	}
	a.requestInfo = requestInfo
	a.crSignature = "crSignature"
	a.pod = mockPod
	tests := []struct {
		name string
		args args
		want v1alpha1.WorkerInstance
	}{
		{"case1", a, v1alpha1.WorkerInstance{PodName: "pod1-500-500", Signature: "crSignature", CreationTime: 100, CPU: 500, Memory: 500}},
		{
			name: "cae case",
			args: args{
				requestInfo:  requestInfo,
				crSignature:  "crSignature",
				pod:          mockPod,
				businessType: "CAE",
			},
			want: v1alpha1.WorkerInstance{PodName: "pod1-500-500", Signature: "crSignature", CreationTime: 100, CPU: 0, Memory: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeWorkerInsWithRequest(tt.args.pod, tt.args.requestInfo, tt.args.crSignature, tt.args.businessType)
			if !reflect.DeepEqual(got.Signature, tt.want.Signature) || !reflect.DeepEqual(got.CreationTime, tt.want.CreationTime) || got.CPU != tt.want.CPU || got.Memory != tt.want.Memory {
				t.Errorf("MakeWorkerInsWithRequest() = %v, want %v", got, tt.want)
			}

		})
	}
}

func TestBuildInstanceByWorkerIns(t *testing.T) {
	type args struct {
		workerIns *v1alpha1.WorkerInstance
	}
	var a args
	instance := &v1alpha1.WorkerInstance{}
	instance.IP = "127.0.0.1"
	a.workerIns = instance
	commonInstance := &commontypes.Instance{}
	commonInstance.IP = "127.0.0.1"
	tests := []struct {
		name string
		args args
		want *commontypes.Instance
	}{
		{"case1", a, commonInstance},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildInstanceByWorkerIns(tt.args.workerIns); !reflect.DeepEqual(got.IP, tt.want.IP) {
				t.Errorf("BuildInstanceByWorkerIns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPackRequestInfo(t *testing.T) {
	type args struct {
		functionSvc *faasfunction.FunctionService
	}
	var a args
	functionSvc := &faasfunction.FunctionService{}
	a.functionSvc = functionSvc
	info := &config.RequestInfo{}
	info.ForceDeploy = "false"
	tests := []struct {
		name string
		args args
		want *config.RequestInfo
	}{
		{"case1", a, info},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PackRequestInfo(tt.args.functionSvc, "", ""); !reflect.DeepEqual(got.ForceDeploy, tt.want.ForceDeploy) {
				t.Errorf("PackRequestInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapToString(t *testing.T) {
	type args struct {
		mapValue map[string]int64
	}
	var a args
	value := map[string]int64{}
	value["aaa"] = 1
	a.mapValue = value
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "aaa-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapToString(tt.args.mapValue); got != tt.want {
				t.Errorf("mapToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFuncKey(t *testing.T) {
	type args struct {
		tenantID     string
		serviceID    string
		functionName string
		version      string
	}
	var a args
	a.version = "version"
	a.functionName = "functionName"
	a.serviceID = "serviceID"
	a.tenantID = "tenantID"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "tenantID@serviceID@functionName@version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFuncKey(tt.args.tenantID, tt.args.serviceID, tt.args.functionName, tt.args.version); got != tt.want {
				t.Errorf("GetFuncKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSetInstanceMetaFromETCD -
func TestSetInstanceMetaFromETCD(t *testing.T) {
	convey.Convey("TestSetInstanceMetaFromETCD", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		defer func() {
			patch.Reset()
		}()
		convey.Convey("functionKeyErr", func() {
			convey.So(setInstanceMetaFromETCD("test", etcd3.Meta, &config.FunctionMetaInfo{}), convey.ShouldBeError)
		})
		convey.Convey("getValueFromEtcdErr", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{}, errors.New("test err")
			})
			convey.So(setInstanceMetaFromETCD(
				"/instances/business/yrk/cluster/cluster/tenant/tenant/function/func/version/version", etcd3.Meta,
				&config.FunctionMetaInfo{}), convey.ShouldBeNil)
			patch.Reset()
		})
		convey.Convey("UnmarshalErr", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{""}, nil
			})
			convey.So(setInstanceMetaFromETCD(
				"/instances/business/yrk/cluster/cluster/tenant/tenant/function/func/version/version", etcd3.Meta,
				&config.FunctionMetaInfo{}), convey.ShouldNotBeNil)
			patch.Reset()
		})
		convey.Convey("setOK", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{`{"instanceMetaData":{"minInstance":-1,"maxInstance":-1}}`}, nil
			})
			convey.So(setInstanceMetaFromETCD(
				"/instances/business/yrk/cluster/cluster/tenant/tenant/function/func/version/version", etcd3.Meta,
				&config.FunctionMetaInfo{}), convey.ShouldBeNil)
			patch.Reset()
		})
	})
}

// TestGetMetaInfoFromEtcd -
func TestGetMetaInfoFromEtcd(t *testing.T) {
	convey.Convey("TestGetMetaInfoFromEtcd", t, func() {
		convey.Convey("getValueFromEtcdErr", func() {
			patch := gomonkey.ApplyFunc(getValuesFromETCD, func(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
				return nil, etcd3.Meta, errors.New("test err")
			})
			fs, _, _, err := GetMetaInfoFromEtcd("test")
			convey.So(fs, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			patch.Reset()
		})
		convey.Convey("funcMetaNotfound", func() {
			patch := gomonkey.ApplyFunc(getValuesFromETCD, func(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
				return []string{}, etcd3.Meta, nil
			})
			fs, _, _, err := GetMetaInfoFromEtcd("test")
			convey.So(fs, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			patch.Reset()
		})
		convey.Convey("UnmarshalErr", func() {
			patch := gomonkey.ApplyFunc(getValuesFromETCD, func(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
				return []string{""}, etcd3.Meta, nil
			})
			fs, _, _, err := GetMetaInfoFromEtcd("test")
			convey.So(fs, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			patch.Reset()
		})
		convey.Convey("setInstanceMetaFromETCDErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(getValuesFromETCD, func(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
					return []string{`{}`}, etcd3.Meta, nil
				}),
				gomonkey.ApplyFunc(setInstanceMetaFromETCD, func(functionKey string, source etcd3.EtcdType,
					functionInfo *config.FunctionMetaInfo) error {
					return errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			fs, _, _, err := GetMetaInfoFromEtcd("test")
			convey.So(fs, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("ValidateStructErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(getValuesFromETCD, func(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
					return []string{`{}`}, etcd3.Meta, nil
				}),
				gomonkey.ApplyFunc(setInstanceMetaFromETCD, func(functionKey string, source etcd3.EtcdType,
					functionInfo *config.FunctionMetaInfo) error {
					return nil
				}),
				gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
					return false, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			fs, _, _, err := GetMetaInfoFromEtcd("test")
			convey.So(fs, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("TransferRepoMetaToFunctionServiceErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(getValuesFromETCD, func(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
					return []string{`{}`}, etcd3.Meta, nil
				}),
				gomonkey.ApplyFunc(setInstanceMetaFromETCD, func(functionKey string, source etcd3.EtcdType,
					functionInfo *config.FunctionMetaInfo) error {
					return nil
				}),
				gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
					return true, nil
				}),
				gomonkey.ApplyFunc(TransferRepoMetaToFunctionService, func(source *config.FunctionMetaInfo,
					target *faasfunction.FunctionService) error {
					return errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			fs, _, _, err := GetMetaInfoFromEtcd("test")
			convey.So(fs, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("getFuncMetaOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(getValuesFromETCD, func(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
					return []string{`{}`}, etcd3.Meta, nil
				}),
				gomonkey.ApplyFunc(setInstanceMetaFromETCD, func(functionKey string, source etcd3.EtcdType,
					functionInfo *config.FunctionMetaInfo) error {
					return nil
				}),
				gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
					return true, nil
				}),
				gomonkey.ApplyFunc(TransferRepoMetaToFunctionService, func(source *config.FunctionMetaInfo,
					target *faasfunction.FunctionService) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			fs, _, _, err := GetMetaInfoFromEtcd("test")
			convey.So(fs, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

// TestSetRuntimeMeta -
func TestSetRuntimeMeta(t *testing.T) {
	convey.Convey("TestSetRuntimeMeta", t, func() {
		deployPath := "/opt/function/code/"
		convey.Convey("nodejs", func() {
			target1 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "nodejs",
					Handler: "index.handler",
				},
			}, target1, deployPath)
			convey.So(target1.Spec.Configuration.Handler.Module, convey.ShouldEqual, "/opt/function/code/index.js")
			convey.So(target1.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "handler")
			target2 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "nodejs",
					Handler: "com.index.handler",
				},
			}, target2, deployPath)
			convey.So(target2.Spec.Configuration.Handler.Module, convey.ShouldEqual, "/opt/function/code/handler.js")
			convey.So(target2.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "com.index.handler")
		})
		convey.Convey("java", func() {
			target1 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "java",
					Handler: "com.package.Class::handler",
				},
			}, target1, deployPath)
			convey.So(target1.Spec.Configuration.Handler.Module, convey.ShouldEqual, "com.package.Class")
			convey.So(target1.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "handler")
			target2 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "java",
					Handler: "com.package::Class::handler",
				},
			}, target2, deployPath)
			convey.So(target2.Spec.Configuration.Handler.Module, convey.ShouldEqual, "com.package::Class::handler")
			convey.So(target2.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "")
		})
		convey.Convey("python", func() {
			target1 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "python",
					Handler: "index.handler",
				},
			}, target1, deployPath)
			convey.So(target1.Spec.Configuration.Handler.Module, convey.ShouldEqual, "/opt/function/code/index.py")
			convey.So(target1.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "handler")
			target2 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "python",
					Handler: "com.index.handler",
				},
			}, target2, deployPath)
			convey.So(target2.Spec.Configuration.Handler.Module, convey.ShouldEqual, "/opt/function/code/handler.py")
			convey.So(target2.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "com.index.handler")
		})
		convey.Convey("cpp", func() {
			target1 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "cpp",
					Handler: "index.handler",
				},
			}, target1, deployPath)
			convey.So(target1.Spec.Configuration.Handler.Module, convey.ShouldEqual, "/opt/function/code/index.handler")
			convey.So(target1.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "index.handler")
		})
		convey.Convey("elseLanguage", func() {
			target1 := &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Handler: faasfunction.HandlerSpec{},
					},
				},
			}
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "custom image",
					Handler: "index.handler",
				},
			}, target1, deployPath)
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "test",
					Handler: "index.handler",
				},
			}, target1, deployPath)
			setRuntimeMeta(&config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					Runtime: "go1.x",
					Handler: "index.handler",
				},
			}, target1, deployPath)
			convey.So(target1.Spec.Configuration.Handler.Module, convey.ShouldEqual, "")
			convey.So(target1.Spec.Configuration.Handler.Entry, convey.ShouldEqual, "index.handler")
		})
	})
}

// TestChangeOldWorkerOwner -
func TestChangeOldWorkerOwner(t *testing.T) {
	convey.Convey("TestChangeOldWorkerOwner", t, func() {
		kubeClient := &kubernetes.Clientset{}
		corev1Client := &typecorev1.CoreV1Client{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return corev1Client
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
				func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
					return mockutils.NewK8sPodImp(namespace)
				}),
			gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
				return kubeClient
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("nameIsWorkerManager", func() {
			convey.So(ChangeOldWorkerOwner(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{Name: WorkerManagerDeployName},
					},
				},
			}, &config.RequestInfo{}), convey.ShouldBeNil)
		})
		convey.Convey("jsonMarshalErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("test err")
				}),
				gomonkey.ApplyFunc(buildPatchPodTemplate, func(ownerReference metav1.OwnerReference,
					requestInfo *config.RequestInfo) *corev1.Pod {
					return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{}}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(ChangeOldWorkerOwner(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{}},
				&config.RequestInfo{}), convey.ShouldBeError)
		})
		convey.Convey("PatchErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(buildPatchPodTemplate, func(ownerReference metav1.OwnerReference,
					requestInfo *config.RequestInfo) *corev1.Pod {
					return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{}}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Patch", func(_ *mockutils.K8sPodImp,
					ctx context.Context, name string, pt types.PatchType, data []byte,
					opts metav1.PatchOptions, subresources ...string) (result *corev1.Pod, err error) {
					return nil, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.Convey("isNotFoundErr", func() {
				patch := gomonkey.ApplyFunc(k8serror.IsNotFound, func(err error) bool {
					return true
				})
				convey.So(ChangeOldWorkerOwner(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{}},
					&config.RequestInfo{}), convey.ShouldBeNil)
				patch.Reset()
			})
			convey.Convey("foundErr", func() {
				patch := gomonkey.ApplyFunc(k8serror.IsNotFound, func(err error) bool {
					return false
				})
				convey.So(ChangeOldWorkerOwner(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{}},
					&config.RequestInfo{}), convey.ShouldBeError)
				patch.Reset()
			})
		})
		convey.Convey("changeOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Patch", func(_ *mockutils.K8sPodImp,
					ctx context.Context, name string, pt types.PatchType, data []byte,
					opts metav1.PatchOptions, subresources ...string) (result *corev1.Pod, err error) {
					return &corev1.Pod{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(ChangeOldWorkerOwner(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{}},
				&config.RequestInfo{
					FunctionName:    "0@123@456",
					BusinessID:      "BusinessID",
					TenantID:        "TenantID",
					FunctionVersion: "FunctionVersion",
				}), convey.ShouldBeNil)
		})
	})
}

// TestRunWatcher -
func TestRunWatcher(t *testing.T) {
	patch := gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}
		return &time.Ticker{C: ch}
	})
	defer func() {
		patch.Reset()
	}()
	convey.Convey("TestRunWatcher", t, func() {
		assertFlag := false
		assertFlag2 := false
		stopCh1 := make(chan struct{})
		close(stopCh1)
		stopCh2 := make(chan struct{})
		once := sync.Once{}
		resultChan := make(chan *etcd3.Event, 1)
		resultChan <- &etcd3.Event{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(parseUserAgencyEvent, func(event *etcd3.Event, etcdType etcd3.EtcdType) {
				assertFlag = true
				assertFlag2 = true
				close(resultChan)
				return
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(_ *etcd3.EtcdClient,
				ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				once.Do(func() {
					assertFlag2 = true
					close(stopCh2)
				})
				return &etcd3.EtcdWatchChan{ResultChan: resultChan}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdWatchChan{}), "Stop", func(_ *etcd3.EtcdWatchChan) {
				return
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		runWatcher(nil)
		convey.So(assertFlag, convey.ShouldBeFalse)
		runWatcher(stopCh1)
		runWatcher(stopCh2)
		convey.So(assertFlag2, convey.ShouldBeTrue)
	})
	convey.Convey("TestRunWatcherCAE", t, func() {
		assertFlag := false
		stopCh := make(chan struct{})
		stopCh1 := make(chan struct{})
		close(stopCh1)

		caeResultChan := make(chan *etcd3.Event, 2)
		caeResultChan <- &etcd3.Event{}
		caeResultChan <- &etcd3.Event{Key: WatchPathPrefixWithAgency}
		once := sync.Once{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(updateUserAgencyCacheMap, func(key string, value []byte, etcdType etcd3.EtcdType) {
				once.Do(func() {
					assertFlag = true
					close(caeResultChan)
					close(stopCh)
				})
				return
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return nil
			}),
			gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{Username: "cae"}}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(client *etcd3.EtcdClient,
				ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				if client.Client.Username == "cae" {
					return &etcd3.EtcdWatchChan{ResultChan: caeResultChan}
				}
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdWatchChan{}), "Stop", func(_ *etcd3.EtcdWatchChan) {
				return
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				kv := &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value")}
				return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{kv}}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		runCAEWatcher(stopCh)
		runCAEWatcher(stopCh1)
		convey.So(assertFlag, convey.ShouldBeTrue)
	})
	convey.Convey("TestRunWatcherCAEClosed", t, func() {
		assertFlag := false
		stopCh := make(chan struct{})
		caeResultChan2 := make(chan *etcd3.Event, 1)
		close(caeResultChan2)
		once := sync.Once{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{Username: "cae"}}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(client *etcd3.EtcdClient,
				ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				if client.Client.Username == "cae" {
					return &etcd3.EtcdWatchChan{ResultChan: caeResultChan2}
				}
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdWatchChan{}), "Stop", func(_ *etcd3.EtcdWatchChan) {
				assertFlag = true
				once.Do(func() {
					close(stopCh)
				})
				return
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		runCAEWatcher(stopCh)
		convey.So(assertFlag, convey.ShouldBeTrue)
	})
	convey.Convey("TestRunWatcherErrorWatchChan", t, func() {
		stopCh := make(chan struct{})
		assertFlag := false
		agencyResultChan := make(chan *etcd3.Event, 1)
		agencyResultChan <- &etcd3.Event{Key: WatchPathPrefixWithAgency}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{Username: "cae"}}
			}),
			gomonkey.ApplyFunc(parseUserAgencyEvent, func(event *etcd3.Event, etcdType etcd3.EtcdType) {
				assertFlag = true
				close(stopCh)
				return
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(client *etcd3.EtcdClient,
				ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				return &etcd3.EtcdWatchChan{ResultChan: agencyResultChan}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdWatchChan{}), "Stop", func(_ *etcd3.EtcdWatchChan) {
				return
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{}, errors.New("error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		runCAEWatcher(stopCh)
		convey.So(assertFlag, convey.ShouldBeFalse)
	})
	convey.Convey("TestCanCachedAgencyBeChanged", t, func() {
		userAgencyCacheMap.Store("key", UserAgency{Source: etcd3.Meta})
		convey.So(canCachedAgencyBeChanged("key", etcd3.CAEMeta), convey.ShouldBeFalse)
		userAgencyCacheMap.Delete("key")
	})
	convey.Convey("TestParseUserAgencyEventIllegal", t, func() {
		parseUserAgencyEvent(&etcd3.Event{Type: -1}, etcd3.Meta)
	})
	convey.Convey("TestUpdateUserAgencyCacheMapUnmarshalFailed", t, func() {
		updateUserAgencyCacheMap("key", []byte("ha"), etcd3.Meta)
		_, ok := userAgencyCacheMap.Load("key")
		convey.So(ok, convey.ShouldBeFalse)
	})
	convey.Convey("TestRunWatcher meta is nil", t, func() {
		stopCh := make(chan struct{})
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		runWatcher(stopCh)
	})
	convey.Convey("TestRunWatcher meta, cluster result chan", t, func() {
		stopCh := make(chan struct{})
		agencyResultChan := make(chan *etcd3.Event, 1)
		clusterWatchChan := make(chan *etcd3.Event, 1)
		clusterWatchChan <- &etcd3.Event{Key: WatchClusterPrefix}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyFunc(getWatchChan, func(metaClient *etcd3.EtcdClient) (*etcd3.EtcdWatchChan, *etcd3.EtcdWatchChan) {
				return &etcd3.EtcdWatchChan{ResultChan: agencyResultChan}, &etcd3.EtcdWatchChan{ResultChan: clusterWatchChan}
			}),
			gomonkey.ApplyFunc(parseFunctionClusterEvent, func(event *etcd3.Event) {
				close(stopCh)
				return
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		runWatcher(stopCh)
	})
}

// TestInitInformer -
func TestInitInformer(t *testing.T) {
	convey.Convey("TestInitInformer", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyFunc(informers.NewSharedInformerFactory, func(client kubernetes.Interface, defaultResync time.Duration) informers.SharedInformerFactory {
				return mockutils.InformerFakeFactory
			}),
			gomonkey.ApplyFunc(informers.NewSharedInformerFactoryWithOptions, func(client kubernetes.Interface, defaultResync time.Duration, options ...informers.SharedInformerOption) informers.SharedInformerFactory {
				return mockutils.InformerFakeFactory
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		InitInformer(nil)
	})
}

func TestGetIPsByDNS(t *testing.T) {
	convey.Convey("TestGetIPsByDNSError", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do", func(_ *http.Client,
				req *http.Request) (*http.Response, error) {
				return nil, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		param := DNSProxyRequestParam{
			DNSAddress:  "mockDNSAddr",
			TenantToken: "mockTenantToken",
		}
		_, err := GetIPsByDNS("mockOmsvcAddr", "mockAK", "mockSK", param)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestGetIPsByDNSNotOK", t, func() {
		resp := &http.Response{}
		resp.Body = http.NoBody
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do", func(_ *http.Client,
				req *http.Request) (*http.Response, error) {
				return resp, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		param := DNSProxyRequestParam{
			DNSAddress:  "mockDNSAddr",
			TenantToken: "mockTenantToken",
		}
		_, err := GetIPsByDNS("mockOmsvcAddr", "mockAK", "mockSK", param)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestGetIPsByDNSBodyError", t, func() {
		resp := &http.Response{}
		resp.Body = http.NoBody
		resp.StatusCode = http.StatusOK
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do", func(_ *http.Client,
				req *http.Request) (*http.Response, error) {
				return resp, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		param := DNSProxyRequestParam{
			DNSAddress:  "mockDNSAddr",
			TenantToken: "mockTenantToken",
		}
		_, err := GetIPsByDNS("mockOmsvcAddr", "mockAK", "mockSK", param)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestGetIPsByDNS", t, func() {
		resp := &http.Response{}
		resp.Body = ioutil.NopCloser(bytes.NewBufferString(mockutils.DNSExampleResponseValue))
		resp.StatusCode = http.StatusOK
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do", func(_ *http.Client,
				req *http.Request) (*http.Response, error) {
				return resp, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		param := DNSProxyRequestParam{
			DNSAddress:  "mockDNSAddr",
			TenantToken: "mockTenantToken",
		}
		_, err := GetIPsByDNS("mockOmsvcAddr", "mockAK", "mockSK", param)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("TestGetIPsNotFound", t, func() {
		resp := &http.Response{}
		resp.Body = ioutil.NopCloser(bytes.NewBufferString(""))
		resp.StatusCode = http.StatusNotFound
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do", func(_ *http.Client,
				req *http.Request) (*http.Response, error) {
				return resp, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		param := DNSProxyRequestParam{
			DNSAddress:  "mockDNSAddr",
			TenantToken: "mockTenantToken",
			VpcID:       "NotFoundID",
			Info: commontypes.DNSDomainInfo{
				DomainName: "test001",
			},
		}
		_, err := GetIPsByDNS("mockOmsvcAddr", "mockAK", "mockSK", param)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(dnsCache.isDomainNotExist("NotFoundID&test001"), convey.ShouldBeTrue)
		dnsCache.ProcessUpdateDomainConfig("func001", "NotFoundID&test001")
		domains, err := dnsCache.GetDNSDomainNames("func001")
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(domains), convey.ShouldEqual, 0)
	})
	convey.Convey("TestGetIPsByDNSOnlyOne", t, func() {
		resp := &http.Response{}
		resp.Body = ioutil.NopCloser(bytes.NewBufferString(mockutils.DNSExampleResponseValue))
		resp.StatusCode = http.StatusOK
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do", func(_ *http.Client,
				req *http.Request) (*http.Response, error) {
				return resp, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		param := DNSProxyRequestParam{
			DNSAddress:  "mockDNSAddr",
			TenantToken: "mockTenantToken",
		}
		result, _ := GetIPsByDNS("mockOmsvcAddr", "mockAK", "mockSK", param)
		convey.So(reflect.DeepEqual(result["www.example.com"], []string{"192.168.10.2"}), convey.ShouldBeTrue)
	})
}

func TestIsCustomContainerSpec(t *testing.T) {
	type args struct {
		spec *faasfunction.FunctionServiceSpec
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test nil",
			args: args{
				spec: nil,
			},
			want: false,
		},
		{
			name: "test not nil",
			args: args{
				spec: &faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						CustomContainerConfig: commontypes.CustomContainerConfig{
							ControlPath: "mock-path",
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCustomContainerSpec(tt.args.spec); got != tt.want {
				t.Errorf("IsCustomContainerSpec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetDefaultTenantMetaInfoFromEtcd(t *testing.T) {
	type args struct {
		defaultTenantMetaKey string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case_01 get etcd key error",
			args: args{"/sn/quota/cluster/cluster001/default/instancemetadata"},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getValuesFromETCD,
						func(defaultTenantMetaKey string, valueType int) ([]string, etcd3.EtcdType, error) {
							return []string{}, etcd3.Meta, fmt.Errorf("get key error")
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case_02 get etcd key is nil",
			args: args{"/sn/quota/cluster/cluster001/default/instancemetadata"},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getValuesFromETCD,
						func(defaultTenantMetaKey string, valueType int) ([]string, etcd3.EtcdType, error) {
							return []string{}, etcd3.Meta, nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case_03 etcd key value invalid",
			args: args{"/sn/quota/cluster/cluster001/default/instancemetadata"},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getValuesFromETCD,
						func(defaultTenantMetaKey string, valueType int) ([]string, etcd3.EtcdType, error) {
							return []string{"error"}, etcd3.Meta, nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case_03 normal scenario",
			args: args{"/sn/quota/cluster/cluster001/default/instancemetadata"},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getValuesFromETCD,
						func(defaultTenantMetaKey string, valueType int) ([]string, etcd3.EtcdType, error) {
							return []string{mockutils.ETCDTenantMetaValueForTest}, etcd3.Meta, nil
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
			_, err := GetDefaultTenantMetaInfoFromEtcd(tt.args.defaultTenantMetaKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("test failed")
			}
			patches.ResetAll()
		})
	}
}

func TestGetOwnerIPByRequest(t *testing.T) {
	assert.Equal(t, "", GetOwnerIPByRequest(&corev1.Pod{}, &config.RequestInfo{OwnerIP: "A"}))
	assert.Equal(t, "1.1.1.1", GetOwnerIPByRequest(&corev1.Pod{
		Status: corev1.PodStatus{
			HostIP: "1.1.1.1",
		},
	}, &config.RequestInfo{OwnerIP: "A", StateID: "aaa"}))
}

func TestGetUserAgencyByFuncMeta(t *testing.T) {
	type args struct {
		funcMeta *faasfunction.FunctionService
	}
	tests := []struct {
		name string
		args args
		want UserAgency
	}{
		{
			name: "test funcMeta nil",
			args: args{funcMeta: nil},
			want: UserAgency{},
		},
		{
			name: "test AppXRole not empty",
			args: args{funcMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{
							AppXRole: "",
						},
					},
				},
			}},
			want: UserAgency{},
		},
		{
			name: "test AppXRole empty but XRole not empty",
			args: args{funcMeta: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						Role: faasfunction.Role{
							AppXRole: "",
							XRole:    "test",
						},
					},
				},
			}},
			want: UserAgency{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equalf(t, tt.want, GetUserAgencyByFuncMeta(tt.args.funcMeta), "GetUserAgencyByFuncMeta(%v)", tt.args.funcMeta)
		})
	}
}

func Test_packPoolStandard(t *testing.T) {
	type args struct {
		cpu      int64
		mem      int64
		Language string
		labels   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		want        string
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case_01 default scheduling case",
			args{
				12,
				12,
				"java8",
				nil,
			},
			"12-12-java8",
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			"case_02 heterogeneous scheduling case",
			args{
				12,
				12,
				"java8",
				map[string]string{"node-type": "lowend"},
			},
			"12-12-java8-lowend",
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
		}, {
			"case_03 heterogeneous scheduling case",
			args{
				12,
				12,
				"java8",
				map[string]string{},
			},
			"12-12-java8-highend",
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := PackPoolStandard(tt.args.cpu, tt.args.mem, tt.args.Language, nil, tt.args.labels); got != tt.want {
				t.Errorf("packPoolStandard() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestPackPoolFullName(t *testing.T) {
	type args struct {
		name     string
		cpu      int64
		mem      int64
		language string
		labels   map[string]string
	}
	tests := []struct {
		name        string
		args        args
		want        string
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case_01 default scheduling case",
			args: args{"pool22", 300, 128, "fusion", nil},
			want: "pool22-300-128-fusion",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name: "case_02 heterogeneous scheduling case",
			args: args{"pool22-1", 300, 128, "fusion", nil},
			want: "pool22-300-128-fusion-highend",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
		}, {
			name: "case_03 heterogeneous scheduling case",
			args: args{"pool22-1", 300, 128, "fusion", map[string]string{"node-type": "lowend"}},
			want: "pool22-300-128-fusion-lowend",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := PackPoolFullName(tt.args.name, tt.args.cpu, tt.args.mem, tt.args.language, tt.args.labels); got != tt.want {
				t.Errorf("PackPoolFullName() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestGetFilterByFuncProfile(t *testing.T) {
	highLabel := make(map[string]string)
	highLabel[LabelNodeType] = HighNodeType
	lowLabel := make(map[string]string)
	lowLabel[LabelNodeType] = LowNodeType
	type args struct {
		profile commontypes.ProfileMetaData
	}
	tests := []struct {
		name        string
		args        args
		want        map[string]string
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case01 force strategy",
			args: args{profile: commontypes.ProfileMetaData{}},
			want: highLabel,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsForceStrategy,
						func() bool {
							return true
						}),
				})
				return patches
			},
		}, {
			name: "case02 profile meta not initialized",
			args: args{profile: commontypes.ProfileMetaData{IsInit: false}},
			want: highLabel,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsForceStrategy,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name: "case03 cpu bound func",
			args: args{profile: commontypes.ProfileMetaData{IsInit: true, ResourceModel: commontypes.ResourceModel{IsCPUBound: true}}},
			want: highLabel,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsForceStrategy,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name: "case04 not cpu bound func",
			args: args{profile: commontypes.ProfileMetaData{IsInit: true, ResourceModel: commontypes.ResourceModel{IsCPUBound: false, IsMemOccupied: true, IsIOBound: false}}},
			want: lowLabel,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsForceStrategy,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name: "case05 not heterogeneous scheduling",
			args: args{profile: commontypes.ProfileMetaData{IsInit: true, ResourceModel: commontypes.ResourceModel{IsCPUBound: false, IsMemOccupied: true, IsIOBound: false}}},
			want: make(map[string]string, DefaultMapSize),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return false
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := GetFilterByFuncProfile(tt.args.profile); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFilterByFuncProfile() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestIsNeedRetryScheduling(t *testing.T) {
	highLabel := make(map[string]string)
	highLabel[LabelNodeType] = HighNodeType
	lowLabel := make(map[string]string)
	lowLabel[LabelNodeType] = LowNodeType
	type args struct {
		err    error
		labels map[string]string
	}
	tests := []struct {
		name        string
		args        args
		want        bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case01 no need to retry when no err",
			args: args{err: nil, labels: highLabel},
			want: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return false
						}),
				})
				return patches
			},
		}, {
			name: "case02 no need to retry when func is cpu bound",
			args: args{err: errors.New("get pod err"), labels: highLabel},
			want: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
		}, {
			name: "case03 need to retry",
			args: args{err: errors.New("get pod err"), labels: lowLabel},
			want: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.IsHeterogeneousScheduling,
						func() bool {
							return true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := IsNeedRetryScheduling(tt.args.err, tt.args.labels); got != tt.want {
				t.Errorf("IsNeedRetry() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestUpdateAllPrewarmPod(t *testing.T) {
	isMocked := false
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
		isMocked    bool
	}{
		{
			name: "test success",
			patchesFunc: func() mockutils.PatchSlice {
				kubeClient := &kubernetes.Clientset{}
				corev1Client := &typecorev1.CoreV1Client{}
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(FunctionNameSpace, func() string { return "default" }),
					gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return corev1Client
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
						func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
							return mockutils.NewK8sPodImp(namespace)
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
						ctx context.Context, name string, opts metav1.DeleteOptions) error {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "List", func(_ *mockutils.K8sPodImp,
						ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						isMocked = true
						return &corev1.PodList{
							Items: []corev1.Pod{
								{
									ObjectMeta: metav1.ObjectMeta{
										Name: "test-pod-name-1",
										Labels: map[string]string{
											LabelShortFuncName: "test-func-name-1",
										},
									},
								},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return kubeClient
					}),
				})
				return patches
			},
			isMocked: true,
		}, // test success
		{
			name: "test with list error",
			patchesFunc: func() mockutils.PatchSlice {
				kubeClient := &kubernetes.Clientset{}
				corev1Client := &typecorev1.CoreV1Client{}
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(FunctionNameSpace, func() string { return "default" }),
					gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return corev1Client
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
						func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
							return mockutils.NewK8sPodImp(namespace)
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
						ctx context.Context, name string, opts metav1.DeleteOptions) error {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "List", func(_ *mockutils.K8sPodImp,
						ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						isMocked = true
						return &corev1.PodList{
							Items: []corev1.Pod{
								{
									ObjectMeta: metav1.ObjectMeta{
										Name: "test-pod-name-1",
										Labels: map[string]string{
											LabelShortFuncName: "test-func-name-1",
										},
									},
								},
							},
						}, errors.New("mock list pods error")
					}),
					gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return kubeClient
					}),
				})
				return patches
			},
			isMocked: true,
		}, // test with list error
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMocked = false
			patches := tt.patchesFunc()
			UpdateAllPrewarmPod()
			patches.ResetAll()
			if isMocked != tt.isMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
			}
		})
	}
}

func TestDNSCache(t *testing.T) {
	dnsCache = GetDNSCache()

	dnsDomainMap := map[string][]string{
		"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
		"test.autotest-001.com":  {"192.168.100.1"},
	}
	/**
	Create Function List:
		FunctionA  vpcID001 www.domain.com vpcID001 www.domain2.com
		FunctionB  vpcID001 www.domain3.com
		FunctionC  vpcID001 www.domain.com
		FunctionD  without domain config
	*/
	// FunctionA Create
	flag := dnsCache.ProcessNewDomainConfig("FunctionA", "vpcID001", "www.domain.com")
	assert.Equal(t, flag, true)
	if flag {
		dnsCache.StoreDNSCache("FunctionA", "vpcID001", "www.domain.com", dnsDomainMap)
	}
	flag = dnsCache.ProcessNewDomainConfig("FunctionA", "vpcID001", "www.domain2.com")
	assert.Equal(t, flag, true)
	if flag {
		dnsCache.StoreDNSCache("FunctionA", "vpcID001", "www.domain2.com", dnsDomainMap)
	}
	val, ok := dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, true)
	domainKeyList, ok := val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(domainKeyList), 2)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcList, ok := val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcList), 1)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain2.com")
	assert.Equal(t, ok, true)
	// FunctionB Create
	flag = dnsCache.ProcessNewDomainConfig("FunctionB", "vpcID001", "www.domain3.com")
	assert.Equal(t, flag, true)
	if flag {
		dnsCache.StoreDNSCache("FunctionB", "vpcID001", "www.domain3.com", dnsDomainMap)
	}
	// FunctionC Create
	flag = dnsCache.ProcessNewDomainConfig("FunctionC", "vpcID001", "www.domain.com")
	assert.Equal(t, flag, false)
	// FunctionD Create
	flag = dnsCache.ProcessNewDomainConfig("FunctionD", "", "")
	assert.Equal(t, flag, false)

	/**
	modify Function metadata:
		case1: FunctionA  www.domain.com vpcID001 www.domain2.com  ->  www.domain.com vpcID001 www.domain3.com
		case2: FunctionA  www.domain.com vpcID001 www.domain3.com  ->  www.domain.com vpcID001 www.domain4.com
		case3: FunctionA  www.domain.com vpcID001 www.domain4.com  ->  www.domain.com
		case4: FunctionA  www.domain.com           ->  FunctionA  without domain config
		case5: FunctionC  vpcID001 www.domain.com  ->  FunctionC  without domain config
		case6: FunctionB  vpcID001 www.domain3.com ->  FunctionB  vpcID001 www.domain3.com
		case7: FunctionA  without domain config    ->  FunctionA  without domain config
		case8: FunctionA  without domain config    ->  FunctionA  vpcID001 www.domain3.com
		case9: FunctionC  without domain config    ->  FunctionC  vpcID001 www.domain.com vpcID001 www.domain3.com
		case10: FunctionA vpcID001 www.domain3.com ->  FunctionA  vpcID001 www.domain.com vpcID001 www.domain3.com
		case11: FunctionB vpcID001 www.domain3.com ->  FunctionC  vpcID001 www.domain3.com vpcID001 www.domain4.com
		case12: FunctionB vpcID001 www.domain3.com vpcID001 www.domain4.com  -> FunctionB  without domain config
	*/

	// case1
	domainKeys, flag := dnsCache.ProcessUpdateDomainConfig("FunctionA",
		"vpcID001&www.domain.com", "vpcID001&www.domain3.com")
	assert.Equal(t, flag, false)
	assert.Equal(t, len(domainKeys), 0)
	val, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, true)
	domainKeyList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(domainKeyList), 2)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok := val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 2)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain2.com")
	assert.Equal(t, ok, false)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)

	// case2
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionA",
		"vpcID001&www.domain4.com", "vpcID001&www.domain.com")
	assert.Equal(t, flag, true)
	assert.Equal(t, len(domainKeys), 1)
	val, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	domainKeyList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(domainKeyList), 2)
	if flag {
		_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain4.com")
		assert.Equal(t, ok, true)
		dnsCache.StoreDNSCache("FunctionA", "vpcID001", "www.domain4.com", dnsDomainMap)
		_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain4.com")
		assert.Equal(t, ok, true)
	}
	val, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	domainKeyList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(domainKeyList), 2)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 2)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain3.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 1)

	// case3
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionA", "vpcID001&www.domain.com")
	assert.Equal(t, flag, false)
	assert.Equal(t, len(domainKeys), 0)
	val, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	domainKeyList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(domainKeyList), 1)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 2)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain4.com")
	assert.Equal(t, ok, false)

	// case4
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionA")
	assert.Equal(t, flag, false)
	assert.Equal(t, len(domainKeys), 0)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, false)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	_, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain.com")
	assert.Equal(t, ok, true)

	// case5
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionC")
	assert.Equal(t, flag, false)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionC")
	assert.Equal(t, ok, false)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, false)
	_, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain.com")
	assert.Equal(t, ok, false)

	// case6
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionB", "vpcID001&www.domain3.com")
	assert.Equal(t, flag, false)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionB")
	assert.Equal(t, ok, true)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 1)
	val, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)

	// case7
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionA")
	assert.Equal(t, flag, false)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, false)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, false)

	// case8
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionA", "vpcID001&www.domain3.com")
	assert.Equal(t, flag, false)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, true)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 2)

	// case9
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionC",
		"vpcID001&www.domain.com", "vpcID001&www.domain3.com")
	assert.Equal(t, flag, true)
	assert.Equal(t, len(domainKeys), 1)
	if flag {
		dnsCache.StoreDNSCache("FunctionC", "vpcID001", "www.domain.com", dnsDomainMap)
	}
	_, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionC")
	assert.Equal(t, ok, true)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 1)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 3)

	// case10
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionA",
		"vpcID001&www.domain.com", "vpcID001&www.domain3.com")
	assert.Equal(t, flag, false)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, true)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 2)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 3)

	// case11
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionB",
		"vpcID001&www.domain3.com", "vpcID001&www.domain4.com")
	assert.Equal(t, flag, true)
	assert.Equal(t, len(domainKeys), 1)
	if flag {
		dnsCache.StoreDNSCache("FunctionB", "vpcID001", "www.domain4.com", dnsDomainMap)
	}
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 3)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain4.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 1)
	_, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	_, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain4.com")
	assert.Equal(t, ok, true)

	// case12
	domainKeys, flag = dnsCache.ProcessUpdateDomainConfig("FunctionB")
	assert.Equal(t, flag, false)
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain3.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 2)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain4.com")
	assert.Equal(t, ok, false)
	_, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	_, ok = dnsCache.dnsCacheMap.Load("vpcID001&www.domain4.com")
	assert.Equal(t, ok, false)

	/**
	delete function event
	*/
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionB")
	assert.Equal(t, ok, false)
	dnsCache.ProcessDeleteDomainConfig("FunctionB", "vpcID001", "www.domain4.com")
	dnsCache.ProcessDeleteDomainConfig("FunctionB", "", "")
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionB")
	assert.Equal(t, ok, false)

	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 2)
	dnsCache.ProcessDeleteDomainConfig("FunctionC", "vpcID001", "www.domain.com")
	val, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, true)
	funcSvcIDList, ok = val.([]string)
	if !ok {
		t.Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", "func_vpcID001&www.domain.com", val)
	}
	assert.Equal(t, len(funcSvcIDList), 1)
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionC")
	assert.Equal(t, ok, false)

	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)
	dnsCache.ProcessDeleteDomainConfig("FunctionC", "vpcID001", "www.domain3.com")
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, true)

	dnsCache.ProcessDeleteDomainConfig("FunctionA", "vpcID001", "www.domain.com")
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, false)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain.com")
	assert.Equal(t, ok, false)
	dnsCache.ProcessDeleteDomainConfig("FunctionA", "vpcID001", "www.domain3.com")
	_, ok = dnsCache.dnsCacheMap.Load("svcID_FunctionA")
	assert.Equal(t, ok, false)
	_, ok = dnsCache.dnsCacheMap.Load("func_vpcID001&www.domain3.com")
	assert.Equal(t, ok, false)

	dnsCache.StoreDNSCache("", "vpc001", "domain001", dnsDomainMap)
	dnsCache.dnsCacheMap.Store(DNSCacheSvcKeyPrefix+"func101", "111")
	ok = dnsCache.storeFuncReflect("func101", "vpc001&domain001")
	assert.Equal(t, ok, false)
}

func Test_UpdateDeployByK8S(t *testing.T) {
	convey.Convey("Test_UpdateDeployByK8S", t, func() {
		err := UpdateDeployByK8S(nil, nil)
		convey.So(err, convey.ShouldEqual, ErrK8SClientNil)

		_, err = ListDeployByK8S(nil, metav1.ListOptions{})
		convey.So(err, convey.ShouldEqual, ErrK8SClientNil)
	})
}

func TestIsTmpExpansionPod(t *testing.T) {
	tests := []struct {
		name    string
		podName string
		want    bool
	}{
		{
			name:    "case01: pod is tmp expansion pod ",
			podName: "pool22-300-128-fusion-tmpexpansion-7sahx3swtdabtkenh5bgbw",
			want:    true,
		},
		{
			name:    "case02: pod isn't tmp expansion pod",
			podName: "pool22-300-128-fusion-7sahx3swtdabtkenh5bgbw",
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equalf(t, tt.want, IsTmpExpansionPod(tt.podName), "IsTmpExpansionPod(%v)", tt.podName)
		})
	}
}

func TestProcessUpdateDomainConfigConcurrency(t *testing.T) {
	dnsCache = &DNSCache{}
	var count int
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			funcSvcID := fmt.Sprintf("funcSvcID%d", i)
			domainKey := fmt.Sprintf("vpcID001&www.domain%d.com", i/100)
			if i%100 == 0 {
				_, ok := dnsCache.ProcessUpdateDomainConfig(funcSvcID)
				assert.Equal(t, ok, false)
				return
			}
			list, ok := dnsCache.ProcessUpdateDomainConfig(funcSvcID, domainKey)
			if ok {
				count += len(list)
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, count, 10)
}

func TestProcessDomainNotExistList(t *testing.T) {
	dnsCache = &DNSCache{}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			domainKey := fmt.Sprintf("vpcID001&www.domain%d.com", i/10)
			dnsCache.addDomainNotExist(domainKey)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, len(dnsCache.domainNotExistList), 100)

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			domainKey := fmt.Sprintf("vpcID001&www.domain%d.com", i/10)
			dnsCache.removeDomainNotExist(domainKey)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, len(dnsCache.domainNotExistList), 50)
}

func TestGetIPsByDNSByAddresses(t *testing.T) {
	tests := []struct {
		name         string
		addresses    string
		expectResult map[string][]string
		expectErr    error
	}{
		{
			name:      "two address success",
			addresses: "127.0.0.1:1234,127.0.0.2:1234",
			expectResult: map[string][]string{
				"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
				"test.autotest-001.com":  {"192.168.100.1"},
			},
			expectErr: nil,
		},
		{
			name:      "two address failed",
			addresses: "127.0.0.1:1234,127.0.0.2:1234",
			expectResult: map[string][]string{
				"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
				"test.autotest-001.com":  {"192.168.100.1"},
			},
			expectErr: errors.New("failed"),
		},
		{
			name:      "one address failed",
			addresses: "127.0.0.1:1234",
			expectResult: map[string][]string{
				"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
				"test.autotest-001.com":  {"192.168.100.1"},
			},
			expectErr: errors.New("failed"),
		},
		{
			name:      "one address success",
			addresses: "127.0.0.1:1234",
			expectResult: map[string][]string{
				"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
				"test.autotest-001.com":  {"192.168.100.1"},
			},
			expectErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := ApplyFunc(GetIPsByDNS, func(omsvcAddress, ak, sk string,
				param DNSProxyRequestParam) (map[string][]string, error) {
				return tt.expectResult, tt.expectErr
			})
			defer patches.Reset()
			param := DNSProxyRequestParam{
				DNSAddress:  "mockDNSAddr",
				TenantToken: "mockTenantToken",
			}
			result, err := GetIPsByDNSByAddresses(tt.addresses, "ak", "sk", param)
			assert.DeepEqual(t, result, tt.expectResult)
			if tt.expectErr == nil {
				assert.NilError(t, err)
			} else {
				assert.Error(t, err, tt.expectErr.Error())
			}
		})
	}
}

func TestLocalMetaEtcdCaches_storeMetaInfo(t *testing.T) {
	convey.Convey("TestLocalMetaEtcdCaches_storeMetaInfo", t, func() {
		convey.Convey("failed to read file", func() {
			readFailedFlag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
					readFailedFlag = true
					return nil, errors.New("fail")
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			metaInfo := &etcd3.EtcdCacheMetaInfo{
				Rev:           0,
				Version:       "",
				PartitionSize: 1,
				MD5s:          mds,
			}
			mc := &LocalMetaEtcdCaches{}
			mc.storeMetaInfo(metaInfo)
			convey.So(readFailedFlag, convey.ShouldEqual, true)
			for idx := range patches {
				patches[idx].Reset()
			}
		})

		convey.Convey("failed unmarshal", func() {
			readFailedFlag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
					return nil, nil
				}),
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					readFailedFlag = true
					return errors.New("fail")
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			metaInfo := &etcd3.EtcdCacheMetaInfo{
				Rev:           0,
				Version:       "",
				PartitionSize: 1,
				MD5s:          mds,
			}
			mc := &LocalMetaEtcdCaches{}
			mc.storeMetaInfo(metaInfo)
			convey.So(readFailedFlag, convey.ShouldEqual, true)
			for idx := range patches {
				patches[idx].Reset()
			}
		})

		convey.Convey("success store Meta Info", func() {
			var etcdCache etcd3.EtcdCache
			event := &etcd3.Event{
				Type:      0,
				Key:       "",
				Value:     nil,
				PrevValue: nil,
				Rev:       0,
			}
			events := make(map[string]*etcd3.Event)
			events["test"] = event
			etcdCache.Events = events
			result, _ := json.Marshal(etcdCache)
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
					return result, nil
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			metaInfo := &etcd3.EtcdCacheMetaInfo{
				Rev:           0,
				Version:       "",
				PartitionSize: 1,
				MD5s:          mds,
			}
			mc := &LocalMetaEtcdCaches{}
			mc.storeMetaInfo(metaInfo)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func TestLocalMetaEtcdCaches_InitMetaEtcdCache(t *testing.T) {
	convey.Convey("TestLocalMetaEtcdCaches_InitMetaEtcdCache", t, func() {
		convey.Convey("failed to read file", func() {
			readFailedFlag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
					readFailedFlag = true
					return nil, errors.New("fail")
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			mc := &LocalMetaEtcdCaches{}
			mc.InitMetaEtcdCache()
			convey.So(readFailedFlag, convey.ShouldEqual, true)
			for idx := range patches {
				patches[idx].Reset()
			}
		})

		convey.Convey("failed to unmarshal", func() {
			readFailedFlag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
					readFailedFlag = true
					return nil, nil
				}),
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					readFailedFlag = true
					return errors.New("fail")
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			mc := &LocalMetaEtcdCaches{}
			mc.InitMetaEtcdCache()
			convey.So(readFailedFlag, convey.ShouldEqual, true)
			for idx := range patches {
				patches[idx].Reset()
			}
		})

		convey.Convey("failed to check Cache Meta Info", func() {
			metaInfo := &etcd3.EtcdCacheMetaInfo{
				Rev:           0,
				Version:       "",
				PartitionSize: 0,
				MD5s:          nil,
			}
			result, _ := json.Marshal(metaInfo)
			readFailedFlag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
					return result, nil
				}),
				gomonkey.ApplyFunc(checkCacheMetaInfo, func(metaInfo *etcd3.EtcdCacheMetaInfo) error {
					readFailedFlag = true
					return errors.New("fail")
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			mc := &LocalMetaEtcdCaches{}
			mc.InitMetaEtcdCache()
			convey.So(readFailedFlag, convey.ShouldEqual, true)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func Test_checkCacheMetaInfo(t *testing.T) {
	convey.Convey("Test_checkCacheMetaInfo", t, func() {
		convey.Convey("failed file md5", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.FileMD5, func(filePath string) (string, error) {
					return "", errors.New("fail")
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			metaInfo := &etcd3.EtcdCacheMetaInfo{
				Rev:           0,
				Version:       etcd3.CacheVersion,
				PartitionSize: 1,
				MD5s:          mds,
			}
			err := checkCacheMetaInfo(metaInfo)
			convey.So(err, convey.ShouldNotBeNil)
			for idx := range patches {
				patches[idx].Reset()
			}
		})

		convey.Convey("failed check md5", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.FileMD5, func(filePath string) (string, error) {
					return "xxx", nil
				}),
			}
			mds := make(map[string]string)
			mds["partition_0"] = "partition_0"
			metaInfo := &etcd3.EtcdCacheMetaInfo{
				Rev:           0,
				Version:       etcd3.CacheVersion,
				PartitionSize: 1,
				MD5s:          mds,
			}
			err := checkCacheMetaInfo(metaInfo)
			convey.So(err, convey.ShouldNotBeNil)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func TestLocalMetaEtcdCaches_getStoreMetaInfo(t *testing.T) {
	type fields struct {
		mux              sync.Mutex
		metaEtcdCacheMap map[string]*etcd3.Event
	}
	type args struct {
		functionKey string
	}
	var f fields
	var a args
	var b args
	metaEtcdCacheMap := make(map[string]*etcd3.Event)
	event := &etcd3.Event{}
	metaEtcdCacheMap["test"] = event
	f.metaEtcdCacheMap = metaEtcdCacheMap
	a.functionKey = "test"
	b.functionKey = "fail"
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{"case1", f, a, []string{string([]byte{})}},
		{"case2", f, b, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &LocalMetaEtcdCaches{
				mux:              tt.fields.mux,
				metaEtcdCacheMap: tt.fields.metaEtcdCacheMap,
			}
			if got := mc.getStoreMetaInfo(tt.args.functionKey); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getStoreMetaInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalMetaEtcdCaches_GetAllStoreMetaInfo(t *testing.T) {
	type fields struct {
		mux              sync.Mutex
		metaEtcdCacheMap map[string]*etcd3.Event
	}
	var f fields
	var info []*etcd3.Event
	metaEtcdCacheMap := make(map[string]*etcd3.Event)
	event := &etcd3.Event{}
	event.Key = "test"
	metaEtcdCacheMap["test"] = event
	f.metaEtcdCacheMap = metaEtcdCacheMap
	info = append(info, event)
	tests := []struct {
		name   string
		fields fields
		want   []*etcd3.Event
	}{
		{"case1", f, info},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &LocalMetaEtcdCaches{
				mux:              tt.fields.mux,
				metaEtcdCacheMap: tt.fields.metaEtcdCacheMap,
			}
			if got := mc.GetAllStoreMetaInfo(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetAllStoreMetaInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalMetaEtcdCaches_ClearAllStoreMetaInfo(t *testing.T) {
	type fields struct {
		mux              sync.Mutex
		metaEtcdCacheMap map[string]*etcd3.Event
	}
	var f fields
	mc := &LocalMetaEtcdCaches{
		mux:              f.mux,
		metaEtcdCacheMap: f.metaEtcdCacheMap,
	}
	mc.ClearAllStoreMetaInfo()
}

func TestDescribeGPUNodeAffinity(t *testing.T) {
	convey.Convey("Test describe gpu node affinity", t, func() {
		key := "testKey"
		value := []string{"testValue"}
		convey.Convey("Case1 without gpu type", func() {
			gpuType := ""
			nodeAffinity := DescribeGPUNodeAffinity(key, value, gpuType)
			returnKey := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Key
			returnValue := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values
			lenExpressions := len(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions)
			convey.So(returnKey, convey.ShouldEqual, key)
			convey.So(returnValue, convey.ShouldResemble, value)
			convey.So(lenExpressions, convey.ShouldEqual, 1)
		})
		convey.Convey("Case2 with gpu type", func() {
			gpuType := "nvidia-a30"
			expectedValue := []string{gpuType}
			nodeAffinity := DescribeGPUNodeAffinity(key, value, gpuType)
			returnKey0 := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Key
			returnValue0 := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values
			returnKey1 := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[1].Key
			returnValue1 := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[1].Values
			lenExpressions := len(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions)
			convey.So(returnKey0, convey.ShouldEqual, key)
			convey.So(returnValue0, convey.ShouldResemble, value)
			convey.So(returnKey1, convey.ShouldEqual, GpuTypeAffinityKey)
			convey.So(returnValue1, convey.ShouldResemble, expectedValue)
			convey.So(lenExpressions, convey.ShouldEqual, 2)
		})
	})
}

func TestConstructIntersect(t *testing.T) {
	convey.Convey("Test Intersect", t, func() {
		a := []string{"apple", "banana", "cherry", "date", "elderberry"}
		b := []string{"banana", "coconut", "date", "fig", "grape"}
		res := ConstructIntersect(a, b)
		expectedList := []string{"banana", "date"}
		expectedListString := strings.Join(expectedList, ",")
		convey.So(strings.Join(res, ","), convey.ShouldEqual, expectedListString)
	})

	convey.Convey("Test Without Intersect", t, func() {
		a := []string{"apple", "cherry", "elderberry"}
		b := []string{"banana", "coconut", "fig", "grape"}
		res := ConstructIntersect(a, b)
		convey.So(res, convey.ShouldBeEmpty)
	})
}

func TestConfigGPUResource(t *testing.T) {
	oldCfg := enableGPUIsolation
	defer func() {
		enableGPUIsolation = oldCfg
	}()

	tests := []struct {
		name     string
		enable   bool
		request  int64
		capacity int64

		excepted corev1.ResourceList
	}{
		{
			enable:   false,
			request:  1024,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				NvidiaGPU: resource.MustParse(fmt.Sprintf("%f", 0.041)),
			},
		},
		{
			enable:   true,
			request:  1024,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				GpuIsolationMemKey:  resource.MustParse(fmt.Sprintf("%d", 8)),
				GpuIsolationCoreKey: resource.MustParse(fmt.Sprintf("%d", 5)),
			},
		},
		{
			enable:   true,
			request:  2048,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				GpuIsolationMemKey:  resource.MustParse(fmt.Sprintf("%d", 16)),
				GpuIsolationCoreKey: resource.MustParse(fmt.Sprintf("%d", 5)),
			},
		},
		{
			enable:   true,
			request:  23552,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				GpuIsolationMemKey:  resource.MustParse(fmt.Sprintf("%d", 184)),
				GpuIsolationCoreKey: resource.MustParse(fmt.Sprintf("%d", 95)),
			},
		},
		{
			enable:   true,
			request:  24576,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				NvidiaGPU: resource.MustParse(fmt.Sprintf("%d", 1)),
			},
		},
		{
			enable:   true,
			request:  49152,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				NvidiaGPU: resource.MustParse(fmt.Sprintf("%d", 2)),
			},
		},
		{
			enable:   false,
			request:  24576,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				NvidiaGPU: resource.MustParse(fmt.Sprintf("%f", 1.0)),
			},
		},
		{
			enable:   false,
			request:  49152,
			capacity: 24,
			excepted: map[corev1.ResourceName]resource.Quantity{
				NvidiaGPU: resource.MustParse(fmt.Sprintf("%f", 2.0)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{},
				Limits:   map[corev1.ResourceName]resource.Quantity{},
			}
			enableGPUIsolation = tt.enable
			ConfigGPUResource(res, tt.request, tt.capacity)
			Equal(t, tt.excepted, res.Limits, tt.name)
		})
	}
}

func TestIsOnDemandPod(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name: "pool pod",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-fusion-xxxx"},
			},
			expected: false,
		},
		{
			name: "custom container normal",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "custom-300-128-xxxxx"},
			},
			expected: true,
		},
		{
			name: "custom container gpu",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "custom.image.gpu-300-128-1024-xxxx"},
			},
			expected: true,
		},
		{
			name: "check point",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "checkpoint-xxx",
					Labels: map[string]string{StandardLabelKey: CheckpointPodStandard},
				},
			},
			expected: true,
		},
		{
			name: "nodeBalance migrated",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "pool22-300-128-migrate-xxxx",
					Labels: map[string]string{LabelMigratedFrom: "pool22-300-128-fusion-xxxx"},
				},
			},
			expected: true,
		},
		{
			name: "temp expansion",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-tmpexpansion-xxxx"},
			},
			expected: true,
		},
		{
			name: "custom runtime gpu",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "custom.runtime.gpu-300-128-1024-xxx"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equal(t, tt.expected, IsOnDemandPod(tt.pod))
		})
	}
}

func TestGenerateMigratePodName(t *testing.T) {
	tests := []struct {
		pod            corev1.Pod
		expectedPrefix string
	}{
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-fusion-5bb9f7dd56-l2r2h", Labels: map[string]string{}}},
			expectedPrefix: "pool22-300-128-migrate-",
		},
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-migrate-5bb9f7dd56-l2r2h", Labels: map[string]string{}}},
			expectedPrefix: "pool22-300-128-migrate-",
		},
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "custom.runtime.gpu-3400-4096-1024-e28a88f7-3ce4-4a20-a6c9-0ff528c74b94", Labels: map[string]string{}}},
			expectedPrefix: "custom.runtime.gpu-3400-4096-1024-migrate-",
		},
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "custom.runtime.gpu-3400-4096-1024-migrate-e28a88f7-3ce4-4a20-a6c9-0ff528c74b94", Labels: map[string]string{}}},
			expectedPrefix: "custom.runtime.gpu-3400-4096-1024-migrate-",
		},
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "custom-600-512-3b22ca86-5440-4b7f-8242-b196e6706102", Labels: map[string]string{}}},
			expectedPrefix: "custom-600-512-migrate-",
		},
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "custom-600-512-migrate-3b22ca86-5440-4b7f-8242-b196e6706102", Labels: map[string]string{}}},
			expectedPrefix: "custom-600-512-migrate-",
		},
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "custom.image.gpu-1800-2048-1024-73496fa5-60be-4a3b-a406-39191c5d0c1a", Labels: map[string]string{}}},
			expectedPrefix: "custom.image.gpu-1800-2048-1024-migrate-",
		},
		{
			pod:            corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "custom.image.gpu-1800-2048-1024-migrate-73496fa5-60be-4a3b-a406-39191c5d0c1a", Labels: map[string]string{}}},
			expectedPrefix: "custom.image.gpu-1800-2048-1024-migrate-",
		},
		{
			pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-fusion-5bb9f7dd56-l2r2h", Labels: map[string]string{
				LabelFunctionCPU:    "400",
				LabelFunctionMemory: "256",
			}}},
			expectedPrefix: "pool-400-256-migrate-",
		},
		{
			pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-fusion-5bb9f7dd56-l2r2h", Labels: map[string]string{
				LabelFunctionCPU:    "300",
				LabelFunctionMemory: "128",
			}}},
			expectedPrefix: "pool-300-128-migrate-",
		},
		{
			pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "custom.image.gpu-1800-2048-1024-migrate-73496fa5-60be-4a3b-a406-39191c5d0c1a", Labels: map[string]string{
				LabelFunctionCPU:    "300",
				LabelFunctionMemory: "128",
			}}},
			expectedPrefix: "custom.image.gpu-300-128-1024-migrate-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.pod.Name, func(t *testing.T) {
			generateName := GenerateMigratePodName(tt.pod)
			True(t, strings.HasPrefix(generateName, tt.expectedPrefix))
			NotEqual(t, tt.pod.Name, generateName)
			// True(t, len(generateName) <= 63)
		})
	}
}

func TestGenerateMigratePodName_badcase(t *testing.T) {
	tests := []struct {
		pod corev1.Pod
	}{
		{
			pod: corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "invalid-pod", Labels: map[string]string{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.pod.Name, func(t *testing.T) {
			generateName := GenerateMigratePodName(tt.pod)
			Equal(t, tt.pod.Name, generateName)
		})
	}
}

func TestGeneratePodTemplate(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected *corev1.Pod
	}{
		{
			name: "simple test",
			pod: &corev1.Pod{
				Status:   corev1.PodStatus{PodIP: "10.0.1.42"},
				TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
				ObjectMeta: metav1.ObjectMeta{
					Name:              "pool22-300-128-fusion-xxxx",
					GenerateName:      "pool22-300-128-fusion-",
					Namespace:         "default",
					UID:               "xxx-xxx",
					ResourceVersion:   "xxx",
					Generation:        1,
					CreationTimestamp: metav1.NewTime(time.Now()),
					Labels: map[string]string{
						"app":             "pool22",
						"applier":         "worker-manager",
						"businessID":      "yrk",
						"coldStartPool":   "true",
						"deployedNode":    "127.0.0.1",
						"funcName":        "test",
						"standard":        "300-128-fusion",
						"specialize":      "poststart",
						IsPoolPodLabelKey: FalseString,
						// ...
					},
					Annotations: map[string]string{
						"app":              "pool22",
						"crSignature":      "123456",
						"ownerIP":          "127.0.0.1",
						"used":             "true",
						AnnotationResource: `{"worker": {"cpuLimit": 1000, "cpuRequest": 100, "memoryLimit": 200, "memoryRequest": 100}, "runtime": {"cpuLimit": 400, "cpuRequest": 60, "memoryLimit": 256, "memoryRequest": 256}}`,
					},
					OwnerReferences: []metav1.OwnerReference{{Name: "worker-manager"}},
				},
				Spec: corev1.PodSpec{
					Volumes:        []corev1.Volume{{Name: "volume1"}, {Name: "volume2"}},
					InitContainers: []corev1.Container{{Name: "init1"}},
					Containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{},
						Limits:   corev1.ResourceList{},
					}}, {Name: "runtime", Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{},
						Limits:   corev1.ResourceList{},
					}}},
					RestartPolicy: corev1.RestartPolicyAlways,
					NodeName:      "127.0.0.1",
					ImagePullSecrets: []corev1.LocalObjectReference{
						{Name: "default-secret"},
						{Name: "custom-600-512-4e879da1-5133-45aa-a3f8-d73828b3f7e3.regcred"},
						{Name: "myregcred"},
						{Name: "custom-400-256-02eb3327-c6de-4b21-abb6-4339a8c4c597.regcred"},
					},
				},
			},
			expected: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Labels: map[string]string{
						IsPoolPodLabelKey: TrueString,
						StandardLabelKey:  "300-128-fusion",
					},
					Annotations: map[string]string{
						"used":                FalseString,
						constants.OndemandKey: TrueString,
					},
				},
				Spec: corev1.PodSpec{
					Volumes:        []corev1.Volume{{Name: "volume1"}, {Name: "volume2"}},
					InitContainers: []corev1.Container{{Name: "init1"}},
					Containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"cpu":    resource.MustParse("100m"),
							"memory": resource.MustParse("100Mi"),
						},
						Limits: corev1.ResourceList{
							"cpu":    resource.MustParse("1000m"),
							"memory": resource.MustParse("200Mi"),
						},
					}}, {Name: "runtime", Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"cpu":    resource.MustParse("60m"),
							"memory": resource.MustParse("256Mi"),
						},
						Limits: corev1.ResourceList{
							"cpu":    resource.MustParse("400m"),
							"memory": resource.MustParse("256Mi"),
						},
					}}},
					RestartPolicy: corev1.RestartPolicyNever,
					ImagePullSecrets: []corev1.LocalObjectReference{
						{Name: "default-secret"},
						{Name: "myregcred"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := GeneratePodTemplate(tt.pod)
			Equal(t, tt.expected, pod)
			NotSame(t, tt.pod, pod)
		})
	}
}

func TestConfigDemandNodes(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		nodes    []string
		expected []corev1.NodeSelectorTerm
	}{
		{
			name:  "add selector to empty pod",
			pod:   &corev1.Pod{},
			nodes: []string{"node1"},
			expected: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      corev1.LabelHostname,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"node1"},
				}}}},
		},
		{
			name: "multi terms",
			pod: &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      corev1.LabelHostname,
									Operator: corev1.NodeSelectorOpExists,
									Values:   []string{"nodetemp1"},
								},
								{
									Key:      "lalala",
									Operator: corev1.NodeSelectorOpExists,
									Values:   []string{"hahaha"},
								},
								{
									Key:      corev1.LabelHostname,
									Operator: corev1.NodeSelectorOpExists,
									Values:   []string{"nodetemp2"},
								},
							},
						},
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "hahaha",
									Operator: corev1.NodeSelectorOpExists,
									Values:   []string{"value"},
								},
							},
						},
					},
				},
			}}}},
			nodes: []string{"node1"},
			expected: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "lalala",
							Operator: corev1.NodeSelectorOpExists,
							Values:   []string{"hahaha"},
						},
						{
							Key:      corev1.LabelHostname,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node1"},
						},
					},
				},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "hahaha",
							Operator: corev1.NodeSelectorOpExists,
							Values:   []string{"value"},
						},
						{
							Key:      corev1.LabelHostname,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node1"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ConfigDemandNodes(tt.pod, tt.nodes)
			Equal(t, tt.expected, tt.pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
		})
	}
}

func TestParseDemandNodes(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected []string
	}{
		{
			name:     "empty pod",
			pod:      &corev1.Pod{},
			expected: nil,
		},
		{
			name: "pod with empty Affinity",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}},
			},
			expected: nil,
		},
		{
			name: "pod wit demand nodes",
			pod: &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "lalala",
									Operator: corev1.NodeSelectorOpExists,
									Values:   []string{"hahaha"},
								},
								{
									Key:      corev1.LabelHostname,
									Operator: corev1.NodeSelectorOpExists,
									Values:   []string{"node1", "node2"},
								},
							},
						},
					},
				},
			}}}},
			expected: []string{"node1", "node2"},
		},
		{
			name: "pod with no demand nodes",
			pod: &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "lalala",
									Operator: corev1.NodeSelectorOpExists,
									Values:   []string{"hahaha"},
								},
							},
						},
					},
				},
			}}}},
			expected: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equal(t, tt.expected, ParseDemandNodes(tt.pod))
		})
	}
}

func Test_isUserAgencyEvent(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"case1", args{key: "/sn/agency/business/yrk/tenant/07116873ed80d59a2f1ac01c48893025/domain/07116873e100d59a0f17c01c1acfbf20/agency/obs_function_agency_20c3d5c0e30eabeed534d6eef7c84c30775ab13a"}, true},
		{"case2", args{key: "a"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equalf(t, tt.want, isUserAgencyEvent(tt.args.key), "isUserAgencyEvent(%v)", tt.args.key)
		})
	}
}

func TestGetFuncClusterByFuncMeta(t *testing.T) {
	key := "/sn/function/urn:fss:cn-north-7:fb6a00ff7ae54a5fbb8ff855d0841d00:function:default:ttt:latest/available/clusters"
	values := []string{"cce-cluster-yuanrong01"}
	functionClusterCacheMap.Store(key, values)
	type args struct {
		funcMeta *faasfunction.FunctionService
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "test funcMeta nil",
			args: args{funcMeta: nil},
			want: nil,
		},
		{
			name: "test result nil",
			args: args{funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					FunctionVersionURN: "test"},
			},
			},
			},
			want: nil,
		},
		{
			name: "test result",
			args: args{funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					FunctionVersionURN: "urn:fss:cn-north-7:fb6a00ff7ae54a5fbb8ff855d0841d00:function:default:ttt:latest"},
			},
			},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equalf(t, tt.want, GetFuncClusterByFuncMeta(tt.args.funcMeta), "GetFuncClusterByFuncMeta(%v)", tt.args.funcMeta)
		})
	}
}

func Test_parseFunctionClusterEvent(t *testing.T) {
	type args struct {
		event *etcd3.Event
	}
	tests := []struct {
		name string
		args args
	}{
		{"case 1",
			args{event: &etcd3.Event{
				Type:  1,
				Key:   "/sn/function/urn:fss:cn-north-7:fc6d40abe7e54492b7c7aa5a29d6cbab:function:default:test2222223:latest/available/clusters",
				Value: []byte("[\"cce-cluster-yuanrong01\"]"),
			}}},
		{"case 2",
			args{event: &etcd3.Event{
				Type:  0,
				Key:   "/sn/function/urn:fss:cn-north-7:fc6d40abe7e54492b7c7aa5a29d6cbab:function:default:test2222223:latest/available/clusters",
				Value: []byte("fgs-yuanrong-az1-01,fgs-yuanrong-az1-02,fgs-yuanrong-az2-01,fgs-yuanrong-az2-01"),
			}}},
		{"case 3",
			args{event: &etcd3.Event{
				Type:  0,
				Key:   "/sn/function/urn:fss:cn-north-7:fc6d40abe7e54492b7c7aa5a29d6cbab:function:default:test2222223:latest/available/clusters",
				Value: []byte("{a: b}"),
			}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseFunctionClusterEvent(tt.args.event)
		})
	}
}

func Test_getValuesFromETCD(t *testing.T) {
	patches := []*gomonkey.Patches{
		ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		}),
		ApplyFunc(getValuesFromSelectETCD, func(selection etcd3.EtcdType, functionKey string, valueType int) ([]string, error) {
			if valueType == valueTypeTenant {
				if selection == etcd3.Meta {
					return nil, nil
				} else {
					return []string{"query tenant from cae"}, nil
				}
			}

			switch functionKey {
			case "metaError":
				if selection == etcd3.Meta {
					return nil, errors.New("failed to query meta etcd")
				}
				return []string{"query from cae"}, nil
			case "metaNotExistCaeExist":
				if selection == etcd3.Meta {
					return nil, nil
				}
				return []string{"query from cae"}, nil
			case "metaExistCaeExist":
				if selection == etcd3.Meta {
					return []string{"query from meta"}, nil
				}
				return []string{"query from cae"}, nil
			case "allNotExist":
				return nil, nil
			case "metaNotExistCAEError":
				if selection == etcd3.Meta {
					return nil, nil
				}
				return nil, errors.New("failed to query cae etcd")
			}

			return nil, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	tests := []struct {
		name      string
		funcKey   string
		valueType int

		expectedVal    []string
		expectedSource etcd3.EtcdType
		expectedErr    bool
	}{
		{
			name:      "query tenant, not query cae",
			funcKey:   "tenant",
			valueType: valueTypeTenant,

			expectedVal:    nil,
			expectedSource: etcd3.Meta,
			expectedErr:    false,
		},
		{
			name:      "meta error, not query cae",
			funcKey:   "metaError",
			valueType: valueTypeMeta,

			expectedVal:    nil,
			expectedSource: etcd3.Meta,
			expectedErr:    true,
		},
		{
			name:      "meta not exist, query from cae",
			funcKey:   "metaNotExistCaeExist",
			valueType: valueTypeInstance,

			expectedSource: etcd3.CAEMeta,
			expectedVal:    []string{"query from cae"},
			expectedErr:    false,
		},
		{
			name:      "all exists",
			funcKey:   "metaExistCaeExist",
			valueType: valueTypeMeta,

			expectedSource: etcd3.Meta,
			expectedVal:    []string{"query from meta"},
			expectedErr:    false,
		},
		{
			name:      "all not exists",
			funcKey:   "allNotExist",
			valueType: valueTypeMeta,

			expectedSource: etcd3.CAEMeta,
			expectedVal:    nil,
			expectedErr:    false,
		},
		{
			name:      "meta not exist and cae error",
			funcKey:   "metaNotExistCAEError",
			valueType: valueTypeMeta,

			expectedSource: etcd3.CAEMeta,
			expectedVal:    nil,
			expectedErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, source, err := getValuesFromETCD(tt.funcKey, tt.valueType)
			Equal(t, tt.expectedVal, val)
			Equal(t, tt.expectedSource, source)
			if tt.expectedErr {
				NotNil(t, err)
			} else {
				Nil(t, err)
			}
		})
	}
}

func Test_IsCurClusterRouteInfo(t *testing.T) {
	convey.Convey("Test_IsCurClusterRouteInfo", t, func() {
		convey.Convey("is current cluster route info", func() {
			clusterID = "cluster001"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.AzEnv, func() string {
					return "cluster001"
				}),
			}
			instanceInfo := "{\"ip\":\"10.42.0.246\",\"port\":\"8080\",\"cluster\":\"cluster001\",\"status\":\"ready\"," +
				"\"p2pPort\":\"22668\",\"nodeIP\":\"7.218.81.229\",\"nodePort\":\"22423\",\"applier\":" +
				"\"worker-manager\",\"ownerIP\":\"7.218.81.229\",\"cpu\":400,\"memory\":256}"
			ok := IsCurClusterRouteInfo([]byte(instanceInfo))
			convey.So(ok, convey.ShouldBeTrue)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
		convey.Convey("not current cluster route info", func() {
			clusterID = "cluster001"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.AzEnv, func() string {
					return "defaultaz"
				}),
			}
			instanceInfo := "{\"ip\":\"10.42.0.246\",\"port\":\"8080\",\"cluster\":\"cluster002\",\"status\":\"ready\"," +
				"\"p2pPort\":\"22668\",\"nodeIP\":\"7.218.81.229\",\"nodePort\":\"22423\",\"applier\":" +
				"\"worker-manager\",\"ownerIP\":\"7.218.81.229\",\"cpu\":400,\"memory\":256}"
			ok := IsCurClusterRouteInfo([]byte(instanceInfo))
			convey.So(ok, convey.ShouldBeFalse)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
		convey.Convey("not current cluster route info, unmarshal error", func() {
			clusterID = "cluster001"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.AzEnv, func() string {
					return "defaultaz"
				}),
			}
			instanceInfo := "{\"ip\":\"10.42.0.246\",\"port\":\"8080\",\"cluster\":\"cluster002\",\"status\":\"ready\"," +
				"\"p2pPort\":\"22668\",\"nodeIP\":\"7.218.81.229\",\"nodePort\":\"22423\",\"applier\":" +
				"\"worker-manager\",\"ownerIP\":\"7.218.81.229\",\"cpu\":400,\"memory\":256}x"
			ok := IsCurClusterRouteInfo([]byte(instanceInfo))
			convey.So(ok, convey.ShouldBeFalse)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func Test_UpdateReplicasByK8s(t *testing.T) {
	convey.Convey("Test_UpdateReplicasByK8s", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "AppsV1",
				func(_ *kubernetes.Clientset) typedappsv1.AppsV1Interface {
					return &appsv1fake.FakeAppsV1{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&appsv1fake.FakeAppsV1{}), "Deployments",
				func(_ *appsv1fake.FakeAppsV1, namespace string) typedappsv1.DeploymentInterface {
					return &appsv1fake.FakeDeployments{}
				}),
			ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Patch",
				func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string, pt types.PatchType,
					data []byte, opts metav1.PatchOptions, subresources ...string) (*corev1.Pod, error) {
					return &corev1.Pod{}, errors.New("test")
				}),
		})
		defer patches.ResetAll()
		err := UpdateReplicasByK8s("name", 10)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_UpdateRuntimeImageByK8s(t *testing.T) {
	convey.Convey("Test_UpdateReplicasByK8s err", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "AppsV1",
				func(_ *kubernetes.Clientset) typedappsv1.AppsV1Interface {
					return &appsv1fake.FakeAppsV1{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&appsv1fake.FakeAppsV1{}), "Deployments",
				func(_ *appsv1fake.FakeAppsV1, namespace string) typedappsv1.DeploymentInterface {
					return &appsv1fake.FakeDeployments{}
				}),
			ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Patch",
				func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string, pt types.PatchType,
					data []byte, opts metav1.PatchOptions, subresources ...string) (*corev1.Pod, error) {
					return &corev1.Pod{}, errors.New("test")
				}),
		})
		defer patches.ResetAll()
		err := UpdateRuntimeImageByK8s("name", "image")
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test_UpdateReplicasByK8s ok", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "AppsV1",
				func(_ *kubernetes.Clientset) typedappsv1.AppsV1Interface {
					return &appsv1fake.FakeAppsV1{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&appsv1fake.FakeAppsV1{}), "Deployments",
				func(_ *appsv1fake.FakeAppsV1, namespace string) typedappsv1.DeploymentInterface {
					return &appsv1fake.FakeDeployments{}
				}),
			ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Patch",
				func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string, pt types.PatchType,
					data []byte, opts metav1.PatchOptions, subresources ...string) (*corev1.Pod, error) {
					return &corev1.Pod{}, nil
				}),
		})
		defer patches.ResetAll()
		err := UpdateRuntimeImageByK8s("name", "image")
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_UpdateFileHandleNumByK8s(t *testing.T) {
	convey.Convey("Test_UpdateFileHandleNumByK8s err", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "AppsV1",
				func(_ *kubernetes.Clientset) typedappsv1.AppsV1Interface {
					return &appsv1fake.FakeAppsV1{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&appsv1fake.FakeAppsV1{}), "Deployments",
				func(_ *appsv1fake.FakeAppsV1, namespace string) typedappsv1.DeploymentInterface {
					return &appsv1fake.FakeDeployments{}
				}),
			ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Patch",
				func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string, pt types.PatchType,
					data []byte, opts metav1.PatchOptions, subresources ...string) (*corev1.Pod, error) {
					return nil, errors.New("test")
				}),
		})
		defer patches.ResetAll()
		err := UpdateFileHandleNumByK8s("name", 20)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test_UpdateFileHandleNumByK8s ok", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "AppsV1",
				func(_ *kubernetes.Clientset) typedappsv1.AppsV1Interface {
					return &appsv1fake.FakeAppsV1{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&appsv1fake.FakeAppsV1{}), "Deployments",
				func(_ *appsv1fake.FakeAppsV1, namespace string) typedappsv1.DeploymentInterface {
					return &appsv1fake.FakeDeployments{}
				}),
			ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Patch",
				func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string, pt types.PatchType,
					data []byte, opts metav1.PatchOptions, subresources ...string) (*corev1.Pod, error) {
					return &corev1.Pod{}, nil
				}),
		})
		defer patches.ResetAll()
		err := UpdateFileHandleNumByK8s("name", 20)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_DeleteStateWorkersFromMetaETCD(t *testing.T) {
	convey.Convey("Test_DeleteStateWorkersFromMetaETCD", t, func() {
		convey.Convey("GetETCDClient is nil", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.AzEnv, func() string {
					return "cluster001"
				}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{}
				}),
				gomonkey.ApplyFunc((*LocalMetaEtcdCaches).InitMetaEtcdCache, func(_ *LocalMetaEtcdCaches) {}),
			}
			DeleteStateWorkersFromMetaETCD("stateID")
			for idx := range patches {
				patches[idx].Reset()
			}
		})
		convey.Convey("Delete error", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.AzEnv, func() string {
					return "cluster001"
				}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*LocalMetaEtcdCaches).InitMetaEtcdCache, func(_ *LocalMetaEtcdCaches) {}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
					return errors.New("error")
				}),
			}
			DeleteStateWorkersFromMetaETCD("stateID")
			for idx := range patches {
				patches[idx].Reset()
			}
		})
		convey.Convey("all clear", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.AzEnv, func() string {
					return "cluster001"
				}),
				gomonkey.ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*LocalMetaEtcdCaches).InitMetaEtcdCache, func(_ *LocalMetaEtcdCaches) {}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
					return nil
				}),
			}
			DeleteStateWorkersFromMetaETCD("stateID")
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func TestGetFullFuncNameByPod(t *testing.T) {
	tests := []struct {
		name        string
		labels      map[string]string
		annotations map[string]string

		expected string
	}{
		{
			name:        "all empty",
			labels:      map[string]string{},
			annotations: map[string]string{},

			expected: "",
		},
		{
			name: "annotations first",
			labels: map[string]string{
				LabelFuncNamePrefix: "0",
				LabelServiceID:      "aaa",
				LabelShortFuncName:  "bbb",
			},
			annotations: map[string]string{
				AnnotationFullFuncName: "0@default@hello",
			},

			expected: "0@default@hello",
		},
		{
			name: "no annotation, use label",
			labels: map[string]string{
				LabelFuncNamePrefix: "0",
				LabelServiceID:      "aaa",
				LabelShortFuncName:  "bbb",
			},
			annotations: map[string]string{},

			expected: "0@aaa@bbb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := GetFullFuncNameByPod(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      tt.labels,
					Annotations: tt.annotations,
				},
			})
			Equal(t, tt.expected, name, tt.name)
		})
	}
}

func Test_IsDsWorker(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: DsWorkerPodNameKey + "123"}}
	if !IsDsWorker(pod) {
		t.Errorf("this is error")
	}
}

func Test_HasDsWorkerCrashTaints(t *testing.T) {
	taints := make([]corev1.Taint, 0)
	if HasDsWorkerCrashTaints(taints) {
		t.Errorf("this is error")
	}
	taints = append(taints, corev1.Taint{Key: DsWorkerTaintKey})
	if !HasDsWorkerCrashTaints(taints) {
		t.Errorf("this is error")
	}
}

func Test_GetEnvDsWorkerCrashTimeout(t *testing.T) {
	convey.Convey("not bridgeFunction pod, clear", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return ""
			}),
		})
		patches.ResetAll()
		var test1 sync.Once
		GetDsWorkerTimeout = test1
		res := GetEnvDsWorkerCrashTimeout()
		if res != DefaultDsWorkerTimeSleep {
			t.Errorf("this is error")
		}
	})
	convey.Convey("not bridgeFunction pod, clear", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return "abc"
			}),
		})
		patches.ResetAll()
		var test2 sync.Once
		GetDsWorkerTimeout = test2
		res := GetEnvDsWorkerCrashTimeout()
		if res != DefaultDsWorkerTimeSleep {
			t.Errorf("this is error")
		}
	})
	convey.Convey("not bridgeFunction pod, clear", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return "40"
			}),
		})
		patches.ResetAll()
		var test3 sync.Once
		GetDsWorkerTimeout = test3
		res := GetEnvDsWorkerCrashTimeout()
		if res != DefaultDsWorkerTimeSleep {
			t.Errorf("this is error")
		}
	})
}

func TestGetPodErrorMsg(t *testing.T) {
	data := GetPodErrorMsg(nil)
	assert.Equal(t, data, "")

	data = GetPodErrorMsg(errors.New("test"))
	assert.Equal(t, data, "test")

	curErr := snerror.NewSNError(4011, "test")
	data = GetPodErrorMsg(curErr)
	assert.Equal(t, data, "test")
}

func TestIsBridgeInstanceByPodLabel(t *testing.T) {
	var label map[string]string
	if IsBridgeInstanceByPodLabel(label) {
		t.Errorf("this is error")
	}
	label = make(map[string]string)
	label[LabelIsBridgeFunction] = "test"
	if IsBridgeInstanceByPodLabel(label) {
		t.Errorf("this is error")
	}
	label[LabelIsBridgeFunction] = TrueString
	if !IsBridgeInstanceByPodLabel(label) {
		t.Errorf("this is error")
	}
}

func TestCreateSecretDataWithUserAuth(t *testing.T) {
	patches := gomonkey.ApplyFunc(localauth.Decrypt, func(s string) ([]byte, error) {
		if s == "" {
			return nil, errors.New("empty string input")
		}
		return []byte(s), nil
	})
	defer patches.Reset()

	tests := []struct {
		name  string
		user  string
		pwd   string
		image string

		expectedErr  bool
		expectedData []byte
	}{
		{
			name: "invalid image addr",

			image: "example.com", user: "some_user", pwd: "some_pwd",

			expectedErr: true,
		},
		{
			name:  "empty user",
			image: "example.com/aaa/bbb:latest", user: "", pwd: "some_pwd",

			expectedErr: true,
		},
		{
			name:  "empty pwd",
			image: "example.com/aaa/bbb:latest", user: "some_user", pwd: "",

			expectedErr: true,
		},
		{
			name:  "valid config",
			image: "example.com/aaa/bbb:latest", user: "some_user", pwd: "some_pwd",

			expectedErr:  false,
			expectedData: []byte(`{"auths": {"example.com": {"auth": "c29tZV91c2VyOnNvbWVfcHdk"}}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := CreateSecretDataWithUserAuth(tt.user, tt.pwd, tt.image)
			if tt.expectedErr {
				NotNil(t, err, tt.name)
			} else {
				Nil(t, err, tt.name)
				Equal(t, tt.expectedData, data)
			}
		})
	}
}

func TestRequestSWR(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
			if src == "fail" {
				return nil, errors.New("failed to decrypt")
			}
			return []byte(src), nil
		}),
		gomonkey.ApplyFunc(config.GetSWRAPI, func() string {
			return "127.0.0.1:2333"
		}),
	}

	oldTr := http.DefaultTransport.(*http.Transport).Clone()
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
		http.DefaultTransport = oldTr
	}()

	certs := mockutils.GenerateTestCert()

	r := gin.Default()
	r.POST("/ok", func(c *gin.Context) {
		c.Data(http.StatusOK, "", []byte("request ok"))
	})
	r.POST("/401", func(c *gin.Context) {
		c.Data(http.StatusUnauthorized, "", []byte("unauthorized"))
	})
	r.POST("/panic", func(c *gin.Context) {
		panic("1111")
	})
	server := &http.Server{
		Handler: r.Handler(),
		Addr:    ":2333",
		TLSConfig: &tls.Config{
			Certificates:       certs,
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			MaxVersion:         tls.VersionTLS12,
		},
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	shutdownCancel()
	defer server.Shutdown(shutdownCtx)

	go func() {
		server.ListenAndServeTLS("", "")
	}()

	time.Sleep(50 * time.Millisecond)

	tests := []struct {
		name   string
		ctx    context.Context
		path   string
		method string
		agency UserAgency
		extra  SWRErrRespHandlers

		expectErr  bool
		expectData []byte
	}{
		{
			name:   "token decrypt failed",
			ctx:    context.Background(),
			agency: UserAgency{Token: "fail"},

			expectErr: true,
		},
		{
			name:   "401",
			ctx:    context.Background(),
			path:   "401",
			method: http.MethodPost,
			agency: UserAgency{Token: "ok"},

			expectErr: true,
		},
		{
			name:   "panic",
			ctx:    context.Background(),
			path:   "panic",
			method: http.MethodPost,
			agency: UserAgency{Token: "ok"},

			expectErr: true,
		},
		{
			name:   "ok",
			ctx:    context.Background(),
			path:   "ok",
			method: http.MethodPost,
			agency: UserAgency{Token: "ok"},

			expectErr:  false,
			expectData: []byte("request ok"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := RequestSWR(tt.ctx, tt.path, tt.method, tt.agency, tt.extra)
			if tt.expectErr {
				NotNil(t, err, tt.name)
				Nil(t, data, tt.name)
			} else {
				Nil(t, err, tt.name)
				Equal(t, tt.expectData, data, tt.name)
			}
		})
	}
}

func Test_getStreamContentTypeWhiteList(t *testing.T) {
	convey.Convey("get stream content type white list", t, func() {
		convey.Convey("test empty white list", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{}, nil
			})
			defer patch.Reset()
			whitelist := GetStreamContentTypeWhiteList()
			convey.So(whitelist, convey.ShouldEqual, "")
		})
		convey.Convey("test not empty white list", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{StreamContentTypeWhiteList: []string{"application/x-www-form-urlencoded", "application/octet-stream"}}, nil
			})
			defer patch.Reset()
			whitelist := GetStreamContentTypeWhiteList()
			convey.So(whitelist, convey.ShouldEqual, "{\"application/octet-stream\":true,\"application/x-www-form-urlencoded\":true}")
		})
	})
}

func Test_IsInSlice(t *testing.T) {
	convey.Convey("IsInSlice", t, func() {
		convey.Convey("in slice", func() {
			isExist := IsInSlice("node1", []string{"node1", "xx", "test"})
			convey.So(isExist, convey.ShouldBeTrue)
		})
		convey.Convey("not in slice", func() {
			isExist := IsInSlice("node1", []string{"node2", "xx", "test"})
			convey.So(isExist, convey.ShouldBeFalse)
		})
	})
}

func TestIsNetErrorAndNotTimeout(t *testing.T) {
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
			name:     "some other error",
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name:     "network error with timeout",
			err:      &net.DNSError{IsTimeout: true},
			expected: false,
		},
		{
			name:     "network error without timeout",
			err:      &net.DNSError{IsTimeout: false},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equal(t, tt.expected, IsNetErrorAndNotTimeout(tt.err), tt.name)
		})
	}
}

func TestIsVPAPod(t *testing.T) {
	pod := &corev1.Pod{}
	pod.Labels = map[string]string{}
	convey.Convey("test IsVPAPod", t, func() {
		convey.Convey("test is vpa pod", func() {
			pod.Labels[AppLabelKey] = VpaPausePodStandard
			res := IsVPAPod(pod)
			convey.So(res, convey.ShouldBeTrue)
		})
		convey.Convey("test is not vpa pod", func() {
			pod.Labels[AppLabelKey] = "test"
			res := IsVPAPod(pod)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test label key is not exist", func() {
			pod.Labels["test"] = "test"
			res := IsVPAPod(pod)
			convey.So(res, convey.ShouldBeFalse)
		})
	})
}

func TestGetVpaPausePodNodeName(t *testing.T) {
	t.Run("pod is nil", func(t *testing.T) {
		nodeName := GetVpaPausePodNodeName(nil)
		assert.Equal(t, "", nodeName)
	})
	t.Run("pod spec nodeName is empty", func(t *testing.T) {
		pod := &corev1.Pod{}
		nodeName := GetVpaPausePodNodeName(pod)
		assert.Equal(t, "", nodeName)
	})
	t.Run("pod spec nodeName is not empty", func(t *testing.T) {
		pod := &corev1.Pod{Spec: corev1.PodSpec{
			NodeName: "nodeName",
		}}
		nodeName := GetVpaPausePodNodeName(pod)
		assert.Equal(t, "nodeName", nodeName)
	})
}

func TestIsCaePoolPod(t *testing.T) {
	isCaePoolPod := IsCaePoolPod(&corev1.Pod{})
	assert.Equal(t, isCaePoolPod, false)
	isCaePoolPod = IsCaePoolPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"poolType": "cae"},
		},
	})
	assert.Equal(t, isCaePoolPod, true)
}

func TestHasVapPausePodFault(t *testing.T) {
	t.Run("labels is nil", func(t *testing.T) {
		has := HasVapPausePodFault(nil)
		assert.Equal(t, false, has)
	})
	t.Run("labels not has VapPausePodFault", func(t *testing.T) {
		has := HasVapPausePodFault(map[string]string{"key": "value"})
		assert.Equal(t, false, has)
	})
	t.Run("labels has VapPausePodFault", func(t *testing.T) {
		has := HasVapPausePodFault(map[string]string{constants.NodeVpaPausePodFaultsLabelKey: "value"})
		assert.Equal(t, true, has)
	})
}

func TestGetPodViewResource(t *testing.T) {
	patch := gomonkey.NewPatches()
	reqPodResource := corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "worker",
					ContainerID: "docker://worker-id",
				},
				{
					Name:        "runtime",
					ContainerID: "docker://runtime-id",
				},
			},
			Phase: corev1.PodRunning,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: reqPodResource,
					Name:      "worker",
				},
				{
					Resources: reqPodResource,
					Name:      "runtime",
				},
			},
		},
	}

	convey.Convey("test get pod view resource", t, func() {
		convey.Convey("test get pod view resource success", func() {
			res := GetPodViewResource(pod)
			convey.So(res.Worker.CPURequest, convey.ShouldEqual, 500)
			convey.So(res.Runtime.CPURequest, convey.ShouldEqual, 500)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestGetExtendedInfo(t *testing.T) {
	convey.Convey("test GetExtendedInfo", t, func() {
		convey.Convey("case1: success", func() {
			funcMounts := []faasfunction.FuncMount{{
				MountType:    sfs30,
				ExtendedInfo: "127.0.0.1/8",
			}, {
				MountType:    "aaa",
				ExtendedInfo: "127.0.0.2/8",
			}}
			extendedInfo := GetExtendedInfo(funcMounts)
			convey.So(extendedInfo, convey.ShouldEqual, "127.0.0.1/8")
		})
		convey.Convey("case2: mountType is not sfs3.0", func() {
			funcMounts := []faasfunction.FuncMount{{
				MountType:    "aaa",
				ExtendedInfo: "127.0.0.2/8",
			}}
			extendedInfo := GetExtendedInfo(funcMounts)
			convey.So(extendedInfo, convey.ShouldEqual, "")
		})
		convey.Convey("case3: mountType is sfs3.0, but extendedInfo is empty", func() {
			funcMounts := []faasfunction.FuncMount{{
				MountType:    sfs30,
				ExtendedInfo: "",
			}}
			extendedInfo := GetExtendedInfo(funcMounts)
			convey.So(extendedInfo, convey.ShouldEqual, "")
		})
	})
}

func TestGetStreamClientAkSkConfigList(t *testing.T) {
	var (
		patch = gomonkey.NewPatches()
	)

	convey.Convey("test GetStreamClientAkSkConfigList", t, func() {
		convey.Convey("test get config failed", func() {
			mockGetConfig(patch, nil, errors.New("get config failed"))
			configList := GetStreamClientAkSkConfigList()
			convey.So(configList, convey.ShouldBeNil)
		})

		convey.Convey("test get stream client ak/sk disabled", func() {
			mockGetConfig(patch, &config.FaasConfig{}, nil)
			configList := GetStreamClientAkSkConfigList()
			convey.So(configList, convey.ShouldBeNil)
		})

		convey.Convey("test get stream tenant config is empty", func() {
			mockGetConfig(patch, &config.FaasConfig{
				StreamClientAkSkConfig: config.StreamClientAkSkConfig{
					StreamClientAkSkEnable:   true,
					StreamTenantIDConfigList: nil,
				},
			}, nil)
			configList := GetStreamClientAkSkConfigList()
			convey.So(configList, convey.ShouldBeNil)
		})

		convey.Convey("test empty ak or sk", func() {
			mockGetConfig(patch, &config.FaasConfig{
				StreamClientAkSkConfig: config.StreamClientAkSkConfig{
					StreamClientAkSkEnable: true,
					StreamTenantIDConfigList: []config.StreamTenantIDConfig{
						{
							TenantID:   "001",
							StreamAKey: "ak",
							StreamSKey: "",
						},
					},
				},
			}, nil)
			configList := GetStreamClientAkSkConfigList()
			convey.So(configList, convey.ShouldBeNil)
		})

		convey.Convey("test repeats tenantID", func() {
			mockGetConfig(patch, &config.FaasConfig{
				StreamClientAkSkConfig: config.StreamClientAkSkConfig{
					StreamClientAkSkEnable: true,
					StreamTenantIDConfigList: []config.StreamTenantIDConfig{
						{
							TenantID:   "001",
							StreamAKey: "ak",
							StreamSKey: "sk",
						},
						{
							TenantID:   "001",
							StreamAKey: "ak1",
							StreamSKey: "sk1",
						},
					},
				},
			}, nil)
			configList := GetStreamClientAkSkConfigList()
			convey.So(configList, convey.ShouldBeNil)
		})

		convey.Convey("test success config", func() {
			mockGetConfig(patch, &config.FaasConfig{
				StreamClientAkSkConfig: config.StreamClientAkSkConfig{
					StreamClientAkSkEnable: true,
					StreamTenantIDConfigList: []config.StreamTenantIDConfig{
						{
							TenantID:   "001",
							StreamAKey: "ak1",
							StreamSKey: "sk1",
						},
						{
							TenantID:   "002",
							StreamAKey: "ak2",
							StreamSKey: "sk2",
						},
					},
				},
			}, nil)
			configList := GetStreamClientAkSkConfigList()
			convey.So(len(configList), convey.ShouldEqual, 2)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_ParseIAMToken(t *testing.T) {
	convey.Convey("test ParseIAMToken", t, func() {
		convey.Convey("case1: success", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Decrypt, func(_ string) ([]byte, error) {
					return []byte("token+123"), nil
				}),
				gomonkey.ApplyFunc(localauth.Encrypt, func(_ string) (string, error) {
					return "encryptToken", nil
				}),
			})
			defer patches.ResetAll()
			token, ok := ParseIAMToken("key", "_tokenxxx")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(token, convey.ShouldEqual, "encryptToken")
		})
		convey.Convey("case2: iam token is not start with _", func() {
			_, ok := ParseIAMToken("key", "tokenxxx")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case3: failed to decrypt iam token", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Decrypt, func(_ string) ([]byte, error) {
					return nil, errors.New("error")
				}),
			})
			defer patches.ResetAll()
			_, ok := ParseIAMToken("key", "_tokenxxx")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case4: decrypted iam token does not contain +", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Decrypt, func(_ string) ([]byte, error) {
					return []byte("token123"), nil
				}),
			})
			defer patches.ResetAll()
			_, ok := ParseIAMToken("key", "_tokenxxx")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case5: failed to encrypt iam token", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Decrypt, func(_ string) ([]byte, error) {
					return []byte("token+123"), nil
				}),
				gomonkey.ApplyFunc(localauth.Encrypt, func(_ string) (string, error) {
					return "", errors.New("error")
				}),
			})
			defer patches.ResetAll()
			_, ok := ParseIAMToken("key", "_tokenxxx")
			convey.So(ok, convey.ShouldBeFalse)
		})
	})
}

func TestGetFunctionPodsByInformer(t *testing.T) {
	objs := []runtime.Object{
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:   "pod1_empty_label",
			Labels: map[string]string{},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "pod2_full_label",
			Labels: map[string]string{
				LabelTenantID:        "123456789",
				LabelServiceID:       "default",
				LabelFuncNamePrefix:  "0",
				LabelShortFuncName:   "testFunc",
				LabelFunctionVersion: "latest",
			},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "pod3_lack_tenantID",
			Labels: map[string]string{
				LabelServiceID:       "default",
				LabelFuncNamePrefix:  "0",
				LabelShortFuncName:   "testFunc",
				LabelFunctionVersion: "latest",
			},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "pod4_lack_serviceID",
			Labels: map[string]string{
				LabelTenantID:        "123456789",
				LabelFuncNamePrefix:  "0",
				LabelShortFuncName:   "testFunc",
				LabelFunctionVersion: "latest",
			},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "pod5_lack_func_version",
			Labels: map[string]string{
				LabelTenantID:       "123456789",
				LabelServiceID:      "default",
				LabelFuncNamePrefix: "0",
				LabelShortFuncName:  "testFunc",
			},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fakeClient := fake.NewSimpleClientset(objs...)
	fakeInformer := informers.NewSharedInformerFactory(fakeClient, 0)
	podInformer := fakeInformer.Core().V1().Pods()
	go podInformer.Informer().Run(ctx.Done())
	cache.WaitForCacheSync(nil, podInformer.Informer().HasSynced)

	gomonkey.ApplyFunc(GetPodInformer, func() informerv1.PodInformer {
		return podInformer
	})

	tests := []struct {
		name     string
		funcMeta *faasfunction.FunctionService

		expected []*corev1.Pod
	}{
		{
			name: "labels all matched",
			funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
				FunctionName: "0@default@testFunc", TenantID: "123456789", FunctionVersion: "latest"}}},
			expected: []*corev1.Pod{objs[1].(*corev1.Pod)},
		},
		{
			name: "lack version, no match",
			funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{
				FunctionName: "0@default@testFunc", TenantID: "123456789"}}},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Equal(t, tt.expected, GetFunctionPodsByInformer(tt.funcMeta))
		})
	}
}

func Test_splitByColon(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"case1: invalid content format", "", ""},
		{"case2: invalid content length", "test-011", ""},
		{"case3: valid tag", "test:v1", "v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SplitImageTagByColon(tt.content); got != tt.want {
				t.Errorf("splitByColon() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_UpdateClusterBridgeInstance(t *testing.T) {
	convey.Convey("Test_UpdateClusterBridgeInstance ok", t, func() {
		clusterID = "cluster1"
		cli := &redisclient.Client{}
		sCmd := &redis.StringCmd{}
		stCmd := &redis.StatusCmd{}
		bridgeIns := &commontypes.ClusterBridgeInstance{}
		count := 0
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
				return cli
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "GetDistributedLockWithRetry",
				func(c *redisclient.Client, ctx context.Context, lockKey, lockValue string, expiration time.Duration, retryTimes int) bool {
					return true
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "ReleaseDistributedLock",
				func(c *redisclient.Client, ctx context.Context, lockKey string) {}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "Get",
				func(c *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
					return sCmd
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(sCmd), "Result",
				func(cmd *redis.StringCmd) (string, error) {
					return "{ \"functionUrn\": \"gotest\", \"bridgeInstances\": { \"cluster1\": { \"instance1\": true } } }", nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "Set",
				func(c *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
					jsonBytes := value.([]byte)
					_ = json.Unmarshal(jsonBytes, bridgeIns)
					return stCmd
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "SRem",
				func(c *redisclient.Client, ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
					count++
					return &redis.IntCmd{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "Del",
				func(c *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
					count++
					return &redis.IntCmd{}
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := UpdateClusterBridgeInstance(true, "gotest", "instance1", "instance2")
		convey.So(err, convey.ShouldBeNil)
		convey.So(bridgeIns.BridgeInstances["cluster1"]["instance2"], convey.ShouldBeTrue)
		err = ClearClusterBridgeInstance(true, "gotest")
		convey.So(err, convey.ShouldBeNil)
		convey.So(count, convey.ShouldEqual, 2)
	})
}

func Test_parseBridgeFuncRelatedParams(t *testing.T) {
	convey.Convey("Test_parseBridgeFuncRelatedParams ok", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10\", \"maxActive\": \"3\" }"), nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		target := &faasfunction.FunctionService{}
		parseBridgeFuncRelatedParams(&config.FunctionMetaInfo{
			FuncMetaData: config.FuncMetaData{
				IsBridgeFunction: true,
			},
			EnvMetaData: config.EnvMetaData{Environment: "{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10\", \"maxActive\": \"3\" }"},
		}, target)
		convey.So(target.Spec.Configuration.MaxActive, convey.ShouldEqual, 3)
		convey.So(target.Spec.Configuration.MaxBaaSConnection, convey.ShouldEqual, 10)
	})
	convey.Convey("Test_parseBridgeFuncRelatedParams Environment Decrypt err", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte(""), errors.New("environment decrypt err")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		target := &faasfunction.FunctionService{}
		parseBridgeFuncRelatedParams(&config.FunctionMetaInfo{
			FuncMetaData: config.FuncMetaData{
				IsBridgeFunction: true,
			},
			EnvMetaData: config.EnvMetaData{Environment: "{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10\", \"maxActive\": \"3\" }"},
		}, target)
		convey.So(target.Spec.Configuration.MaxActive, convey.ShouldEqual, 10)
		convey.So(target.Spec.Configuration.MaxBaaSConnection, convey.ShouldEqual, 0)
	})
	convey.Convey("Test_parseBridgeFuncRelatedParams Environment Unmarshal err", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte(" \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10\", \"maxActive\": \"3\" }"), nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		target := &faasfunction.FunctionService{}
		parseBridgeFuncRelatedParams(&config.FunctionMetaInfo{
			FuncMetaData: config.FuncMetaData{
				IsBridgeFunction: true,
			},
			EnvMetaData: config.EnvMetaData{Environment: "{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10\", \"maxActive\": \"3\" }"},
		}, target)
		convey.So(target.Spec.Configuration.MaxActive, convey.ShouldEqual, 10)
		convey.So(target.Spec.Configuration.MaxBaaSConnection, convey.ShouldEqual, 0)
	})
	convey.Convey("Test_parseBridgeFuncRelatedParams Environment Atoi err", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10x\", \"maxActive\": \"3x\" }"), nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		target := &faasfunction.FunctionService{}
		parseBridgeFuncRelatedParams(&config.FunctionMetaInfo{
			FuncMetaData: config.FuncMetaData{
				IsBridgeFunction: true,
			},
			EnvMetaData: config.EnvMetaData{Environment: "{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10\", \"maxActive\": \"3\" }"},
		}, target)
		convey.So(target.Spec.Configuration.MaxActive, convey.ShouldEqual, 10)
		convey.So(target.Spec.Configuration.MaxBaaSConnection, convey.ShouldEqual, 0)
	})
	convey.Convey("Test_parseBridgeFuncRelatedParams Environment assert err", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\" }"), nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		target := &faasfunction.FunctionService{}
		parseBridgeFuncRelatedParams(&config.FunctionMetaInfo{
			FuncMetaData: config.FuncMetaData{
				IsBridgeFunction: true,
			},
			EnvMetaData: config.EnvMetaData{Environment: "{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"10\", \"maxActive\": \"3\" }"},
		}, target)
		convey.So(target.Spec.Configuration.MaxActive, convey.ShouldEqual, 10)
		convey.So(target.Spec.Configuration.MaxBaaSConnection, convey.ShouldEqual, 0)
	})
	convey.Convey("Test_parseBridgeFuncRelatedParams Environment value illegal", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"-10\", \"maxActive\": \"0\" }"), nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		target := &faasfunction.FunctionService{}
		parseBridgeFuncRelatedParams(&config.FunctionMetaInfo{
			FuncMetaData: config.FuncMetaData{
				IsBridgeFunction: true,
			},
			EnvMetaData: config.EnvMetaData{Environment: "{ \"RUNTIME_FUNC_LOG_LEVEL\": \"INFO\", \"maxBaaSConnections\": \"-10\", \"maxActive\": \"0\" }"},
		}, target)
		convey.So(target.Spec.Configuration.MaxActive, convey.ShouldEqual, 10)
		convey.So(target.Spec.Configuration.MaxBaaSConnection, convey.ShouldEqual, 0)
	})
}

func TestParseGPUIsolation(t *testing.T) {
	defer func() {
		os.Unsetenv("ENABLE_GPU_ISOLATION")
	}()
	tests := []struct {
		name     string
		env      string
		expected bool
	}{
		{
			name:     "empty env",
			env:      "",
			expected: true,
		},
		{
			name:     "env invalid value",
			env:      "abcde",
			expected: true,
		},
		{
			name:     "disabled",
			env:      "false",
			expected: false,
		},
		{
			name:     "enable",
			env:      "true",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("ENABLE_GPU_ISOLATION", tt.env)
			Equal(t, tt.expected, ParseGPUIsolation(), tt.name)
		})
	}
}

var (
	mockeCache = UserCipherCache{
		LastUseTime: time.Now(),
		Ciphertext:  "test",
		Plaintext:   "test",
		IsUseful:    false,
		UserAgency: UserAgency{
			AccessKey:     "test",
			SecretKey:     "test",
			Token:         "test",
			SecurityAk:    "test",
			SecuritySk:    "test",
			SecurityToken: "test",
			Source:        0,
		},
	}
	mockNeedDeleteCache = UserCipherCache{
		LastUseTime: time.Now().AddDate(0, 0, -2),
		Ciphertext:  "test",
		Plaintext:   "test",
		IsUseful:    false,
		UserAgency: UserAgency{
			AccessKey:     "test",
			SecretKey:     "test",
			Token:         "test",
			SecurityAk:    "test",
			SecuritySk:    "test",
			SecurityToken: "test",
			Source:        0,
		},
	}
	mockSource = &config.FunctionMetaInfo{
		FuncMetaData: config.FuncMetaData{
			FunctionURN: "test",
		},
	}
)

func Test_decryptKmsCipher(t *testing.T) {
	userCipherMap.Store("test#test#test", mockeCache)
	t.Run("test GetFunctionInfo failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(urnutils.GetFunctionInfo, func(urn string) (urnutils.BaseURN, error) {
			return urnutils.BaseURN{}, errors.New("test")
		})
		defer p.Reset()
		err := decryptKmsCipher(mockSource, "test", "test", "test")
		assert.Error(t, err, "test")
	})

	t.Run("test get cipher cache success", func(t *testing.T) {
		p := gomonkey.ApplyFunc(urnutils.GetFunctionInfo, func(urn string) (urnutils.BaseURN, error) {
			return urnutils.BaseURN{TenantID: "test"}, nil
		})
		defer p.Reset()
		err := decryptKmsCipher(mockSource, "test", "test", "test")
		assert.NilError(t, err)
	})

	t.Run("test getKmeClient failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(urnutils.GetFunctionInfo, func(urn string) (urnutils.BaseURN, error) {
			return urnutils.BaseURN{TenantID: "testMiss"}, nil
		})
		defer p.Reset()
		p.ApplyFunc(getKmeClient, func(source *config.FunctionMetaInfo, decType string) (*kms.Client, error) {
			return nil, errors.New("test")
		})
		err := decryptKmsCipher(mockSource, "testMiss", "testMiss", "testMiss")
		assert.Error(t, err, "test")
	})

	t.Run("test getKmeClient failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(urnutils.GetFunctionInfo, func(urn string) (urnutils.BaseURN, error) {
			return urnutils.BaseURN{TenantID: "testMiss"}, nil
		})
		defer p.Reset()
		p.ApplyFunc(getKmeClient, func(source *config.FunctionMetaInfo, decType string) (*kms.Client, error) {
			return &kms.Client{}, nil
		})
		p.ApplyMethod(reflect.TypeOf(&kms.Client{}), "DecryptDataKey",
			func(_ *kms.Client, keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
				return nil, errors.New("test")
			})
		err := decryptKmsCipher(mockSource, "testMiss", "testMiss", "testMiss")
		assert.Error(t, err, "test")
	})

	t.Run("test getKmeClient key nil", func(t *testing.T) {
		p := gomonkey.ApplyFunc(urnutils.GetFunctionInfo, func(urn string) (urnutils.BaseURN, error) {
			return urnutils.BaseURN{TenantID: "testMiss"}, nil
		})
		defer p.Reset()
		p.ApplyFunc(getKmeClient, func(source *config.FunctionMetaInfo, decType string) (*kms.Client, error) {
			return &kms.Client{}, nil
		})
		p.ApplyMethod(reflect.TypeOf(&kms.Client{}), "DecryptDataKey",
			func(_ *kms.Client, keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
				return nil, nil
			})
		err := decryptKmsCipher(mockSource, "testMiss", "testMiss", "testMiss")
		assert.Error(t, err, "get key bytes failed")
	})

	t.Run("test decryptKmsCipher success", func(t *testing.T) {
		testKmsDataKey := "test"
		p := gomonkey.ApplyFunc(urnutils.GetFunctionInfo, func(urn string) (urnutils.BaseURN, error) {
			return urnutils.BaseURN{TenantID: "testMiss"}, nil
		})
		defer p.Reset()
		p.ApplyFunc(getKmeClient, func(source *config.FunctionMetaInfo, decType string) (*kms.Client, error) {
			return &kms.Client{}, nil
		})
		p.ApplyMethod(reflect.TypeOf(&kms.Client{}), "DecryptDataKey",
			func(_ *kms.Client, keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
				return &kmsModel.DecryptDatakeyResponse{
					DataKey: &testKmsDataKey,
				}, nil
			})
		p.ApplyFunc(GetUserAgencyByFuncMetaInfo, func(source *config.FunctionMetaInfo) UserAgency {
			return UserAgency{}
		})

		err := decryptKmsCipher(mockSource, "testMiss", "testMiss", "testMiss")
		assert.NilError(t, err)
	})
}

func Test_getKmeClient(t *testing.T) {
	t.Run("test GetUserAgencyByFuncMetaInfo failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(GetUserAgencyByFuncMetaInfo, func(source *config.FunctionMetaInfo) UserAgency {
			return UserAgency{}
		})
		defer p.Reset()
		_, err := getKmeClient(mockSource, "test")
		assert.Error(t, err, "get agency failed")
	})

	t.Run("test GetUserAgencyByFuncMetaInfo success", func(t *testing.T) {
		p := gomonkey.ApplyFunc(GetUserAgencyByFuncMetaInfo, func(source *config.FunctionMetaInfo) UserAgency {
			return UserAgency{
				AccessKey: "test",
				SecretKey: "test",
			}
		})
		defer p.Reset()
		p.ApplyFunc(GetKmsClientByAgency, func(agency UserAgency, tenantID string) (*kms.Client, error) {
			return &kms.Client{}, nil
		})
		_, err := getKmeClient(mockSource, "test")
		assert.NilError(t, err)
	})
}

func Test_GetKmsClientByAgency(t *testing.T) {
	mockDecrypt := func(src string) ([]byte, error) {
		if src == "" {
			return []byte{}, errors.New("test")
		}
		return []byte{}, nil
	}

	t.Run("test Decrypt Ak failed", func(t *testing.T) {
		mockAgency := UserAgency{SecurityAk: ""}
		defer gomonkey.ApplyFunc(localauth.Decrypt, mockDecrypt).Reset()
		_, err := GetKmsClientByAgency(mockAgency, "test")
		assert.Error(t, err, "test")
	})

	t.Run("test Decrypt Sk failed", func(t *testing.T) {
		mockAgency := UserAgency{
			SecurityAk: "test",
			SecuritySk: "",
		}
		defer gomonkey.ApplyFunc(localauth.Decrypt, mockDecrypt).Reset()
		_, err := GetKmsClientByAgency(mockAgency, "test")
		assert.Error(t, err, "test")
	})

	t.Run("test Decrypt token failed", func(t *testing.T) {
		mockAgency := UserAgency{
			SecurityAk:    "test",
			SecuritySk:    "test",
			SecurityToken: "",
		}
		defer gomonkey.ApplyFunc(localauth.Decrypt, mockDecrypt).Reset()
		_, err := GetKmsClientByAgency(mockAgency, "test")
		assert.Error(t, err, "test")
	})

	t.Run("test get success", func(t *testing.T) {
		mockAgency := UserAgency{
			SecurityAk:    "test",
			SecuritySk:    "test",
			SecurityToken: "test",
		}
		p := gomonkey.ApplyFunc(localauth.Decrypt, mockDecrypt)
		defer p.Reset()
		p.ApplyFunc(kms.NewKmsClient, func(projectID string, ak, sk, securityToken string) *kms.Client {
			return &kms.Client{}
		})
		_, err := GetKmsClientByAgency(mockAgency, "test")
		assert.NilError(t, err)
	})
}

func Test_GetUserAgencyByFuncMetaInfo(t *testing.T) {
	t.Run("test agency nil", func(t *testing.T) {
		info := GetUserAgencyByFuncMetaInfo(nil)
		assert.Equal(t, info, UserAgency{})
	})

	t.Run("test no role", func(t *testing.T) {
		mockSource := &config.FunctionMetaInfo{
			ExtendedMetaData: config.ExtendedMetaData{
				Role: commontypes.Role{},
			},
		}
		info := GetUserAgencyByFuncMetaInfo(mockSource)
		assert.Equal(t, info, UserAgency{})
	})

	t.Run("test AppXRole not empty", func(t *testing.T) {
		mockSource := &config.FunctionMetaInfo{
			ExtendedMetaData: config.ExtendedMetaData{
				Role: commontypes.Role{
					AppXRole: "test",
				},
			},
		}

		p := gomonkey.ApplyFunc(GetTenantIDFromURN, func(funcUrn string) string {
			return "test"
		})
		defer p.Reset()
		p.ApplyFunc(GetUserAgencyFromCacheMap, func(tenant string, domainID string, agencyID string) UserAgency {
			return UserAgency{}
		})
		GetUserAgencyByFuncMetaInfo(mockSource)
	})

	t.Run("test XRole not empty", func(t *testing.T) {
		mockSource := &config.FunctionMetaInfo{
			ExtendedMetaData: config.ExtendedMetaData{
				Role: commontypes.Role{
					XRole: "test",
				},
			},
		}

		p := gomonkey.ApplyFunc(GetTenantIDFromURN, func(funcUrn string) string {
			return "test"
		})
		defer p.Reset()
		p.ApplyFunc(GetUserAgencyFromCacheMap, func(tenant string, domainID string, agencyID string) UserAgency {
			return UserAgency{}
		})
		GetUserAgencyByFuncMetaInfo(mockSource)
	})
}

func Test_GetUserCipherCache(t *testing.T) {
	userCipherMap.Store("test", UserCipherCache{})
	t.Run("test not get", func(t *testing.T) {
		_, b := GetUserCipherCache("testFailed")
		assert.Equal(t, b, false)
	})

	t.Run("test not get", func(t *testing.T) {
		_, b := GetUserCipherCache("test")
		assert.Equal(t, b, true)
	})
}

func Test_CheckUserCipherUsefulAndUpdateLastUseTime(t *testing.T) {
	mockCache := UserCipherCache{
		LastUseTime: time.Now(),
		IsUseful:    false,
	}

	t.Run("test no useful", func(t *testing.T) {
		ok := CheckUserCipherUsefulAndUpdateLastUseTime("test", mockCache)
		assert.Equal(t, ok, false)
	})

	t.Run("test useful", func(t *testing.T) {
		mockCache.IsUseful = true
		ok := CheckUserCipherUsefulAndUpdateLastUseTime("test", mockCache)
		assert.Equal(t, ok, true)
	})
}

func Test_CheckAndStoreCipherCache(t *testing.T) {
	mockFunctionService := &faasfunction.FunctionService{}
	mockCheckInfo := CheckKmsCipherInfo{
		TenantID:      "test",
		KmsKeyID:      "test",
		CipherKey:     "test",
		EncryptCipher: "test",
	}
	t.Run("test GetUserAgencyByFuncMeta failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) UserAgency {
			return UserAgency{}
		})
		defer p.Reset()
		err := CheckAndStoreCipherCache(mockFunctionService, mockCheckInfo)
		assert.Error(t, err, "failed to get agency")
	})

	mockAgency := UserAgency{
		SecurityAk: "test",
		SecuritySk: "test",
	}

	t.Run("test GetKmsClientByAgency failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) UserAgency {
			return mockAgency
		})
		defer p.Reset()
		p.ApplyFunc(GetKmsClientByAgency, func(agency UserAgency, tenantID string) (*kms.Client, error) {
			return &kms.Client{}, errors.New("test")
		})
		err := CheckAndStoreCipherCache(mockFunctionService, mockCheckInfo)
		assert.Error(t, err, "failed to get kms client, err: test")
	})

	t.Run("test DecryptDataKey failed", func(t *testing.T) {
		dataKey := "test"
		p := gomonkey.ApplyFunc(GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) UserAgency {
			return mockAgency
		})
		defer p.Reset()
		p.ApplyFunc(GetKmsClientByAgency, func(agency UserAgency, tenantID string) (*kms.Client, error) {
			return &kms.Client{}, nil
		})
		p.ApplyMethod(reflect.TypeOf(&kms.Client{}), "DecryptDataKey",
			func(_ *kms.Client, keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
				return &kmsModel.DecryptDatakeyResponse{DataKey: &dataKey}, errors.New("test")
			})
		err := CheckAndStoreCipherCache(mockFunctionService, mockCheckInfo)
		assert.Error(t, err, "user's cipher is not useful")
	})

	t.Run("test Success", func(t *testing.T) {
		mockDataKey := "test"
		p := gomonkey.ApplyFunc(GetUserAgencyByFuncMeta, func(funcMeta *faasfunction.FunctionService) UserAgency {
			return mockAgency
		})
		defer p.Reset()
		p.ApplyFunc(GetKmsClientByAgency, func(agency UserAgency, tenantID string) (*kms.Client, error) {
			return &kms.Client{}, nil
		})
		p.ApplyMethod(reflect.TypeOf(&kms.Client{}), "DecryptDataKey",
			func(_ *kms.Client, keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
				return &kmsModel.DecryptDatakeyResponse{
					DataKey: &mockDataKey,
				}, nil
			})
		err := CheckAndStoreCipherCache(mockFunctionService, mockCheckInfo)
		assert.NilError(t, err)
	})
}
