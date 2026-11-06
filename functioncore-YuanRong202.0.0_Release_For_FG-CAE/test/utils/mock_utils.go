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

package utils

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"
	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	corev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	k8scache "k8s.io/client-go/tools/cache"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
	fakemetrics "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

var K8sFakeClient kubernetes.Interface
var InformerFakeFactory informers.SharedInformerFactory
var K8sMetricsFakeClient metrics.Interface = fakemetrics.NewSimpleClientset()

var RedisFakeClient *redis.Client

func init() {
	FakeK8sInit()
	FakeRedisInit()
}

type K8sPodImp struct {
	client restclient.Interface
	ns     string
}
type K8sWatchImp struct{}
type RestResponseWrapperImp struct{}
type ReadCloserImp struct{}

func NewK8sPodImp(namespace string) typecorev1.PodInterface {
	return &K8sPodImp{
		client: &restclient.RESTClient{},
		ns:     namespace,
	}
}

func (resp *RestResponseWrapperImp) DoRaw(context.Context) ([]byte, error) {
	return []byte{}, nil
}
func (resp *RestResponseWrapperImp) Stream(context.Context) (io.ReadCloser, error) {
	return &ReadCloserImp{}, nil
}

func (w *K8sWatchImp) Stop() {}
func (w *K8sWatchImp) ResultChan() <-chan watch.Event {
	return make(<-chan watch.Event)
}

func (p *K8sPodImp) Create(ctx context.Context, pod *v1.Pod, opts metav1.CreateOptions) (*v1.Pod, error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) Update(ctx context.Context, pod *v1.Pod, opts metav1.UpdateOptions) (*v1.Pod, error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) UpdateStatus(ctx context.Context, pod *v1.Pod, opts metav1.UpdateOptions) (*v1.Pod, error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return nil
}

func (p *K8sPodImp) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return nil
}

func (p *K8sPodImp) Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.Pod, error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) List(ctx context.Context, opts metav1.ListOptions) (*v1.PodList, error) {
	return &v1.PodList{}, nil
}

func (p *K8sPodImp) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return &K8sWatchImp{}, nil
}

func (p *K8sPodImp) Patch(ctx context.Context, name string, pt types.PatchType,
	data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.Pod, err error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) Apply(ctx context.Context, pod *corev1.PodApplyConfiguration,
	opts metav1.ApplyOptions) (result *v1.Pod, err error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) ApplyStatus(ctx context.Context, pod *corev1.PodApplyConfiguration,
	opts metav1.ApplyOptions) (result *v1.Pod, err error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) UpdateEphemeralContainers(ctx context.Context, podName string, pod *v1.Pod,
	opts metav1.UpdateOptions) (*v1.Pod, error) {
	return &v1.Pod{}, nil
}

func (p *K8sPodImp) Bind(ctx context.Context, binding *v1.Binding, opts metav1.CreateOptions) error {
	return nil
}

func (p *K8sPodImp) Evict(ctx context.Context, eviction *policyv1beta1.Eviction) error {
	return nil
}

func (p *K8sPodImp) EvictV1(ctx context.Context, eviction *policyv1.Eviction) error {
	return nil
}

func (p *K8sPodImp) EvictV1beta1(ctx context.Context, eviction *policyv1beta1.Eviction) error {
	return nil
}

func (p *K8sPodImp) GetLogs(name string, opts *v1.PodLogOptions) *restclient.Request {
	return &restclient.Request{}
}

func (p *K8sPodImp) ProxyGet(scheme, name, port, path string, params map[string]string) restclient.ResponseWrapper {
	return &RestResponseWrapperImp{}
}

func FakeK8sInit() {
	node1 := &v1.Node{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Status: v1.NodeStatus{

			Conditions: []v1.NodeCondition{
				{Type: "Ready", Status: "True"},
			},
		},
	}

	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod1",
			Namespace:         "default",
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
	}
	K8sFakeClient = fake.NewSimpleClientset(node1, pod1)
	InformerFakeFactory = informers.NewSharedInformerFactory(K8sFakeClient, 0)
	// make node informer has data
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeInformers := InformerFakeFactory.Core().V1().Nodes().Informer()
	podInformers := InformerFakeFactory.Core().V1().Pods().Informer()
	secretInformers := InformerFakeFactory.Core().V1().Secrets().Informer()
	jobInformers := InformerFakeFactory.Batch().V1().Jobs().Informer()
	InformerFakeFactory.Start(ctx.Done())
	k8scache.WaitForCacheSync(nil, nodeInformers.HasSynced, podInformers.HasSynced, secretInformers.HasSynced, jobInformers.HasSynced)
}

func FakeRedisInit() {
	RedisFakeClient = redis.NewClient(&redis.Options{})
}

type PatchSlice []*gomonkey.Patches
type PatchesFunc func() PatchSlice

func InitPatchSlice() PatchSlice {
	return make([]*gomonkey.Patches, 0)
}

func (p *PatchSlice) Append(patches PatchSlice) {
	if len(patches) > 0 {
		*p = append(*p, patches...)
	}
}

func (p PatchSlice) ResetAll() {
	for _, item := range p {
		item.Reset()
	}
}

type FakeEtcdLease struct {
}

func (m FakeEtcdLease) Grant(_ context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 1}, nil
}

func (m FakeEtcdLease) Revoke(_ context.Context, _ clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return nil, nil
}

func (m FakeEtcdLease) TimeToLive(_ context.Context, _ clientv3.LeaseID, _ ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	return nil, nil
}

func (m FakeEtcdLease) Leases(_ context.Context) (*clientv3.LeaseLeasesResponse, error) {
	return nil, nil
}

func (m FakeEtcdLease) KeepAlive(_ context.Context, _ clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return nil, nil
}

func (m FakeEtcdLease) KeepAliveOnce(_ context.Context, _ clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	return nil, nil
}

func (m FakeEtcdLease) Close() error {
	return errors.New("close error")
}

type MockResponseWriter struct {
}

func (m MockResponseWriter) Header() http.Header {
	return http.Header{}
}

func (m MockResponseWriter) Write(_ []byte) (int, error) {
	return 0, nil
}

func (m MockResponseWriter) WriteHeader(_ int) {

}

func (rc ReadCloserImp) Read(r []byte) (n int, err error) {
	return 0, errors.New("EOF")
}

func (ReadCloserImp) Close() error {
	return nil
}

type FakeMaintenance struct {
}

func (f *FakeMaintenance) AlarmList(ctx context.Context) (*clientv3.AlarmResponse, error) {
	return nil, nil
}

func (f *FakeMaintenance) AlarmDisarm(ctx context.Context, m *clientv3.AlarmMember) (*clientv3.AlarmResponse, error) {
	return nil, nil
}

func (f *FakeMaintenance) Defragment(ctx context.Context, endpoint string) (*clientv3.DefragmentResponse, error) {
	return nil, nil
}

func (f *FakeMaintenance) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	return nil, nil
}

func (f *FakeMaintenance) HashKV(ctx context.Context, endpoint string, rev int64) (*clientv3.HashKVResponse, error) {
	return nil, nil
}

func (f *FakeMaintenance) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return nil, nil
}

func (f *FakeMaintenance) MoveLeader(ctx context.Context, transfereeID uint64) (*clientv3.MoveLeaderResponse, error) {
	return nil, nil
}

// MockLocalMountPrefix -
const MockLocalMountPrefix = "MockLocalMountPrefix"

func GenerateTestCert() []tls.Certificate {
	key, err := rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		return nil
	}
	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	keyPEM := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: keyBytes,
		},
	)

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * 10 * time.Hour)

	template := x509.Certificate{
		SerialNumber:          big.NewInt(0),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		SignatureAlgorithm:    x509.SHA256WithRSA,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyAgreement | x509.KeyUsageKeyEncipherment | x509.KeyUsageDataEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil

	}
	certPem := pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: derBytes,
		},
	)

	cert, _ := tls.X509KeyPair(certPem, keyPEM)
	return []tls.Certificate{cert}
}
