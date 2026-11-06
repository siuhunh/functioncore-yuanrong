/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package preload -
package preload

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	tools "functioncore/pkg/worker-manager/utils"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1 "k8s.io/client-go/informers/core/v1"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	k8cache "k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/cffclient"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
)

func TestGetClient(t *testing.T) {
	client = &Client{}
	c := GetClient()
	assert.NotNil(t, c)
}

func TestClient_AddAllPreloadToOneNode(t *testing.T) {
	c := mockClient()
	t.Run("node in nil", func(t *testing.T) {
		err := c.AddAllPreloadToOneNode(nil)
		assert.EqualError(t, err, "node is nil")
	})
	t.Run("node has no address", func(t *testing.T) {
		err := c.AddAllPreloadToOneNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"}})
		assert.EqualError(t, err, "node test has no address")
	})
	t.Run("cff agent is not ready", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(checkCffAgentIsReady, func(nodeName string) bool {
			return false
		})
		defer patch.Reset()
		err := c.AddAllPreloadToOneNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"},
			Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{Address: "127.0.0.1"}}}})
		assert.EqualError(t, err, "cff-agent of node test is not ready")
	})
	t.Run("success", func(t *testing.T) {
		cache.GetPreloadCache().AddLayer("GelALLPreloadInfo_1",
			"GelALLPreloadInfo_layer1", config.PreloadInfo{Sha512: "1"})
		cache.GetPreloadCache().AddLayer("GelALLPreloadInfo_1",
			"GelALLPreloadInfo_layer2", config.PreloadInfo{Sha512: "2"})
		cache.GetPreloadCache().AddFunction("GelALLPreloadInfo_1",
			"GelALLPreloadInfo_function1", config.PreloadInfo{Sha512: "3"})
		cache.GetPreloadCache().AddFunction("GelALLPreloadInfo_1",
			"GelALLPreloadInfo_function2", config.PreloadInfo{Sha512: "4"})
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.cffClient), "POST", func(_ *cffclient.Client,
			ctx context.Context, address, apiPath string, payload []byte) (*http.Response, error) {
			return &http.Response{StatusCode: 200}, nil
		})
		patch.ApplyFunc(checkCffAgentIsReady, func(nodeName string) bool {
			return true
		})
		err := c.AddAllPreloadToOneNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test"},
			Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{Address: "127.0.0.1"}}}})
		defer patch.Reset()
		assert.NoError(t, err)
	})
}

func mockClient() *Client {
	addTaskPool, _ := ants.NewPool(maxBlockingTasks, ants.WithPreAlloc(true),
		ants.WithMaxBlockingTasks(maxNumAddPreloadTaskRun))
	delTaskPool, _ := ants.NewPool(maxBlockingTasks, ants.WithPreAlloc(true),
		ants.WithMaxBlockingTasks(maxNumDeletePreloadTaskRun))
	return &Client{
		cffClient:      &cffclient.Client{},
		addTaskPool:    addTaskPool,
		deleteTaskPool: delTaskPool,
	}
}

func TestClient_ReAddPreloadToOneNode(t *testing.T) {
	t.Run("preload info not exists", func(t *testing.T) {
		c := mockClient()
		err := c.ReAddPreloadToOneNode("not exists", "xxx",
			"hostIP", constants.PreloadFunctionType)
		assert.EqualError(t, err, "preload info not exists")
	})
	t.Run("failed to Marshal payloadInfo", func(t *testing.T) {
		c := mockClient()
		cache.GetPreloadCache().AddFunction("ReAddPreloadToOneNode_1",
			"ReAddPreloadToOneNode_function1", config.PreloadInfo{Sha512: "1"})
		patch := gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
			return nil, errors.New("failed Marshal")
		})
		defer patch.Reset()
		err := c.ReAddPreloadToOneNode("ReAddPreloadToOneNode_1", "ReAddPreloadToOneNode_function1",
			"hostIP", constants.PreloadFunctionType)
		assert.EqualError(t, err, "failed Marshal")
	})
	t.Run("failed to post", func(t *testing.T) {
		c := mockClient()
		cache.GetPreloadCache().AddFunction("ReAddPreloadToOneNode_1",
			"ReAddPreloadToOneNode_function1", config.PreloadInfo{Sha512: "1"})
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.cffClient), "POST", func(_ *cffclient.Client,
			ctx context.Context, address, apiPath string, payload []byte) (*http.Response, error) {
			return nil, errors.New("failed post")
		})
		defer patch.Reset()
		err := c.ReAddPreloadToOneNode("ReAddPreloadToOneNode_1", "ReAddPreloadToOneNode_function1",
			"hostIP", constants.PreloadFunctionType)
		assert.EqualError(t, err, "failed post")
	})
	t.Run("success", func(t *testing.T) {
		c := mockClient()
		cache.GetPreloadCache().AddFunction("ReAddPreloadToOneNode_1",
			"ReAddPreloadToOneNode_function1", config.PreloadInfo{Sha512: "1"})
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.cffClient), "POST", func(_ *cffclient.Client,
			ctx context.Context, address, apiPath string, payload []byte) (*http.Response, error) {
			return &http.Response{StatusCode: 200}, nil
		})
		defer patch.Reset()
		err := c.ReAddPreloadToOneNode("ReAddPreloadToOneNode_1", "ReAddPreloadToOneNode_function1",
			"hostIP", constants.PreloadFunctionType)
		assert.NoError(t, err)
	})
	t.Run("repeat readd when previous readd is doing ", func(t *testing.T) {
		c := mockClient()
		cache.GetPreloadCache().AddFunction("ReAddPreloadToOneNode_1",
			"ReAddPreloadToOneNode_function1", config.PreloadInfo{Sha512: "1"})
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.cffClient), "POST", func(_ *cffclient.Client,
			ctx context.Context, address, apiPath string, payload []byte) (*http.Response, error) {
			time.Sleep(1 * time.Second)
			return &http.Response{StatusCode: 200}, nil
		})
		defer patch.Reset()
		go c.ReAddPreloadToOneNode("ReAddPreloadToOneNode_1", "ReAddPreloadToOneNode_function1",
			"hostIP", constants.PreloadFunctionType)
		time.Sleep(100 * time.Millisecond)
		err := c.ReAddPreloadToOneNode("ReAddPreloadToOneNode_1", "ReAddPreloadToOneNode_function1",
			"hostIP", constants.PreloadFunctionType)
		assert.Errorf(t, err, "repeat add preload on same node")
	})
}

type fakeNodeInformer struct {
}

func (f fakeNodeInformer) Informer() k8cache.SharedIndexInformer {
	return nil
}

func (f fakeNodeInformer) Lister() listerscorev1.NodeLister {
	return &fakeNodeLister{}
}

type fakeNodeLister struct {
}

func (f fakeNodeLister) List(selector labels.Selector) (ret []*v1.Node, err error) {
	return nil, nil
}

func (f fakeNodeLister) Get(name string) (*v1.Node, error) {
	return nil, nil
}

func mockNodeInformer(patch *gomonkey.Patches, nodeInformer corev1.NodeInformer) {
	patch.ApplyFunc(tools.GetNodeInformer, func() corev1.NodeInformer {
		return nodeInformer
	})
}

func mockNodeLister(patch *gomonkey.Patches, nodeLister listerscorev1.NodeLister) {
	patch.ApplyMethod(reflect.TypeOf(fakeNodeInformer{}), "Lister",
		func(_ fakeNodeInformer) listerscorev1.NodeLister {
			return nodeLister
		})
}

func mockNodeListerList(patch *gomonkey.Patches, ret []*v1.Node, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fakeNodeLister{}), "List",
		func(_ *fakeNodeLister, selector labels.Selector) ([]*v1.Node, error) {
			return ret, err
		})
}

func TestClient_DelPreloadOfALLNodes(t *testing.T) {
	c := mockClient()
	patch := gomonkey.NewPatches()
	mockNodeInformer(patch, fakeNodeInformer{})
	mockNodeLister(patch, fakeNodeLister{})
	mockNodeListerList(patch, []*v1.Node{{Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{Address: "127.0.0.1"}}}}}, nil)
	patch.ApplyFunc(c.deleteTaskPool.Submit, func(_ func()) error { return nil })
	defer patch.Reset()
	ctx, cancel := context.WithTimeout(context.Background(), preloadToAllNodesTaskTimeout)
	d := DelOfAllNodesTask{
		ctx:        ctx,
		cancelFunc: cancel,
		client:     c,
	}
	err := d.Run("TestClient_AddPreloadToALLNodes", constants.PreloadFunctionType, "versionName")
	assert.NoError(t, err)
}

func TestClient_AddPreloadToALLNodes(t *testing.T) {
	c := mockClient()
	ctx, cancel := context.WithTimeout(context.Background(), preloadToAllNodesTaskTimeout)
	a := AddToAllNodesTask{
		ctx:        ctx,
		cancelFunc: cancel,
		client:     c,
	}
	t.Run("success addPreloadToALLNodes", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeInformer(patch, fakeNodeInformer{})
		mockNodeLister(patch, fakeNodeLister{})
		mockNodeListerList(patch, []*v1.Node{{Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{Address: "127.0.0.1"}}}}}, nil)
		patch.ApplyFunc(c.addTaskPool.Submit, func(_ func()) error { return nil })
		defer patch.Reset()
		err := a.Run("TestClient_AddPreloadToALLNodes", constants.PreloadFunctionType, config.PreloadInfo{})
		time.Sleep(1 * time.Second)
		assert.NoError(t, err)
	})
	t.Run("failed to marshal preload info", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeInformer(patch, fakeNodeInformer{})
		mockNodeLister(patch, fakeNodeLister{})
		mockNodeListerList(patch, []*v1.Node{{Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{Address: "127.0.0.1"}}}}}, nil)
		patch.ApplyFunc(json.Marshal, func(v any) ([]byte, error) { return nil, errors.New("marshal err") })
		defer patch.Reset()
		err := a.Run("TestClient_AddPreloadToALLNodes", constants.PreloadFunctionType, config.PreloadInfo{})
		time.Sleep(1 * time.Second)
		assert.Error(t, err)
	})
}

func TestIsEnable(t *testing.T) {
	defer func() {
		preloadEnable = "false"
	}()
	enable := IsEnable()
	assert.False(t, enable)
	preloadEnable = "true"
	enable = IsEnable()
	assert.True(t, enable)
}

func TestCreateClient(t *testing.T) {
	p := gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
		func(a *alarm.Client, almType alarm.Type) {
			return
		})
	p.ApplyFunc(cffclient.WithAuth, func(ak, sk string) cffclient.ClientOption {
		return func(*cffclient.Client) {}
	})
	p.ApplyFunc(cffclient.WithTLSConfig, func(tlsConfig *tls.Config) cffclient.ClientOption {
		return func(*cffclient.Client) {}
	})
	defer func() {
		preloadEnable = "false"
		p.Reset()
	}()
	t.Run("preload is not enable", func(t *testing.T) {
		preloadEnable = "false"
		stopChan := make(chan struct{})
		err := CreateClient(stopChan)
		assert.NoError(t, err)
	})
	t.Run("failed to get config", func(t *testing.T) {
		preloadEnable = "true"
		stopChan := make(chan struct{})
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return nil, errors.New("config error")
		})
		defer patch.Reset()
		err := CreateClient(stopChan)
		assert.EqualError(t, err, "config error")
	})
	t.Run("failed to new cff client", func(t *testing.T) {
		preloadEnable = "true"
		stopChan := make(chan struct{})
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{AKey: "", SKey: ""}}, nil
		})
		patch.ApplyFunc(cffclient.NewClient, func(timeout time.Duration,
			opts ...cffclient.ClientOption) (*cffclient.Client, error) {
			return nil, errors.New("cffclient error")
		})
		defer patch.Reset()
		err := CreateClient(stopChan)
		assert.EqualError(t, err, "cffclient error")
	})
	t.Run("failed to new ants.NewPool", func(t *testing.T) {
		preloadEnable = "true"
		stopChan := make(chan struct{})
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{AKey: "", SKey: ""}}, nil
		})
		patch.ApplyFunc(cffclient.NewClient, func(timeout time.Duration,
			opts ...cffclient.ClientOption) (*cffclient.Client, error) {
			return &cffclient.Client{}, nil
		})
		patch.ApplyFunc(ants.NewPool, func(size int, options ...ants.Option) (*ants.Pool, error) {
			return nil, errors.New("ants error")
		})
		defer patch.Reset()
		err := CreateClient(stopChan)
		assert.EqualError(t, err, "ants error")
	})
	t.Run("succeed to create client", func(t *testing.T) {
		preloadEnable = "true"
		stopChan := make(chan struct{})
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{AKey: "", SKey: ""}}, nil
		})
		patch.ApplyFunc(cffclient.NewClient, func(timeout time.Duration,
			opts ...cffclient.ClientOption) (*cffclient.Client, error) {
			return &cffclient.Client{}, nil
		})
		patch.ApplyFunc(ants.NewPool, func(size int, options ...ants.Option) (*ants.Pool, error) {
			return &ants.Pool{}, nil
		})
		defer func() {
			patch.Reset()
			close(stopChan)
		}()
		err := CreateClient(stopChan)
		assert.NoError(t, err)
	})
}

func TestAddAndRemoveRequestFailedNode(t *testing.T) {
	defer requestFailedNodeCount.Store(0)
	requestFailedNodeCount.Store(0)
	// add a failed node
	addRequestFailedNode("test1")
	assert.Equal(t, int64(1), requestFailedNodeCount.Load())
	// repeat add same failed node
	addRequestFailedNode("test1")
	assert.Equal(t, int64(1), requestFailedNodeCount.Load())
	// add other failed node
	addRequestFailedNode("test2")
	assert.Equal(t, int64(2), requestFailedNodeCount.Load())
	// remove a failed node
	removeRequestFailedNodeIfExists("test2")
	assert.Equal(t, int64(1), requestFailedNodeCount.Load())
	// remove all failed node
	removeRequestFailedNodeIfExists("test1")
	assert.Equal(t, int64(0), requestFailedNodeCount.Load())
	// remove not exists failed node
	removeRequestFailedNodeIfExists("test3")
	assert.Equal(t, int64(0), requestFailedNodeCount.Load())
}

func TestCheckIsExistsFailedNode(t *testing.T) {
	t.Run("not exists failed node", func(t *testing.T) {
		exists := checkIsExistsFailedNode()
		assert.False(t, exists)
	})
	t.Run("exists failed node", func(t *testing.T) {
		requestFailedNodeCount.Add(1)
		exists := checkIsExistsFailedNode()
		defer func() {
			requestFailedNodeCount.Store(0)
			existsFailedNode = false
		}()
		assert.True(t, exists)
	})
	t.Run("exists a failed node and turn to not exists", func(t *testing.T) {
		requestFailedNodeCount.Add(1)
		defer func() {
			requestFailedNodeCount.Store(0)
			existsFailedNode = false
		}()
		exists := checkIsExistsFailedNode()
		assert.True(t, exists)
		requestFailedNodeCount.Add(-1)
		exists = checkIsExistsFailedNode()
		assert.False(t, exists)
	})
}

func TestCheckCffAgentIsReady(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(getCffAgentPod, func(nodeName, podName string) (*v1.Pod, error) {
			return &v1.Pod{Status: v1.PodStatus{Phase: "Running"}}, nil
		})
		defer patch.Reset()
		ready := checkCffAgentIsReady("test")
		assert.True(t, ready)
	})
	t.Run("check three times to ready", func(t *testing.T) {
		staticCount := 0
		patch := gomonkey.ApplyFunc(getCffAgentPod, func(nodeName, podName string) (*v1.Pod, error) {
			staticCount++
			switch staticCount {
			case 1:
				return nil, errors.New("error")
			case 2:
				return &v1.Pod{Status: v1.PodStatus{Phase: "Failed"}}, nil
			case 3:
				return &v1.Pod{Status: v1.PodStatus{Phase: "Running"}}, nil
			default:
				return &v1.Pod{Status: v1.PodStatus{Phase: "Running"}}, nil
			}
		})
		defer patch.Reset()
		ready := checkCffAgentIsReady("test")
		assert.True(t, ready)
	})
}

type fakePodLister struct{}

func (s *fakePodLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	return nil, nil
}

func (s *fakePodLister) Pods(namespace string) listerscorev1.PodNamespaceLister {
	return fakePodNamespaceLister{}
}

type fakePodNamespaceLister struct{}

func (s fakePodNamespaceLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	return nil, nil
}

func (s fakePodNamespaceLister) Get(name string) (*v1.Pod, error) {
	return nil, nil
}

type fakePodInformer struct {
}

func (f fakePodInformer) Informer() k8cache.SharedIndexInformer {
	return nil
}

func (f fakePodInformer) Lister() listerscorev1.PodLister {
	return &fakePodLister{}
}

func mockGetPodInformer(patch *gomonkey.Patches, informer corev1.PodInformer) {
	patch.ApplyFunc(tools.GetPodInformer, func() corev1.PodInformer {
		return informer
	})
}

func TestGetCffAgentPod(t *testing.T) {
	t.Run("succeed to get cffAgent pod by podName", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetPodInformer(patch, &fakePodInformer{})
		patch.ApplyMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "Get",
			func(_ *fakePodNamespaceLister, name string) (*v1.Pod, error) {
				return &v1.Pod{}, nil
			})
		defer patch.Reset()
		_, err := getCffAgentPod("testNode", "testPod")
		assert.NoError(t, err)
	})
	t.Run("failed to get cffAgent pod by podName", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetPodInformer(patch, &fakePodInformer{})
		patch.ApplyMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "Get",
			func(_ *fakePodNamespaceLister, name string) (*v1.Pod, error) {
				return nil, errors.New("error")
			})
		defer patch.Reset()
		_, err := getCffAgentPod("testNode", "testPod")
		assert.Error(t, err)
	})
	t.Run("cffagent pod not exists by nodeName", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetPodInformer(patch, &fakePodInformer{})
		patch.ApplyMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "List",
			func(_ *fakePodNamespaceLister, selector labels.Selector) ([]*v1.Pod, error) {
				return []*v1.Pod{{Spec: v1.PodSpec{NodeName: "notTestNode"}}}, nil
			})
		defer patch.Reset()
		_, err := getCffAgentPod("testNode", "")
		assert.Error(t, err, "cffagent of node testNode not exists")
	})
	t.Run("cffagent pod exists by nodeName", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetPodInformer(patch, &fakePodInformer{})
		patch.ApplyMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "List",
			func(_ *fakePodNamespaceLister, selector labels.Selector) ([]*v1.Pod, error) {
				return []*v1.Pod{{Spec: v1.PodSpec{NodeName: "testNode"}}}, nil
			})
		defer patch.Reset()
		pod, err := getCffAgentPod("testNode", "")
		assert.NoError(t, err)
		assert.Equal(t, "testNode", pod.Spec.NodeName)
	})
}

func TestClient_StartAddPreloadToAllNodes(t *testing.T) {
	c := mockClient()
	patch := gomonkey.NewPatches()
	patch.ApplyMethod(reflect.TypeOf(&AddToAllNodesTask{}), "Run",
		func(_ *AddToAllNodesTask, tenantID, typeStr string, preloadInfo config.PreloadInfo) error {
			return nil
		})
	defer patch.Reset()
	preloadInfo := config.PreloadInfo{
		VersionUrn: "vur",
	}
	c.StartAddPreloadToAllNodes("123", "function", preloadInfo)
	preloadTaskKey := getPreloadTaskKey("123", "function", preloadInfo.VersionUrn)
	v, ok := c.addToAllNodeTaskMap.Load(preloadTaskKey)
	assert.True(t, ok)
	previousTask, ok := v.(*AddToAllNodesTask)
	assert.True(t, ok)
	c.StartAddPreloadToAllNodes("123", "function", preloadInfo)
	time.Sleep(1 * time.Second)
	assert.NotNil(t, previousTask.ctx.Err())
}

func TestClient_StartDelPreloadOfALLNodes(t *testing.T) {
	c := mockClient()
	patch := gomonkey.NewPatches()
	patch.ApplyMethod(reflect.TypeOf(&DelOfAllNodesTask{}), "Run",
		func(_ *DelOfAllNodesTask, tenantID, typeStr, versionName string) error {
			return nil
		})
	defer patch.Reset()
	c.StartDelPreloadOfALLNodes("123", "function", "func-latest")
	preloadTaskKey := getPreloadTaskKey("123", "function", "func-latest")
	v, ok := c.delOfAllNodeTaskMap.Load(preloadTaskKey)
	assert.True(t, ok)
	previousTask, ok := v.(*DelOfAllNodesTask)
	assert.True(t, ok)
	c.StartDelPreloadOfALLNodes("123", "function", "func-latest")
	time.Sleep(1 * time.Second)
	assert.NotNil(t, previousTask.ctx.Err())
}

func TestCheckAndReportWarn(t *testing.T) {
	defer func() {
		preloadEnable = "false"
	}()
	t.Run("preload is not enable", func(t *testing.T) {
		preloadEnable = "false"
		pod := &v1.Pod{}
		err := CheckAndReportWarn(pod)
		assert.NoError(t, err)
	})
	t.Run("preload is not enable", func(t *testing.T) {
		preloadEnable = "true"
		pod := &v1.Pod{}
		patch := gomonkey.NewPatches()
		patch.ApplyMethod(reflect.TypeOf(pod), "GetLabels",
			func(_ *v1.Pod) map[string]string {
				return map[string]string{tools.LabelIsPreload: "false"}
			})
		defer patch.Reset()
		err := CheckAndReportWarn(pod)
		assert.NoError(t, err)
	})
}

func TestProcessAddPreloadErrorResponse(t *testing.T) {
	t.Run("failed to read body", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(io.ReadAll,
			func(r io.Reader) ([]byte, error) {
				return nil, errors.New("err")
			})
		defer patch.Reset()
		errMsg := processAddPreloadErrorResponse(&http.Response{StatusCode: 500}, "127.0.0.1", "TD1")
		assert.Equal(t, "response statusCode 500", errMsg)
	})
	t.Run("failed to unmarshal body", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(io.ReadAll,
			func(r io.Reader) ([]byte, error) {
				return []byte{'1'}, nil
			})
		defer patch.Reset()
		errMsg := processAddPreloadErrorResponse(&http.Response{StatusCode: 500}, "127.0.0.1", "TD1")
		assert.Equal(t, "response statusCode 500", errMsg)
	})
	t.Run("disk usage of preload exceeds the limit", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(io.ReadAll,
			func(r io.Reader) ([]byte, error) {
				data, _ := json.Marshal(&AddPreloadError{ErrCode: reachPreloadDiskLimitCode})
				return data, nil
			})
		defer func() {
			patch.Reset()
			resetPreloadRequestFailedTestData()
		}()
		errMsg := processAddPreloadErrorResponse(&http.Response{StatusCode: 500}, "127.0.0.1", "TD1")
		assert.Equal(t, "disk usage of preload on node 127.0.0.1 exceeds the limit", errMsg)
		tenantIDSet, ok := exceedTenantPreloadDiskLimitNodeMap["127.0.0.1"]
		assert.True(t, ok)
		_, ok = tenantIDSet["TD1"]
		assert.False(t, ok)
		_, ok = tenantIDSet[exceedPreloadDiskLimitTenant]
		assert.True(t, ok)
	})
	t.Run("disk usage of preload tenant exceeds the limit", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(io.ReadAll,
			func(r io.Reader) ([]byte, error) {
				data, _ := json.Marshal(&AddPreloadError{ErrCode: reachTenantPreloadDiskLimitCode})
				return data, nil
			})
		defer func() {
			patch.Reset()
			resetPreloadRequestFailedTestData()
		}()
		errMsg := processAddPreloadErrorResponse(&http.Response{StatusCode: 500}, "127.0.0.1", "TD1")
		assert.Equal(t, "disk usage of the tenant TD1 preload on node 127.0.0.1 exceeds the limit", errMsg)
		tenantIDSet, ok := exceedTenantPreloadDiskLimitNodeMap["127.0.0.1"]
		assert.True(t, ok)
		_, ok = tenantIDSet["TD1"]
		assert.True(t, ok)
	})
	t.Run("other problem", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(io.ReadAll,
			func(r io.Reader) ([]byte, error) {
				data, _ := json.Marshal(&AddPreloadError{ErrCode: "xxxxx", ErrMessage: "other problem"})
				return data, nil
			})
		defer func() {
			patch.Reset()
			resetPreloadRequestFailedTestData()
		}()
		errMsg := processAddPreloadErrorResponse(&http.Response{StatusCode: 500}, "127.0.0.1", "TD1")
		assert.Equal(t, "other problem", errMsg)
		tenantIDSet, ok := otherProblemNodeMap["127.0.0.1"]
		assert.True(t, ok)
		_, ok = tenantIDSet["TD1"]
		assert.True(t, ok)
	})
}
