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

package functionlb

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	fhttp "github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/frontend-invoke/constant"
	frontendConstants "functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/grpcconn"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/loadbalancer/service"
	"functioncore/pkg/frontend-invoke/model"
	"functioncore/pkg/frontend-invoke/statuscode"
	"functioncore/pkg/frontend-invoke/util"
	mockutils "functioncore/test/utils"
)

func TestWorker_String(t *testing.T) {
	workerMap := &sync.Map{}
	workerMap.Store("aaa", &Worker{})
	workerMap.Store(111, &Worker{})
	workerMap.Store("bbb", &Worker{})
	workerMap.Store("ccc", "ccc")
	workerMap.Store("ddd", &Worker{})
	task := struct {
		workers Workers
		wantLen int
	}{
		workers: Workers{
			WorkerMap: workerMap,
		},
		wantLen: 3,
	}
	assert.Equal(t, task.wantLen, len(strings.Split(task.workers.String(), ",")))
}

func Test_setLubanBody(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		body    []byte

		exceptedBody []byte
	}{
		{
			name:         "empty header, arbitrary body",
			body:         []byte("Hello World"),
			exceptedBody: []byte("Hello World"),
		},
		{
			name:         "empty header, json body",
			body:         []byte(`{"key": "value"}`),
			exceptedBody: []byte(`{"key": "value"}`),
		},
		{
			name:         "apm header, non-json body",
			headers:      map[string]string{constants.HeaderLuBanGTraceID: "id1"},
			body:         []byte("Hello World"),
			exceptedBody: []byte("Hello World"),
		},
		{
			name:         "apm header, json body",
			headers:      map[string]string{constants.HeaderLuBanGTraceID: "id1"},
			body:         []byte(`{"key": "value"}`),
			exceptedBody: []byte(`{"key":"value","lubanops-gtrace-id":"id1","lubanops-ndomain-id":"","lubanops-nenv-id":"","lubanops-nspan-id":"","lubanops-ntrace-id":"","lubanops-sevent-id":""}`),
		},
	}

	for _, test := range tests {
		req := fasthttp.AcquireRequest()
		for k, v := range test.headers {
			req.Header.Set(k, v)
		}
		req.SetBody(test.body)

		setLubanBody(req)

		assert.Equal(t, test.exceptedBody, req.Body(), test.name)
	}
}

func Test_DelFromLbByWorkerType(t *testing.T) {
	w := &Worker{
		workerType: util.NormalWorkerIns,
		lbWrapper: &lbWrapper{
			lb: service.CreateLoadBalancer("funcKeyWithRes", 1000, false, false),
		},
	}
	infos := &util.WorkerInstance{
		IP:         "127.0.0.1",
		InstanceID: "NormalWorkerIns",
	}
	w.addToLbByWorkerType(infos)
	w.delFromLbByWorkerType(infos)
	workerProxy := &Worker{
		workerType: util.ProxyIns,
		lbWrapper: &lbWrapper{
			lb: service.CreateLoadBalancer("funcKeyWithRes", 1000, false, false),
		},
	}
	infosProxy := &util.WorkerInstance{
		IP:         "127.0.0.1",
		InstanceID: "ProxyIns",
	}
	workerProxy.addToLbByWorkerType(infosProxy)
	workerProxy.delFromLbByWorkerType(infosProxy)
}

func Test_Worker_checkInnerCode(t *testing.T) {
	w := Worker{
		lbWrapper: createLBWrapper(loadbalance.NewRandomRR()),
	}
	params := &model.FunctionRequestInfo{}

	tests := []struct {
		name      string
		innerCode int
		excepted  bool
	}{
		{
			name:      "request ok",
			innerCode: 0,
			excepted:  true,
		},
		{
			name:      "reach max instance",
			innerCode: snerror.ReachMaxInstancesCode,
			excepted:  true,
		},
		{
			name:      "retry error code",
			innerCode: statuscode.RetryError,
			excepted:  true,
		},
		{
			name:      "worker exit error",
			innerCode: snerror.WorkerExitErrCode,
			excepted:  false,
		},
		{
			name:      "user function update error",
			innerCode: snerror.UserFuncIsUpdatedCode,
			excepted:  false,
		},
		{
			name:      "client exit error",
			innerCode: snerror.ClientExitErrCode,
			excepted:  false,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.excepted, w.checkInnerCode(test.innerCode, params), test.name)
	}
}

func Test_Nodes(t *testing.T) {
	nodes := Nodes{}

	tests := []struct {
		name      string
		key       string
		eventType int

		exceptedNodeLen int
	}{
		{
			name:      "error key of add event",
			key:       "/sn/workers/business/",
			eventType: etcd3.PUT,

			exceptedNodeLen: 0,
		},
		{
			name:      "error key of delete event",
			key:       "/sn/workers/business/",
			eventType: etcd3.DELETE,

			exceptedNodeLen: 0,
		},
		{
			name:      "normal add event",
			key:       "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/node1",
			eventType: etcd3.PUT,

			exceptedNodeLen: 1,
		},
		{
			name:      "repeat add event",
			key:       "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/node1",
			eventType: etcd3.PUT,

			exceptedNodeLen: 1,
		},
		{
			name:      "delete event not exist",
			key:       "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/noexist",
			eventType: etcd3.DELETE,

			exceptedNodeLen: 1,
		},
		{
			name:      "delete exist",
			key:       "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/node1",
			eventType: etcd3.DELETE,

			exceptedNodeLen: 0,
		},
	}

	for _, test := range tests {
		switch test.eventType {
		case etcd3.PUT:
			nodes.AddNode(&Node{}, test.key)
		case etcd3.DELETE:
			nodes.RemoveNode(test.key)
		default:
			t.Errorf("test case's event type error, %s", test.name)
		}

		assert.Equal(t, test.exceptedNodeLen, nodes.GetNodeNum(), test.name)
	}
}

func Test_Worker_getWorkerInstanceConn(t *testing.T) {
	lb := loadbalance.NewRandomRR()
	w := Worker{
		WorkerInstancesConn: new(sync.Map),
		lbWrapper:           createLBWrapper(lb),
	}
	urn := "sn:cn:yrk:1111:function:0@test@hello:latest"

	conn, err := w.getWorkerInstanceConn(urn, "", "", false)
	assert.Equal(t, ErrNoWorkerInstanceConnection, err, "no node should return error")
	assert.Nil(t, conn, "no node should return nil conn")

	w.lbWrapper.Add(urn)
	conn, err = w.getWorkerInstanceConn(urn, "", "", false)
	assert.Equal(t, ErrNoWorkerInstancePool, err, "no node should return error")
	assert.Nil(t, conn, "no node should return nil conn")

	w.WorkerInstancesConn.Store(urn, "invalid conn type")
	conn, err = w.getWorkerInstanceConn(urn, "", "", false)
	assert.Equal(t, ErrNoWorkerInstanceConnection, err, "no node should return error")
	assert.Nil(t, conn, "no node should return nil conn")

	instanceConn := &WorkerInstanceConnection{}
	w.WorkerInstancesConn.Store(urn, instanceConn)
	conn, err = w.getWorkerInstanceConn(urn, "", "", false)
	assert.Nil(t, err, "no error should return")
	assert.Equal(t, instanceConn, conn, "should return valid conn")
}

func Test_Worker_checkAndUpdateIP(t *testing.T) {
	w := Worker{
		WorkerInstancesConn: new(sync.Map),
		nodeLB:              loadbalance.NewNodeRR(),
	}

	oldIP := "127.0.0.1:8080"
	newIP := "127.0.0.2:8080"
	w.nodeLB.Add(oldIP)

	// ip not update
	w.checkAndUpdateIP(&WorkerInstanceConnection{WorkerInstance: &WorkerInstance{Host: oldIP}}, &util.WorkerInstance{IP: oldIP})
	assert.Equal(t, oldIP, w.nodeLB.Next(""), "node lb should be oldIP")

	// ip updated
	w.checkAndUpdateIP(&WorkerInstanceConnection{WorkerInstance: &WorkerInstance{Host: oldIP}}, &util.WorkerInstance{IP: newIP})
	assert.Equal(t, newIP, w.nodeLB.Next(""), "node lb should be newIP")
}

func Test_Worker_getNodeConnection(t *testing.T) {
	w := Worker{
		WorkerInstancesConn: new(sync.Map),
		nodeLB:              loadbalance.NewNodeRR(),
	}

	// no node in worker
	conn, err := w.getNodeConnection("127.0.0.1:3333")
	assert.Equal(t, ErrNoWorkerInstancePool, err, "no node should return error")
	assert.Nil(t, conn, "no node should return nil conn")

	// add an invalid conn type
	w.nodeLB.Add("127.0.0.1:3333")
	w.WorkerInstancesConn.Store("127.0.0.1:3333", "invalid conn type")
	conn, err = w.getNodeConnection("127.0.0.1:3333")
	assert.Equal(t, ErrNoWorkerInstancePool, err, "invalid conn type should return error")
	assert.Nil(t, conn, "invalid conn type should return nil conn")

	// add a valid conn
	instanceConn := &WorkerInstanceConnection{}
	w.WorkerInstancesConn.Store("127.0.0.1:3334", instanceConn)
	w.nodeLB.Add("127.0.0.1:3334")
	conn, err = w.getNodeConnection("127.0.0.1:3333")
	assert.Nil(t, err, "no error should return")
	assert.Equal(t, instanceConn, conn, "should return valid conn")
}

func Test_Worker_getConnection(t *testing.T) {
	lb := loadbalance.NewRandomRR()
	w := Worker{
		WorkerInstancesConn: new(sync.Map),
		nodeLB:              loadbalance.NewNodeRR(),
		lbWrapper:           createLBWrapper(lb),
	}
	instanceConn := &WorkerInstanceConnection{WorkerInstance: &WorkerInstance{}}
	w.nodeLB.Add("127.0.0.1:8080")
	w.lbWrapper.Add("127.0.0.1:8080")
	w.WorkerInstancesConn.Store("127.0.0.1:8080", instanceConn)

	tests := []struct {
		name  string
		param *model.FunctionRequestInfo

		exceptedError bool
	}{
		{
			name:  "backpressure",
			param: &model.FunctionRequestInfo{BackPressure: true, URN: "127.0.0.1:8080"},

			exceptedError: true,
		},
		{
			name:  "has start node",
			param: &model.FunctionRequestInfo{BackPressure: false, StartNode: "127.0.0.2", URN: "127.0.0.1:8080"},

			exceptedError: true,
		},
		{
			name: "has start node",
			param: &model.FunctionRequestInfo{BackPressure: false, BusinessType: constants.BusinessTypeWebSocket,
				WSReqInfo: model.WSRequestInfo{
					DemandNodePort: "8800",
					DemandNodeIP:   "127.0.0.1",
				}},

			exceptedError: false,
		},
	}
	patch := gomonkey.NewPatches()
	patch.ApplyFunc(util.GetGlobalClient, func() *fasthttp.Client {
		return new(fasthttp.Client)
	})
	defer patch.Reset()
	for _, test := range tests {
		req := fasthttp.AcquireRequest()
		conn, err := w.getConnection(req, test.param)
		if test.exceptedError {
			assert.Error(t, err, test.name)
			assert.Nil(t, conn, test.name)
		} else {
			assert.Nil(t, err, test.name)
			assert.NotNil(t, conn, test.name)
		}
	}
}

func Test_WorkerInstanceConnection_String(t *testing.T) {
	tests := []struct {
		name string
		wic  WorkerInstanceConnection

		excepted string
	}{
		{
			name: "connection a",
			wic: WorkerInstanceConnection{
				WorkerInstance: &WorkerInstance{Host: "127.0.0.1", Port: "3344"},
			},
			excepted: "127.0.0.1:3344",
		},
	}
	for _, test := range tests {
		assert.Equal(t, test.excepted, test.wic.String(), test.name)
		assert.Equal(t, test.excepted, fmt.Sprintf("%s", &test.wic), test.name)
	}
}

func Test_processURN(t *testing.T) {
	tests := []struct {
		name   string
		urn    string
		insRes types.InstanceResource

		exceptedURN  string
		exceptedType string
		hasError     bool
	}{
		{
			name: "invalid urn length",
			urn:  "sn:cn:xxx",

			hasError: true,
		},
		{
			name: "task urn",
			urn:  "sn:cn:yrk:0:function:function-task:latest",

			hasError:     false,
			exceptedURN:  "function-task:latest",
			exceptedType: util.ProxyIns,
		},
		{
			name: "worker urn without resource",
			urn:  "sn:cn:yrk:1111:function:0@test@hello:latest",

			hasError:     false,
			exceptedURN:  "sn:cn:yrk:1111:function:0@test@hello:latest",
			exceptedType: util.NormalWorkerIns,
		},
		{
			name:   "worker urn with resource",
			urn:    "sn:cn:yrk:1111:function:0@test@hello:latest",
			insRes: types.InstanceResource{CPU: "300", Memory: "128"},

			hasError:     false,
			exceptedURN:  "sn:cn:yrk:1111:function:0@test@hello:latest:300-128",
			exceptedType: util.NormalWorkerIns,
		},
	}

	for _, test := range tests {
		urn, workerType, err := processURN(test.urn, test.insRes)
		if test.hasError {
			assert.Error(t, err, test.name)
		} else {
			assert.Nil(t, err, test.name)
			assert.Equal(t, test.exceptedURN, urn, test.name)
			assert.Equal(t, test.exceptedType, workerType, test.name)
		}
	}

}

func TestWorkers_DeleteWorkerInstance(t *testing.T) {
	tests := []struct {
		name        string
		az          string
		urn         string
		instance    string
		deployNode  string
		instanceRes types.InstanceResource

		hasError bool
	}{
		{
			name: "no az",
			az:   "",

			hasError: true,
		},
		{
			name:     "invalid urn",
			az:       constants.PlatformTenantID,
			urn:      "invalid",
			hasError: true,
		},
		{
			name: "no-existed urn",
			az:   constants.PlatformTenantID,
			urn:  "sn:cn:yrk:0:function:function-task:latest",

			hasError: false,
		},
	}

	workers := Workers{WorkerMap: &sync.Map{}, instancesMutex: &sync.Mutex{}}
	for _, test := range tests {
		err := workers.DeleteWorkerInstance(test.az, test.urn, test.instance, test.instanceRes)
		if test.hasError {
			assert.Error(t, err, test.name)
		} else {
			assert.Nil(t, err, test.name)
		}
	}
}

func TestWorker_BackpressureWithoutWorker(t *testing.T) {
	resp := &fasthttp.Response{}
	// checkBackpressureWithoutWorker
	{
		ok := checkBackpressureWithoutWorker(constants.BackpressureCode, resp)
		assert.Equal(t, true, ok)
	}
}

func TestWorkers_addBackPressureWorker(t *testing.T) {
	lastNativeAz := frontendConstants.NativeAz
	frontendConstants.NativeAz = ""
	workers := Workers{WorkerMap: &sync.Map{}, instancesMutex: &sync.Mutex{}}
	stopCh := make(chan struct{})
	workers.Start(stopCh)
	w := &Worker{
		WorkerInstancesConn: new(sync.Map),
		lbWrapper:           createLBWrapper(loadbalance.NewRandomRR()),
		nodeLB:              loadbalance.NewNodeRR(),
	}
	params := &model.FunctionRequestInfo{}
	resp := &fasthttp.Response{}
	workers.processRequestWithBackPressureWorker(w, params, resp)
	resp.Header.Set(constants.HeaderBackPressureNodeID, "node")
	resp.Header.Set(constants.HeaderBackPressureInstanceID, "instance")
	workers.processRequestWithBackPressureWorker(w, params, resp)

	assert.Equal(t, params.TargetNode, "node")
	assert.Equal(t, true, params.BackPressure)
	assert.Equal(t, 0, len(w.WorkerInstances))
	assert.Equal(t, 0, len(workers.bpWorkers.workers))
	frontendConstants.NativeAz = lastNativeAz
}

func TestGetWorkerWithInvalidUrn(t *testing.T) {
	convey.Convey("TestGetWorkerWithInvalidUrn", t, func() {
		workers := Workers{WorkerMap: &sync.Map{}, instancesMutex: &sync.Mutex{}}
		_, err := workers.getWorker("", "", "", "")
		convey.ShouldEqual(err, convey.ShouldNotBeNil)
	})
}

// TestWorker_getStateInstanceConn -
func TestWorker_getStateInstanceConn(t *testing.T) {
	convey.Convey("Test: getConnection: getStateInstanceConn", t, func() {
		w := &Worker{
			WorkerInstancesConn: new(sync.Map),
			stateSingleFlight:   singleflight.NewSingleFlight(),
		}
		req := new(fasthttp.Request)
		stateID := "testStateID"
		instanceID := "testInstanceID"
		params := &model.FunctionRequestInfo{
			StateID: stateID,
		}
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(util.GetGlobalClient, func() *fasthttp.Client {
			return &fasthttp.Client{}
		})
		patch.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		defer patch.Reset()
		// cache exist
		convey.Convey("test: the state route exists in the cache", func() {
			w.WorkerInstancesConn.Store(stateID, &WorkerInstanceConnection{
				WorkerInstance: &WorkerInstance{
					Host: "10.10.10.10",
				},
			})
			defer func() {
				w.WorkerInstancesConn.Delete(stateID)
			}()
			ins, err := w.getConnection(req, params)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ins.Host, convey.ShouldEqual, "10.10.10.10")
		})
		// cache does not exist
		convey.Convey("test: cache miss, concurrent query of ETCD", func() {
			convey.Convey("test: failed to query state route from etcd: error", func() {
				patch := gomonkey.NewPatches()
				patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					return nil, errors.New("test err")
				})
				defer func() {
					patch.Reset()
					w.GetStateSingleFlight().Remove(params.StateID)
				}()
				var err error
				_, err = w.getConnection(req, params)
				convey.So(err, convey.ShouldNotBeNil)
			})
			convey.Convey("test: failed to query state route from etcd: not found", func() {
				patch := gomonkey.NewPatches()
				patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					return nil, nil
				})
				defer func() {
					patch.Reset()
					params.InnerCode = 0
					w.GetStateSingleFlight().Remove(params.StateID)
				}()
				_, err := w.getConnection(req, params)
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(params.InnerCode, convey.ShouldEqual, snerror.NoSuchInstanceName)
			})
			convey.Convey("test: failed to query task info from etcd, err", func() {
				patch := gomonkey.NewPatches()
				patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					if strings.HasPrefix(key, "/sn/stateworkers") {
						return []string{fmt.Sprintf(`{"%s":"%s"}`, stateID, instanceID)}, nil
					}
					return nil, errors.New("test err")
				})
				defer func() {
					patch.Reset()
					w.GetStateSingleFlight().Remove(params.StateID)
				}()
				_, err := w.getConnection(req, params)
				convey.So(err, convey.ShouldNotBeNil)
			})
			convey.Convey("test: failed to query task info from etcd, not found", func() {
				patch := gomonkey.NewPatches()
				patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					if strings.HasPrefix(key, "/sn/stateworkers") {
						return []string{fmt.Sprintf(`{"%s":"%s"}`, stateID, instanceID)}, nil
					}
					return nil, nil
				})
				defer func() {
					patch.Reset()
					w.GetStateSingleFlight().Remove(params.StateID)
				}()
				_, err := w.getConnection(req, params)
				convey.So(err, convey.ShouldNotBeNil)
			})
			convey.Convey("test: succeed to query from etcd", func() {
				patch := gomonkey.NewPatches()
				patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					if strings.HasPrefix(key, "/sn/stateworkers") {
						return []string{fmt.Sprintf(`{"%s":"%s"}`, stateID, instanceID)}, nil
					}
					return []string{`{"nodeIP":"10.10.10.10","nodePort":"22423"}`}, nil
				})
				defer func() {
					patch.Reset()
					w.GetStateSingleFlight().Remove(params.StateID)
				}()
				ins, err := w.getConnection(req, params)
				convey.So(err, convey.ShouldBeNil)
				convey.So(ins.Host, convey.ShouldEqual, "10.10.10.10")
			})
		})
		// cache does not exist, test for single flight
		convey.Convey("test: single flight", func() {
			count := 0
			patch := gomonkey.NewPatches()
			patch.ApplyFunc((*Worker).queryStateInsFromETCD, func(_ *Worker,
				params *model.FunctionRequestInfo) (*WorkerInstanceConnection, error) {
				count++
				return nil, errors.New("test err")
			})
			defer func() {
				patch.Reset()
			}()
			var err error
			for i := 0; i < 10; i++ {
				_, err = w.getConnection(req, params)
			}
			convey.So(err, convey.ShouldBeError)
			// single flight, query etcd only once
			convey.So(count, convey.ShouldEqual, 1)
		})
	})
}

// Test_deleteBackupPressureWorker -
func Test_deleteBackupPressureWorker(t *testing.T) {
	convey.Convey("test: delete backup pressure worker", t, func() {
		ws := &Workers{
			WorkerMap: new(sync.Map),
		}
		ws.WorkerMap.Store("testKey1", "")
		ws.WorkerMap.Store("testKey2", &Worker{
			lbWrapper: createLBWrapper(loadbalance.NewRandomRR()),
		})
		// test: not exist
		ws.deleteBackupPressureWorker("", "", "testKey")
		count := 0
		ws.WorkerMap.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		convey.So(count, convey.ShouldEqual, 2)
		// test: invalid type
		ws.deleteBackupPressureWorker("", "", "testKey1")
		count = 0
		ws.WorkerMap.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		convey.So(count, convey.ShouldEqual, 2)
		// test: succeed to delete
		ws.deleteBackupPressureWorker("", "", "testKey2")
		count = 0
		ws.WorkerMap.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		convey.So(count, convey.ShouldEqual, 1)
	})
}

func Test_Worker_getBackpressureConn(t *testing.T) {
	frontendConstants.FConfig.LoadBalanceConfig = &frontendConstants.LoadBalanceConfig{}
	w := &Worker{
		nodeLB:              loadbalance.NewNodeRR(),
		WorkerInstancesConn: &sync.Map{},
		lbWrapper:           createLBWrapper(loadbalance.NewRandomRR()),
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	w.getBackpressureConn(req, &model.FunctionRequestInfo{TargetNode: "node1"})
	assert.Equal(t, "", string(req.Header.Peek(constants.HeaderBackPressureNodeID)))

	frontendConstants.FConfig.LoadBalanceConfig.DisableBackpressureBalance = 1
	w.getBackpressureConn(req, &model.FunctionRequestInfo{TargetNode: "node1"})
	assert.Equal(t, "node1", string(req.Header.Peek(constants.HeaderBackPressureNodeID)))
}

func Test_Function_Bus_Worker_Heartbeat_OK(t *testing.T) {
	frontendConstants.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	frontendConstants.FConfig.HTTPConfig = &frontendConstants.FrontendHTTP{
		WorkerInstanceReadTimeOut: 20,
	}
	frontendConstants.FConfig.HeartbeatConfig = &frontendConstants.HeartbeatConfig{
		HeartbeatTimeout:          1,
		HeartbeatInterval:         1,
		HeartbeatTimeoutThreshold: 1,
	}
	frontendConstants.FConfig.LocalAuth = &localauth.AuthConfig{
		AKey:     "AKey",
		SKey:     "SKey",
		Duration: 5,
	}
	infos := &WorkerInstance{
		Host:       "127.0.0.1",
		Port:       "18080",
		InstanceID: "wsl",
		Node:       "",
	}

	wik := &WorkerInstanceKeeper{
		WorkerInstance:  infos,
		heartBeatClient: util.GetHeartbeatClient(),
		heartbeatOk:     false,
		stopCh:          make(chan struct{}),
	}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*Workers).getProxyIns, func(ws *Workers) *Worker {
			worker := &Worker{}
			instances := make(map[string]*WorkerInstanceKeeper)
			instances[infos.InstanceID] = wik
			worker.WorkerInstances = instances
			return worker
		}),
		gomonkey.ApplyFunc((*fasthttp.Client).DoTimeout, func(c *fasthttp.Client, req *fasthttp.Request,
			resp *fasthttp.Response, timeout time.Duration) error {
			resp.SetStatusCode(statuscode.Ok)
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	assert.Equal(t, false, wik.heartbeatOk)
	ResetHeartbeatTasks()
	time.Sleep(time.Millisecond * 1100)
	assert.Equal(t, true, wik.heartbeatOk)
}

func Test_Function_Bus_Worker_Heartbeat_NOK(t *testing.T) {
	frontendConstants.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	frontendConstants.FConfig.HTTPConfig = &frontendConstants.FrontendHTTP{
		WorkerInstanceReadTimeOut: 20,
	}
	frontendConstants.FConfig.HeartbeatConfig = &frontendConstants.HeartbeatConfig{
		HeartbeatTimeout:          1,
		HeartbeatInterval:         1,
		HeartbeatTimeoutThreshold: 1,
	}
	frontendConstants.FConfig.LocalAuth = &localauth.AuthConfig{
		AKey:     "AKey",
		SKey:     "SKey",
		Duration: 5,
	}

	infos := &WorkerInstance{
		Host:       "127.0.0.1",
		Port:       "18080",
		InstanceID: "az-#-wsl",
		Node:       "",
	}

	wik := &WorkerInstanceKeeper{
		WorkerInstance:  infos,
		heartBeatClient: util.GetHeartbeatClient(),
		heartbeatOk:     true,
		stopCh:          make(chan struct{}),
	}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*Workers).getProxyIns, func(ws *Workers) *Worker {
			worker := &Worker{}
			instances := make(map[string]*WorkerInstanceKeeper)
			instances[infos.InstanceID] = wik
			worker.WorkerInstances = instances
			return worker
		}),
		gomonkey.ApplyFunc((*fasthttp.Client).DoTimeout, func(c *fasthttp.Client, req *fasthttp.Request,
			resp *fasthttp.Response, timeout time.Duration) error {
			resp.SetStatusCode(statuscode.InternalError)
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	assert.Equal(t, true, wik.heartbeatOk)
	ResetHeartbeatTasks()
	time.Sleep(time.Millisecond * 1100)
	assert.Equal(t, false, wik.heartbeatOk)
}

func TestWorkerInstanceConnection_DoRequest(t *testing.T) {
	r := gin.New()
	requestCount := int64(0)
	r.POST("/invoke", func(c *gin.Context) {
		atomic.AddInt64(&requestCount, 1)
		time.Sleep(550 * time.Millisecond)
		c.Status(http.StatusOK)
	})
	s := http.Server{
		Handler: r,
		Addr:    "127.0.0.1:3333",
	}
	go func() {
		s.ListenAndServe()
	}()
	defer s.Close()

	time.Sleep(100 * time.Millisecond)
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.SignLocally, func(_, _, _ string, _ int) (string, string) {
			return "", ""
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	frontendConstants.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: false}
	frontendConstants.FConfig.LocalAuth = &localauth.AuthConfig{}
	wic := &WorkerInstanceConnection{
		WorkerInstance: &WorkerInstance{
			Host: "127.0.0.1",
			Port: "3333",
		},
		conn: &fasthttp.Client{},
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI("/invoke")
	req.Header.SetMethod("POST")

	reqInfo := &model.FunctionRequestInfo{Deadline: time.Now().Add(time.Second)}

	assert.Nil(t, wic.DoRequest(reqInfo, req, resp))

	assert.Error(t, wic.DoRequest(reqInfo, req, resp))

	assert.Error(t, wic.DoRequest(reqInfo, req, resp))

	assert.Equal(t, int64(2), atomic.LoadInt64(&requestCount))
}

func TestWorker_releaseWSRequest(t *testing.T) {
	mockClient, _ := grpcconn.CreateGrpcClient(common.RPCClientConfig{})
	remotescheduler.CreateInstance()
	constant.InitDefaultConfig()
	constant.FConfig.LoadBalanceConfig.ConcurrencyThreshold = 10
	convey.Convey("test release websocket request", t, func() {
		lb := loadbalance.NewRandomRR()
		w := Worker{
			WorkerInstancesConn: new(sync.Map),
			nodeLB:              loadbalance.NewNodeRR(),
			lbWrapper:           createLBWrapper(lb),
		}
		reqInfo := &model.FunctionRequestInfo{TraceID: "a"}
		convey.Convey("test no client", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return nil
				}),
				gomonkey.ApplyFunc(releaseInstance, func(
					scheduler common.RPCClient, releaseReq *schedulerapi.ReleaseInstanceReq) error {
					return errors.New("failed")
				}),
			})
			defer patches.ResetAll()
			w.releaseWSRequest(reqInfo)
		})
		convey.Convey("test can not release", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return mockClient
				}),
				gomonkey.ApplyFunc(releaseInstance, func(
					scheduler common.RPCClient, releaseReq *schedulerapi.ReleaseInstanceReq) error {
					return errors.New("failed")
				}),
			})
			defer patches.ResetAll()
			w.releaseWSRequest(reqInfo)
		})
		convey.Convey("test release successfully", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return mockClient
				}),
				gomonkey.ApplyFunc(releaseInstance, func(
					scheduler common.RPCClient, releaseReq *schedulerapi.ReleaseInstanceReq) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			w.releaseWSRequest(reqInfo)
		})
	})
}

func Test_NativeInvoke(t *testing.T) {
	convey.Convey("Test_NativeInvoke", t, func() {
		convey.Convey("test no client", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*Worker).doInvoke, func(w *Worker, req *fasthttp.Request, resp *fasthttp.Response,
					params *model.FunctionRequestInfo) (bool, bool, error) {
					params.InnerCode = constants.BackpressureCode
					return true, false, nil
				}),
			})
			defer patches.ResetAll()
			worker := &Worker{}
			reqInfo := &model.FunctionRequestInfo{InnerCode: constants.BackpressureCode}
			reqInfo.BackPressureDeadline = time.Now()
			req := &fasthttp.Request{}
			resp := &fasthttp.Response{}
			var err error
			var i int
			for ; i < maxBackPressureTimesWithWorker; i++ {
				if err == nil && reqInfo.InnerCode == constants.BackpressureCode {
					err = worker.nativeInvoke(reqInfo, req, resp, i+1)
				} else {
					break
				}
			}
			assert.NotNil(t, err)
			assert.Equal(t, "timeout", err.Error())
			assert.Equal(t, 6, i)
		})
	})

}

func TestWorker_NativeInvoke_ForLoop(t *testing.T) {
	type fields struct {
		WorkerInstancesConn *sync.Map
		bpWorkers           *BackPressureWorkers
		lbWrapper           *lbWrapper
		stateSingleFlight   *singleflight.SingleFlight
		WorkerInstances     map[string]*WorkerInstanceKeeper
		urn                 string
		lbType              loadbalance.LBType
		workerType          string
		nodeLB              *loadbalance.NodeRR
		requestNum          uint64
		funcKeyWithRes      string
		mtx                 sync.RWMutex
	}
	type args struct {
		reqInfo          *model.FunctionRequestInfo
		req              *fhttp.Request
		resp             *fhttp.Response
		backPressureNums int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "test for loop",
			fields: fields{},
			args: args{
				reqInfo: &model.FunctionRequestInfo{
					BusinessType:      constants.BusinessTypeCAE,
					EnableRemoteDebug: true,
					TraceID:           "mockTraceID",
				},
				req: &fhttp.Request{
					Header: fhttp.RequestHeader{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				WorkerInstancesConn: tt.fields.WorkerInstancesConn,
				bpWorkers:           tt.fields.bpWorkers,
				lbWrapper:           tt.fields.lbWrapper,
				stateSingleFlight:   tt.fields.stateSingleFlight,
				WorkerInstances:     tt.fields.WorkerInstances,
				urn:                 tt.fields.urn,
				lbType:              tt.fields.lbType,
				workerType:          tt.fields.workerType,
				nodeLB:              tt.fields.nodeLB,
				requestNum:          tt.fields.requestNum,
				funcKeyWithRes:      tt.fields.funcKeyWithRes,
				mtx:                 tt.fields.mtx,
			}
			constant.FConfig.InvokeMaxRetryTimes = 1
			tt.wantErr(t, w.nativeInvoke(tt.args.reqInfo, tt.args.req, tt.args.resp, tt.args.backPressureNums), fmt.Sprintf("nativeInvoke(%v, %v, %v, %v)", tt.args.reqInfo, tt.args.req, tt.args.resp, tt.args.backPressureNums))
		})
	}
}
