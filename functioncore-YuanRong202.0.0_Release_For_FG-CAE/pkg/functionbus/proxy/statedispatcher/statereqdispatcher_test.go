package statedispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"gotest.tools/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	instance    = "worker01"
	stateId     = "state01"
	functionKey = "TenantID01/function01/latest"
	cpu         = "300"
	memory      = "128"
	localIP     = "127.0.0.1"
	remoteIP    = "127.0.0.2"
	retryTimes  = 1
)

var (
	patches          testutils.PatchSlice
	backupSelfNodeIP string
	backupHostName   string
	workerIP         string
)

func TestGetStateQueueMetadataChannel(t *testing.T) {
	testChannel := GetStateQueueMetadataChannel()
	assert.Equal(t, cap(testChannel), stateQueueMetadataChanLen)
}

func buildStateDispatcher() *RequestDispatcher {
	funcKeyWithResource := functionKey
	isNeedInstanceIdleCheck := false
	return CreateRequestDispatcher(funcKeyWithResource, isNeedInstanceIdleCheck, servicediscovery.FunctionMetaInfo{})
}

func setup() {
	stopCh := make(chan struct{}, 1)
	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetFuncKeyFromFuncKeyWithResource, func(funcKeyWithResource string) string {
			return functionKey
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
			info := servicediscovery.FunctionMetaInfo{
				FuncMetaData:     servicediscovery.FuncMetaData{IsBridgeFunction: false},
				InstanceMetaData: servicediscovery.InstanceMetaData{ConcurrentNum: 9},
			}
			return json.Marshal(info)
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetFuncKeyWithResource, func(functionKey string, insResource types.InstanceResource) string {
			return functionKey
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(action.ScaleUp, func(scaleUpParam common.ScaleUpParam) (*types.Instance, uint32, string) {
			return &types.Instance{}, 1, ""
		}),
	})
	workerevaluator.CreateInstance(stopCh)

	backupSelfNodeIP = internal.SelfNodeIP
	backupHostName = internal.HostName

	internal.SelfNodeIP = localIP
	internal.HostName = localIP

	client := &http.Client{}

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return client
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", mockClientDo),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURL, func() string {
			return ""
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(tcpbus.BuildClientConnection,
			func(_ tcpbus.ClientOption) (*tcpbus.Connection, error) {
				return &tcpbus.Connection{}, nil
			}),
	})

	bus := &tcpbus.TCPBus{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "Call",
			func(_ *tcpbus.TCPBus, _ context.Context, _ *common.FunctionProperty, req *types.TCPRequest,
				_ bool) (*types.TCPResponse, error) {
				return &types.TCPResponse{
					RequestID:    req.RequestID,
					ErrorCode:    proxyerrors.StateOKCode,
					ErrorMessage: "",
				}, nil
			}),
	})
}

func TestReceiveStateQueueMetadataPutEvent(t *testing.T) {
	rd := &RequestDispatcher{
		funcKeyWithResource: "1/prefix-ServiceID-FuncName/3",
	}
	rd.ReceiveStateQueueMetadataPutEvent("stateID")
}

func TestStart_Stop(t *testing.T) {
	stopCh := make(chan struct{}, 1)
	rd := &RequestDispatcher{
		funcKeyWithResource: "1/prefix-ServiceID-FuncName/3",
		stopChan:            stopCh,
	}
	workerevaluator.CreateInstance(stopCh)
	rd.Start()
	rd.Stop()
}

func mockClientDo(c *http.Client, _ *http.Request) (*http.Response, error) {
	rsp := action.GetWorkerSuccessResponse{
		Worker: &action.Worker{},
		Instance: &types.Instance{
			InstanceID: instance,
			DeployedIP: workerIP,
		},
		Code: 150200,
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		log.GetLogger().Errorf("Marshal failed")
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, nil
}

func tearDown() {
	patches.ResetAll()

	internal.SelfNodeIP = backupSelfNodeIP
	internal.HostName = backupHostName
}

func buildTestRequest() *types.DispatcherRequest {
	return &types.DispatcherRequest{
		FunctionKey: functionKey,
		Request: &types.TCPRequest{
			StateID: stateId,
			CPU:     cpu,
			Memory:  memory,
		},
		Response: make(chan *types.TCPResponse, 1),
	}
}

func TestReceiveInsDeleteEvent(t *testing.T) {
	dispatcher := buildStateDispatcher()
	dispatcher.ReceiveInsDeleteEvent("statId")
	dispatcher.ReceiveStateQueueMetadataPutEvent("testId")
	ID := <-dispatcher.insChan
	assert.Equal(t, "statId", ID)
}

func TestOnObserved(t *testing.T) {
	convey.Convey("TestRequestWithLocalWorker", t, func() {
		workerIP = localIP
		setup()
		defer func() {
			tearDown()
		}()

		req := buildTestRequest()
		dispatcher := buildStateDispatcher()

		var rsp *types.TCPResponse
		for i := 0; i < retryTimes; i++ {
			err := dispatcher.Request(req)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
			} else {
				rsp = <-req.Response
				break
			}
		}

		if rsp != nil {
			convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
		}

		dispatcher.OnObserved(&workerevaluator.WorkerMetrics{}, false)
	})
	convey.Convey("TestRequestWithLocalWorker1", t, func() {
		workerIP = localIP
		setup()
		defer func() {
			tearDown()
		}()

		req := buildTestRequest()
		dispatcher := buildStateDispatcher()

		var rsp *types.TCPResponse
		for i := 0; i < retryTimes; i++ {
			err := dispatcher.Request(req)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
			} else {
				rsp = <-req.Response
				break
			}
		}

		if rsp != nil {
			convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
		}

		dispatcher.OnObserved(&workerevaluator.WorkerEvaluator{}, false)
	})
}
