package bridgefunc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"plugin"
	"testing"

	"gotest.tools/assert"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager"
	stateUtil "functioncore/pkg/state-manager/utils"
	"functioncore/test/utils"
)

func TestCheckParamValid(t *testing.T) {
	result := checkParamValid(nil, &bgtypes.FuncRequestInfo{ClientInstance: bgtypes.ClientInstance{FuncInfo: &bgtypes.BridgeFuncInfo{}}})
	assert.Equal(t, result, true)

	responseWriter := utils.MockResponseWriter{}
	result = checkParamValid(responseWriter, nil)
	assert.Equal(t, result, false)

	result = checkParamValid(responseWriter, &bgtypes.FuncRequestInfo{ClientInstance: bgtypes.ClientInstance{}})
	assert.Equal(t, result, false)
}

func TestReleaseBridge(t *testing.T) {
	requestHandler = &bridgemanager.BridgeHandler{}
	test := []struct {
		name        string
		w           http.ResponseWriter
		r           *http.Request
		patchesFunc utils.PatchesFunc
	}{
		// test parseRequest return directly
		{
			"case1",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return errors.New("authCheck err")
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return nil, true
					}),
				})
				return patches
			},
		},
		// test func AuthCheck error
		{
			"case2",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func IsStateful return false
		{
			"case3",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return false
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func GetBridgeHandler error
		{
			"case4",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return nil, errors.New("failed to get bridge handler")
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func GetBridgeHandler error
		{
			"case5",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ReleaseBridge" {
							return func(request []byte) ([]byte, error) {
								return nil, errors.New("test")
							}, errors.New("test")
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func Release error
		{
			"case6",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return requestHandler, nil
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ReleaseBridge" {
							return func(request []byte) ([]byte, error) {
								return nil, errors.New("test")
							}, errors.New("test")
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func Release success
		{
			"case7",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return requestHandler, nil
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ReleaseBridge" {
							return func() {}, nil
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(invokeSuccessResponse,
						func(w http.ResponseWriter, stateKey string, isNewStateKeyScenario bool, logger log.FormatLogger) error {
							return nil
						}),
				})
				return patches
			},
		},
		// test func invokeSuccessResponse error
		{
			"case8",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return requestHandler, nil
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ReleaseBridge" {
							return func() {}, nil
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(invokeSuccessResponse,
						func(w http.ResponseWriter, stateKey string, isNewStateKeyScenario bool, logger log.FormatLogger) error {
							return errors.New("")
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range test {
		patches := tt.patchesFunc()
		fmt.Println(tt.name)
		release(tt.w, tt.r)
		patches.ResetAll()
	}
}

func TestApplyStateKey(t *testing.T) {
	requestHandler = &bridgemanager.BridgeHandler{}
	test := []struct {
		name        string
		w           http.ResponseWriter
		r           *http.Request
		patchesFunc utils.PatchesFunc
	}{
		// test parseRequest return directly
		{
			"case1",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return nil, true
					}),
				})
				return patches
			},
		},
		// test func AuthCheck error
		{
			"case2",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return errors.New("authCheck err")
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func IsStateful return false
		{
			"case3",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return false
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func GetBridgeHandler error
		{
			"case4",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return nil, errors.New("failed to get bridge handler")
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func GetBridgeHandler error
		{
			"case5",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ApplyStateKey" {
							return func(request []byte) ([]byte, error) {
								return nil, errors.New("test")
							}, errors.New("test")
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func ApplyStateKey error
		{
			"case6",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return requestHandler, nil
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ApplyStateKey" {
							return func(request []byte) ([]byte, error) {
								return nil, errors.New("test")
							}, errors.New("test")
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(InvokeErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
		},
		// test func ApplyStateKey success
		{
			"case7",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return requestHandler, nil
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ApplyStateKey" {
							return func() {}, nil
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(invokeSuccessResponse,
						func(w http.ResponseWriter, stateKey string, isNewStateKeyScenario bool, logger log.FormatLogger) error {
							return nil
						}),
				})
				return patches
			},
		},
		// test func invokeSuccessResponse error
		{
			"case8",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
						return &bgtypes.FuncRequestInfo{FunctionName: "test123", ClientInstance: bgtypes.ClientInstance{PodName: "pod1",
							FuncInfo: &bgtypes.BridgeFuncInfo{TenantID: "test1", ServiceID: "test123", BridgeFuncName: "test123"}}}, false
					}),
					gomonkey.ApplyFunc(stateUtil.IsStateful, func(funcName string) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetRequestHandler, func() (*bridgemanager.BridgeHandler, error) {
						return requestHandler, nil
					}),
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "ApplyStateKey" {
							return func() {}, nil
						}
						return func() {}, nil
					}),
					gomonkey.ApplyFunc(invokeSuccessResponse,
						func(w http.ResponseWriter, stateKey string, isNewStateKeyScenario bool, logger log.FormatLogger) error {
							return errors.New("")
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range test {
		patches := tt.patchesFunc()
		fmt.Println(tt.name)
		ApplyStateKey(tt.w, tt.r)
		patches.ResetAll()
	}
}

func TestInitialize(t *testing.T) {
	Initialize()
}

func TestReleaseSuccessResponse(t *testing.T) {
	responseWriter := utils.MockResponseWriter{}
	isTerminate := false
	ReleaseSuccessResponse(responseWriter, nil, isTerminate)
}

func TestParseFuncRequest(t *testing.T) {
	requestInfo := bgtypes.FuncRequestInfo{}
	byteData, err := json.Marshal(requestInfo)
	if err != nil {
		return
	}
	req := httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader(byteData))
	req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)

	_, err = parseFuncRequest(req)
	assert.Equal(t, nil, err)

	patch := gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
		return []byte("test"), errors.New("")
	})
	defer patch.Reset()
	_, err = parseFuncRequest(req)
	assert.Equal(t, "", err.Error())
}

func TestParseRequest(t *testing.T) {
	requestInfo := bgtypes.FuncRequestInfo{}
	byteData, err := json.Marshal(requestInfo)
	if err != nil {
		return
	}
	req := httptest.NewRequest("POST", "/test", bytes.NewReader(byteData))
	responseWriter := utils.MockResponseWriter{}
	_, flag := parseRequest(responseWriter, req)
	assert.Equal(t, flag, false)

	patch := gomonkey.ApplyFunc(parseFuncRequest, func(r *http.Request) (*bgtypes.FuncRequestInfo, error) {
		return nil, errors.New("")
	})
	defer patch.Reset()

	_, flag = parseRequest(responseWriter, req)
	assert.Equal(t, flag, true)
}

func TestRegisterRouter(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
	}{
		// test register success
		{
			"case1",
			args{
				name: "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RegisterRouter()
		})
	}
}

func Test_invokeSuccessResponse(t *testing.T) {
	type args struct {
		w                     http.ResponseWriter
		stateKey              string
		createStateLogger     log.FormatLogger
		isNewStateKeyScenario bool
	}
	tests := []struct {
		name string
		args args
	}{
		// test errorResponse error
		{
			"case1",
			args{
				createStateLogger: log.GetLogger(),
				w:                 utils.MockResponseWriter{},
			},
		},
		// test success
		{
			"case2",
			args{
				createStateLogger: log.GetLogger(),
				w:                 utils.MockResponseWriter{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			invokeSuccessResponse(tt.args.w, tt.args.stateKey, tt.args.isNewStateKeyScenario, tt.args.createStateLogger)
		})
	}
}
