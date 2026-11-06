package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/faas-scheduler/config"
	mockUtils "functioncore/test/utils"
)

const mockFuncKey = "7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/0@default@testyrhttpwebsocket/latest/400-256"

func TestScaleOutInstance(t *testing.T) {
	type args struct {
		req *schedulerapi.AcquireInstanceReq
	}
	config.InitDefaultConfig()
	config.FConfig = &config.Config{
		LocalAuth: &localauth.AuthConfig{
			AKey:     "000",
			SKey:     "111",
			Duration: 5,
		},
		HTTPSConfig: &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
			TLSProtocol: "TLSv1.2",
			TLSCiphers:  "TLS_ECDHE_RSA",
		},
	}
	mockArg := args{
		req: &schedulerapi.AcquireInstanceReq{
			TraceID:                   "testTraceID",
			FuncKeyWithRes:            mockFuncKey,
			Timeout:                   "1s",
			ReqType:                   schedulerapi.NormalRequest,
			WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{},
		},
	}
	mockRespBody := `{"code":150200,"instance":{"instanceID":"pool30-xxx"},"worker":{"instances":[],
		"functionname":"","functionversion":"","tenant":"","business":""}}`

	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 acquire websocket instance successfully when no idle instance to use",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte(mockRespBody))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 2 acquire websocket instance failed when no idle instance to use",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 500,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte(`{"code": 5000,"message":"mockMessage"}`))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 3 acquire websocket instance failed when do request error",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("do request mock error")
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 4 acquire websocket instance failed when unmarshal scale out bad response error",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 500,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte(`{`))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 5 acquire websocket instance successfully when unmarshal scale out response error",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("}"))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 6 response code in GetWorkerSuccessResponse is not 200",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 7 acquire websocket instance successfully when failed to make scale out request",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
						}, nil
					}),
					ApplyFunc(getResourceInfo, func(insResource *types.InstanceResource) (int, int, error) {
						return 0, 0, errors.New("mock getResourceInfo error")
					}),
				})
				return patches
			},
		}, {
			name: "case 8 acquire websocket instance failed when read body error",
			args: mockArg,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte(mockRespBody))),
						}, nil
					}),
					ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("read body mock error")
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 9 acquire websocket instance failed when received bad timeout parameter",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:                   "testTraceID",
					FuncKeyWithRes:            mockFuncKey,
					Timeout:                   "1ss",
					ReqType:                   schedulerapi.NormalRequest,
					WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 10 acquire websocket instance failed because marshal parameter failed",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:                   "testTraceID",
					FuncKeyWithRes:            mockFuncKey,
					Timeout:                   "1s",
					ReqType:                   schedulerapi.NormalRequest,
					WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("json marshal error")
					}),
				})
				return patches
			},
		}, {
			name: "case 11 acquire websocket instance failed because new http request failed",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:                   "testTraceID",
					FuncKeyWithRes:            mockFuncKey,
					Timeout:                   "1s",
					ReqType:                   schedulerapi.NormalRequest,
					WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("http.NewRequestWithContext")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ScaleOutInstance(tt.args.req)
			patches.ResetAll()
		})
	}
}

func TestScaleInInstance(t *testing.T) {
	config.InitDefaultConfig()
	config.FConfig = &config.Config{
		LocalAuth: &localauth.AuthConfig{
			AKey:     "000",
			SKey:     "111",
			Duration: 5,
		},
		HTTPSConfig: &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
			TLSProtocol: "TLSv1.2",
			TLSCiphers:  "TLS_ECDHE_RSA",
		},
	}
	type args struct {
		instanceID  string
		functionKey string
		traceID     string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 scale in function instance",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte(`{"code":200,"message":"mock message"}`))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 2 scale in function instance when do request err",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("do request mock error")
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 3 scale in function instance when ioutil.ReadAll error",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte(`{"code":200,"message":"mock message"}`))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
					ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("read body mock error")
					}),
				})
				return patches
			},
		}, {
			name: "case 4 scale in function instance when returned bad response body",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 500,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte(`{"code":500,"message":"mock message"}`))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 5 scale in function instance when failed to unmarshal bad response body",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 500,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte(`{`),
							)),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 6 scale in function instance when failed to unmarshal DeleteWorkerResponse",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte(`{`))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 7 scale in function instance when status code in DeleteWorkerResponse is not 200",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body:       ioutil.NopCloser(bytes.NewReader([]byte(`{"code":500}`))),
						}, nil
					}),
					ApplyFunc(FillInWorkerManagerRequestHeaders, func(request *http.Request) { return }),
				})
				return patches
			},
		}, {
			name: "case 8 scale in function instance failed because json marshal failed",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("json marshal error")
					}),
				})
				return patches
			},
		}, {
			name: "case 9 scale in function instance failed because new http request failed",
			args: args{
				instanceID:  "default-#-pool30",
				functionKey: mockFuncKey,
				traceID:     "mockTraceID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("http.NewRequestWithContext")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ScaleInInstance(tt.args.instanceID, tt.args.functionKey, tt.args.traceID)
			patches.ResetAll()
		})
	}
}

func Test_getResourceInfo(t *testing.T) {
	type args struct {
		insResource *types.InstanceResource
	}
	tests := []struct {
		name    string
		args    args
		cpu     int
		mem     int
		wantErr bool
	}{
		{
			name: "invalid cpu",
			args: args{insResource: &types.InstanceResource{
				CPU:    "",
				Memory: "256",
			}},
			wantErr: true,
		}, {
			name: "invalid mem",
			args: args{insResource: &types.InstanceResource{
				CPU:    "300",
				Memory: "",
			}},
			wantErr: true,
		}, {
			name: "valid cpu and mem",
			args: args{insResource: &types.InstanceResource{
				CPU:    "300",
				Memory: "128",
			}},
			wantErr: false,
			cpu:     300,
			mem:     128,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpu, mem, err := getResourceInfo(tt.args.insResource)
			if (err != nil) != tt.wantErr {
				t.Errorf("getResourceInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if cpu != tt.cpu {
				t.Errorf("getResourceInfo() cpu got = %v, cpu want %v", cpu, tt.cpu)
			}
			if mem != tt.mem {
				t.Errorf("getResourceInfo() mem got = %v, mem want %v", mem, tt.mem)
			}
		})
	}
}
