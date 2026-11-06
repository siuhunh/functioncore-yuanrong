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

// Package web is web runtime
package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/oom"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/utils"
	mockutils "functioncore/test/utils"
)

func getHTTPRuntimeConfig() *config.Configuration {
	runtimeManagerConfigForHTTP := rmconfig.RuntimeConfig{
		Port:               8000,
		Enable:             false,
		CustomArgs:         nil,
		Addition:           make(map[string]string, 2),
		TraceMasterAddress: "",
	}
	runtimeManagerConfigForHTTP.Addition["initTimeout"] = "30"
	runtimeManagerConfigForHTTP.Addition["maxResponseBodySize"] = "6"
	workerConfig := &config.Configuration{
		RuntimeConfig: config.RuntimeConfig{
			Type:         "http",
			URN:          "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
			FuncLogLevel: "",
			FunctionType: functionType,
			Timeout:      1,
			LoadTime:     0,
			EnvInfo:      "",
			Initializer: config.Initializer{
				InitializerHandler: "abc",
				InitializerTimeout: 1,
				Extra:              nil,
			},
			RPCPort:  8000,
			Language: "http",
			RMConfig: rmconfig.Config{
				RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
			},
		},
	}
	workerConfig.RMConfig.RuntimeConfigs["http"] = runtimeManagerConfigForHTTP
	return workerConfig
}

func TestHotLoad(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
			return functionlog.NewFunctionLogger(nil, "", ""), nil
		}),
		gomonkey.ApplyFunc(runtime.InitExtensionProcess, func(timeout time.Duration, configuration *config.Configuration,
			functionLog *functionlog.FunctionLogger) error {
			return nil
		}),
		gomonkey.ApplyFunc(getLocalMountPrefix, func(configuration *config.Configuration) (string, error) {
			return mockutils.MockLocalMountPrefix, nil
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	configuration := getHTTPRuntimeConfig()
	configuration.StartArgs.LogDir = "/home/sn/logs"
	webCreator := &creator{}
	webRuntime, err := webCreator.NewRuntime(configuration, runtime.Handlers{})
	if err != nil {
		t.Errorf("failed to create web runtime, error: %s", err)
	}
	// 1. await ready timeout
	err = webRuntime.HotLoad(context.Background(), configuration)
	if err.(snerror.SNError).Code() != snerror.RuntimeInitTimeoutCode {
		t.Errorf("failed to test await ready timeout, error: %v", err)
	}

	log.Println("test await ready timeout ok")

	// 2. await ready and init success
	srv := http.Server{Addr: "127.0.0.1:8000"}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go srv.ListenAndServe()

	err = webRuntime.HotLoad(context.Background(), configuration)
	if err != nil {
		t.Errorf("failed to test hot load success, error: %v", err)
	}
	log.Println("test hot load success ok")

	configuration.Language = worker.CustomContainerLanguage
	err = webRuntime.HotLoad(context.Background(), configuration)
	if err != nil {
		t.Errorf("failed to test hot load success, error: %v", err)
	}
	log.Println("test container runtime hot load success ok")

	runtime.PodApplier = constants.WorkerManagerApplier
	err = webRuntime.HotLoad(context.Background(), configuration)
	if err != nil {
		t.Errorf("failed to test container runtime and reserve instance hot load success, error: %v", err)
	}
	log.Println("test container runtime and reserve instance hot load success ok")

	configuration.Language = ""
	err = webRuntime.HotLoad(context.Background(), configuration)
	if err != nil {
		t.Errorf("failed to test other runtime and reserve instance hot load success, error: %v", err)
	}
	log.Println("test other runtime and reserve instance hot load success ok")

	srv.Shutdown(context.TODO())
	runtime.CloseExitChannel()
}

func TestRunProcess(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(filepath.Abs, func(path string) (string, error) { return "", nil })
	p.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) { return nil, nil })
	cmd := &exec.Cmd{}
	p.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd { return cmd })
	p.ApplyMethod(reflect.TypeOf(cmd), "Start", func(_ *exec.Cmd) error { return nil })
	defer p.Reset()
	_, err := RunProcess(&runtimemanagerrequest.RunProcessParams{}, &config.Configuration{}, func([]byte) {})
	assert.NoError(t, err)
}

func TestProcessCancel(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
		return functionlog.NewFunctionLogger(nil, "", ""), nil
	})
	p.ApplyFunc(diskusage.MonitorDiskOfCustomImage, func(containerID string, mountPath []string, isUserDatasExpansion bool,
		stopCh chan struct{}, callback func(int64)) (*diskusage.EvictionManager, error) {
		return &diskusage.EvictionManager{}, nil
	})
	p.ApplyFunc(getLocalMountPrefix, func(configuration *config.Configuration) (string, error) {
		return mockutils.MockLocalMountPrefix, nil
	})
	defer p.Reset()

	{
		c := &creator{}
		r, err := c.NewRuntime(&config.Configuration{RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}}, runtime.Handlers{})
		require.NoError(t, err)

		expectInvokeID := "abc"
		resp := r.ProcessCancelEvent(&runtime.CancelRequest{InvokeID: expectInvokeID})
		require.NotNil(t, resp)
		assert.Equal(t, expectInvokeID, resp.InvokeID)
	}

	{
		c := &creator{}
		r, err := c.NewRuntime(&config.Configuration{}, runtime.Handlers{})
		require.NoError(t, err)

		resp := r.ProcessCancelEvent(&runtime.CancelRequest{})
		require.NotNil(t, resp)
		require.NotNil(t, resp.Error)
		assert.Equal(t, int32(snerror.WorkerInternalErrorCode), resp.Error.Code)
	}
}

func TestProcessEvent(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
		return functionlog.NewFunctionLogger(nil, "", ""), nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return &http.Response{Body: ioutil.NopCloser(bytes.NewBufferString("abc"))}, nil
	})
	p.ApplyFunc(getLocalMountPrefix, func(configuration *config.Configuration) (string, error) {
		return mockutils.MockLocalMountPrefix, nil
	})
	defer p.Reset()

	c := &creator{}
	expectInvokeID := 123
	var wg sync.WaitGroup
	wg.Add(1)
	r, err := c.NewRuntime(&config.Configuration{RuntimeConfig: config.RuntimeConfig{
		Initializer: config.Initializer{
			Extra: map[string]string{
				constants.HeaderAccessKey: "ak",
				constants.HeaderSecretKey: "sk",
				constants.HeaderAuthToken: "token",
			},
		},
		URN:                "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
		EnableAuthInHeader: true,
		RMConfig: rmconfig.Config{
			ServerPort: 1234,
			RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
				worker.WebRuntimeLanguage: {
					Addition: map[string]string{
						"maxResponseBodySize": "10",
					},
				},
			},
		},
	}}, runtime.Handlers{
		RecvProcessEventHandler: func(ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {
			require.NotNil(t, resp)
			assert.Equal(t, uint64(expectInvokeID), ic.ID)
			assert.Nil(t, resp.Error)
			wg.Done()
		},
	})
	require.NoError(t, err)

	conf := &config.Configuration{}
	runtime.ExtensionMgr = runtime.GetExtensionManager(conf)
	r.SendProcessEvent(invokectx.InvokeCtx{ID: 123}, runtime.InvokeRequest{}, conf)
	wg.Wait()
}

func TestWatchRuntimeMemory(t *testing.T) {
	exit := false

	p := gomonkey.NewPatches()
	p.ApplyFunc(utils.GetCPUMemory, func() (int64, int64) {
		return 0, 0
	})
	p.ApplyFunc(monitor.NewCGroupMemoryParserWithPath, func(_ string) (*monitor.Parser, error) {
		return monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
			return uint64(2 * 1024 * 1024), nil
		}), nil
	})
	p.ApplyFunc(runtime.CloseExitChannel, func() { exit = true })
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	w.watchingRuntimeMemoryUsedLoop(ctx)

	<-w.oomChan
	assert.True(t, exit)
}

func Test_watchingRuntimeMemoryUsedLoop_Fail(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "failed to create parser",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(monitor.NewCGroupMemoryParserWithPath, func(_ string) (*monitor.Parser, error) {
						return nil, errors.New("failed to create parser")
					}),
				})
				return patches
			},
		},
		{
			name: "failed to parse",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(monitor.NewCGroupMemoryParserWithPath, func(_ string) (*monitor.Parser, error) {
						return monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
							return 0, errors.New("failed to parse")
						}), nil
					}),
				})
				return patches
			},
		},
	}

	for _, tt := range tests {
		patches := tt.patchesFunc()
		w := &web{
			AbstractRuntime: runtime.AbstractRuntime{
				Configuration: &config.Configuration{
					RuntimeConfig: config.RuntimeConfig{
						RuntimeContainerID: "abc",
					},
				},
			},
			oomChan: make(chan struct{}),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		w.watchingRuntimeMemoryUsedLoop(ctx)
		cancel()
		patches.ResetAll()
	}
}

func TestInitializeRequest(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(http.NewRequestWithContext, func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
		b, err := io.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, []byte("{}"), b)
		return nil, errors.New("my error")
	})
	defer p.Reset()

	w := &web{}
	e := w.doInitializeRequest(context.TODO())
	assert.NotNil(t, e)
}

func TestInitializeRequest2(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("my error")
	})
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
	}
	e := w.doInitializeRequest(context.TODO())
	assert.NotNil(t, e)
}

func TestInitializeRequest3(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
	}
	e := w.doInitializeRequest(context.TODO())
	assert.NotNil(t, e)
}

func TestDoHTTPRequest(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
					URN:                "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest",
				},
			},
		},
		oomChan: make(chan struct{}),
	}

	extra := make(map[string]string)
	extra[constants.HeaderWorkflowRunID] = "test"
	extra[constants.HeaderWorkflowStateID] = "test"
	extra[constants.HeaderAccessKey] = "test"
	extra[constants.HeaderSecretKey] = "test"
	extra[constants.HeaderSecurityAccessKey] = "test"
	extra[constants.HeaderSecuritySecretKey] = "test"
	extra[constants.HeaderAuthToken] = "test"
	extra[constants.HeaderSecurityToken] = "test"

	_, e := w.doHTTPRequest(context.TODO(), "id", nil, extra)
	assert.NotNil(t, e)
}

func TestDoHTTPRequest2(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("my error")
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	p.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return true })
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					Language:           worker.CustomContainerLanguage,
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
	}
	_, e := w.doHTTPRequest(context.TODO(), "id", nil, nil)
	assert.NotNil(t, e)
}

func TestDoHTTPRequest3(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("my error")
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	p.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return true })
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
		handlers: runtime.Handlers{
			GetExceptionHandler: func() *runtime.Error { return &runtime.Error{} },
		},
	}
	_, e := w.doHTTPRequest(context.TODO(), "id", nil, nil)
	assert.NotNil(t, e)
}

func TestDoHTTPRequest4(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("my error")
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	p.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return true })
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
		handlers: runtime.Handlers{
			GetExceptionHandler: func() *runtime.Error { return nil },
		},
	}
	_, e := w.doHTTPRequest(context.TODO(), "id", nil, nil)
	assert.NotNil(t, e)
}

type fakeReaderWithERROR struct{}

func (fakeReaderWithERROR) Read(p []byte) (n int, err error) {
	return 0, errors.New("my error")
}

type fakeReader struct{}

func (fakeReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

func TestDoHTTPRequest5(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return &http.Response{Body: ioutil.NopCloser(fakeReaderWithERROR{})}, nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	p.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return true })
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
		handlers: runtime.Handlers{
			GetExceptionHandler: func() *runtime.Error { return nil },
		},
	}
	_, e := w.doHTTPRequest(context.TODO(), "id", nil, nil)
	assert.NotNil(t, e)
}

type contextDeadlineReader struct{}

func (contextDeadlineReader) Read(p []byte) (n int, err error) {
	return 0, context.DeadlineExceeded
}

func TestDoHTTPRequest6(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return &http.Response{Body: ioutil.NopCloser(contextDeadlineReader{})}, nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	p.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return true })
	defer p.Reset()

	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
		handlers: runtime.Handlers{
			GetExceptionHandler: func() *runtime.Error { return nil },
		},
	}
	_, e := w.doHTTPRequest(context.TODO(), "id", nil, nil)
	assert.NotNil(t, e)
}

func TestDoHTTPRequest7(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return &http.Response{Body: ioutil.NopCloser(bytes.NewBufferString("abc"))}, nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: functionType}}, nil
	})
	p.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return true })
	defer p.Reset()

	w := &web{
		maxResponseBodyLength: 1,
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
		handlers: runtime.Handlers{
			GetExceptionHandler: func() *runtime.Error { return nil },
		},
	}
	_, e := w.doHTTPRequest(context.TODO(), "id", nil, nil)
	assert.NotNil(t, e)
}

func TestDoHTTPRequest8(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return &http.Response{Body: ioutil.NopCloser(bytes.NewBufferString("abc"))}, nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: "no functionType"}}, nil
	})
	defer p.Reset()

	w := &web{
		maxResponseBodyLength: 1,
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan: make(chan struct{}),
		handlers: runtime.Handlers{
			GetExceptionHandler: func() *runtime.Error { return nil },
		},
	}
	apigEvent := APIGTriggerEvent{IsBase64Encoded: true, Body: "&!@$#&)(_"}
	event, _ := json.Marshal(apigEvent)
	_, e := w.doHTTPRequest(context.TODO(), "id", event, nil)
	assert.NotNil(t, e)
}

func TestDoHTTPRequestBrokenPipe(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("write: broken pipe")
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: "no functionType"}}, nil
	})
	defer p.Reset()

	w := &web{
		maxResponseBodyLength: 1,
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan:  make(chan struct{}),
		handlers: runtime.Handlers{},
	}
	apigEvent := APIGTriggerEvent{IsBase64Encoded: true, Body: ""}
	event, _ := json.Marshal(apigEvent)
	_, e := w.doHTTPRequest(context.TODO(), "id", event, nil)
	assert.NotNil(t, e)
}

func TestDoHTTPRequestConnectionResetByPeer(t *testing.T) {
	p := gomonkey.NewPatches()
	var cli *http.Client
	p.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset by peer")
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: "no functionType"}}, nil
	})
	defer p.Reset()

	w := &web{
		maxResponseBodyLength: 1,
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan:  make(chan struct{}),
		handlers: runtime.Handlers{},
	}
	apigEvent := APIGTriggerEvent{IsBase64Encoded: true, Body: ""}
	event, _ := json.Marshal(apigEvent)
	_, e := w.doHTTPRequest(context.TODO(), "id", event, nil)
	assert.NotNil(t, e)
}

func TestOOM(t *testing.T) {
	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan:     make(chan struct{}),
		cancelChan:  make(chan struct{}),
		readyChan:   make(chan struct{}),
		initTimeout: 5 * time.Second,
	}
	close(w.oomChan)
	resp := w.ProcessEvent(invokectx.InvokeCtx{ID: 123}, &runtime.InvokeRequest{})
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Error)
	assert.Equal(t, int32(snerror.RuntimeMemoryExceedLimit), resp.Error.Code)

	err := w.processInitializer(context.TODO())
	assert.NotNil(t, err)
	assert.Equal(t, int32(snerror.RuntimeMemoryExceedLimit), err.Code)

	err = w.awaitReady(context.TODO())
	assert.NotNil(t, err)
	assert.Equal(t, int32(snerror.RuntimeMemoryExceedLimit), err.Code)
}

func TestCancel(t *testing.T) {
	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan:     make(chan struct{}),
		cancelChan:  make(chan struct{}),
		readyChan:   make(chan struct{}),
		initTimeout: 5 * time.Second,
	}
	close(w.cancelChan)
	resp := w.ProcessEvent(invokectx.InvokeCtx{ID: 123}, &runtime.InvokeRequest{})
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Error)
	assert.Equal(t, int32(snerror.RequestCanceled), resp.Error.Code)

	err := w.processInitializer(context.TODO())
	assert.NotNil(t, err)
	assert.Equal(t, int32(snerror.RequestCanceled), err.Code)

	err = w.awaitReady(context.TODO())
	assert.NotNil(t, err)
	assert.Equal(t, int32(snerror.RequestCanceled), err.Code)
}

func Test_awaitReady(t *testing.T) {
	e := &runtime.Error{}
	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan:     make(chan struct{}),
		cancelChan:  make(chan struct{}),
		readyChan:   make(chan struct{}),
		initTimeout: 5 * time.Second,
		handlers: runtime.Handlers{
			GetExceptionHandler: func() *runtime.Error {
				return e
			},
		},
	}

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	ee := w.awaitReady(ctx)
	assert.Equal(t, e, ee)
}

func Test_awaitReady2(t *testing.T) {
	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RuntimeContainerID: "abc",
				},
			},
		},
		oomChan:     make(chan struct{}),
		cancelChan:  make(chan struct{}),
		readyChan:   make(chan struct{}),
		initTimeout: 5 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	e := w.awaitReady(ctx)
	require.NotNil(t, e)
	assert.Equal(t, int32(snerror.RuntimeProcessExit), e.Code)
}

func Test_checkReady(t *testing.T) {
	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyFunc(net.Dial, func(network, address string) (net.Conn, error) {
			return nil, errors.New("connection refused")
		}),
		gomonkey.ApplyFunc(commUtils.IsConnRefusedErr, func(err error) bool {
			return true
		}),
	})
	defer patches.ResetAll()
	w := &web{
		readyChan:   make(chan struct{}),
		initTimeout: 100 * time.Millisecond,
		port:        8000,
	}
	w.checkReady()
	w.initTimer = time.NewTimer(200 * time.Millisecond)
	select {
	case <-w.initTimer.C:
		return
	case <-w.readyChan:
		t.Errorf("failed to Test_checkReady")
	}
}

func Test_constructHTTPResponse_getConfig_error(t *testing.T) {
	patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return nil, errors.New("my error")
	})
	defer patches.Reset()
	_, runtimeErr := constructHTTPResponse(nil, nil)
	assert.NotNil(t, runtimeErr)
}

func Test_constructHTTPResponse_apig(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: "http"}}, nil
	})
	defer patches.Reset()
	headers := http.Header{}
	headers.Add("abc", "def")
	headers.Add("aaa", "bbb")
	b, runtimeErr := constructHTTPResponse(&http.Response{
		StatusCode: http.StatusOK,
		Status:     http.StatusText(http.StatusOK),
		Header:     headers,
	}, bytes.NewBuffer([]byte("abc")))
	assert.Nil(t, runtimeErr)
	assert.NotNil(t, b)
}

func Test_constructHTTPResponse_apigErr(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{RuntimeConfig: config.RuntimeConfig{FunctionType: "http"}}, nil
	})
	patches.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("failed to marshal") })
	defer patches.Reset()
	headers := http.Header{}
	headers.Add("abc", "def")
	headers.Add("aaa", "bbb")
	_, runtimeErr := constructHTTPResponse(&http.Response{
		StatusCode: http.StatusOK,
		Status:     http.StatusText(http.StatusOK),
		Header:     headers,
	}, bytes.NewBuffer([]byte("abc")))
	assert.NotNil(t, runtimeErr)
}

func Test_doHealthCheckRequest(t *testing.T) {
	type fields struct {
		logger                *RuntimeContainerLogger
		httpClient            *http.Client
		initTimer             *time.Timer
		startTime             time.Time
		serverInitTime        time.Time
		initTimeout           time.Duration
		once                  sync.Once
		port                  int32
		readyChan             chan struct{}
		oomChan               chan struct{}
		cancelChanOnce        sync.Once
		cancelChan            chan struct{}
		maxResponseBodyLength int64
		invokeTimeout         time.Duration
		AbstractRuntime       runtime.AbstractRuntime
		handlers              runtime.Handlers
		customDiskMgr         *diskusage.EvictionManager
	}
	type args struct {
		path    string
		timeout int
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			fields: fields{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				},
			},
			args: args{
				path:    "/health",
				timeout: 5,
			},
			patchesFunc: func() mockutils.PatchSlice {
				var cli *http.Client
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200}, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "test new request error",
			fields: fields{},
			args: args{
				path:    "health",
				timeout: 5,
			},
			patchesFunc: func() mockutils.PatchSlice {
				var cli *http.Client
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("my error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "test do request error",
			fields: fields{},
			args: args{
				path:    "health",
				timeout: 5,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("my error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &web{
				logger:                tt.fields.logger,
				httpClient:            tt.fields.httpClient,
				initTimer:             tt.fields.initTimer,
				startTime:             tt.fields.startTime,
				serverInitTime:        tt.fields.serverInitTime,
				initTimeout:           tt.fields.initTimeout,
				once:                  tt.fields.once,
				port:                  tt.fields.port,
				readyChan:             tt.fields.readyChan,
				oomChan:               tt.fields.oomChan,
				cancelChanOnce:        tt.fields.cancelChanOnce,
				cancelChan:            tt.fields.cancelChan,
				maxResponseBodyLength: tt.fields.maxResponseBodyLength,
				invokeTimeout:         tt.fields.invokeTimeout,
				AbstractRuntime:       tt.fields.AbstractRuntime,
				handlers:              tt.fields.handlers,
				customDiskMgr:         tt.fields.customDiskMgr,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			_, err := w.doHealthCheckRequest(tt.args.path, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func Test_WebReportCustomMetrics(t *testing.T) {
	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{},
		},
	}
	w.ReportCustomMetrics(10)
}

func Test_WebHealthz(t *testing.T) {
	type fields struct {
		logger                *RuntimeContainerLogger
		httpClient            *http.Client
		initTimer             *time.Timer
		startTime             time.Time
		serverInitTime        time.Time
		initTimeout           time.Duration
		once                  sync.Once
		port                  int32
		readyChan             chan struct{}
		oomChan               chan struct{}
		cancelChanOnce        sync.Once
		cancelChan            chan struct{}
		maxResponseBodyLength int64
		invokeTimeout         time.Duration
		AbstractRuntime       runtime.AbstractRuntime
		handlers              runtime.Handlers
		customDiskMgr         *diskusage.EvictionManager
	}
	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{},
		},
	}
	type args struct {
		path    string
		timeout int
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test error 1",
			fields: fields{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				},
			},
			args: args{
				path:    "/health",
				timeout: 5,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*web).doHealthCheckRequest, func(_ *web, path string, timeout int) (*http.Response, error) {
						return &http.Response{StatusCode: 0, Body: io.NopCloser(bytes.NewBufferString("dummy secret"))}, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test error 2",
			fields: fields{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				},
			},
			args: args{
				path:    "/health",
				timeout: 5,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*web).doHealthCheckRequest, func(_ *web, path string, timeout int) (*http.Response, error) {
						return &http.Response{StatusCode: 400, Body: io.NopCloser(bytes.NewBufferString("dummy secret"))}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test error 3",
			fields: fields{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				},
			},
			args: args{
				path:    "/health",
				timeout: 5,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*web).doHealthCheckRequest, func(_ *web, path string, timeout int) (*http.Response, error) {
						return &http.Response{StatusCode: 500, Body: io.NopCloser(bytes.NewBufferString("dummy secret"))}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte{}, nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test success",
			fields: fields{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				},
			},
			args: args{
				path:    "/health",
				timeout: 5,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*web).doHealthCheckRequest, func(_ *web, path string, timeout int) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("dummy secret"))}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte{}, nil
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
			defer patches.ResetAll()
			_, err := w.Healthz(tt.args.path, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func Test_StartupHealthz(t *testing.T) {
	w := &web{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{},
		},
	}
	ok, err := w.StartupHealthz("/healthcheck", 5)
	assert.Nil(t, err)
	assert.True(t, ok)
}

func Test_web_doHTTPRequestForGRPC(t *testing.T) {
	type fields struct {
		logger                *RuntimeContainerLogger
		httpClient            *http.Client
		http2Client           *http.Client
		initTimer             *time.Timer
		startTime             time.Time
		serverInitTime        time.Time
		initTimeout           time.Duration
		once                  sync.Once
		http2Once             sync.Once
		port                  int32
		readyChan             chan struct{}
		oomChan               chan struct{}
		cancelChanOnce        sync.Once
		cancelChan            chan struct{}
		maxResponseBodyLength int64
		invokeTimeout         time.Duration
		AbstractRuntime       runtime.AbstractRuntime
		handlers              runtime.Handlers
		customDiskMgr         *diskusage.EvictionManager
	}
	type args struct {
		ctx       context.Context
		requestID string
		event     []byte
		extra     map[string]string
	}
	apigEvent := &APIGTriggerEvent{
		IsBase64Encoded: true,
		Headers: map[string]interface{}{
			"content-type": "application/grpc",
		},
	}
	event, _ := json.Marshal(apigEvent)
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			fields: fields{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				},
			},
			args: args{
				ctx:       context.Background(),
				requestID: "mock-request-id",
				event:     event,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
						cfg := &config.Configuration{}
						cfg.FunctionType = "http"
						return cfg, nil
					}),
					gomonkey.ApplyFunc((*urnutils.BaseURN).ParseFrom, func(_ *urnutils.BaseURN, urn string) error {
						return nil
					}),
					gomonkey.ApplyFunc(urnutils.GetFunctionNameAndServiceName, func(funcName string) (string, string, error) {
						return "", "", nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							Body: ioutil.NopCloser(fakeReader{}),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test success with trailer response",
			fields: fields{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				},
			},
			args: args{
				ctx:       context.Background(),
				requestID: "mock-request-id",
				event:     event,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
						cfg := &config.Configuration{}
						cfg.FunctionType = "http"
						return cfg, nil
					}),
					gomonkey.ApplyFunc((*urnutils.BaseURN).ParseFrom, func(_ *urnutils.BaseURN, urn string) error {
						return nil
					}),
					gomonkey.ApplyFunc(urnutils.GetFunctionNameAndServiceName, func(funcName string) (string, string, error) {
						return "", "", nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							Body: ioutil.NopCloser(fakeReader{}),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
							Trailer: http.Header{
								"Grpc-Status":  []string{"0"},
								"Grpc-Message": []string{},
							},
						}, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &web{
				logger:                tt.fields.logger,
				httpClient:            tt.fields.httpClient,
				http2Client:           tt.fields.http2Client,
				initTimer:             tt.fields.initTimer,
				startTime:             tt.fields.startTime,
				serverInitTime:        tt.fields.serverInitTime,
				initTimeout:           tt.fields.initTimeout,
				once:                  tt.fields.once,
				http2Once:             tt.fields.http2Once,
				port:                  tt.fields.port,
				readyChan:             tt.fields.readyChan,
				oomChan:               tt.fields.oomChan,
				cancelChanOnce:        tt.fields.cancelChanOnce,
				cancelChan:            tt.fields.cancelChan,
				maxResponseBodyLength: tt.fields.maxResponseBodyLength,
				invokeTimeout:         tt.fields.invokeTimeout,
				AbstractRuntime:       tt.fields.AbstractRuntime,
				handlers:              tt.fields.handlers,
				customDiskMgr:         tt.fields.customDiskMgr,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			respBytes, err := w.doHTTPRequest(tt.args.ctx, tt.args.requestID, tt.args.event, tt.args.extra)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
			fmt.Println(string(respBytes))
			apigTriggerResp := &APIGTriggerResponse{}
			checkErr := json.Unmarshal(respBytes, apigTriggerResp)
			if checkErr != nil {
				t.Errorf("json unmarshal error %v", checkErr)
			}
			if apigTriggerResp.Headers["Trailer:Grpc-Status"] != nil &&
				apigTriggerResp.Headers["Trailer:Grpc-Status"][0] != "0" {
				t.Errorf("want %v but found %v", "0", apigTriggerResp.Headers["Trailer:Grpc-Status"][0])
			}
		})
	}
}

func Test_web_getHTTPClient(t *testing.T) {
	t.Run("When the api is /invoke, redirect is not nil", func(t *testing.T) {
		w := &web{
			AbstractRuntime: runtime.AbstractRuntime{
				Configuration: &config.Configuration{},
			},
		}
		client := w.getHTTPClient("invoke")
		assert.NotNil(t, client.CheckRedirect)
	})
}

func Test_web_constructHTTPRequest(t *testing.T) {
	jsonData := `p{"name": "Alice", "age": 25}`
	bytes := []byte(jsonData)
	p := gomonkey.NewPatches()
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				FunctionType: "apigEvent",
			},
		}, nil
	})
	_, err := constructHTTPRequest(context.TODO(), "http://127.0.0.1:1234", bytes)
	assert.Equal(t, err.Code, int32(snerror.FunctionExceptionCode))
}

func Test_web_constructApigRequest(t *testing.T) {
	jsonData := `{"body": {"a":"b"}, "age": 25}`
	bytes := []byte(jsonData)
	p := gomonkey.NewPatches()
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				FunctionType: "apigEvent",
			},
		}, nil
	})
	_, err := constructHTTPRequest(context.TODO(), "http://127.0.0.1:1234", bytes)
	assert.Equal(t, err.Code, int32(snerror.FunctionExceptionCode))
}

type mockCli struct {
}

func (m *mockCli) Info(ctx context.Context, containerID string) (engine.ContainerInfo, error) {
	return engine.ContainerInfo{
		RootFsPath: mockutils.MockLocalMountPrefix,
	}, nil
}
func (m *mockCli) Kill(ctx context.Context, containerID string) error {
	return nil
}
func (m *mockCli) Close() error {
	return nil
}

func Test_startMonitorCustomDisk(t *testing.T) {
	type args struct {
		configuration *config.Configuration
	}
	configuration := &config.Configuration{}
	configuration.RuntimeConfig.FuncMountConfig.FuncMounts = []types.FuncMount{
		{
			LocalMountPath: mockutils.MockLocalMountPrefix,
		},
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test local mount path has prefix",
			args: args{
				configuration: configuration,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getLocalMountPrefix, func(configuration *config.Configuration) (string, error) {
						return mockutils.MockLocalMountPrefix, nil
					}),
					gomonkey.ApplyFunc(diskusage.MonitorDiskOfCustomImage, func(containerID string, mountPath []string, isUserDatasExpansion bool,
						stopCh chan struct{}, callback func(int64)) (*diskusage.EvictionManager, error) {
						return &diskusage.EvictionManager{}, nil
					}),
				})
				return patches
			},
		},
		{
			name: "test getLocalMountPrefix",
			args: args{
				configuration: configuration,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(engine.NewContainerClient, func() (engine.ContainerClient, error) {
						return &mockCli{}, nil
					}),
					gomonkey.ApplyFunc(diskusage.MonitorDiskOfCustomImage, func(containerID string, mountPath []string, isUserDatasExpansion bool,
						stopCh chan struct{}, callback func(int64)) (*diskusage.EvictionManager, error) {
						return &diskusage.EvictionManager{}, nil
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
			startMonitorCustomDisk(tt.args.configuration)
			cfg := tt.args.configuration.FuncMountConfig
			for i := range cfg.FuncMounts {
				if !strings.HasPrefix(cfg.FuncMounts[i].LocalMountPath, mockutils.MockLocalMountPrefix) {
					t.Errorf("want prefix %v but found %v", mockutils.MockLocalMountPrefix, cfg.FuncMounts[i].LocalMountPath)
				}
			}
		})
	}
}

func TestProcessStreamResponse(t *testing.T) {
	data, err := processStreamResponse(context.Background(), &http.Response{}, make(map[string]string))
	assert.Nil(t, data)
	assert.NotNil(t, err)
	assert.Equal(t, int32(snerror.StreamConnException), err.Code)
}

func Test_eventToHTTPRequest(t *testing.T) {
	body := []byte("{\"body\":\"123\",\"requestContext\":{\"apiId\":\"bc1dcffd-aa35-474d-897c-d53425a4c08e\",\"requestId\":\"11cdcdcf33949dc6d722640a13091c77\",\"stage\":\"RELEASE\"},\"queryStringParameters\":{\"responseType\":\"html\"},\"httpMethod\":\"GET\",\"pathParameters\":{},\"headers\":{\"accept-language\":\"zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2\",\"accept-encoding\":\"gzip, deflate, br\",\"x-forwarded-port\":\"443\",\"x-forwarded-for\":\"103.218.216.98\",\"accept\":\"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\",\"upgrade-insecure-requests\":\"1\",\"host\":\"50eedf92-c9ad-4ac0-827e-d7c11415d4f1.apigw.region.huaweicloud.com\",\"x-forwarded-proto\":\"https\",\"pragma\":\"no-cache\",\"cache-control\":\"no-cache\",\"x-real-ip\":\"103.218.216.98\",\"user-agent\":\"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0\",\"testssss\":\"testsss\"},\"path\":\"/disconnect_all\",\"isBase64Encoded\":false}")
	httpReq, _ := eventToHTTPRequest(context.Background(), "", body)
	if httpReq.Header.Get("Testssss") != "testsss" {
		t.Errorf("failed to Test_eventToHTTPRequest")
	}
}
