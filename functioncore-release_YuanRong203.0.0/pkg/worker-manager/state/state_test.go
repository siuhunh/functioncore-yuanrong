package state

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/metrics"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/worker-manager/config"
	"functioncore/test/utils"
)

func Test_InitHTTPClient(t *testing.T) {
	convey.Convey("Test InitHTTPClient, get config error", t, func() {
		patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return nil, errors.New("error")
		})
		defer patch.Reset()
		if err := InitHTTPClient(); err == nil {
			t.Errorf("this should error")
		}
	})

	convey.Convey("Test InitHTTPClient, HTTPSEnable", t, func() {
		patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		defer patch.Reset()
		var err error
		if err = InitHTTPClient(); err != nil {
			t.Errorf("this should not error")
		}
	})
}

func Test_deleteStateByStateManager(t *testing.T) {
	convey.Convey("Test deleteStateByStateManager, json marshal error", t, func() {
		patch := gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
			return nil, errors.New("error")
		})
		defer patch.Reset()
		if err := deleteStateByStateManager("stateID", metrics.FunctionInfo{}); err == nil {
			t.Errorf("this should error")
		}
	})

	convey.Convey("Test deleteStateByStateManager, get config error", t, func() {
		patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return nil, errors.New("error")
		})
		defer patch.Reset()
		if err := deleteStateByStateManager("stateID", metrics.FunctionInfo{}); err == nil {
			t.Errorf("this should error")
		}
	})

	convey.Convey("Test deleteStateByStateManager, new request error", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc(http.NewRequest, func(method, url string, body io.Reader) (*http.Request, error) {
				return nil, errors.New("error")
			}),
		})
		defer patches.ResetAll()
		if err := deleteStateByStateManager("stateID", metrics.FunctionInfo{}); err == nil {
			t.Errorf("this should error")
		}
	})

	convey.Convey("Test deleteStateByStateManager, all clear", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc(doDeleteRequest, func(request *http.Request) snerror.SNError {
				return nil
			}),
		})
		defer patches.ResetAll()
		if err := deleteStateByStateManager("stateID", metrics.FunctionInfo{}); err != nil {
			t.Errorf("this should error")
		}
	})
	convey.Convey("Test deleteStateByStateManager with version, all clear", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc(doDeleteRequest, func(request *http.Request) snerror.SNError {
				requestBytes, err := ioutil.ReadAll(request.Body)
				if err != nil {
					return snerror.NewWithError(-1, err)
				}
				var functionInfo = &FuncInfo{}
				err = json.Unmarshal(requestBytes, functionInfo)
				if err != nil {
					return snerror.NewWithError(-1, err)
				}
				if functionInfo.FunctionName == "TestFunction:v1" {
					return nil
				}
				return snerror.NewWithError(-1, errors.New("error"))
			}),
		})
		defer patches.ResetAll()
		if err := deleteStateByStateManager("stateID", metrics.FunctionInfo{
			FunctionName: "TestFunction",
			Version:      "v1",
		}); err != nil {
			t.Errorf("this should error")
		}
	})
	convey.Convey("Test deleteStateByStateManager with default version, all clear", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc(doDeleteRequest, func(request *http.Request) snerror.SNError {
				requestBytes, err := ioutil.ReadAll(request.Body)
				if err != nil {
					return snerror.NewWithError(-1, err)
				}
				var functionInfo = &FuncInfo{}
				err = json.Unmarshal(requestBytes, functionInfo)
				if err != nil {
					return snerror.NewWithError(-1, err)
				}
				if functionInfo.FunctionName == "TestFunction" {
					return nil
				}
				return snerror.NewWithError(-1, errors.New("error"))
			}),
		})
		defer patches.ResetAll()
		if err := deleteStateByStateManager("stateID", metrics.FunctionInfo{
			FunctionName: "TestFunction",
			Version:      "latest",
		}); err != nil {
			t.Errorf("this should error")
		}
	})
}

func Test_doDeleteRequest(t *testing.T) {
	convey.Convey("Test http client is nil", t, func() {
		if err := doDeleteRequest(&http.Request{}); err == nil {
			t.Errorf("this should be error")
		}
	})

	convey.Convey("Test Do error", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return nil, errors.New("this should error")
			}),
		})
		defer patches.ResetAll()
		if err := InitHTTPClient(); err != nil {
			t.Errorf("this should not be error")
		}
		if err := doDeleteRequest(&http.Request{}); err == nil {
			t.Errorf("this should be error")
		}
	})

	convey.Convey("Test resp.StatusCode != http.StatusOK", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 404,
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   http.NoBody}, nil
			}),
		})
		defer patches.ResetAll()
		if err := InitHTTPClient(); err != nil {
			t.Errorf("this should not be error")
		}
		if err := doDeleteRequest(&http.Request{}); err == nil {
			t.Errorf("this should be error")
		}
	})

	convey.Convey("Test read all error", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200,
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   http.NoBody}, nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return nil, errors.New("error")
			}),
		})
		defer patches.ResetAll()
		if err := InitHTTPClient(); err != nil {
			t.Errorf("this should not be error")
		}
		if err := doDeleteRequest(&http.Request{}); err == nil {
			t.Errorf("this should be error")
		}
	})

	convey.Convey("Test unmarshal error", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200,
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   http.NoBody}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200,
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   http.NoBody}, nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return []byte{123}, nil
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return errors.New("error")
			}),
		})
		defer patches.ResetAll()
		if err := InitHTTPClient(); err != nil {
			t.Errorf("this should not be error")
		}
		if err := doDeleteRequest(&http.Request{}); err == nil {
			t.Errorf("this should be error")
		}
	})

	convey.Convey("Test, all clear", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200,
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   http.NoBody}, nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return nil
			}),
		})
		defer patches.ResetAll()
		if err := InitHTTPClient(); err != nil {
			t.Errorf("this should not be error")
		}
		if err := doDeleteRequest(&http.Request{}); err != nil {
			t.Errorf("this should be error")
		}
	})
}

func Test_SendDeleteStateReq(t *testing.T) {
	convey.Convey("Test deleteStateByStateManager, json marshal error", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: false}, LocalAuth: &localauth.AuthConfig{}}, nil
			}),
			gomonkey.ApplyFunc(doDeleteRequest, func(request *http.Request) snerror.SNError {
				return nil
			}),
		})
		defer patches.ResetAll()
		SendDeleteStateReq("stateID", metrics.FunctionInfo{})
	})
}
