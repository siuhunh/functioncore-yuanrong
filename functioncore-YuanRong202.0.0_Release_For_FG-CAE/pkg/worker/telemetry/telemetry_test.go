/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"
	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	mockUtils "functioncore/test/utils"
)

func Test_ParseTelemetryConfig(t *testing.T) {
	jsonBytes, _ := json.Marshal("abc")
	jsonBytes1, _ := json.Marshal(Configuration{Types: []string{"function"}})
	jsonBytes2, _ := json.Marshal(Configuration{Types: []string{"function", "a", "b", "c"}})
	jsonBytes3, _ := json.Marshal(Configuration{Types: []string{"err"}})
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 json Unmarshal error",
			args{w: httptest.NewRecorder(), r: httptest.NewRequest("POST", "/telemetry", bytes.NewReader([]byte{}))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return nil, errors.New("failed to ReadAll")
						}),
				})
				return patches
			}},
		{"case2 Unmarshal error",
			args{w: httptest.NewRecorder(), r: httptest.NewRequest("POST", "/telemetry", bytes.NewReader(jsonBytes))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
		{"case3 succeed to ParseTelemetryConfig",
			args{w: httptest.NewRecorder(), r: httptest.NewRequest("POST", "/telemetry", bytes.NewReader(jsonBytes1))},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
		{"case4 empty event type",
			args{w: httptest.NewRecorder(), r: httptest.NewRequest("POST", "/telemetry", bytes.NewReader(jsonBytes2))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
		{"case4 err event type",
			args{w: httptest.NewRecorder(), r: httptest.NewRequest("POST", "/telemetry", bytes.NewReader(jsonBytes3))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			err := ParseTelemetryConfig(tt.args.w, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTelemetryConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_sendTelemetryMessage(t *testing.T) {
	type args struct {
		bytes   []byte
		traceID string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to send idle mode,DoTimeout error", args{
			bytes:   []byte{1},
			traceID: "traceID",
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return errors.New("test")
					}),
			})
			return patches
		}},
		{"case1 send telemetry message ok", args{
			bytes:   []byte{1},
			traceID: "traceID",
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&fhttp.Client{}), "DoTimeout",
					func(_ *fhttp.Client, req *fhttp.Request, resp *fhttp.Response, timeout time.Duration) error {
						return nil
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			sendTelemetryMessage(tt.args.bytes, tt.args.traceID)
			patches.ResetAll()
		})
	}
}

func Test_SendEvent(t *testing.T) {
	type args struct {
		eventData Event
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 build event ok", args{
			eventData: Event{
				Type: "type",
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
			})
			return patches
		}},
		{"case2 do not contain event type", args{
			eventData: Event{
				Type: constants.TelemetryPlatformType,
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return false
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			SendEvent(tt.args.eventData)
			patches.ResetAll()
		})
	}
}

func Test_parseEvent(t *testing.T) {
	type args struct {
		eventData Event
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 build telemetry start event ok", args{
			eventData: Event{
				Type:    constants.TelemetryPlatformType,
				Name:    constants.TelemetryStart,
				TraceID: "traceID",
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			return patches
		}},
		{"case2 build telemetry runtimeDone event ok", args{
			eventData: Event{
				Type:    constants.TelemetryPlatformType,
				Name:    constants.TelemetryRuntimeDone,
				TraceID: "traceID",
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return false
					}),
			})
			return patches
		}},
		{"case3 build telemetry report event ok", args{
			eventData: Event{
				Type:    constants.TelemetryPlatformType,
				Name:    constants.TelemetryReport,
				TraceID: "traceID",
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return false
					}),
			})
			return patches
		}},
		{"case4 build telemetry function event ok", args{
			eventData: Event{
				Type:    constants.TelemetryFunctionType,
				Name:    constants.TelemetryFunctionType,
				TraceID: "traceID",
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return false
					}),
			})
			return patches
		}},
		{"case5 build telemetry extension event ok", args{
			eventData: Event{
				Type:    constants.TelemetryExtensionType,
				Name:    constants.TelemetryExtensionType,
				TraceID: "traceID",
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return false
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			byteValue := parseEvent(tt.args.eventData)
			assert.NotNil(t, byteValue)
			patches.ResetAll()
		})
	}
}

func TestServer_OpenTelemetryAPI(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1", args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/telemetry", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "traceID")
				return req
			}()}, http.StatusOK, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(ParseTelemetryConfig, func(w http.ResponseWriter, r *http.Request) error {
					return nil
				}),
			})
			return patches
		}},
		{"case2", args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/telemetry", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "traceID")
				return req
			}()}, http.StatusBadRequest, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(ParseTelemetryConfig, func(w http.ResponseWriter, r *http.Request) error {
					return errors.New("error")
				}),
			})
			return patches
		}},
		{"case2", args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/telemetry", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "traceID")
				return req
			}()}, http.StatusBadRequest, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(ParseTelemetryConfig, func(w http.ResponseWriter, r *http.Request) error {
					return errors.New("error")
				}),
				ApplyFunc(json.Marshal,
					func(v interface{}) ([]byte, error) {
						return []byte{1}, errors.New("aaa")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			OpenTelemetryAPI(tt.args.w, tt.args.r)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			time.Sleep(3 * time.Second)
			patches.ResetAll()
		})
	}
}
