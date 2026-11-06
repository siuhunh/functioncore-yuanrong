/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package workermanagerutils is common utils of querying instances from worker-manager
package workermanagerutils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
	mockutils "functioncore/test/utils"
)

func TestQueryInstancesFromWmByIP(t *testing.T) {
	queryInsResp := types.QueryInstanceResponse{
		Code:      200,
		Message:   "success",
		Instances: types.QueryInstance{},
	}
	byteResp, _ := json.Marshal(queryInsResp)
	type args struct {
		wantResp   types.QueryInstanceResponse
		wantErr    bool
		wantErrMsg string
	}
	urlValue := &url.URL{
		Scheme:   "https",
		User:     url.UserPassword("me", "pass"),
		Host:     "example.com",
		Path:     "foo/bar",
		RawQuery: "x=1&y=2",
		Fragment: "anchor"}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"url parse failed",
			args{
				types.QueryInstanceResponse{},
				true,
				"failed to parse url",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return nil, fmt.Errorf("failed to parse url")
					}),
				})
				return patches
			},
		},
		{
			"create request failed",
			args{
				types.QueryInstanceResponse{},
				true,
				"failed to create request",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("http.NewRequestWithContext")
					}),
				})
				return patches
			},
		},
		{
			name: "do rerequest failed",
			args: args{
				types.QueryInstanceResponse{},
				true,
				"failed to send request to worker-manager",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{
							Scheme:   "https",
							User:     url.UserPassword("me", "pass"),
							Host:     "example.com",
							Path:     "foo/bar",
							RawQuery: "x=1&y=2",
							Fragment: "anchor"}, nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("do request error")
					}),
				})
				return patches
			},
		},
		{
			"response code not OK",
			args{
				types.QueryInstanceResponse{},
				true,
				"resp code",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 500,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("{}"))),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), nil
					}),
				})
				return patches
			},
		},
		{
			"response code not OK, body is nil",
			args{
				types.QueryInstanceResponse{},
				true,
				"resp code",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 500,
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), nil
					}),
				})
				return patches
			},
		},
		{
			"failed to unmarshal resp body",
			args{
				types.QueryInstanceResponse{},
				true,
				"failed to unmarshal resp body",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("error"))),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte(""), nil
					}),
				})
				return patches
			},
		},
		{
			"read resp body failed",
			args{
				types.QueryInstanceResponse{},
				true,
				"failed to read resp body",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("{}"))),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("err")
					}),
				})
				return patches
			},
		},
		{
			"query success",
			args{
				queryInsResp,
				false,
				"",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(url.Parse, func(rawURL string) (*url.URL, error) {
						return &url.URL{}, nil
					}),
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(
						ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{
							Header: http.Header{},
							URL:    urlValue,
						}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: 200,
							Body: ioutil.NopCloser(bytes.NewReader(
								[]byte("{}"))),
							Header: http.Header{
								"content-type": []string{"application/grpc"},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return byteResp, nil
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
			auth := &localauth.AuthConfig{
				AKey:     "akey",
				SKey:     "skey",
				Duration: 1,
			}
			got, err := QueryInstancesFromWmByIP("func1", "192.168.1.1", false, auth)
			if (err != nil) != tt.args.wantErr {
				t.Errorf("queryInstancesFromWmByIP() = %v, want %v, wantErr %v", got, tt.args.wantResp, tt.args.wantErr)
			}
			if tt.args.wantErrMsg != "" {
				assert.Contains(t, err.Error(), tt.args.wantErrMsg)
			}
		})
	}
}
