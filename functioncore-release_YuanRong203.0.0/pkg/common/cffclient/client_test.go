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

// Package cffclient -
package cffclient

import (
	"context"
	"crypto/tls"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/localauth"
)

func TestNewClient(t *testing.T) {
	client, err := NewClient(1*time.Second, WithTLSConfig(&tls.Config{}),
		WithAuth("ak", "sk"))
	assert.NoError(t, err)
	assert.Equal(t, "ak", client.ak)
	assert.Equal(t, "sk", client.sk)
	assert.Equal(t, "https", client.scheme)
	assert.Equal(t, 1*time.Second, client.httpClient.Timeout)
}

func TestClient_DELETE(t *testing.T) {
	c := Client{
		httpClient: &http.Client{
			Timeout: 1 * time.Second,
		},
		scheme: "http",
	}
	patch := gomonkey.ApplyFunc(localauth.SignOMSVC, func(ak, sk, url string, data []byte) (string, string) {
		return "", ""
	})
	patch.ApplyMethod(reflect.TypeOf(c.httpClient), "Do", func(*http.Client, *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200}, nil
	})
	defer patch.Reset()
	resp, err := c.DELETE(context.Background(), "127.0.0.1:8080", "/api")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestClient_POST(t *testing.T) {
	c := Client{
		httpClient: &http.Client{
			Timeout: 1 * time.Second,
		},
		scheme: "http",
	}
	patch := gomonkey.ApplyFunc(localauth.SignOMSVC, func(ak, sk, url string, data []byte) (string, string) {
		return "", ""
	})
	patch.ApplyMethod(reflect.TypeOf(c.httpClient), "Do", func(*http.Client, *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200}, nil
	})
	defer patch.Reset()
	resp, err := c.POST(context.Background(), "127.0.0.1:8080", "/api", nil)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}
