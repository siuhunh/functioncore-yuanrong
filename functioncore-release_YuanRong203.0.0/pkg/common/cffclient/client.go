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
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
)

const (
	// HeaderAuthTimestamp is the timestamp for alarm authorization
	HeaderAuthTimestamp = "X-CFF-Timestamp-Auth"
	// HeaderAuthorization is authorization
	HeaderAuthorization = "X-CFF-Authorization"
)

// ClientOption client option function
type ClientOption func(*Client)

// WithTLSConfig enable TLS option
func WithTLSConfig(tlsConfig *tls.Config) ClientOption {
	if tlsConfig == nil {
		// disable tls
		return func(c *Client) {
			c.scheme = "http"
		}
	}

	// enable tls
	return func(c *Client) {
		tr, ok := http.DefaultTransport.(*http.Transport)
		if ok {
			tr.TLSClientConfig = tlsConfig
			c.httpClient.Transport = tr
			c.scheme = "https"
		}
	}
}

// WithAuth auth option
func WithAuth(ak, sk string) ClientOption {
	return func(c *Client) {
		c.ak = ak
		c.sk = sk
	}
}

// NewClient new once
func NewClient(timeout time.Duration, opts ...ClientOption) (*Client, error) {
	client := Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		scheme: "http",
	}

	for _, opt := range opts {
		opt(&client)
	}
	return &client, nil
}

// Client to report an alarm as a client when an error occurs
type Client struct {
	httpClient *http.Client
	sync.RWMutex
	scheme string

	// enable auth
	ak string
	sk string
}

func (c *Client) newPOSTRequest(ctx context.Context, url string, payload []byte) *http.Request {
	curRequest, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		log.GetLogger().Warnf("failed to make request, err %s", err.Error())
		return nil
	}
	authorization, timestamp := localauth.SignOMSVC(c.ak, c.sk, url, payload)
	curRequest.Header.Set(HeaderAuthorization, authorization)
	curRequest.Header.Set(HeaderAuthTimestamp, timestamp)
	curRequest.Header.Set("Content-Type", "application/json")
	curRequest.Header.Set("Accept", "application/json")
	return curRequest
}

func (c *Client) newDELETERequest(ctx context.Context, url string) *http.Request {
	curRequest, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		log.GetLogger().Warnf("failed to make request, err %s", err.Error())
		return nil
	}
	authorization, timestamp := localauth.SignOMSVC(c.ak, c.sk, url, nil)
	curRequest.Header.Set(HeaderAuthorization, authorization)
	curRequest.Header.Set(HeaderAuthTimestamp, timestamp)
	return curRequest
}

// POST -
func (c *Client) POST(ctx context.Context, address, apiPath string, payload []byte) (*http.Response, error) {
	url := fmt.Sprintf("%s://%s%s", c.scheme, address, apiPath)
	req := c.newPOSTRequest(ctx, url, payload)
	if req == nil {
		return nil, fmt.Errorf("new post request error")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.GetLogger().Errorf("failed to request url %s, err %s", url, err.Error())
		return nil, err
	}
	return resp, nil
}

// DELETE -
func (c *Client) DELETE(ctx context.Context, address, apiPath string) (*http.Response, error) {
	url := fmt.Sprintf("%s://%s%s", c.scheme, address, apiPath)
	req := c.newDELETERequest(ctx, url)
	if req == nil {
		return nil, fmt.Errorf("new delete request error")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.GetLogger().Errorf("failed to request url %s, err %s", url, err.Error())
		return nil, err
	}
	return resp, nil
}
