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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"functioncore/pkg/common/logger/log"
)

// APIGTriggerEvent extern interface of web request
type APIGTriggerEvent struct {
	IsBase64Encoded       bool                   `json:"isBase64Encoded"`
	HTTPMethod            string                 `json:"httpMethod"`
	Path                  string                 `json:"path"`
	Body                  string                 `json:"body"`
	PathParameters        map[string]string      `json:"pathParameters"`
	RequestContext        APIGRequestContext     `json:"requestContext"`
	Headers               map[string]interface{} `json:"headers"`
	QueryStringParameters map[string]interface{} `json:"queryStringParameters"`
	UserData              string                 `json:"user_data"`
}

// APIGRequestContext -
type APIGRequestContext struct {
	APIID     string `json:"apiId"`
	RequestID string `json:"requestId"`
	Stage     string `json:"stage"`
	SourceIP  string `json:"sourceIp"`
	Protocol  string `json:"protocol"`
}

// APIGTriggerResponse extern interface of web response
type APIGTriggerResponse struct {
	Body            string              `json:"body"`
	Headers         map[string][]string `json:"headers"`
	StatusCode      int                 `json:"statusCode"`
	IsBase64Encoded bool                `json:"isBase64Encoded"`
}

func isHTTP2Request(event *APIGTriggerEvent) bool {
	contentTypeIf := event.Headers["content-type"]
	contentType, ok := contentTypeIf.(string)
	if !ok {
		return false
	}
	return contentType == "application/grpc" || event.RequestContext.Protocol == "HTTP/2"
}

func parseAPIGEvent(ctx context.Context, serializedEvent []byte, baseURLPath string) (*http.Request, bool, error) {
	apigEvent := &APIGTriggerEvent{}
	if err := json.Unmarshal(serializedEvent, apigEvent); err != nil {
		log.GetLogger().Errorf("failed to unmarshal event to APIG event, error: %s", err)
		return nil, true, err
	}
	httpRequest, err := apigToHTTPRequest(ctx, apigEvent, baseURLPath)
	return httpRequest, false, err
}

func constructAPIGResponse(resp *http.Response, body *bytes.Buffer) ([]byte, error) {
	apigResponse, err := fromHTTPResponse(resp, body)
	if err != nil {
		return nil, err
	}

	serializedResponse, err := json.Marshal(apigResponse)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal APIG response, error: %s", err)
		return nil, err
	}
	return serializedResponse, nil
}

func apigToHTTPRequest(ctx context.Context, event *APIGTriggerEvent, baseURLPath string,
) (*http.Request, error) {
	var requestBody []byte
	var err error
	if event.IsBase64Encoded {
		requestBody, err = base64.StdEncoding.DecodeString(event.Body)
		if err != nil {
			log.GetLogger().Errorf("failed to decode body string, error: %s", err)
			return nil, err
		}
	} else {
		requestBody = []byte(event.Body)
	}
	requestURI := baseURLPath + event.Path
	request, err := http.NewRequestWithContext(ctx, event.HTTPMethod, requestURI, bytes.NewBuffer(requestBody))
	if err != nil {
		log.GetLogger().Errorf("failed to construct HTTP request, error: %s", err.Error())
		return nil, err
	}
	queries := request.URL.Query()
	for k, v := range event.QueryStringParameters {
		switch valueType := v.(type) {
		case string:
			queries.Set(k, v.(string))
		case []string:
			for _, param := range v.([]string) {
				queries.Add(k, param)
			}
		default:
			log.GetLogger().Warnf("invalid type in query parameters: %T", valueType)
		}
	}
	request.URL.RawQuery = queries.Encode()

	initRequestHeader(request, event)
	if isHTTP2Request(event) {
		request.Proto = "HTTP/2"
	}
	return request, nil
}

func initRequestHeader(request *http.Request, event *APIGTriggerEvent) {
	for k, v := range event.Headers {
		if k == "Transfer-Encoding" {
			continue
		}
		switch valueType := v.(type) {
		case string:
			request.Header.Set(k, v.(string))
		case []string:
			for _, param := range v.([]string) {
				request.Header.Add(k, param)
			}
		case []interface{}:
			addInterfaceArrHeader(&request.Header, k, v)
		default:
			log.GetLogger().Warnf("invalid type in headers: %T", valueType)
		}
	}

	request.Header.Set("X-APIG-Api-Id", event.RequestContext.APIID)
	request.Header.Set("X-APIG-Request-Id", event.RequestContext.RequestID)
	request.Header.Set("X-APIG-Source-Ip", event.RequestContext.SourceIP)
	request.Header.Set("X-APIG-Source-Stage", event.RequestContext.Stage)
	if host := request.Header.Get("Host"); host != "" {
		request.Host = host
	}
}

func addInterfaceArrHeader(Header *http.Header, k string, v interface{}) {
	for _, param := range v.([]interface{}) {
		switch valueType := param.(type) {
		case string:
			Header.Add(k, param.(string))
		default:
			log.GetLogger().Warnf("invalid []interface{} type in query parameters: %T", valueType)
		}
	}
}

func fromHTTPResponse(response *http.Response, body *bytes.Buffer) (*APIGTriggerResponse, error) {
	apigResponse := &APIGTriggerResponse{
		Body:            base64.StdEncoding.EncodeToString(body.Bytes()),
		StatusCode:      response.StatusCode,
		IsBase64Encoded: true,
	}
	if response.Header != nil {
		apigResponse.Headers = make(map[string][]string)
		for k, v := range response.Header {
			apigResponse.Headers[k] = v
		}
		for k, v := range response.Trailer {
			trailerKey := "Trailer:" + k
			apigResponse.Headers[trailerKey] = v
		}
	}
	return apigResponse, nil
}
