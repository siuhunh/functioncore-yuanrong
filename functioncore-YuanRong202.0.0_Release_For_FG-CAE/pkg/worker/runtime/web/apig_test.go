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
	"context"
	"testing"
)

func TestParseAPIGEvent(t *testing.T) {
	const apigContentWithStrArrayHeaders = "{\n    \"body\": \"\",\n    \"requestContext\": {\n       " +
		" \"apiId\": \"bc1dcffd-aa35-474d-897c-d53425a4c08e\",\n        " +
		"\"requestId\": \"11cdcdcf33949dc6d722640a13091c77\",\n        " +
		"\"stage\": \"RELEASE\"\n    },\n    \"queryStringParameters\": {\n        \"responseType\": \"html\",\n" +
		"        \"aaa\": [\"aaa\", \"bbb\"]\n    },\n    \"httpMethod\": \"GET\",\n    \"pathParameters\": {},\n    " +
		"\"headers\": {\n        \"accept-language\": \"zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2\",\n" +
		"        \"Accept-Encoding\": [\n            \"gzip, deflate, br\"\n        ],\n" +
		"        \"accept-encoding\": \"gzip, deflate, br\",\n        \"x-forwarded-port\": \"443\",\n        " +
		"\"x-forwarded-for\": \"103.218.216.98\",\n        " +
		"\"accept\": \"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\",\n        " +
		"\"upgrade-insecure-requests\": \"1\",\n        " +
		"\"host\": \"50eedf92-c9ad-4ac0-827e-d7c11415d4f1.apigw.cn-north-1.huaweicloud.com\",\n        " +
		"\"x-forwarded-proto\": \"https\",\n        \"pragma\": \"aaa\",\n        \"cache-control\": \"no-cache\",\n" +
		"        \"x-real-ip\": \"103.218.216.98\",\n        " +
		"\"user-agent\": \"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0\"\n    },\n" +
		"    \"path\": \"/apig-event-template\",\n    \"isBase64Encoded\": true\n}"

	httpRequest, _, err := parseAPIGEvent(context.Background(), []byte(apigContentWithStrArrayHeaders), "http://127.0.0.1:8000")
	if httpRequest == nil || err != nil {
		t.Errorf("test headers string array error")
	}
}
