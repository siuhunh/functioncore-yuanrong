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

// Package httputils provides http utils
package httputils

import (
	"net/url"

	"functioncore/pkg/common/logger/log"
)

// SetQuery -
func SetQuery(queries *url.Values, parameters map[string]interface{}) {
	for k, v := range parameters {
		switch valueType := v.(type) {
		case string:
			queries.Set(k, v.(string))
		case []string:
			for _, param := range v.([]string) {
				queries.Add(k, param)
			}
		case []interface{}:
			addInterfaceArrQuery(queries, k, v)
		default:
			log.GetLogger().Warnf("invalid type in query parameters: %T", valueType)
		}
	}
}

func addInterfaceArrQuery(queries *url.Values, k string, v interface{}) {
	for _, param := range v.([]interface{}) {
		switch valueType := param.(type) {
		case string:
			queries.Add(k, param.(string))
		default:
			log.GetLogger().Warnf("invalid []interface{} type in query parameters: %T", valueType)
		}
	}
}
