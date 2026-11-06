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

package httputils

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateWsPath(t *testing.T) {
	t.Run("parameters is string", func(t *testing.T) {
		queryURL := &url.URL{}
		queries := queryURL.Query()
		parameters := map[string]interface{}{
			"a": "1",
			"b": []string{"1", "2"},
			"c": []interface{}{"1", "2"},
			"d": []interface{}{1, 2},
		}
		SetQuery(&queries, parameters)
		assert.Equal(t, "a=1&b=1&b=2&c=1&c=2", queries.Encode())
	})
	t.Run("parameters is empty", func(t *testing.T) {
		queryURL := &url.URL{}
		queries := queryURL.Query()
		SetQuery(&queries, map[string]interface{}{})
		assert.Equal(t, "", queries.Encode())
	})
	t.Run("parameters with interface array", func(t *testing.T) {
		queryURL := &url.URL{}
		queries := queryURL.Query()
		parameters := map[string]interface{}{
			"c": []interface{}{"1", "2"},
		}
		SetQuery(&queries, parameters)
		assert.Equal(t, "c=1&c=2", queries.Encode())
	})
}
