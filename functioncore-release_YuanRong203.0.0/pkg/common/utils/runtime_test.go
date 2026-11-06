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

// Package utils for common functions
package utils

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestGetRuntimeLanguage(t *testing.T) {
	t.Run("agc agcnodejs20.15", func(t *testing.T) {
		language := GetRuntimeLanguage("agcnodejs20.15")
		assert.Equal(t, "nodejs20.15", language)
	})
	t.Run("agc agccloudobjectnodejs20.15", func(t *testing.T) {
		language := GetRuntimeLanguage("agccloudobjectnodejs20.15")
		assert.Equal(t, "nodejs20.15", language)
	})
	t.Run("agc runtimeAgcJava8", func(t *testing.T) {
		language := GetRuntimeLanguage(runtimeAgcJava8)
		assert.Equal(t, "java8", language)
	})
	t.Run("agc runtimeAgcPython312", func(t *testing.T) {
		language := GetRuntimeLanguage(runtimeAgcPython312)
		assert.Equal(t, "python3.12", language)
	})
	t.Run("python3.9", func(t *testing.T) {
		language := GetRuntimeLanguage("python3.9")
		assert.Equal(t, "python3.9", language)
	})
}
