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

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/worker-manager/config"
)

func TestPreloadCache_AddFunction(t *testing.T) {
	err := preloadCache.AddFunction("tenantID", "versionName", config.PreloadInfo{})
	assert.NoError(t, err)
}

func TestPreloadCache_AddLayer(t *testing.T) {
	err := preloadCache.AddLayer("tenantID", "versionName", config.PreloadInfo{})
	assert.NoError(t, err)
}

func TestPreloadCache_RemoveFunction(t *testing.T) {
	err := preloadCache.AddFunction("tenantID", "removeFunc", config.PreloadInfo{})
	assert.NoError(t, err)
	err = preloadCache.RemoveFunction("tenantID", "removeFunc")
	assert.NoError(t, err)
	isPreload := preloadCache.CheckFunctionIsPreload("tenantID", "removeFunc")
	assert.False(t, isPreload)
}

func TestPreloadCache_RemoveLayer(t *testing.T) {
	err := preloadCache.AddLayer("tenantID", "removeLayer", config.PreloadInfo{})
	assert.NoError(t, err)
	err = preloadCache.RemoveLayer("tenantID", "remove")
	assert.NoError(t, err)
	isPreload := preloadCache.CheckLayerIsPreload("tenantID", "remove")
	assert.False(t, isPreload)
}

func TestPreloadCache_CheckFunctionIsPreload(t *testing.T) {
	isPreload := preloadCache.CheckFunctionIsPreload("tenantID", "CheckFunctionIsPreload")
	assert.False(t, isPreload)
	err := preloadCache.AddFunction("tenantID", "CheckFunctionIsPreload", config.PreloadInfo{})
	assert.NoError(t, err)
	isPreload = preloadCache.CheckFunctionIsPreload("tenantID", "CheckFunctionIsPreload")
	assert.True(t, isPreload)
}

func TestPreloadCache_CheckLayerIsPreload(t *testing.T) {
	isPreload := preloadCache.CheckLayerIsPreload("tenantID", "CheckLayerIsPreload")
	assert.False(t, isPreload)
	err := preloadCache.AddLayer("tenantID", "CheckLayerIsPreload", config.PreloadInfo{})
	assert.NoError(t, err)
	isPreload = preloadCache.CheckLayerIsPreload("tenantID", "CheckLayerIsPreload")
	assert.True(t, isPreload)
}

func TestGetPreloadCache(t *testing.T) {
	ca := GetPreloadCache()
	assert.NotNil(t, ca)
}
func TestPreloadCache_GelPreloadInfo(t *testing.T) {
	t.Run("tenantID not exists", func(t *testing.T) {
		preloadInfo := preloadCache.GelPreloadInfo("GelPreloadInfo_noTenantID", constants.PreloadFunctionType, "xxx")
		assert.Nil(t, preloadInfo)
	})
	t.Run("invalid tenantPreloadCache", func(t *testing.T) {
		preloadCache.tenantCache.Store("GelPreloadInfo_invalid", "123")
		preloadInfo := preloadCache.GelPreloadInfo("GelPreloadInfo_invalid", constants.PreloadFunctionType, "xxx")
		assert.Nil(t, preloadInfo)
	})
	t.Run("not support preload type", func(t *testing.T) {
		preloadCache.tenantCache.Store("GelPreloadInfo_not_support_type", &tenantPreloadCache{})
		preloadInfo := preloadCache.GelPreloadInfo("GelPreloadInfo_not_support_type", "xxx", "xxxx")
		assert.Nil(t, preloadInfo)
	})
	t.Run("get function preloadInfo", func(t *testing.T) {
		err := preloadCache.AddFunction("GelPreloadInfo_tenantID", "GelPreloadInfo_function", config.PreloadInfo{Sha512: "123"})
		assert.NoError(t, err)
		preloadInfo := preloadCache.GelPreloadInfo("GelPreloadInfo_tenantID", constants.PreloadFunctionType, "GelPreloadInfo_function")
		assert.NotNil(t, preloadInfo)
		assert.Equal(t, "123", preloadInfo.Sha512)
	})
	t.Run("get layer preloadInfo", func(t *testing.T) {
		err := preloadCache.AddLayer("GelPreloadInfo_tenantID", "GelPreloadInfo_layer", config.PreloadInfo{Sha512: "456"})
		assert.NoError(t, err)
		preloadInfo := preloadCache.GelPreloadInfo("GelPreloadInfo_tenantID", constants.PreloadLayerType, "GelPreloadInfo_layer")
		assert.NotNil(t, preloadInfo)
		assert.Equal(t, "456", preloadInfo.Sha512)
	})
	t.Run("get layer not exists preloadInfo", func(t *testing.T) {
		err := preloadCache.AddLayer("GelPreloadInfo_tenantID", "GelPreloadInfo_layer", config.PreloadInfo{Sha512: "456"})
		assert.NoError(t, err)
		preloadInfo := preloadCache.GelPreloadInfo("GelPreloadInfo_tenantID", constants.PreloadLayerType, "GelPreloadInfo_noExists")
		assert.Nil(t, preloadInfo)
	})
	t.Run("get function not exists preloadInfo", func(t *testing.T) {
		err := preloadCache.AddFunction("GelPreloadInfo_tenantID", "GelPreloadInfo_function", config.PreloadInfo{Sha512: "456"})
		assert.NoError(t, err)
		preloadInfo := preloadCache.GelPreloadInfo("GelPreloadInfo_tenantID", constants.PreloadFunctionType, "GelPreloadInfo_noExists")
		assert.Nil(t, preloadInfo)
	})
}

func TestPreloadCache_GelALLPreloadInfo(t *testing.T) {
	preloadCache.AddLayer("GelALLPreloadInfo_1", "GelALLPreloadInfo_layer1", config.PreloadInfo{Sha512: "1"})
	preloadCache.AddLayer("GelALLPreloadInfo_1", "GelALLPreloadInfo_layer2", config.PreloadInfo{Sha512: "2"})
	preloadCache.AddFunction("GelALLPreloadInfo_1", "GelALLPreloadInfo_function1", config.PreloadInfo{Sha512: "3"})
	preloadCache.AddFunction("GelALLPreloadInfo_1", "GelALLPreloadInfo_function2", config.PreloadInfo{Sha512: "4"})

	preloadCache.AddLayer("GelALLPreloadInfo_2", "GelALLPreloadInfo_layer1", config.PreloadInfo{Sha512: "1"})
	preloadCache.AddLayer("GelALLPreloadInfo_2", "GelALLPreloadInfo_layer2", config.PreloadInfo{Sha512: "2"})
	preloadCache.AddLayer("GelALLPreloadInfo_2", "GelALLPreloadInfo_layer3", config.PreloadInfo{Sha512: "2"})
	preloadCache.AddFunction("GelALLPreloadInfo_2", "GelALLPreloadInfo_function1", config.PreloadInfo{Sha512: "3"})
	preloadCache.AddFunction("GelALLPreloadInfo_2", "GelALLPreloadInfo_function2", config.PreloadInfo{Sha512: "4"})
	preloadCache.AddFunction("GelALLPreloadInfo_2", "GelALLPreloadInfo_function3", config.PreloadInfo{Sha512: "4"})

	tenantFunctionPreloadMap, tenantLayerPreloadMap := preloadCache.GelALLPreloadInfo()
	assert.Equal(t, 2, len(tenantFunctionPreloadMap["GelALLPreloadInfo_1"]))
	assert.Equal(t, 2, len(tenantLayerPreloadMap["GelALLPreloadInfo_1"]))
	assert.Equal(t, 3, len(tenantFunctionPreloadMap["GelALLPreloadInfo_2"]))
	assert.Equal(t, 3, len(tenantLayerPreloadMap["GelALLPreloadInfo_2"]))
}
