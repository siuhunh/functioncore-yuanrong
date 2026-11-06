/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package remotescheduler

import (
	"sync"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func Test_updateSyncMapByMap(t *testing.T) {
	originData := map[string]string{
		"a": "aa",
		"b": "bb",
		"c": "cc",
	}

	tests := []struct {
		name   string
		source map[string]string
	}{
		{
			name:   "nil source",
			source: nil,
		},
		{
			name: "case 1",
			source: map[string]string{
				"a": "ac",
				"c": "cc",
				"d": "dd",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := sync.Map{}
			for k, v := range originData {
				target.Store(k, v)
			}

			updateSyncMapByMap(&target, tt.source)

			expected := sync.Map{}
			for k, v := range tt.source {
				expected.Store(k, v)
			}

			target.Range(func(key, value any) bool {
				val, loaded := expected.Load(key)
				assert.True(t, loaded, tt.name)
				assert.Equal(t, value, val, tt.name)
				return true
			})
			expected.Range(func(key, value any) bool {
				val, loaded := target.Load(key)
				assert.True(t, loaded, tt.name)
				assert.Equal(t, value, val, tt.name)
				return true
			})
		})
	}
}

func TestUpdateTenantWhitelist(t *testing.T) {
	convey.Convey("UpdateTenantWhitelist", t, func() {
		wl := &Whitelist{}
		wl.UpdateTenantWhitelist(map[string]string{"tenantID": "faaS-scheduler-01"})
		wl.tenantWhitelist.Store("tenantID1", 123)
		res := wl.Query("funcKey", "tenantID")
		convey.So(res, convey.ShouldEqual, "faaS-scheduler-01")
		res = wl.Query("funcKey", "tenantID1")
		convey.So(res, convey.ShouldEqual, "")
	})
}

func TestUpdateFuncWhitelist(t *testing.T) {
	convey.Convey("UpdateFuncWhitelist ok", t, func() {
		wl := &Whitelist{}
		wl.UpdateFuncWhitelist(map[string]string{"tenantID/funcServiceName/version": "faaS-scheduler-01"})
		wl.funcWhitelist.Store("funcKey1", 123)
		res := wl.Query("tenantID/funcServiceName/version", "xxx")
		convey.So(res, convey.ShouldEqual, "faaS-scheduler-01")
		res = wl.Query("funcKey1", "tenantIDxx")
		convey.So(res, convey.ShouldEqual, "")
	})
}
