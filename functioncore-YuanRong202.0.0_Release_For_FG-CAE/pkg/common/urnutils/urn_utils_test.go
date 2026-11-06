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

package urnutils

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestProductUrn_ParseFrom(t *testing.T) {
	absURN := BaseURN{
		"absPrefix",
		"absZone",
		"absBusinessID",
		"absTenantID",
		"absProductID",
		"absName",
		"latest",
	}
	absURNStr := "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:latest"
	type args struct {
		urn string
	}
	tests := []struct {
		name   string
		fields BaseURN
		args   args
		want   BaseURN
	}{
		{
			name: "normal test",
			args: args{
				absURNStr,
			},
			want: absURN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BaseURN{}
			if _ = p.ParseFrom(tt.args.urn); !reflect.DeepEqual(*p, tt.want) {
				t.Errorf("ParseFrom() p = %v, want %v", *p, tt.want)
			}
		})
	}
}

func TestProductUrn_String(t *testing.T) {
	tests := []struct {
		name   string
		fields BaseURN
		want   string
	}{
		{
			"stringify with version",
			BaseURN{
				"absPrefix",
				"absZone",
				"absBusinessID",
				"absTenantID",
				"absProductID",
				"absName",
				"latest",
			},
			"absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:latest",
		},
		{
			"stringify without version",
			BaseURN{
				ProductID:  "absPrefix",
				RegionID:   "absZone",
				BusinessID: "absBusinessID",
				TenantID:   "absTenantID",
				TypeSign:   "absProductID",
				Name:       "absName",
			},
			"absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BaseURN{
				ProductID:  tt.fields.ProductID,
				RegionID:   tt.fields.RegionID,
				BusinessID: tt.fields.BusinessID,
				TenantID:   tt.fields.TenantID,
				TypeSign:   tt.fields.TypeSign,
				Name:       tt.fields.Name,
				Version:    tt.fields.Version,
			}
			if got := p.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProductUrn_StringWithoutVersion(t *testing.T) {
	tests := []struct {
		name   string
		fields BaseURN
		want   string
	}{
		{
			"stringify without version",
			BaseURN{
				"absPrefix",
				"absZone",
				"absBusinessID",
				"absTenantID",
				"absProductID",
				"absName",
				"latest",
			},
			"absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &BaseURN{
				ProductID:  tt.fields.ProductID,
				RegionID:   tt.fields.RegionID,
				BusinessID: tt.fields.BusinessID,
				TenantID:   tt.fields.TenantID,
				TypeSign:   tt.fields.TypeSign,
				Name:       tt.fields.Name,
				Version:    tt.fields.Version,
			}
			if got := p.StringWithoutVersion(); got != tt.want {
				t.Errorf("StringWithoutVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnonymize(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"0", anonymization},
		{"123", anonymization},
		{"123456", anonymization},
		{"1234567", "123*567"},
		{"12345678901234546", "123*546"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, Anonymize(tt.input))
	}
}

func TestAnonymizeTenantURN(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName", "absPrefix:absZone:absBusinessID:abs*tID:absProductID:absName"},
		{"absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:latest", "absPrefix:absZone:absBusinessID:abs*tID:absProductID:absName:latest"},
		{"a:b:c", "a:b:c"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, AnonymizeTenantURN(tt.input))
	}
}

func TestBaseURN_Valid(t *testing.T) {
	Separator = "@"
	tests := []struct {
		name              string
		inputFunctionName string
		expectErr         error
	}{
		{
			name:              "normal test",
			inputFunctionName: "0@servic._-e@function._-e",
		},
		{
			name:              "failed to parsing a function name",
			inputFunctionName: "0@service",
			expectErr:         errors.New("parsing a function name error"),
		},
		{
			name:              "failed to match reg",
			inputFunctionName: "0@0service@0function",
			expectErr: fmt.Errorf("failed to match reg%s",
				funcNameRegexp),
		},
		{
			name:              "incorrect function name length",
			inputFunctionName: "0@service@A23456789012345678901234567890123456789012345678901234567890123412345678901234567890123456789012345678901234567890123456789012341",
			expectErr: fmt.Errorf("serviceID or functionName's len is out of range %d",
				defaultFunctionMaxLen),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urn := BaseURN{
				Name: tt.inputFunctionName,
			}
			actual := urn.Valid()
			assert.Equal(t, tt.expectErr, actual)
		})
	}
	Separator = "-"
}

func TestGetFuncInfoWithVersion(t *testing.T) {
	absURN := BaseURN{
		"absPrefix",
		"absZone",
		"absBusinessID",
		"absTenantID",
		"absProductID",
		"absName",
		"latest",
	}
	tests := []struct {
		name    string
		urn     string
		want    BaseURN
		wantErr error
	}{
		{
			name: "normal test",
			urn:  "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:latest",
			want: absURN,
		},
		{
			name:    "normal test",
			urn:     "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:latest:1",
			want:    absURN,
			wantErr: errors.New("parsing an URN error"),
		},
		{
			name:    "no version",
			urn:     "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:",
			want:    absURN,
			wantErr: errors.New("incorrect URN length, no version"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualFunctionInfo, err := GetFuncInfoWithVersion(tt.urn)
			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr, err)
			} else {
				assert.Equal(t, tt.want, actualFunctionInfo)
			}
		})
	}
}

func TestGetBillingFunctionURN(t *testing.T) {
	absURN := BaseURN{
		"absPrefix",
		"absZone",
		"absBusinessID",
		"absTenantID",
		"absProductID",
		"0-service-functionName",
		"latest",
	}
	assert.Equal(t, "absTenantID@service@functionName@latest@100", GetBillingFunctionURN(absURN, "100"))
}

func TestParseAliasURN(t *testing.T) {
	urn := "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:!alias"
	assert.Equal(t, "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:alias",
		ParseAliasURN(urn))
	urn = "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName"
	assert.Equal(t, urn, ParseAliasURN(urn))
}

func TestBaseURN_GetAlias(t *testing.T) {
	tests := []struct {
		name string
		urn  BaseURN
		want string
	}{
		{
			name: "normal test",
			urn: BaseURN{
				Version: DefaultURNVersion,
			},
			want: "",
		},
		{
			name: "version",
			urn: BaseURN{
				Version: "1",
			},
			want: "",
		},
		{
			name: "alias",
			urn: BaseURN{
				Version: "alias",
			},
			want: "alias",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.urn.GetAlias())
		})
	}
}

func TestBaseURN_GetAliasForFuncBranch(t *testing.T) {
	tests := []struct {
		name string
		urn  BaseURN
		want string
	}{
		{
			name: "normal test",
			urn: BaseURN{
				Version: "!alias",
			},
			want: "alias",
		},
		{
			name: "not HasPrefix(!)",
			urn: BaseURN{
				Version: "alias",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.urn.GetAliasForFuncBranch())
		})
	}
}

func TestAnonymizeTenantKey(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"absTenantID/absFunctionName/latest", "abs*tID/absFunctionName/latest"},
		{"", ""},
		{"abstID/absFunctionName/latest", "*/absFunctionName/latest"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, AnonymizeTenantKey(tt.input))
	}
}

func TestAnonymizeTenantURNSlice(t *testing.T) {
	assert.Equal(t, []string{"absPrefix:absZone:absBusinessID:abs*tID:absProductID:absName"},
		AnonymizeTenantURNSlice([]string{"absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName"}))
}

func TestGetFunctionNameAndServiceName(t *testing.T) {
	tests := []struct {
		name              string
		inputFunctionName string
		wantServiceName   string
		wantFunctionName  string
		err               error
	}{
		{
			name:              "normal test",
			inputFunctionName: "0-service-function",
			wantServiceName:   "service",
			wantFunctionName:  "function",
		},
		{
			name:              "incorrect function name length",
			inputFunctionName: "0-service",
			err:               errors.New("parsing a function name error"),
		},
		{
			name:              "incorrect function name",
			inputFunctionName: "1-service",
			err:               errors.New("parsing a function name error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualServiceName, actualFunctionName, err := GetFunctionNameAndServiceName(tt.inputFunctionName)
			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			} else {
				assert.Equal(t, tt.wantServiceName, actualServiceName)
				assert.Equal(t, tt.wantFunctionName, actualFunctionName)
			}
		})
	}
}

func TestGetFuncKey(t *testing.T) {
	convey.Convey("TestGetFuncKey", t, func() {
		functionKey := GetFuncKey("tenantID", "funcName", "latest")
		convey.So(functionKey, convey.ShouldEqual, "tenantID/funcName/latest")
		tenantID, funcName, funcVersion := ParseFuncKey(functionKey)
		convey.So(tenantID, convey.ShouldEqual, "tenantID")
		convey.So(funcName, convey.ShouldEqual, "funcName")
		convey.So(funcVersion, convey.ShouldEqual, "latest")
	})
}

func TestGetFuncKeyWithResource(t *testing.T) {
	convey.Convey("TestGetFuncKeyWithResource", t, func() {
		funcKey := "tenantID/funcName/latest"
		funcKeyWithRes := GetFuncKeyWithResource(funcKey, "300", "128")
		convey.So(funcKeyWithRes, convey.ShouldEqual, "tenantID/funcName/latest/300-128")
		parsedFuncKey := GetFuncKeyFromFuncKeyWithResource(funcKeyWithRes)
		convey.So(parsedFuncKey, convey.ShouldEqual, funcKey)
	})

}

func TestGetFuncKeyAndResFromFuncKeyWithResource(t *testing.T) {
	convey.Convey("TestGetFuncKeyWithResource", t, func() {
		funcKeyWithRes := "tenantID/funcName/latest/300-128"
		newFuncKey, cpu, mem := GetFuncKeyAndResFromFuncKeyWithResource(funcKeyWithRes)
		convey.So(newFuncKey, convey.ShouldEqual, "tenantID/funcName/latest")
		convey.So(cpu, convey.ShouldEqual, "300")
		convey.So(mem, convey.ShouldEqual, "128")
	})
}

func TestCrName(t *testing.T) {
	type args struct {
		business string
		tenant   string
		funcName string
		version  string
	}
	var a args
	a.business = "business"
	a.funcName = "funcName"
	a.tenant = "tenant"
	a.version = "version"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "yrktena-funcName-version-1551981861"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CrName(tt.args.tenant, tt.args.funcName, tt.args.version); got != tt.want {
				t.Errorf("CrName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractFuncName(t *testing.T) {
	convey.Convey("test ExtractFuncName", t, func() {
		convey.Convey("case1: valid funcName", func() {
			funcName := ExtractFuncName("0@default@funcA")
			convey.So(funcName, convey.ShouldEqual, "funcA")
		})
		convey.Convey("case1: invalid funcName", func() {
			funcName := ExtractFuncName("aaa")
			convey.So(funcName, convey.ShouldEqual, "aaa")
		})
	})
}

func TestFuncKeyWithResToShort(t *testing.T) {
	convey.Convey("test FuncKeyWithResToShort", t, func() {
		convey.Convey("case1: valid funcKeyWithRes", func() {
			res := FuncKeyWithResToShort("123456789/0@default@funcA/latest/300-128")
			convey.So(res, convey.ShouldEqual, "123*789/funcA/latest/300-128")
		})
		convey.Convey("case2: invalid funcKeyWithRes", func() {
			res := FuncKeyWithResToShort("aaa")
			convey.So(res, convey.ShouldEqual, "aaa")
		})
	})
}

func TestFuncKeyToShort(t *testing.T) {
	convey.Convey("test FuncKeyToShort", t, func() {
		convey.Convey("case1: valid funcKey", func() {
			res := FuncKeyToShort("123456789/0@default@funcA/latest")
			convey.So(res, convey.ShouldEqual, "123*789/funcA/latest")
		})
		convey.Convey("case2: invalid funcKey", func() {
			res := FuncKeyToShort("aaa")
			convey.So(res, convey.ShouldEqual, "aaa")
		})
	})
}

func TestExtractPodName(t *testing.T) {
	convey.Convey("test ExtractPodName", t, func() {
		convey.Convey("case1: valid podName", func() {
			podName := ExtractPodName("defaultaz-#-pod-xxx")
			convey.So(podName, convey.ShouldEqual, "pod-xxx")
		})
		convey.Convey("case1: invalid podName", func() {
			podName := ExtractPodName("aaa")
			convey.So(podName, convey.ShouldEqual, "aaa")
		})
	})
}

func TestFuncURNtoShort(t *testing.T) {
	convey.Convey("test FuncURNtoShort", t, func() {
		convey.Convey("case1: valid funcURN", func() {
			res := FuncURNtoShort("sn:cn:yrk:123456789:function:0@default@funcA:latest")
			convey.So(res, convey.ShouldEqual, "123*789/funcA/latest")
		})
		convey.Convey("case2: invalid funcURN", func() {
			res := FuncURNtoShort("aaa")
			convey.So(res, convey.ShouldEqual, "aaa")
		})
	})
}
