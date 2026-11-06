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

package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

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
		{"case1", a, "busitena-funcName-version-1772412309"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CrName(tt.args.business, tt.args.tenant, tt.args.funcName, tt.args.version); got != tt.want {
				t.Errorf("CrName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCrNameWithSNServiceID(t *testing.T) {
	type args struct {
		business string
		tenant   string
		funcName string
		version  string
	}
	var a args
	a.business = "business"
	a.funcName = "0-0123456789112345-testsnservice0012345678901234567"
	a.tenant = "tenant"
	a.version = "version"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "busitena-0-0123456789112345-testsnservice0012345678901234567-version-1146293160"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CrName(tt.args.business, tt.args.tenant, tt.args.funcName, tt.args.version); got != tt.want {
				t.Errorf("CrName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCrNameByKey(t *testing.T) {
	type args struct {
		key string
	}
	var a args
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "---3303332998"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CrNameByKey(tt.args.key); got != tt.want {
				t.Errorf("CrNameByKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCrNameByUrn(t *testing.T) {
	type args struct {
		args []string
	}
	var a args
	a.args = make([]string, 4)
	a.args[0] = "0"
	a.args[1] = "1"
	a.args[2] = "2"
	a.args[3] = "3"
	var b args
	b.args = []string{}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "01-2-3-4202833182"},
		{"case2", b, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CrNameByURN(tt.args.args...); got != tt.want {
				t.Errorf("CrNameByURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFunctionKeyByCr(t *testing.T) {
	type args struct {
		funcSvc *faasfunction.FunctionService
	}
	var a args
	a.funcSvc = &faasfunction.FunctionService{}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "/sn/functions/business//tenant//function//version/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FunctionKeyByCr(tt.args.funcSvc); got != tt.want {
				t.Errorf("FunctionKeyByCr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFunctionInfoByKey(t *testing.T) {
	type args struct {
		key string
	}
	var a args
	var b FunctionInfo
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, b.FuncName},
		{
			name: "case2",
			args: args{
				key: "/sn/functions/business/iot/tenant/j6db015e6ed74a1cab8644bc78fb22b0/function/func001/version/$latest",
			},
			want: "func001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFunctionInfoByKey(tt.args.key); !reflect.DeepEqual(got.FuncName, tt.want) {
				t.Errorf("GetFunctionInfoByKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVersionRestore(t *testing.T) {
	type args struct {
		v string
	}
	var a args
	a.v = "l.s.t"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "$latest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := VersionRestore(tt.args.v); got != tt.want {
				t.Errorf("VersionRestore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorkerKeyByCr(t *testing.T) {
	type args struct {
		funcSvc  *faasfunction.FunctionService
		instance *v1alpha1.WorkerInstance
	}
	var a args
	a.funcSvc = &faasfunction.FunctionService{}
	a.instance = &v1alpha1.WorkerInstance{}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "/sn/workers/business//tenant//function//version//defaultaz/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WorkerKeyByCr(tt.args.funcSvc, tt.args.instance.InstanceID); got != tt.want {
				t.Errorf("WorkerKeyByCr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAcquireBrief(t *testing.T) {
	type args struct {
		business string
		tenant   string
	}
	var a args
	a.tenant = "0"
	a.business = "yrk"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "yrk0"},
		{
			name: "case2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := acquireBrief(tt.args.business, tt.args.tenant); got != tt.want {
				t.Errorf("acquireBrief() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenFunctionCRStr(t *testing.T) {
	type args struct {
		business string
		tenant   string
		funcName string
		version  string
	}
	var a args
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "---"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genFunctionCRStr(tt.args.business, tt.args.tenant, tt.args.funcName, tt.args.version); got != tt.want {
				t.Errorf("genFunctionCRStr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFunctionUrn(t *testing.T) {
	type args struct {
		business string
		tenant   string
		funcName string
	}
	var a args
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "sn:cn:::function::$latest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFunctionURN(tt.args.business, tt.args.tenant, tt.args.funcName); got != tt.want {
				t.Errorf("GetFunctionURN() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestGetFuncNameByKey(t *testing.T) {
	type args struct {
		funcKey string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case2",
			args: args{
				funcKey: "/sn/functions/business/iot/tenant/j6db015e6ed74a1cab8644bc78fb22b0/function/func001/version/$latest",
			},
			want: "func001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFuncNameByKey(tt.args.funcKey); got != tt.want {
				t.Errorf("GetFuncNameByKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetWorkerKeyByInstance(t *testing.T) {
	type args struct {
		instance v1alpha1.WorkerInstance
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				instance: v1alpha1.WorkerInstance{
					InstanceID: "mock-instanceID",
					BusinessID: "mock-businessID",
					TenantID:   "mock-tenantID",
					FuncName:   "mock-funcName",
					Version:    "mock-version",
				},
			},
			want: "/sn/workers/business/mock-businessID/tenant/mock-tenantID/function/mock-funcName/version/mock-version/defaultaz/mock-instanceID",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetWorkerKeyByInstance(tt.args.instance); got != tt.want {
				t.Errorf("GetWorkerKeyByInstance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerateInitInstanceKey(t *testing.T) {
	tests := []struct {
		name     string
		instance v1alpha1.WorkerInstance

		expected string
	}{
		{
			name: "case01",
			instance: v1alpha1.WorkerInstance{
				InstanceID: "mock-instanceID",
				BusinessID: "mock-businessID",
				TenantID:   "mock-tenantID",
				FuncName:   "mock-funcName",
				Version:    "mock-version",
			},
			expected: "/sn/init/workers/cluster//business/mock-businessID/tenant/mock-tenantID/function/mock-funcName/version/mock-version/defaultaz/mock-instanceID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GenerateInitInstanceKey(tt.instance), tt.name)
		})
	}
}

func TestGetETCDFuncKeyByPod1(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	testPod001 := &corev1.Pod{}
	testPod001.Labels = map[string]string{
		LabelFuncNamePrefix:  "0",
		LabelServiceID:       "default",
		LabelShortFuncName:   "puresleep-java-10ms",
		LabelBusinessID:      "yrk",
		LabelTenantID:        "fb6a00ff7ae54a5fbb8ff855d0841d00",
		LabelFunctionVersion: "latest",
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				pod: testPod001,
			},
			want: "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetETCDFuncKeyByPod(tt.args.pod); got != tt.want {
				t.Errorf("GetETCDFuncKeyByPod() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTenantIDFromURN(t *testing.T) {
	type args struct {
		funcUrn string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				funcUrn: "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms:latest",
			},
			want: "fb6a00ff7ae54a5fbb8ff855d0841d00",
		},
		{
			name: "test002",
			args: args{
				funcUrn: "",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTenantIDFromURN(tt.args.funcUrn); got != tt.want {
				t.Errorf("GetTenantIDFromURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFunctionInfoByURN(t *testing.T) {
	type args struct {
		urn string
	}
	tests := []struct {
		name string
		args args
		want FunctionInfo
	}{
		{
			name: "test001",
			args: args{
				urn: "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms:latest",
			},
			want: FunctionInfo{
				Business: "yrk",
				Tenant:   "fb6a00ff7ae54a5fbb8ff855d0841d00",
				FuncName: "0@default@puresleep-java-10ms",
				Version:  "latest",
			},
		},
		{
			name: "test001",
			args: args{
				urn: "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms",
			},
			want: FunctionInfo{
				Business: "yrk",
				Tenant:   "fb6a00ff7ae54a5fbb8ff855d0841d00",
				FuncName: "0@default@puresleep-java-10ms",
			},
		},
		{
			name: "test001",
			args: args{
				urn: "",
			},
			want: FunctionInfo{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFunctionInfoByURN(tt.args.urn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFunctionInfoByURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFuncKeyFromURN(t *testing.T) {
	urn := "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest"
	funky := GetFuncKeyFromURN(urn)
	assert.Equal(t, funky, "5cb29fa73b564c90b9878a6bf8e10d78@default@test_java_log@latest")

	urn = "sn:cn:yrk"
	funky = GetFuncKeyFromURN(urn)
	assert.Equal(t, funky, "")

	urn = "sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0:latest"
	funky = GetFuncKeyFromURN(urn)
	assert.Equal(t, funky, "")
}

func TestGetFunctionTaskWorkerNodeNameKeyByInstance(t *testing.T) {
	type args struct {
		instance v1alpha1.WorkerInstance
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				instance: v1alpha1.WorkerInstance{},
			},
			want: "/sn/workers/business//tenant//function/function-task/version//defaultaz/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFunctionTaskWorkerNodeNameKeyByInstance(tt.args.instance); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFunctionTaskWorkerNodeNameKeyByInstance = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSNStateWorkerPath(t *testing.T) {
	type args struct {
		instance v1alpha1.WorkerInstance
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				instance: v1alpha1.WorkerInstance{},
			},
			want: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSNStateWorkerPath(tt.args.instance); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFunctionTaskWorkerNodeNameKeyByInstance = %v, want %v", got, tt.want)
			}
		})
	}
}
