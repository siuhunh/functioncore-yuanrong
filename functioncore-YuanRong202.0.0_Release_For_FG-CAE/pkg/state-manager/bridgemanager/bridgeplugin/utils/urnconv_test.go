package utils

import "testing"

func TestGetComplexFuncNameFromURN(t *testing.T) {
	type args struct {
		functionURN string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal test",
			args: args{
				"sn:cn:yrk:ID1:function:0@default@rds",
			},
			want: "0@default@rds",
		},
		{
			name: "invalid function urn test",
			args: args{
				"cn:yrk:ID1:function:0@default@rds",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetComplexFuncNameFromURN(tt.args.functionURN); got != tt.want {
				t.Errorf("GetComplexFuncNameFromURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFuncURNByWorkersPath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal test",
			args: args{
				"/sn/workers/business/yrk/tenant/ID/function/0@base@rds/version/latest/defaultaz/defaultaz-#-${podName}",
			},
			want: "sn:cn:yrk:ID:function:0@base@rds",
		},
		{
			name: "invalid workers path test",
			args: args{
				"/business/yrk/tenant/ID/function/0@base@rds/version/latest/defaultaz/defaultaz-#-${podName}",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFuncURNByWorkersPath(tt.args.path); got != tt.want {
				t.Errorf("GetFuncURNByWorkersPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFuncNameByWorkersPath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name      string
		args      args
		wantValue string
	}{
		{
			name: "normal test",
			args: args{
				"/sn/workers/business/yrk/tenant/ID/function/0@base@rds/version/latest/defaultaz/defaultaz-#-${podName}",
			},
			wantValue: "rds",
		},
		{
			name: "invalid workers path test",
			args: args{
				"/business/yrk/tenant/ID/function/rds/version/latest/defaultaz/defaultaz-#-${podName}",
			},
			wantValue: "",
		},
		{
			name: "invalid function name",
			args: args{
				"/sn/workers/business/yrk/tenant/ID/function/0@/version/latest/defaultaz/defaultaz-#-${podName}",
			},
			wantValue: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFuncNameByWorkersPath(tt.args.path); got != tt.wantValue {
				t.Errorf("GetFuncNameByWorkersPath() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestGetPodNameByPath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal test",
			args: args{
				"/sn/workers/business/yrk/tenant/ID/function/0@base@rds/version/latest/defaultaz/defaultaz-#-pool0-xxxx",
			},
			want: "pool0-xxxx",
		},
		{
			name: "failed test",
			args: args{
				"/sn/workers/business/yrk/tenant/ID/function/0@base@rds/version/latest/defaultaz/defaultaz",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPodNameByPath(tt.args.path); got != tt.want {
				t.Errorf("GetPodNameByPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetStateIdByPath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal test",
			args: args{
				"/sn/bridgemanager/business/yrk/tenant/0/function/0@base@CURDUserFunc/version/latest/defaultaz/stateID1",
			},
			want: "stateID1",
		},
		{
			name: "failed test",
			args: args{
				"/sn/bridgemanager/business/yrk/tenant/0/function/0@base@CURDUserFunc/version/latest/defaultaz",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStateIDByPath(tt.args.path); got != tt.want {
				t.Errorf("GetStateIDByPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTenantIdFromURN(t *testing.T) {
	type args struct {
		functionURN string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal test",
			args: args{
				"sn:cn:yrk:ID1:function:0@base@rds",
			},
			want: "ID1",
		},
		{
			name: "failed test",
			args: args{
				"sn:cn:yrk:ID1:function",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTenantIDFromURN(tt.args.functionURN); got != tt.want {
				t.Errorf("GetTenantIDFromURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewComplexFuncName(t *testing.T) {
	type args struct {
		serviceID string
		funcName  string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "new complex funcName",
			args: args{
				serviceID: "0@base",
				funcName:  "rdstest",
			},
			want: "0@base@rdstest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewComplexFuncName(tt.args.serviceID, tt.args.funcName); got != tt.want {
				t.Errorf("NewComplexFuncName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewFunctionURN(t *testing.T) {
	type args struct {
		tenantID  string
		serviceID string
		funcName  string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "new functionURN",
			args: args{
				tenantID:  "tenant01",
				serviceID: "0@base",
				funcName:  "rdstest",
			},
			want: "sn:cn:yrk:tenant01:function:0@base@rdstest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewFunctionURN(tt.args.tenantID, tt.args.serviceID, tt.args.funcName); got != tt.want {
				t.Errorf("NewFunctionURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFunctionInfoByURN(t *testing.T) {
	type args struct {
		functionURN string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal test",
			args: args{
				"sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@testservicebridgescale022B",
			},
			want: "7e1ad6a6-cc5c-44fa-bd54-25873f72a86a",
		},
		{
			name: "invalid function urn test",
			args: args{
				"cn:yrk:ID1:function:0@default@rds",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _, _ := GetFunctionInfoByURN(tt.args.functionURN); got != tt.want {
				t.Errorf("GetFunctionInfoByURN() = %v, want %v", got, tt.want)
			}
		})
	}
}
