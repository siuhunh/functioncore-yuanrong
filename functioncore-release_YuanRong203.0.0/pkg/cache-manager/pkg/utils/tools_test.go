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
	"testing"

	"functioncore/pkg/cache-manager/config"
)

func TestGenerateChecksum(t *testing.T) {
	type args struct {
		buffer []byte
		info   *config.FunctionMetaInfo
	}
	var a args
	a.buffer = []byte{}
	info := &config.FunctionMetaInfo{
		FuncMetaData:     config.FuncMetaData{},
		S3MetaData:       config.S3MetaData{},
		EnvMetaData:      config.EnvMetaData{},
		ResourceMetaData: config.ResourceMetaData{},
		InstanceMetaData: config.InstanceMetaData{},
		ExtendedMetaData: config.ExtendedMetaData{},
	}
	a.info = info
	a.info.FuncMetaData.CodeSha512 = ""
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateChecksum(tt.args.buffer, tt.args.info); got != tt.want {
				t.Errorf("GenerateChecksum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerateMD5(t *testing.T) {
	type args struct {
		str string
	}
	var a args
	a.str = "a"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "0cc175b9c0f1b6a831c399e269772661"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateMD5(tt.args.str); got != tt.want {
				t.Errorf("GenerateMD5() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseObjectID(t *testing.T) {
	tests := []struct {
		name    string
		objID   string
		isLayer bool
		want    string
	}{
		{"case1", "abcde", false, "abcde"},
		{"case2", "/a/b/c/d/e.zip", false, "/a/b/c/d"},
		{"case3", "/aaa.txt", false, ""},
		{"case3", "/aaa.txt", true, "/aaa.txt"},
		{"case3", "/a/b/c/d/e.zip", true, "/a/b/c/d/e.zip"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseObjectID(tt.objID, tt.isLayer); got != tt.want {
				t.Errorf("ParseObjectID() = %v, want %v", got, tt.want)
			}
		})
	}
}
