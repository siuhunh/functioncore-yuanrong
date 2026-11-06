/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package kvstorage

import (
	"reflect"
	"regexp"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/test/utils"
)

func TestInMemoryStorage_Scan(t *testing.T) {
	type fields struct {
		values map[string]string
		mutex  sync.RWMutex
	}
	type args struct {
		pattern string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			fields{
				values: map[string]string{
					"test": "test",
				},
			},
			args{
				pattern: "test",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(regexp.MustCompile, func(str string) *regexp.Regexp {
						return &regexp.Regexp{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&regexp.Regexp{}), "MatchString",
						func(_ *regexp.Regexp, s string) bool {
							return true
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &InMemoryStorage{
				values: tt.fields.values,
				mutex:  tt.fields.mutex,
			}

			patches := tt.patchesFunc()
			got, _ := s.Scan(tt.args.pattern)
			if (got != nil) != tt.want {
				t.Errorf("Scan() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}
