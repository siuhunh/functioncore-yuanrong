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

package priority

import (
	"testing"
)

func TestNewPriorityAlgorithm(t *testing.T) {
	type args struct {
		name string
		conf interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// test name not exist
		{
			"case1",
			args{
				name: "test",
			},
			true,
		},
		// test success
		{
			"case2",
			args{
				name: naiveAverageScorerName,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPriorityAlgorithm(tt.args.name, tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPriorityAlgorithm() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
