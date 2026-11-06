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

package verify

import "testing"

func TestCheckServiceName(t *testing.T) {
	var cases = []struct {
		in       string // input
		expected error  // expected result
	}{
		{"service", nil},
		{"012345678901234567890123456789012", ErrServiceNameFormat},
		{"", ErrServiceNameRequired},
		{"AAAA", ErrServiceNameFormat},
	}
	for _, c := range cases {
		actual := CheckServiceName(c.in)
		if actual != c.expected {
			t.Errorf("CheckServiceName(%s) = %v; expected %v", c.in, actual, c.expected)
		}
	}
}

func TestMatchServiceName(t *testing.T) {
	var cases = []struct {
		in       string // input
		expected bool   // expected result
	}{
		{"", false},
		{"s", true},
		{"service", true},
		{"123456service", true},
		{"service-1234", false},
		{"0123456789012345", true},
		{"012345678901234567890123456789012", false},
		{"AAAA", false},
	}
	for _, c := range cases {
		actual := MatchServiceName(c.in)
		if actual != c.expected {
			t.Errorf("MatchServiceName(%s) = %v; expected %v", c.in, actual, c.expected)
		}
	}
}
