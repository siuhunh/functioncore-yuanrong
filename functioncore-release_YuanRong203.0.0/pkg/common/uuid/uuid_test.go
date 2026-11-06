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

// Package uuid for common functions
package uuid

import (
	"testing"
)

func TestNew(t *testing.T) {
	m := make(map[UUID]bool)
	for x := 1; x < 32; x++ {
		s := New()
		if m[s] {
			t.Errorf("New returned duplicated UUID %s", s)
		}
		m[s] = true
	}
}

func TestSHA1(t *testing.T) {
	uuid := NewSHA1(NameSpaceURL, []byte("python.org")).String()
	want := "7af94e2b-4dd9-50f0-9c9a-8a48519bdef0"
	if uuid != want {
		t.Errorf("SHA1: got %q expected %q", uuid, want)
	}
}

func Test_parseUUID(t *testing.T) {
	type args struct {
		uuidStr string
	}
	validUUID := "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
	invalidFormatUUID := "6ba7b8119dad11d180b400c04fd430c8"
	illegalCharUUID := "6ba7b811-9dad-11d1-80b4-00c04fd430cG"
	shortUUID := "6ba7b811-9dad-11d1-80b4"
	longUUID := "6ba7b811-9dad-11d1-80b4-00c04fd430c8-extra"
	emptyUUID := ""
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "Test_parseUUID_with_validUUID",
			args: args{
				uuidStr: validUUID,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Test_parseUUID_with_invalidFormatUUID",
			args: args{
				uuidStr: invalidFormatUUID,
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "Test_parseUUID_with_illegalCharUUID",
			args: args{
				uuidStr: illegalCharUUID,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Test_parseUUID_with_shortUUID",
			args: args{
				uuidStr: shortUUID,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Test_parseUUID_with_longUUID",
			args: args{
				uuidStr: longUUID,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Test_parseUUID_with_emptyUUID",
			args: args{
				uuidStr: emptyUUID,
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUUID(tt.args.uuidStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUUID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (got.String() != tt.args.uuidStr) == tt.want {
				t.Errorf("parseUUID() got = %v, want %v", got.String(), tt.args.uuidStr)
			}
		})
	}
}

func BenchmarkParseUUID(b *testing.B) {
	uuidStr := "6ba7b811-9dad-11d1-80b4-00c04fd430c8"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parseUUID(uuidStr)
		if err != nil {
			b.Fatal(err)
		}
	}
}
