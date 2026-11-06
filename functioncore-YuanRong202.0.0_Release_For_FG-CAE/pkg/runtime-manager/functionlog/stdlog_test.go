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

package functionlog

import (
	"bytes"
	"testing"
)

func TestFixSizeWriter_Write(t *testing.T) {
	var wBuffer bytes.Buffer
	for i := 0; i < 1000; i++ {
		wBuffer.WriteString("xxxxxxxxxxxxxxxxxxxx")
	}
	writeBuffer := wBuffer.Bytes()
	type fields struct {
		byteSlice    []byte
		recycleIndex int
	}
	type args struct {
		p []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1 test fix size writer length", fields{
			byteSlice: make([]byte, defaultFixSizeWriterSize),
		}, args{writeBuffer}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &FixSizeWriter{
				byteSlice:    tt.fields.byteSlice,
				recycleIndex: tt.fields.recycleIndex,
			}
			_, _ = w.Write(tt.args.p)
			if len(w.byteSlice) != defaultFixSizeWriterSize {
				t.Errorf("fix size writer length is not correct")
			}
			if len(w.Read()) != defaultFixSizeWriterSize {
				t.Errorf("fix size writer length is not correct")
			}
		})
	}
}
