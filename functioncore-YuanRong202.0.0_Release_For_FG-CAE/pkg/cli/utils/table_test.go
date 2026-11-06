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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	testCases := []struct {
		name    string
		headers []string
		bulks   [][]string
		wantStd string
	}{
		{
			name:    "test",
			headers: []string{"test"},
			bulks:   [][]string{{"test"}},
			wantStd: `+------+
| TEST |
+------+
| test |
+------+
`,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			table := NewTable(out, true, true, true)
			table.SetHeader(tt.headers)
			table.AppendBulk(tt.bulks)
			table.Render()
			assert.Equal(t, tt.wantStd, out.String())
		})
	}
}
