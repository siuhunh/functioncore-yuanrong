/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package functiontypes

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFuncMetaDataParts_UnmarshalJSON_IsCustomContainer(t *testing.T) {
	tests := []struct {
		name              string
		data              []byte
		isCustomContainer bool
	}{
		{
			name:              "not custom container",
			data:              []byte(`{"timeout": 1, "runtime": "python3.6"}`),
			isCustomContainer: false,
		},
		{
			name:              "is custom container",
			data:              []byte(`{"timeout": 1, "runtime": "custom image"}`),
			isCustomContainer: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := FuncMetaDataParts{}
			err := json.Unmarshal(tt.data, &data)
			assert.Nil(t, err, tt.name)
			assert.Equal(t, tt.isCustomContainer, data.IsCustomContainer)
		})
	}
}
