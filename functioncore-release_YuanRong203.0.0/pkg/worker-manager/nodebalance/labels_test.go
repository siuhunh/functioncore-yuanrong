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

package nodebalance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"functioncore/pkg/common/constants"
)

func Test_generateNodeLabelSelector(t *testing.T) {
	r, err := labels.NewRequirement(constants.NodeRoleLabelKey, selection.In, constants.GetValidNodeRoleValuesWithGPU())
	if err != nil {
		t.Fatal("failed to generate label requirement", err.Error())
		return
	}
	assert.Equal(t, r.String(), generateNodeLabelSelector())
}
