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

package autogc

import (
	"os"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/test/utils"
)

func TestInitAutoGOGC(t *testing.T) {
	InitAutoGOGC()
	runtime.GC()
	assert.Equal(t, 100, previousGOGC)
}

func TestInitAutoGOGC2(t *testing.T) {
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyFunc(debug.SetGCPercent,
			func(percent int) int {
				return 100
			})})
	defer patches.ResetAll()
	os.Setenv("AUTO_GC_MEMORY_THRESHOLD", "120")
	InitAutoGOGC()
	runtime.GC()
	assert.Equal(t, 100, previousGOGC)
}
