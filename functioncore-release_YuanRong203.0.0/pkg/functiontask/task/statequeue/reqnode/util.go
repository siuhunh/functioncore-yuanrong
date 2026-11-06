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

package reqnode

import (
	"functioncore/pkg/functiontask/task/common"
)

const (
	parentRel = iota
	equalRel
	childRel
	noRel
)

func getRelation(src common.ReqIDList, dst common.ReqIDList) int {
	srcLen := len(src)
	dstLen := len(dst)
	if srcLen == dstLen {
		for i := 0; i < srcLen; i++ {
			if src[i] != dst[i] {
				return noRel
			}
		}
		return equalRel
	}
	if srcLen > dstLen {
		for i := 0; i < dstLen; i++ {
			if src[i] != dst[i] {
				return noRel
			}
		}
		return childRel
	}
	for i := 0; i < srcLen; i++ {
		if src[i] != dst[i] {
			return noRel
		}
	}
	return parentRel
}

// IsParentOf checks whether the call chain of src is prefixed to dst (another way of saying that,
// src is the father of dst).
func IsParentOf(src common.ReqIDList, dst common.ReqIDList, contain bool) bool {
	srcLen := len(src)
	dstLen := len(dst)
	if !contain && srcLen == dstLen {
		return false
	}
	if srcLen > dstLen {
		return false
	}
	for i := 0; i < srcLen; i++ {
		if src[i] != dst[i] {
			return false
		}
	}
	return true
}

// Equals checks whether the call chain of src is same to dst.
func Equals(src common.ReqIDList, dst common.ReqIDList) bool {
	srcLen := len(src)
	dstLen := len(dst)
	if srcLen != dstLen {
		return false
	}
	for i := 0; i < srcLen; i++ {
		if src[i] != dst[i] {
			return false
		}
	}
	return true
}
