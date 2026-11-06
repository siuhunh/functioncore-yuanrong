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

// Package verify is verification of function params accuracy
package verify

import "errors"

const pageSizeMaximum = 100

// CheckPageInfo check page info
func CheckPageInfo(pageIndex, pageSize int) error {
	if pageIndex < 1 {
		return errors.New("the minimum value of pageIndex is 1")
	}
	if pageSize < 1 {
		return errors.New("the minimum value of pageSize is 1")
	}
	if pageSize > pageSizeMaximum {
		return errors.New("the maximum value of pageSize is 100")
	}
	return nil
}
