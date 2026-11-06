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

package functionlog

import "functioncore/pkg/common/constants"

// LogRecorderOption -
type LogRecorderOption func(r *LogRecorder)

// WithLogOption -
func WithLogOption(option string) LogRecorderOption {
	return func(r *LogRecorder) {
		r.logOption = option
	}
}

// WithLogOptionTail -
func WithLogOptionTail() LogRecorderOption {
	return WithLogOption(constants.RuntimeLogOptTail)
}

// WithoutLineBreak -
func WithoutLineBreak() LogRecorderOption {
	return func(r *LogRecorder) {
		r.separatedWithLineBreak = false
	}
}

// WithLivedataID -
func WithLivedataID(livedataID string) LogRecorderOption {
	return func(r *LogRecorder) {
		r.livedataID = livedataID
	}
}

// WithLogHeaderFooterGenerator -
func WithLogHeaderFooterGenerator(g LogHeaderFooterGenerator) LogRecorderOption {
	return func(r *LogRecorder) {
		r.generator = g
	}
}
