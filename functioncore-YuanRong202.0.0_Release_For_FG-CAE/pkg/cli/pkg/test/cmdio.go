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

// Package test is used for unit tests
package test

import (
	"bytes"
	"io/ioutil"

	"functioncore/pkg/cli/pkg/cmdio"
)

// CmdIO mock io of cmd
type CmdIO struct {
	In     *bytes.Buffer
	Out    *bytes.Buffer
	ErrOut *bytes.Buffer
}

// MockCmdIO mock cmdio for unit tests
func MockCmdIO() (*cmdio.CmdIO, *CmdIO) {
	in := &bytes.Buffer{}
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	cio := &cmdio.CmdIO{
		In:     ioutil.NopCloser(in),
		Out:    out,
		ErrOut: errOut,
	}
	tio := &CmdIO{
		In:     in,
		Out:    out,
		ErrOut: errOut,
	}
	return cio, tio
}
