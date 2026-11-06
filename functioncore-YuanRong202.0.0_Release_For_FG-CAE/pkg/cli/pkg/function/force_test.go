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
package function

import (
	"os"
	"testing"

	"github.com/agiledragon/gomonkey"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/utils/terminal"
)

func TestForce(t *testing.T) {
	cmdIO := &cmdio.CmdIO{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: os.Stderr,
	}
	convey.Convey("Force", t, func() {
		convey.Convey("CheckInstanceID arr empty", func() {
			res := Force(cmdIO, false, []string{}, "", true)
			convey.So(res, convey.ShouldEqual, false)
		})

		convey.Convey("CheckInstanceID retryCountLimit", func() {
			patches := gomonkey.ApplyFuncSeq(terminal.ScanLine, []gomonkey.OutputCell{
				{Values: gomonkey.Params{"1"}},
				{Values: gomonkey.Params{"1"}},
				{Values: gomonkey.Params{"1"}},
				{Values: gomonkey.Params{"1"}},
			})
			res := Force(cmdIO, false, []string{"123"}, "", true)
			convey.So(res, convey.ShouldEqual, false)
			patches.Reset()
		})
	})
}
