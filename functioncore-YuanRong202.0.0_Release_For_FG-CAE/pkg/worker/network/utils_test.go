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

package network

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestExecuteShellCommandLineWithCombinedOutput(t *testing.T) {
	value, output, err := ExecuteShellCommandLineWithCombinedOutput("sleep 0.001")
	if value != 0 || err != nil {
		t.Errorf("exec sleep failed, output: %s", output)
	}

	value, output, err = ExecuteShellCommandLineWithCombinedOutput("sh no_exist.sh")
	if value == 0 {
		t.Errorf("exec error for wrong command, output: %s", output)
	}
}

func Test_getNetMask(t *testing.T) {
	convey.Convey("test", t, func() {
		haxMask := "00000000"
		result, _ := getNetMask(haxMask)
		convey.So(result, convey.ShouldEqual, "0.0.0.0")
	})
}
