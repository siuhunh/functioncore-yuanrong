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
package terminal

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/cli/utils"
)

func TestSaveHistory(t *testing.T) {
	convey.Convey("TestSaveHistory", t, func() {
		currentDir, _ := os.Getwd()
		patch := gomonkey.ApplyFunc(utils.GetResourcePath, func() (string, error) {
			return currentDir, nil
		})

		saveHistoryCMD([]byte("a"))
		saveHistoryCMD([]byte("a"))

		var str string
		str = string(getLastHistory().word)
		convey.So(str, convey.ShouldEqual, "a")

		str = string(getLastHistory().word)
		convey.So(str, convey.ShouldEqual, "a")

		str = string(getNextHistory().word)
		convey.So(str, convey.ShouldEqual, "a")

		str = string(getNextHistory().word)
		convey.So(str, convey.ShouldEqual, "a")

		time.Sleep(10 * time.Millisecond)
		defer func(patch *gomonkey.Patches) {
			patch.Reset()
			os.RemoveAll(path.Join(currentDir, "logs"))
			os.Remove(path.Join(currentDir, historyFileName))
			resetHistory()
		}(patch)
	})
}
