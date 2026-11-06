/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package tools

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func Test_ExtractFirstLevelDir(t *testing.T) {
	convey.Convey("test extractFirstLevelDir", t, func() {
		convey.Convey("case1: success", func() {
			dir, ok := ExtractFirstLevelDir("///cae//log/*/aaa/**/bbb/***/*.log", "urn", "pod")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(dir, convey.ShouldEqual, "/cae")
		})
		convey.Convey("case2: logPath is not abstract path", func() {
			_, ok := ExtractFirstLevelDir("cae/log/log.log", "urn", "pod")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case3: length of logPath first level directory is 0", func() {
			_, ok := ExtractFirstLevelDir("/", "urn", "pod")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case4: logPath root directory is /", func() {
			_, ok := ExtractFirstLevelDir("/app.log", "urn", "pod")
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("case5: logPath first level directory is existed", func() {
			_, ok := ExtractFirstLevelDir("/home/cae/log.log", "urn", "pod")
			convey.So(ok, convey.ShouldBeFalse)
		})
	})
}
