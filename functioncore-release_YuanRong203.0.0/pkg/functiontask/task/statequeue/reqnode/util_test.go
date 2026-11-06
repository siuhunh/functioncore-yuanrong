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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// TestEqual checks equal relation.
func TestEqual(t *testing.T) {
	l1 := []string{"1"}
	l2 := []string{"1"}
	Convey("equal relation", t, func() {
		So(getRelation(l1, l2), ShouldEqual, equalRel)
		So(IsParentOf(l1, l2, true), ShouldBeTrue)
		So(IsParentOf(l1, l2, false), ShouldBeFalse)
		So(Equals(l1, l2), ShouldBeTrue)
	})
}

// TestParent checks parent relation.
func TestParent(t *testing.T) {
	l1 := []string{"1"}
	l2 := []string{"1", "2"}
	Convey("parent relation", t, func() {
		So(getRelation(l1, l2), ShouldEqual, parentRel)
		So(IsParentOf(l1, l2, true), ShouldBeTrue)
		So(IsParentOf(l1, l2, false), ShouldBeTrue)
		So(Equals(l1, l2), ShouldBeFalse)
	})
}

// TestChild checks child relation.
func TestChild(t *testing.T) {
	l1 := []string{"1", "2"}
	l2 := []string{"1"}
	Convey("child relation", t, func() {
		So(getRelation(l1, l2), ShouldEqual, childRel)
		So(IsParentOf(l1, l2, true), ShouldBeFalse)
		So(IsParentOf(l1, l2, false), ShouldBeFalse)
		So(Equals(l1, l2), ShouldBeFalse)
	})
}

// TestNoRel1 checks no relation.
func TestNoRel1(t *testing.T) {
	l1 := []string{"1", "2"}
	l2 := []string{"1", "3"}
	Convey("no relation", t, func() {
		So(getRelation(l1, l2), ShouldEqual, noRel)
		So(IsParentOf(l1, l2, true), ShouldBeFalse)
		So(IsParentOf(l1, l2, false), ShouldBeFalse)
		So(Equals(l1, l2), ShouldBeFalse)
	})
}

// TestNoRel2 checks no relation.
func TestNoRel2(t *testing.T) {
	l1 := []string{"1", "2"}
	l2 := []string{"1", "3", "5"}
	Convey("no relation", t, func() {
		So(getRelation(l1, l2), ShouldEqual, noRel)
		So(IsParentOf(l1, l2, true), ShouldBeFalse)
		So(IsParentOf(l1, l2, false), ShouldBeFalse)
		So(Equals(l1, l2), ShouldBeFalse)
	})
}

// TestNoRel3 checks no relation.
func TestNoRel3(t *testing.T) {
	l1 := []string{"1", "2", "5"}
	l2 := []string{"1", "3"}
	Convey("no relation", t, func() {
		So(getRelation(l1, l2), ShouldEqual, noRel)
		So(IsParentOf(l1, l2, true), ShouldBeFalse)
		So(IsParentOf(l1, l2, false), ShouldBeFalse)
		So(Equals(l1, l2), ShouldBeFalse)
	})
}
