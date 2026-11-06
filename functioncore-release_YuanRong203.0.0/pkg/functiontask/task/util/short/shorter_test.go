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

package short

import (
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// TestHashNil checks the hash of nil.
func TestHashNil(t *testing.T) {
	var value []byte
	Convey("get short hash for nil", t, func() {
		res := Hash(value)
		So(res, ShouldEqual, "")
	})
}

// TestHashZero checks the hash of zero slice.
func TestHashZero(t *testing.T) {
	value := make([]byte, 0)
	Convey("get short hash for empty slice", t, func() {
		res := Hash(value)
		So(res, ShouldEqual, "")
	})
}

// TestHashLess checks the hash of small slice.
func TestHashLess(t *testing.T) {
	value := []byte{1, 2}
	Convey("get short hash for values which has less than " + strconv.Itoa(shortBytes)+" bytes", t, func() {
		res := Hash(value)
		So(res, ShouldEqual, "0102")
	})
}

// TestHashExactly checks the hash of appropriate slice.
func TestHashExactly(t *testing.T) {
	value := []byte{1, 2, 3}
	Convey("get short hash for values which has exactly " + strconv.Itoa(shortBytes)+" bytes", t, func() {
		res := Hash(value)
		So(res, ShouldEqual, "010203")
	})
}

// TestHashLonger checks the hash of large slice.
func TestHashLonger(t *testing.T) {
	value := []byte{1, 2, 3, 4}
	Convey("get short hash for values which has longer than " + strconv.Itoa(shortBytes)+" bytes", t, func() {
		res := Hash(value)
		So(res, ShouldEqual, "010203")
	})
}

// TestStrZero checks the str of zero string.
func TestStrZero(t *testing.T) {
	value := ""
	Convey("get short string for empty string", t, func() {
		res := Str(value)
		So(res, ShouldEqual, "")
	})
}

// TestStrLess checks the str of small string.
func TestStrLess(t *testing.T) {
	value := "12345"
	Convey("get short string for values which has less than " + strconv.Itoa(shortCharacters)+" bytes", t, func() {
		res := Str(value)
		So(res, ShouldEqual, "12345")
	})
}

// TestStrExactly checks the str of appropriate string.
func TestStrExactly(t *testing.T) {
	value := "123456"
	Convey("get short string for values which has exactly " + strconv.Itoa(shortCharacters)+" bytes", t, func() {
		res := Str(value)
		So(res, ShouldEqual, "123456")
	})
}

// TestStrLonger checks the str of large string.
func TestStrLonger(t *testing.T) {
	value := "1234567"
	Convey("get short string for values which has longer than " + strconv.Itoa(shortCharacters)+" characters", t, func() {
		res := Str(value)
		So(res, ShouldEqual, "123456")
	})
}

// TestListNil checks the list of nil.
func TestListNil(t *testing.T) {
	var value []string
	Convey("get short hash for nil", t, func() {
		res := List(value)
		So(res, ShouldEqual, "")
	})
}

// TestListZero checks the list of zero slice.
func TestListZero(t *testing.T) {
	value := make([]string, 0)
	Convey("get short hash for empty slice", t, func() {
		res := List(value)
		So(res, ShouldEqual, "")
	})
}

// TestList checks the list.
func TestList(t *testing.T) {
	value := []string{"1234", "123", "12", "", ""}
	Convey("get short hash for values which has less than " + strconv.Itoa(shortBytes)+" bytes", t, func() {
		res := List(value)
		So(res, ShouldEqual, "1234,123,12,,")
	})
}

// TestLists checks the list of list.
func TestLists(t *testing.T) {
	Convey("get short hash for values which has less than " + strconv.Itoa(shortBytes)+" bytes", t, func() {
		var values [][]string
		num := 10
		for i := 0; i < num; i++ {
			tmp := ""
			values = append(values, make([]string, 0, num))
			for j := 0; j < num; j++ {
				tmp += strconv.Itoa(j)
				values[i] = append(values[i], tmp)
			}
		}
		res := Lists(values)
		So(res, ShouldEqual, "[0,01,012,0123,01234,012345,012345,012345,012345,012345],[0,01,012,0123,01234,"+
			"012345,012345,012345,012345,012345],[0,01,012,0123,01234,012345,012345,012345,012345,012345],[0,01,012,0123,"+
			"01234,012345,012345,012345,012345,012345],[0,01,012,0123,01234,012345,012345,012345,012345,012345],[0,01,012,"+
			"0123,01234,012345,012345,012345,012345,012345],[0,01,012,0123,01234,012345,012345,012345,012345,012345],[0,01,"+
			"012,0123,01234,012345,012345,012345,012345,012345],[0,01,012,0123,01234,012345,012345,012345,012345,012345],[0,"+
			"01,012,0123,01234,012345,012345,012345,012345,012345]")
	})
}
