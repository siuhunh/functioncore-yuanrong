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

package waitfuture

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait001(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	f1.SetResponse(common.SystematicError(common.Error, ""))
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{f1.ID}
		resp := waitStateless(futureIDs, 1, 0, futureStore)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.FutureIDs, ShouldResemble, futureIDs)
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait002(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	f1.SetResponse(common.SystematicError(common.Error, ""))
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{"xxx"}
		resp := waitStateless(futureIDs, 1, 0, futureStore)
		So(resp.Code, ShouldEqual, common.EInvalidParam)
		So(resp.FutureIDs, ShouldResemble, []string{})
		So(resp.Message, ShouldEqual, "invalid future id: "+"xxx")
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait003(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	f1.SetResponse(common.SystematicError(common.Error, ""))
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{f1.ID}
		resp := waitStateless(futureIDs, 1, time.Millisecond, futureStore)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.FutureIDs, ShouldResemble, futureIDs)
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait004(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	f1.SetResponse(common.SystematicError(common.Error, ""))
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{"xxx"}
		resp := waitStateless(futureIDs, 1, time.Millisecond, futureStore)
		So(resp.Code, ShouldEqual, common.EInvalidParam)
		So(resp.FutureIDs, ShouldResemble, []string{})
		So(resp.Message, ShouldEqual, "invalid future id: "+"xxx")
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait005(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{f1.ID}
		resp := waitStateless(futureIDs, 1, time.Millisecond, futureStore)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.FutureIDs, ShouldResemble, []string{})
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait006(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{"xxx"}
		resp := waitStateless(futureIDs, 1, time.Millisecond, futureStore)
		So(resp.Code, ShouldEqual, common.EInvalidParam)
		So(resp.FutureIDs, ShouldResemble, []string{})
		So(resp.Message, ShouldEqual, "invalid future id: "+"xxx")
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait007(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{f1.ID}
		go func() {
			<-time.After(time.Second)
			f1.SetResponse(common.SystematicError(common.Error, ""))
		}()
		resp := waitStateless(futureIDs, 1, time.Millisecond, futureStore)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.FutureIDs, ShouldResemble, []string{})
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait008(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{"xxx"}
		go func() {
			<-time.After(time.Second)
			f1.SetResponse(common.SystematicError(common.Error, ""))
		}()
		resp := waitStateless(futureIDs, 1, time.Millisecond, futureStore)
		So(resp.Code, ShouldEqual, common.EInvalidParam)
		So(resp.FutureIDs, ShouldResemble, []string{})
		So(resp.Message, ShouldEqual, "invalid future id: "+"xxx")
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait009(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{f1.ID}
		go func() {
			<-time.After(time.Millisecond)
			f1.SetResponse(common.SystematicError(common.Error, ""))
		}()
		resp := waitStateless(futureIDs, 1, time.Second, futureStore)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.FutureIDs, ShouldResemble, futureIDs)
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestList1K1Wait010(t *testing.T) {
	futureStore := futurestore.New()
	f1 := snfuture.NewExternalEvent("1")
	futureStore.Add(f1)
	Convey("create invoke future", t, func(c C) {
		futureIDs := []string{"xxx"}
		go func() {
			<-time.After(time.Millisecond)
			f1.SetResponse(common.SystematicError(common.Error, ""))
		}()
		resp := waitStateless(futureIDs, 1, time.Second, futureStore)
		So(resp.Code, ShouldEqual, common.EInvalidParam)
		So(resp.FutureIDs, ShouldResemble, []string{})
		So(resp.Message, ShouldEqual, "invalid future id: "+"xxx")
	})
}
