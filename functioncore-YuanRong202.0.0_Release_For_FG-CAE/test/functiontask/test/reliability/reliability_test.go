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

package reliability

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/efs"
	"functioncore/pkg/functiontask/task/efs/kvstorage"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/functiontask/test/testcontext"
)

func init() {
	/*
	   The default failure mode is FailureHalts, which causes test execution
	   within a `Convey` block to halt at the first failure. You could use
	   that mode if the test were re-worked to aggregate all results into
	   a collection that was verified after all goroutines have finished.
	   But, as the code stands, you need to use the FailureContinues mode.

	   The following line sets the failure mode for all tests in the package:
	*/

	SetDefaultFailureMode(FailureContinues)
}

func setCallback(c C, wg *sync.WaitGroup, m *sync.Map, taskCtx taskcontext.Context, requiredFuture *snfuture.Future) {
	defer func() {
		wg.Done()
	}()
	req := &invoke.OnCompleteRequest{OnCompleteRequestPb: invoke.OnCompleteRequestPb{
		FuncName:        "",
		Version:         "",
		StateID:         taskcontext.NewState,
		FutureID:        requiredFuture.ID,
		CountOfFuncName: 0,
	}}
	rawResp := req.Caller(taskCtx)
	resp, ok := rawResp.(*invoke.Response)
	c.So(ok, ShouldBeTrue)
	m.Store(resp.StateID, true)
	fmt.Println(resp.StateID)
	f := taskCtx.FutureStore().Get(resp.FutureID)
	c.So(f, ShouldNotBeNil)
	result := f.GetResponse()
	c.So(result.Code, ShouldEqual, common.ETimeout)
	c.So(result.Message, ShouldEqual, "timeout")
}

func setCallbackWithEFS(c C, store efs.ExtFutureStore) {
	taskCtx := testcontext.New(common.MethodOnComplete)
	config.Conf.PersistentFuture = true
	config.Conf.PersistentState = true
	taskCtx.SetSource(common.SourceRuntimeToLocalFunctionTask)
	taskCtx.SetFilter(taskcontext.NewFilter(), taskcontext.NewFilter())
	taskCtx.SetExtFutureStore(store)
	f := snfuture.NewExternalEvent("xxx")
	taskCtx.FutureStore().Add(f)
	m := new(sync.Map)
	go func() {
		time.Sleep(time.Second)
		f.SetResponse(&common.Response{
			Code:   0,
			Result: "hello",
		})
	}()
	wg := new(sync.WaitGroup)
	Convey("setCallback", func(c C) {
		for i := 0; i < 3; i++ {
			wg.Add(1)
			fmt.Println(i)
			go setCallback(c, wg, m, taskCtx, f)
			time.Sleep(time.Millisecond * 50)
		}
	})
	wg.Wait()
	cnt := 0
	m.Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	So(cnt, ShouldEqual, 1)
}

// TestReliability checks reliability.
func TestReliability(t *testing.T) {
	Convey("setCallback", t, func() {
		SkipConvey("setCallback with redis efs", func(c C) {
			storage, err := NewRedisStorage(testRedisConfig)
			So(err, ShouldBeNil)
			setCallbackWithEFS(c, kvstorage.New(storage))
		})
		SkipConvey("setCallback with 1 second efs", func(c C) {
			setCallbackWithEFS(c, efs.Mock(1*time.Second))
		})
		Convey("setCallback with random ms efs", func(c C) {
			setCallbackWithEFS(c, efs.Mock(time.Duration(rand.Int()%1000)*time.Millisecond))
		})
	})
}
