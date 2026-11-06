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

package ratelimit

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
)

func TestGetTenantLimiter(t *testing.T) {
	limiterContainer := GetTenantLimiterContainer()
	tenantID := "123456789"
	key := fmt.Sprintf("/sn/qos/business/yrk/tenant/%s", tenantID)
	event := &etcd3.Event{Key: key, Value: []byte(`{"quota":10}`)}

	invalidTenantID := "987654321"
	invalidKey := fmt.Sprintf("/sn/qos/business/yrk/tenantxxx/%s", invalidTenantID)
	invalidEvent := &etcd3.Event{Key: invalidKey, Value: []byte(`{"quota":5}`)}

	convey.Convey("test tenant limiter ", t, func() {
		convey.So(limiterContainer, convey.ShouldNotBeNil)

		convey.Convey("update", func() {
			if TenantQOSFilter(event.Key) {
				Update(event)
			}
			if TenantQOSFilter(invalidEvent.Key) {
				Update(event)
			}
			_, ok := limiterContainer.tenantInfo.Load(tenantID)
			convey.So(ok, convey.ShouldBeTrue)

			_, ok1 := limiterContainer.tenantInfo.Load(invalidTenantID)
			convey.So(ok1, convey.ShouldBeFalse)
		})

		convey.Convey("one request", func() {
			err := limiterContainer.TakeOneToken(tenantID)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("15 requests", func() {
			quoat := 10
			var wg sync.WaitGroup
			errors := make([]error, 15)
			for i := 0; i < 15; i++ {
				wg.Add(1)
				go func(index int) {
					err := limiterContainer.TakeOneToken(tenantID)
					errors[index] = err
					wg.Done()
				}(i)
			}
			wg.Wait()

			expectPassReqNums := int(math.Ceil(float64(quoat)) / constants.TrafficRedundantRate)
			fmt.Println("bucket size :", expectPassReqNums)
			actualPassReqNums := 0
			for _, err := range errors {
				if err == nil {
					actualPassReqNums++
				} else {
					fmt.Println("err:", err.Error())
				}
			}
			fmt.Println("actual pass requests:", actualPassReqNums)
			convey.So(actualPassReqNums, convey.ShouldBeLessThanOrEqualTo, expectPassReqNums)
		})

		convey.Convey("update quota 20, 15 requests", func() {
			quoat := 20
			event.Value = []byte(`{"quota":20}`)
			if TenantQOSFilter(event.Key) {
				Update(event)
			}
			time.Sleep(2 * time.Second)
			var wg sync.WaitGroup
			errors := make([]error, 15)
			for i := 0; i < 15; i++ {
				wg.Add(1)
				go func(index int) {
					err := limiterContainer.TakeOneToken(tenantID)
					errors[index] = err
					wg.Done()
				}(i)
			}
			wg.Wait()

			expectPassReqNums := int(math.Ceil(float64(quoat)) / constants.TrafficRedundantRate)
			fmt.Println("bucket size :", expectPassReqNums)
			actualPassReqNums := 0
			for _, err := range errors {
				if err == nil {
					actualPassReqNums++
				} else {
					fmt.Println("err:", err.Error())
				}
			}
			fmt.Println("actual pass requests:", actualPassReqNums)
			convey.So(actualPassReqNums, convey.ShouldBeLessThanOrEqualTo, expectPassReqNums)
			convey.So(actualPassReqNums, convey.ShouldEqual, 15)
		})

		convey.Convey("delete limiter, 15 requests", func() {
			if TenantQOSFilter(event.Key) {
				Delete(event)
			}
			_, ok := limiterContainer.tenantInfo.Load(tenantID)
			convey.So(ok, convey.ShouldBeFalse)
			var wg sync.WaitGroup
			errors := make([]error, 15)
			for i := 0; i < 15; i++ {
				wg.Add(1)
				go func(index int) {
					err := limiterContainer.TakeOneToken(tenantID)
					errors[index] = err
					wg.Done()
				}(i)
			}
			wg.Wait()
			actualPassReqNums := 0
			for _, err := range errors {
				if err == nil {
					actualPassReqNums++
				} else {
					fmt.Println("err:", err.Error())
				}
			}
			fmt.Println("actual pass requests:", actualPassReqNums)
			convey.So(actualPassReqNums, convey.ShouldEqual, 15)
		})
	})
}

func TestLimiterErrors(t *testing.T) {
	convey.Convey("TestLimiterErrors", t, func() {
		limiterContainer := GetTenantLimiterContainer()
		tenantID := "1234567890"
		key := fmt.Sprintf("/sn/qos/business/yrk/tenant/%s/xxx", tenantID)
		event := &etcd3.Event{Key: key, Value: []byte(`{"quota":10}`)}

		if TenantQOSFilter(event.Key) {
			Update(event)
		}
		_, ok := limiterContainer.tenantInfo.Load(tenantID)
		convey.So(ok, convey.ShouldBeFalse)

		key = fmt.Sprintf("/sn/qos/business/yrk/tenant/%s", tenantID)
		event.Key = key
		event.Value = []byte(`{"quotaxxx":10}`)
		if TenantQOSFilter(event.Key) {
			Update(event)
		}
		_, ok = limiterContainer.tenantInfo.Load(tenantID)
		convey.So(ok, convey.ShouldBeFalse)

		tenantID = ""
		key = fmt.Sprintf("/sn/qos/business/yrk/tenant/%s", tenantID)
		event.Key = key
		event.Value = []byte(`{"quota":10}`)
		if TenantQOSFilter(event.Key) {
			Update(event)
		}
		_, ok = limiterContainer.tenantInfo.Load(tenantID)
		convey.So(ok, convey.ShouldBeFalse)

		tenantID = "123"
		key = fmt.Sprintf("/sn/qos/business/yrk/tenant/%s", tenantID)
		event.Key = key
		event.Value = []byte(`123`)
		if TenantQOSFilter(event.Key) {
			Update(event)
		}
		_, ok = limiterContainer.tenantInfo.Load(tenantID)
		convey.So(ok, convey.ShouldBeFalse)
	})
}
