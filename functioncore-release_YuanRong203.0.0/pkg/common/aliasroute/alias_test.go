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

// Package aliasroute alias routing
package aliasroute

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"functioncore/pkg/common/etcd3"
)

type AliasTestSuite struct {
	suite.Suite
	alias Alias
}

// TestCase init
func (suite *AliasTestSuite) SetupTest() {
	aliasValue := `{
		"aliasUrn": "sn:cn:yrk:12345678901234561234567890123456:function:helloworld:myaliasv1",
		"functionUrn": "sn:cn:yrk:12345678901234561234567890123456:function:helloworld",
		"functionVersionUrn": "sn:cn:yrk:12345678901234561234567890123456:function:helloworld:$latest",
		"name": "myaliasv1",
		"functionVersion": "$latest",
		"revisionId": "20210617023315921",
		"description": "",
		"routingconfig": [
			{
				"functionVersionUrn": "sn:cn:yrk:12345678901234561234567890123456:function:helloworld:$latest",
				"weight": 100
			}
		]
	}`
	value := []byte(aliasValue)
	err := json.Unmarshal(value, &suite.alias)
	if err != nil {
		suite.T().Errorf(err.Error())
		return
	}

	suite.T().Log("init alias testcase, construct alias struct")
}

// TestCase clean up
func (suite *AliasTestSuite) TearDownTest() {
	suite.alias = Alias{}
}

func TestFetchInfoFromAliasKey(t *testing.T) {
	path := "/sn/aliases/business/yrk/tenant/12345678901234561234567890123456/function/helloworld/myalias"
	aliasKey := FetchInfoFromAliasKey(path)

	assert.Equal(t, aliasKey.FunctionID, "helloworld")
	assert.Equal(t, aliasKey.AliasName, "myalias")

	path = "/sn/aliases/business/yrk/tenant/12345678901234561234567890123456/function/helloworld"
	aliasKey = FetchInfoFromAliasKey(path)
	assert.Empty(t, aliasKey)
}

func TestIsAlias(t *testing.T) {
	path := "/sn/aliases/business/yrk/tenant/12345678901234561234567890123456/function/helloworld/myalias"
	flag := IsAlias(path)
	assert.True(t, flag)

	path = "/sn/xxx/business/yrk/tenant/12345678901234561234567890123456/function/helloworld/myalias"
	flag = IsAlias(path)
	assert.False(t, flag)

	path = "/sn/aliases/business/yrk/xxx/12345678901234561234567890123456/function/helloworld/myalias"
	flag = IsAlias(path)
	assert.False(t, flag)

	path = "/sn/aliases/business/yrk/tenant/12345678901234561234567890123456/xxx/helloworld/myalias"
	flag = IsAlias(path)
	assert.False(t, flag)
}

func TestBuildURNFromAliasKey(t *testing.T) {
	path := "/sn/aliases/business/yrk/tenant/12345678901234561234567890123456/function/helloworld/myalias"
	urn := BuildURNFromAliasKey(path)
	assert.Contains(t, urn, "myalias")
}

func (suite *AliasTestSuite) TestAddAlias() {
	as := Aliases{AliasMap: &sync.Map{}}
	patches := ApplyGlobalVar(AliasRoute, as)
	defer patches.Reset()

	AliasRoute.AddAlias(&suite.alias)
	version := AliasRoute.GetFuncVersionURN(suite.alias.AliasURN)
	if version != "sn:cn:yrk:12345678901234561234567890123456:function:helloworld:$latest" {
		suite.T().Errorf("route version should be $latest, but get %s", version)
		return
	}

	assert.Contains(suite.T(), AliasRoute.String(),
		"sn:cn:yrk:12345678901234561234567890123456:function:helloworld:$latest")

	len := 0
	AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
		len++
		return true
	})
	assert.Equal(suite.T(), len, 1)

	// Duplicate add exist alias
	len = 0
	AliasRoute.AddAlias(&suite.alias)
	AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
		len++
		return true
	})
	assert.Equal(suite.T(), len, 1)

	// Update exist alias
	len = 0
	suite.alias.AliasURN = "sn:cn:yrk:12345678901234561234567890123456:function:helloworld:v1"
	AliasRoute.AddAlias(&suite.alias)
	AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
		len++
		return true
	})
	assert.Equal(suite.T(), len, 2)
}

// This is test for goroutine simultaneously add alias
func (suite *AliasTestSuite) TestMultiAddAlias() {
	as := Aliases{AliasMap: &sync.Map{}}
	patches := ApplyGlobalVar(AliasRoute, as)
	defer patches.Reset()

	wg := sync.WaitGroup{}
	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			AliasRoute.AddAlias(&suite.alias)
			wg.Done()
		}()
	}

	wg.Wait()
	len := 0
	AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
		len++
		return true
	})
	assert.Equal(suite.T(), len, 1)
}

func (suite *AliasTestSuite) TestRemoveAlias() {
	as := Aliases{AliasMap: &sync.Map{}}
	patches := ApplyGlobalVar(AliasRoute, as)
	defer patches.Reset()

	AliasRoute.AddAlias(&suite.alias)

	// Remove exist alias
	AliasRoute.RemoveAlias(suite.alias.AliasURN)
	len := 0
	AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
		len++
		return true
	})
	assert.Equal(suite.T(), len, 0)

	// Duplicate remove
	AliasRoute.RemoveAlias(suite.alias.AliasURN)
	len = 0
	AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
		len++
		return true
	})
	assert.Equal(suite.T(), len, 0)
}

func TestAliasTestSuite(t *testing.T) {
	suite.Run(t, new(AliasTestSuite))
}

func TestGetFuncVersionURN(t *testing.T) {
	as := Aliases{AliasMap: &sync.Map{}}
	patches := ApplyGlobalVar(AliasRoute, as)
	defer patches.Reset()
	version := AliasRoute.GetFuncVersionURN("a")
	assert.Empty(t, version)

	AliasRoute.AliasMap.Store("a", "b")
	version = AliasRoute.GetFuncVersionURN("a")
	assert.Empty(t, version)
}

func TestString(t *testing.T) {
	as := Aliases{AliasMap: &sync.Map{}}
	patches := ApplyGlobalVar(AliasRoute, as)
	defer patches.Reset()
	AliasRoute.AliasMap.Store(1, 2)
	v := AliasRoute.String()
	assert.Equal(t, v, "{}")

	AliasRoute.AliasMap.Delete(1)
	AliasRoute.AliasMap.Store("1", 2)
	v = AliasRoute.String()
	assert.Equal(t, v, "{\"1\":null}")

	AliasRoute.AliasMap.Delete("1")
	AliasRoute.AliasMap.Store("1", &Aliases{})
	patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("s") })
	defer patch.Reset()
	v = AliasRoute.String()
	assert.Equal(t, v, "")
}

func TestProcessAliasEvent(t *testing.T) {
	convey.Convey("Test Process Alias Event PUT", t, func() {
		event := &etcd3.Event{Type: 0, Value: []byte{}}
		err := ProcessAliasEvent(event)
		convey.So(err, convey.ShouldNotBeNil)

		patch := ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
			return nil
		})
		defer patch.Reset()
		err = ProcessAliasEvent(&etcd3.Event{Type: 0, Value: []byte{}})
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test Process Alias Event Delete", t, func() {
		event := &etcd3.Event{Type: 1, Value: []byte{}}
		err := ProcessAliasEvent(event)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test Process Alias Event ERROR", t, func() {
		event := &etcd3.Event{Type: 2, Value: []byte{}}
		err := ProcessAliasEvent(event)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test Process Alias Event DEFAULT", t, func() {
		event := &etcd3.Event{Type: 3, Value: []byte{}}
		err := ProcessAliasEvent(event)
		convey.So(err, convey.ShouldBeNil)
	})
}
