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

// Package aliasroute alias routing in busclient
package aliasroute

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
)

const (
	weightRatio = 100 // max weight of a node
	// AliasKeySeparator is the separator in an alias key
	AliasKeySeparator = "/"
	// DefaultAliasSign is the default alias sign
	DefaultAliasSign = "aliases"
	// DefaultTenantSign is the default tenant sign
	DefaultTenantSign = "tenant"
	// DefaultFuncSign is the default function sign
	DefaultFuncSign = "function"
)

// example of an aliasKey:
// /<ProductID>/<AliasSign>/<BusinessSign>/<BusinessID>/<TenantSign>/<TenantID>/<FunctionSign>/<FunctionID>/<AliasName>
const (
	ProductIDIndex = iota + 1
	AliasSignIndex
	BusinessSignIndex
	BusinessIDIndex
	TenantSignIndex
	TenantIDIndex
	FunctionSignIndex
	FunctionIDIndex
	AliasNameIndex
	aliasKeyLength
)

// Aliases map for stateless function alias
type Aliases struct {
	// Key: aliasURN
	// Value: *Alias
	AliasMap *sync.Map
}

// AliasRoute for alias routing
var (
	AliasRoute = &Aliases{
		AliasMap: &sync.Map{},
	}
)

// AddAlias add alias to Aliases map
func (a *Aliases) AddAlias(alias *Alias) {
	existAliasIf, exist := a.AliasMap.Load(alias.AliasURN)
	var existAlias *Alias
	var ok bool
	if !exist {
		// new alias, initialize RR and Mutex
		existAlias = &Alias{
			AliasURN:           alias.AliasURN,
			FunctionURN:        alias.FunctionURN,
			FunctionVersionURN: alias.FunctionVersionURN,
			Name:               alias.Name,
			Description:        alias.Description,
			FunctionVersion:    alias.FunctionVersion,
			RevisionID:         alias.RevisionID,
			RoutingConfigs:     alias.RoutingConfigs,

			lb: loadbalance.LBFactory(loadbalance.RoundRobinNginx),
			m:  &sync.RWMutex{},
		}
		existAlias.resetRR()
		a.AliasMap.Store(alias.AliasURN, existAlias)
		return
	}
	existAlias, ok = existAliasIf.(*Alias)
	if ok {
		aliasUpdate(existAlias, alias)
		existAlias.resetRR()
	}
}

// RemoveAlias remove alias to aliases map
func (a *Aliases) RemoveAlias(aliasURN string) {
	a.AliasMap.Delete(aliasURN)
}

// GetFuncVersionURN gets the routing version URN of stateless functionName
func (a *Aliases) GetFuncVersionURN(aliasURN string) string {
	existAliasIf, exist := a.AliasMap.Load(aliasURN)
	if !exist {
		return ""
	}
	existAlias, ok := existAliasIf.(*Alias)
	if !ok {
		return ""
	}
	return existAlias.getFuncVersionURN()
}

// String return Marshal To String
func (a *Aliases) String() string {
	aliases := map[string]*Alias{}
	a.AliasMap.Range(func(k, v interface{}) bool {
		urn, ok := k.(string)
		if !ok {
			return false
		}
		aliases[urn], ok = v.(*Alias)
		if !ok {
			return false
		}
		return true
	})
	str, err := json.Marshal(aliases)
	if err != nil {
		return ""
	}
	return string(str)
}

// Alias struct stores an alias configs of stateless function
type Alias struct {
	RoutingConfigs     []*routingConfig `json:"routingconfig"`
	m                  *sync.RWMutex
	lb                 loadbalance.LoadBalance
	AliasURN           string `json:"aliasUrn"`
	FunctionURN        string `json:"functionUrn"`
	FunctionVersionURN string `json:"functionVersionUrn"`
	Name               string `json:"name"`
	Description        string `json:"description"`
	FunctionVersion    string `json:"functionVersion"`
	RevisionID         string `json:"revisionId"`
}

func (a *Alias) getFuncVersionURN() string {
	a.m.RLock()
	defer a.m.RUnlock()
	routingConfigIf := a.lb.Next("", true)
	if routingConfigIf == nil {
		return ""
	}
	routingConfig, ok := routingConfigIf.(*routingConfig)
	if !ok {
		return ""
	}
	return routingConfig.FunctionVersionURN
}

func (a *Alias) resetRR() {
	a.m.Lock()
	defer a.m.Unlock()
	a.lb.RemoveAll()
	for _, v := range a.RoutingConfigs {
		a.lb.Add(v, int(v.Weight*weightRatio))
	}
}

type routingConfig struct {
	FunctionVersionURN string  `json:"functionVersionUrn"`
	Weight             float64 `json:"weight"`
}

func aliasUpdate(destAlias, srcAlias *Alias) {
	destAlias.AliasURN = srcAlias.AliasURN
	destAlias.FunctionURN = srcAlias.FunctionURN
	destAlias.FunctionVersionURN = srcAlias.FunctionVersionURN
	destAlias.Name = srcAlias.Name
	destAlias.Description = srcAlias.Description
	destAlias.FunctionVersion = srcAlias.FunctionVersion
	destAlias.RevisionID = srcAlias.RevisionID
	destAlias.RoutingConfigs = srcAlias.RoutingConfigs
}

// AliasKey contains the elements of an alias key
type AliasKey struct {
	ProductID    string
	AliasSign    string
	BusinessSign string
	BusinessID   string
	TenantSign   string
	TenantID     string
	FunctionSign string
	FunctionID   string
	AliasName    string
}

// ParseFrom parses elements from an alias key
func (a *AliasKey) ParseFrom(key string) error {
	elements := strings.Split(key, AliasKeySeparator)
	urnLen := len(elements)
	if urnLen != aliasKeyLength {
		return fmt.Errorf("failed to parse an alias key %s, incorrect length", key)
	}
	a.ProductID = elements[ProductIDIndex]
	a.AliasSign = elements[AliasSignIndex]
	a.BusinessSign = elements[BusinessSignIndex]
	a.BusinessID = elements[BusinessIDIndex]
	a.TenantSign = elements[TenantSignIndex]
	a.TenantID = elements[TenantIDIndex]
	a.FunctionSign = elements[FunctionSignIndex]
	a.FunctionID = elements[FunctionIDIndex]
	a.AliasName = elements[AliasNameIndex]
	return nil
}

// FetchInfoFromAliasKey collects alias information from an alias key
func FetchInfoFromAliasKey(key string) *AliasKey {
	var aliasKey AliasKey
	if err := aliasKey.ParseFrom(key); err != nil {
		log.GetLogger().Errorf("error while parsing an URN: %s", err.Error())
		return &AliasKey{}
	}
	return &aliasKey
}

// IsAlias distinguishes if a key is an alias key
func IsAlias(key string) bool {
	aliasKey := FetchInfoFromAliasKey(key)
	if aliasKey.AliasSign != DefaultAliasSign || aliasKey.TenantSign != DefaultTenantSign ||
		aliasKey.FunctionSign != DefaultFuncSign {
		log.GetLogger().Warnf("the aliasKey %s is in the wrong format", key)
		return false
	}
	return true
}

// BuildURNFromAliasKey builds a URN from a alias key
func BuildURNFromAliasKey(key string) string {
	aliasKey := FetchInfoFromAliasKey(key)
	productURN := &urnutils.BaseURN{
		ProductID:  urnutils.DefaultURNProductID,
		RegionID:   urnutils.DefaultURNRegion,
		BusinessID: aliasKey.BusinessID,
		TenantID:   aliasKey.TenantID,
		TypeSign:   urnutils.DefaultURNFuncSign,
		Name:       aliasKey.FunctionID,
		Version:    aliasKey.AliasName,
	}
	return productURN.String()
}
