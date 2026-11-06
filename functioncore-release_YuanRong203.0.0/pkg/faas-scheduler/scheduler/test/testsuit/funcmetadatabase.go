/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package testsuit

import (
	"errors"
	"sync"

	"functioncore/pkg/faas-scheduler/types"
)

// FuncMetaDatabase -
type FuncMetaDatabase struct {
	funcMetaCache sync.Map
}

func createFuncMetaDatabase() *FuncMetaDatabase {
	return &FuncMetaDatabase{
		funcMetaCache: sync.Map{},
	}
}

// AddFuncMeta -
func (database *FuncMetaDatabase) AddFuncMeta(funcKey string, info *types.FunctionMetaInfo) {
	database.funcMetaCache.Store(funcKey, info)
}

// FetchFuncMeta -
func (database *FuncMetaDatabase) FetchFuncMeta(funcKey string) (*types.FunctionMetaInfo, error) {
	infoIf, ok := database.funcMetaCache.Load(funcKey)
	if !ok {
		return nil, errors.New("funcMeta not exist in database")
	}
	info, _ := infoIf.(*types.FunctionMetaInfo)
	return info, nil
}
