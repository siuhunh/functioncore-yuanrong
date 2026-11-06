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

package cluster

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/logger/log"
)

func TestSingleNode(t *testing.T) {
	singleNodeCluster, _ := NewSingleNodeCluster(":8087", nil, &sync.WaitGroup{}, log.GetLogger())

	singleNodeCluster.SetCallback(startWatch)
	err := singleNodeCluster.Discovery()
	assert.Error(t, err)

	singleNodeCluster.SetCallback(emptyCallBack)
	err = singleNodeCluster.Discovery()
	assert.NoError(t, err)

	singleNodeCluster.ForwardRequest(nil, nil)

	leader := singleNodeCluster.Leader()
	assert.NotEmpty(t, leader)

	singleNodeCluster.role = "a"
	leader = singleNodeCluster.Leader()
	assert.Empty(t, leader)

	role := singleNodeCluster.Role()
	assert.Equal(t, role, "a")
}

func startWatch(stopper <-chan struct{}) error {
	return errors.New("err")
}

func emptyCallBack(stopper <-chan struct{}) error {
	return nil
}
