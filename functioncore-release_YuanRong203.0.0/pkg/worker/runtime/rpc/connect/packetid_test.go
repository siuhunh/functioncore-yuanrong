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

package connect

import (
	"functioncore/pkg/common/utils"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinPacketID(t *testing.T) {
	s := genPacketID(1)
	assert.Equal(t, packetIDSize, len(s))
	i := parsePacketID(s)
	assert.Equal(t, uint64(1), i)
}

func TestMaxPacketID(t *testing.T) {
	s := genPacketID(math.MaxUint64)
	assert.Equal(t, packetIDSize, len(s))
	i := parsePacketID(s)
	assert.Equal(t, uint64(math.MaxUint64), i)
}

func TestParseInvalidPacketID(t *testing.T) {
	{
		s := utils.UniqueID()
		i := parsePacketID(s)
		assert.Equal(t, uint64(0), i)
	}
	{
		s := genPacketID(0)
		i := parsePacketID(s)
		assert.Equal(t, uint64(0), i)
	}
	{
		s := "00abc"
		i := parsePacketID(s)
		assert.Equal(t, uint64(0), i)
	}
	{
		s := strings.Repeat("0", packetIDSize-1) + "a"
		i := parsePacketID(s)
		assert.Equal(t, uint64(0), i)
	}
}
