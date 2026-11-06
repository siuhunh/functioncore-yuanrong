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
	"math"
	"strconv"
	"strings"

	"functioncore/pkg/common/logger/log"
)

const (
	packetIDSize  = 36
	formatBase    = 10
	formatBitSize = 64
)

var (
	minPadding = packetIDSize - len(strconv.FormatUint(math.MaxUint64, formatBase))
)

func genPacketID(id uint64) string {
	idStr := strconv.FormatUint(id, formatBase)
	// Max uint64 has 20 digits, so we should never get a negative 'n'
	n := packetIDSize - len(idStr)

	var b strings.Builder
	b.Grow(packetIDSize)

	// Pat '0's at front
	b.WriteString("0")
	for b.Len() < n {
		if b.Len() <= n>>1 {
			b.WriteString(b.String())
		} else {
			b.WriteString(b.String()[:n-b.Len()])
			break
		}
	}

	b.WriteString(idStr)
	return b.String()
}

func parsePacketID(s string) uint64 {
	// NOTE: 0 is not a valid id
	i := 0
	for ; i < len(s); i++ {
		if s[i] != '0' {
			break
		}
	}
	if i < minPadding || i >= len(s) {
		return 0
	}

	s = s[i:]

	n, err := strconv.ParseUint(s, formatBase, formatBitSize)
	if err != nil {
		log.GetLogger().Errorf("invalid packet id %s, %s", s, err.Error())
		return 0
	}
	return n
}
