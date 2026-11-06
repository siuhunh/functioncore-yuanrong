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

package timeutil

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const unixMillisecondLength = 13

func TestNowUnixMillisecond(t *testing.T) {
	assert.Equal(t, unixMillisecondLength, len(strconv.FormatInt(NowUnixMillisecond(), 10)))
}

func TestUnixMillisecond(t *testing.T) {
	now := time.Now()
	assert.Equal(t, unixMillisecondLength, len(strconv.FormatInt(UnixMillisecond(now), 10)))
}
