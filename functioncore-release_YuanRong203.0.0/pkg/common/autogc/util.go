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

package autogc

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const (
	cgroupMemLimitPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	rssValueFieldIndex = 1
	base               = 10
	bitSize            = 64
)

// constants of memory unit
const (
	B = 1 << (10 * iota)
	KB
	MB
	GB
)

var (
	pageSize = uint64(os.Getpagesize())
	memPath  = fmt.Sprintf("/proc/%d/statm", os.Getpid())
)

func parseCGroupMemoryLimit() (uint64, error) {
	v, err := ioutil.ReadFile(cgroupMemLimitPath)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(v)), base, bitSize)
}

func parseRSS(f io.ReadSeeker, buffer []byte) (uint64, error) {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}
	_, err = f.Read(buffer)
	if err != nil && err != io.EOF {
		return 0, err
	}
	fields := strings.Split(string(buffer), " ")
	if len(fields) < (rssValueFieldIndex + 1) {
		return 0, errors.New("invalid statm fields")
	}
	rss, err := strconv.ParseUint(fields[rssValueFieldIndex], base, bitSize)
	if err != nil {
		return 0, err
	}
	return rss * pageSize, nil
}
