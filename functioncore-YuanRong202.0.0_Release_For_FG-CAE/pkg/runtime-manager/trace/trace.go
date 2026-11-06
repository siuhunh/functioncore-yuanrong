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

// Package trace is runtime trace config
package trace

import (
	"bufio"
	"io"
	"os"
	"strings"

	"functioncore/pkg/common/logger/log"
)

const perm = 0666

// EditTraceConfig -
func EditTraceConfig(address, configFile string) error {
	var result string
	f, err := os.Open(configFile)
	if err != nil {
		log.GetLogger().Errorf("failed to open trace config file")
		return err
	}
	defer f.Close()
	buf := bufio.NewReader(f)
	logPathExist := false
	for {
		content, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		if strings.HasPrefix(string(content), "log.path") {
			logPathExist = true
		}
		if strings.Contains(string(content), "master.address") {
			result += "master.address=" + address + "\n"
		} else {
			result += string(content) + "\n"
		}
	}
	if !logPathExist {
		result = "log.path=/tmp" + "\n" + result
	}

	fw, err := os.OpenFile(configFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		log.GetLogger().Errorf("failed to open trace config file")
		return err
	}
	defer fw.Close()
	w := bufio.NewWriter(fw)
	_, err = w.WriteString(result)
	if err != nil {
		log.GetLogger().Errorf("failed to write trace config file: %s", err.Error())
		return err
	}
	err = w.Flush()
	if err != nil {
		log.GetLogger().Errorf("failed to flush trace config file: %s", err.Error())
		return err
	}
	return nil
}
