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

// Package oom is to judge if out of memory
package oom

import (
	"os"
	"os/exec"
	"strings"
	"sync/atomic"

	"functioncore/pkg/common/logger/log"
)

var (
	oomDetection int32
	oldPID       = ""
)

// StartOOMDetection initiates OOM
func StartOOMDetection() {
	log.GetLogger().Infof("start to detect oom event")
	atomic.StoreInt32(&oomDetection, 1)
}

// IsRuntimeOOMKilled check dmesg
func IsRuntimeOOMKilled() bool {
	if start := atomic.LoadInt32(&oomDetection); start != 0 {
		return checkRuntimeOOMInDmesg()
	}
	log.GetLogger().Infof("oom detection is not enabled")
	return false
}

func checkRuntimeOOMInDmesg() bool {
	rawResult, err := exec.Command("sudo", "/bin/bash", "-c", "dmesg -T | tail -100").Output()
	if err != nil {
		log.GetLogger().Errorf("failed to get 'dmesg' result: %s", err.Error())
		return false
	}
	result := string(rawResult)

	podID := os.Getenv("POD_ID")
	if podID == "" {
		log.GetLogger().Warnf("failed to find POD_ID in environment")
	}
	killMessage := "killed as a result of limit of /kubepods/burstable/pod" + podID
	killMessageEuler29 := "oom_memcg=/kubepods/burstable/pod" + podID
	_, found := findAndMoveForward(result, killMessage)
	_, foundEuler29 := findAndMoveForward(result, killMessageEuler29)
	if !found && !foundEuler29 {
		return false
	}

	lines := strings.Split(result, "\n")
	for _, line := range lines {
		log.GetLogger().Infof("dmesg: %s", line)
	}
	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]
		_, found = findAndMoveForward(line, "Memory cgroup out of memory")
		if found {
			newPID := findPid(line)
			if oldPID != newPID {
				oldPID = newPID
				log.GetLogger().Infof("found oom event for pid: %s", newPID)
				return true
			}
			log.GetLogger().Infof("ignore oom event for a previous generation of the runtime. pid: %s", newPID)
			return false
		}
	}
	return false
}

func findPid(line string) string {
	beginStr := "process "
	beginIndex := strings.Index(line, beginStr)
	endIndex := strings.Index(line, " (")
	return line[beginIndex+len(beginStr) : endIndex]
}

func findAndMoveForward(s string, substr string) (string, bool) {
	idx := strings.Index(s, substr)
	if idx == -1 {
		return "", false
	}
	return s[idx+len(substr):], true
}
