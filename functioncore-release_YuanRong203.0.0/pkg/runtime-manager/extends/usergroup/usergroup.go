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

// Package usergroup is used for setting the file user group
package usergroup

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
	"strings"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/runtime-manager/utils"
)

const (
	minIDValueAndDefault = -1
	maxIDValue           = 65535
	groupTokenSize       = 4
	groupGidPos          = 2
	groupUsernamesPos    = 3
)

// CheckID -
func CheckID(uid, gid int) (int, int, error) {
	if uid == 0 || uid == minIDValueAndDefault {
		uid = constants.DefaultRuntimeUID
	}
	if gid == 0 || gid == minIDValueAndDefault {
		gid = constants.DefaultRuntimeGID
	}
	if uid == constants.DefaultRuntimeUID && gid == constants.DefaultRuntimeGID {
		return uid, gid, nil
	}
	if uid < minIDValueAndDefault || uid > maxIDValue {
		return 0, 0, fmt.Errorf("invalid input UID %d", uid)
	}
	if gid < minIDValueAndDefault || gid > maxIDValue {
		return 0, 0, fmt.Errorf("invalid input GID %d", gid)
	}
	return uid, gid, CreateMountUser(uid, gid)
}

// CreateMountUser -
func CreateMountUser(uid, gid int) error {
	uidStr := strconv.Itoa(uid)
	gidStr := strconv.Itoa(gid)
	defaultRuntimeGIDStr := strconv.Itoa(constants.DefaultRuntimeGID)
	defaultWorkerGIDStr := strconv.Itoa(constants.DefaultWorkerGID)

	uName, err := createUserIfNotExist(uidStr, gidStr)
	if err != nil {
		log.GetLogger().Errorf("failed to create user if not exist, uid: %s, gid: %s, error: %s",
			uidStr, gidStr, err.Error())
		return fmt.Errorf("create user if not exist, uid %s, gid %s", uidStr, gidStr)
	}
	if err = createGroupIfNotExist(gidStr); err != nil {
		log.GetLogger().Errorf("failed to create group if not exist, gid %s, error: %s", gidStr, err.Error())
		return fmt.Errorf("create group if not exist, gid %s", gidStr)
	}

	if err = addUserToGroup(defaultRuntimeGIDStr, uName); err != nil {
		log.GetLogger().Errorf("failed to add user %s(%s) to default runtime gid, error: %s",
			uName, uidStr, err.Error())
		return fmt.Errorf("add user to default runtime gid")
	}

	if err = addUserToGroup(defaultWorkerGIDStr, uName); err != nil {
		log.GetLogger().Errorf("failed to add user %s(%s) to default worker gid, error: %s",
			uName, uidStr, err.Error())
		return fmt.Errorf("add user to default worker gid")
	}

	return nil
}

func createUserIfNotExist(uidStr, gidStr string) (string, error) {
	var uName string
	if plainUser, err := user.LookupId(uidStr); err != nil {
		uName = "user" + uidStr
		if err := utils.AppendToFile("/etc/passwd",
			fmt.Sprintf("%s:*:%s:%s::/home/%s:/sbin/nologin\n", uName, uidStr, gidStr, uName)); err != nil {
			return "", err
		}

		log.GetLogger().Infof("create new user: %s", uName)
	} else {
		uName = plainUser.Username
	}

	return uName, nil
}

func createGroupIfNotExist(gidStr string) error {
	if _, err := user.LookupGroupId(gidStr); err != nil {
		gName := "group" + gidStr
		if err := utils.AppendToFile("/etc/group", fmt.Sprintf("%s:x:%s:\n", gName, gidStr)); err != nil {
			return err
		}

		log.GetLogger().Infof("create new group: %s", gName)
	}
	return nil
}

func addUserToGroup(gidStr string, uName string) error {
	lines, err := fileToLines("/etc/group")
	if err != nil {
		return err
	}

	for i, line := range lines {
		tokens := strings.Split(strings.TrimSpace(line), ":")
		if len(tokens) != groupTokenSize || tokens[groupGidPos] != gidStr {
			continue
		}

		if tokens[groupUsernamesPos] == "" {
			tokens[groupUsernamesPos] = uName
		} else {
			users := strings.Split(tokens[groupUsernamesPos], ",")
			if exist(users, uName) {
				return nil
			}
			users = append(users, uName)
			tokens[groupUsernamesPos] = strings.Join(users, ",")
		}

		lines[i] = strings.Join(tokens, ":")
	}

	if err := ioutil.WriteFile("/etc/group", []byte(strings.Join(lines, "\n")+"\n"), 0); err != nil {
		return err
	}

	log.GetLogger().Infof("add user %s to groupId %s", uName, gidStr)

	return nil
}

func fileToLines(fileName string) ([]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var res []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		res = append(res, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func exist(arr []string, item string) bool {
	for _, v := range arr {
		if item == v {
			return true
		}
	}
	return false
}

// ChownRuntimeLogFile  chown runtime log file by given uid gid
func ChownRuntimeLogFile(uid, gid int) error {
	logfiles := [...]string{"/home/" + constants.DefaultRuntimeUName + "/log/nodejs-runtime.log",
		"/home/" + constants.DefaultRuntimeUName + "/log/python-runtime.log",
		"/home/" + constants.DefaultRuntimeUName + "/log/java-runtime.log",
		"/home/" + constants.DefaultRuntimeUName + "/log/cangjie-runtime.log"}
	for _, logFile := range logfiles {
		if _, err := os.Stat(logFile); err != nil && !os.IsExist(err) {
			// log file is not exist
			continue
		}
		if err := os.Chown(logFile, uid, gid); err != nil {
			log.GetLogger().Errorf("failed to chown the log file: %s, error: %s", logFile, err.Error())
			return err
		}
	}
	return nil
}
