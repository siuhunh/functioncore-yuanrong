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

// Package network network is modifying the Worker Network
package network

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

const (
	netmaskLen = 8
	base       = 16
	bitSize    = 0
	hexbit     = 2
)

// ExecuteShellCommandWithCombinedOutput run the command and return the information in the stream.
func ExecuteShellCommandWithCombinedOutput(name string, arg ...string) (int, string, error) {
	ret := -1
	result := ""
	cmd := exec.Command(name, arg...)
	var outBuffer bytes.Buffer
	cmd.Stdout = &outBuffer
	cmd.Stderr = &outBuffer
	err := cmd.Start()
	if err != nil {
		errOutput, err := cmd.CombinedOutput()
		if err != nil {
			return ret, string(errOutput), err
		}
		return ret, result, err
	}

	err = cmd.Wait()
	if err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code != 0
			// There is no plattform independent way to retrieve
			// the exit code, but the following will work on Unix
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				ret = status.ExitStatus()
				return ret, strings.TrimRight(outBuffer.String(), "\n"), nil
			}
		}
		return ret, strings.TrimRight(outBuffer.String(), "\n"), err
	}

	return 0, strings.TrimRight(outBuffer.String(), "\n"), err
}

// ExecuteShellCommandLineWithCombinedOutput run the command and return the flow information
// The input parameter has only one command
func ExecuteShellCommandLineWithCombinedOutput(cmdline string) (int, string, error) {
	args := Split(cmdline, " ")
	var cmdArgs []string
	cmdName := args[0]
	if len(args) > 1 {
		cmdArgs = args[1:]
	} else {
		cmdArgs = make([]string, 0)
	}

	return ExecuteShellCommandWithCombinedOutput(cmdName, cmdArgs...)
}

// Split command line arguments
func Split(src string, delimeter string) []string {
	result := make([]string, 0, 0)
	if len(strings.TrimSpace(src)) == 0 {
		return result
	}

	toks := strings.Split(src, delimeter)
	for _, tok := range toks {
		trimedTok := strings.TrimSpace(tok)
		if len(trimedTok) == 0 {
			continue
		}

		result = append(result, trimedTok)
	}

	return result
}

// GetIntfNameAndNetmaskOfIp obtain the network adapter name and subnet mask based on the entered IP address.
func GetIntfNameAndNetmaskOfIp(ip string) (string, string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", "", err
	}

	intfName, netmask, err := getIntfNameAndNetMask(ip, ifaces)
	if err != nil {
		return intfName, netmask, err
	}

	if intfName == "" {
		return intfName, "", fmt.Errorf("get net interface of ip '%s' failed", ip)
	}

	return intfName, netmask, nil
}

func getIntfNameAndNetMask(ip string, ifaces []net.Interface) (string, string, error) {
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}

		intfName, netmask, err := parsesIfaces(ip, addrs, i)
		if err != nil {
			return "", "", err
		}

		if intfName != "" && netmask != "" {
			return intfName, netmask, nil
		}
	}
	return "", "", fmt.Errorf("failed to found intfName and netmask")
}

func parsesIfaces(ip string, addrs []net.Addr, i net.Interface) (string, string, error) {
	netmask := ""
	intfName := ""
	var err error
	for _, a := range addrs {
		addr := ""
		hexMask := ""
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			addr = ipnet.IP.String()
			hexMask = ipnet.Mask.String()
		}

		if addr == ip {
			intfName = i.Name
			netmask, err = getNetMask(hexMask)
			if err != nil {
				return intfName, netmask, fmt.Errorf("get netmask failed, not a valid hex number: %s",
					hexMask)
			}
			return intfName, netmask, nil
		}
	}
	return intfName, netmask, nil
}

func getNetMask(hexMask string) (string, error) {
	if len(hexMask) != netmaskLen {
		return "", fmt.Errorf("failed to get netmask of the input hex value: %s", hexMask)
	}
	netmask := ""
	// netmask is a hexadecimal character string (for example, fffffe00). It is a string of 8
	// characters separated by two digits and converted into a decimal int.
	for i := 0; i < 8; i += 2 {
		seg := hexMask[i : i+hexbit]
		segNum, err := strconv.ParseInt(seg, base, bitSize)
		if err != nil {
			return netmask, err
		}
		netmask = netmask + strconv.Itoa(int(segNum)) + "."
	}
	return netmask[:len(netmask)-1], nil
}

// Match -
func Match(r *regexp.Regexp, src string) (*MatchedResult, error) {
	match := r.FindStringSubmatch(src)
	if match == nil {
		return nil, fmt.Errorf("match err")
	}

	if len(match) == 0 {
		// non-group match mode
		return nil, nil
	}

	result := newMatchedResult()
	for idx, name := range r.SubexpNames() {
		result.set(idx, name, match[idx])
	}

	return result, nil
}

// MatchedResult -
type MatchedResult struct {
	values []string
	names  map[string]int
}

func newMatchedResult() *MatchedResult {
	return &MatchedResult{
		names:  make(map[string]int),
		values: make([]string, 0),
	}
}

func (r *MatchedResult) set(index int, name, value string) {
	r.names[name] = index
	r.values = append(r.values, value)
}

// Group -
func (r *MatchedResult) Group(group string) string {
	if index, ok := r.names[group]; ok && index < len(r.values) {
		return r.values[index]
	}

	return ""
}
