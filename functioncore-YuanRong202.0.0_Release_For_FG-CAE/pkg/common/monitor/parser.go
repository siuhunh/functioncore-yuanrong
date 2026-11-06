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

package monitor

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"functioncore/pkg/common/strutil"
)

const (
	rBpsIndex = 0
	rPpsIndex = 1
	tBpsIndex = 8
	tPpsIndex = 9

	defaultFieldLen   = 2
	fieldLenWithOutOp = 3
	fieldLenWithOp    = 4

	netColonSplitLen = 2

	netCardEth0 = "eth0"

	spaceSplitRune = ' '
	colonSplitRune = ':'

	cgroupCPUPath    = "/sys/fs/cgroup/cpuacct/cpuacct.usage"
	cgroupMemoryPath = "/sys/fs/cgroup/memory/memory.stat"
	systemCPUPath    = "/proc/stat"
	netPath          = "/proc/net/dev"
	fileSystemPath   = "/sys/fs/cgroup/blkio/blkio.throttle.io_service_bytes_recursive"
)

var (
	rssPrefix  = []byte("rss ")
	cpuPrefix  = []byte("cpu  ")
	spaceSplit = []byte(" ")
	colonSplit = []byte(":")
)

// NewCGroupCPUParser creates parser of /sys/fs/cgroup/cpuacct/cpuacct.usage
func NewCGroupCPUParser() (*Parser, error) {
	return NewParser(cgroupCPUPath, cgroupCPUParserFunc)
}

// NewSystemCPUParser creates parser of /proc/stat
func NewSystemCPUParser() (*Parser, error) {
	return NewParser(systemCPUPath, systemCPUParserFunc)
}

// NewCGroupMemoryParser creates parser of /sys/fs/cgroup/memory/memory.stat
func NewCGroupMemoryParser() (*Parser, error) {
	return NewParser(cgroupMemoryPath, cgroupMemoryParserFunc)
}

// NewCGroupMemoryParserWithPath creates parser with cgroupMemoryParserFunc
func NewCGroupMemoryParserWithPath(path string) (*Parser, error) {
	return NewParser(path, cgroupMemoryParserFunc)
}

// NewProcMemoryParser creates parser of /proc/{pid}/statm
// The parsed value should * pageSize to get actual memory used in bytes.
// pageSize is usually 4096 and can get by os.Getpagesize()
func NewProcMemoryParser(pid string) (*Parser, error) {
	return NewParser("/proc/"+pid+"/statm", procMemoryParserFunc)
}

// NewNetParser creates parser of /proc/net/dev
func NewNetParser() (*Parser, error) {
	// return val , [4]int, 0 receiveBps 1 receivePps, 2 TransmitBps, 3 TransmitPps
	return NewParser(netPath, func(reader *bufio.Reader) (interface{}, error) {
		netData := &NetData{}
		for {
			lineBytes, _, err := reader.ReadLine()
			if errors.Is(err, io.EOF) {
				return netData, fmt.Errorf("failed to get net data of %s from %s", netCardEth0, netPath)
			}

			if err != nil {
				return netData, err
			}

			if !bytes.Contains(lineBytes, colonSplit) {
				continue
			}

			fields := bytes.Split(lineBytes, colonSplit)
			if len(fields) < netColonSplitLen {
				continue
			}

			iFace := bytes.TrimSpace(fields[0])
			if string(iFace) == netCardEth0 {
				data := bytes.Fields(fields[1])
				if len(data) < tPpsIndex+1 {
					return netData, fmt.Errorf("failed to parse net data because len not right, len: %d", len(data))
				}
				receiveBytes, err := strconv.ParseUint(strutil.BytesToString(data[rBpsIndex]), base, bitSize)
				if err != nil {
					return netData, err
				}
				receivePackage, err := strconv.ParseUint(strutil.BytesToString(data[rPpsIndex]), base, bitSize)
				if err != nil {
					return netData, err
				}
				transmitBytes, err := strconv.ParseUint(strutil.BytesToString(data[tBpsIndex]), base, bitSize)
				if err != nil {
					return netData, err
				}
				transmitPackage, err := strconv.ParseUint(strutil.BytesToString(data[tPpsIndex]), base, bitSize)
				if err != nil {
					return netData, err
				}
				netData.ReceiveBytes = receiveBytes
				netData.ReceivePackage = receivePackage
				netData.TransmitBytes = transmitBytes
				netData.TransmitPackage = transmitPackage
				return netData, nil
			}

		}
	})
}

// NewFileSystemParser creates parser of /sys/fs/cgroup/blkio/blkio.throttle.io_service_bytes_recursive
func NewFileSystemParser() (*Parser, error) {
	// return val , [4]int, 0 receiveBps 1 receivePps, 2 TransmitBps, 3 TransmitPps
	return NewParser(fileSystemPath, fileSystemParserFunc)
}

func fileSystemParserFunc(reader *bufio.Reader) (interface{}, error) {
	blkioStats := InitBlkioStateEntrys()

	for {
		lineBytes, _, err := reader.ReadLine()
		if errors.Is(err, io.EOF) {
			return blkioStats, nil
		}

		if err != nil {
			return blkioStats, err
		}

		fields := bytes.FieldsFunc(lineBytes, func(r rune) bool {
			return r == spaceSplitRune || r == colonSplitRune
		})

		if len(fields) < fieldLenWithOutOp {
			continue
		}

		major, err := strconv.ParseUint(strutil.BytesToString(fields[0]), base, bitSize)
		if err != nil {
			return blkioStats, err
		}

		minor, err := strconv.ParseUint(strutil.BytesToString(fields[1]), base, bitSize)
		if err != nil {
			return blkioStats, err
		}

		op := ""
		valueField := defaultFieldLen
		if len(fields) == fieldLenWithOp {
			op = string(fields[2])
			valueField = fieldLenWithOutOp
		}

		val, err := strconv.ParseUint(strutil.BytesToString(fields[valueField]), base, bitSize)
		if err != nil {
			return blkioStats, err
		}

		blkioStats.Append(BlkioStatEntry{Major: major, Minor: minor, Op: op, Value: val})
	}
}

var procMemoryParserFunc = func(reader *bufio.Reader) (interface{}, error) {
	line, _, err := reader.ReadLine()
	if err != nil {
		return uint64(0), err
	}
	l := len(line)
	st, ed := 0, 0
	for st < l && line[st] != ' ' {
		st++
	}
	for st < l && line[st] == ' ' {
		st++
	}
	ed = st
	for ed < l && line[ed] != ' ' {
		ed++
	}
	if ed < l {
		return strconv.ParseUint(strutil.BytesToString(line[st:ed]), 10, 64)
	}
	return uint64(0), errors.New("invalid statm fields")
}

var cgroupCPUParserFunc = func(reader *bufio.Reader) (interface{}, error) {
	line, _, err := reader.ReadLine()
	if err != nil {
		return uint64(0), err
	}
	return strconv.ParseUint(strutil.BytesToString(line), 10, 64)
}

var systemCPUParserFunc = func(reader *bufio.Reader) (interface{}, error) {
	for {
		lineBytes, _, err := reader.ReadLine()
		if err != nil {
			return uint64(0), err
		}

		var total uint64

		if bytes.HasPrefix(lineBytes, cpuPrefix) {
			lineBytes = bytes.TrimSpace(lineBytes[len(cpuPrefix):])
			slice := bytes.Split(lineBytes, spaceSplit)
			for i := range slice {
				parseValue, err := strconv.ParseUint(strutil.BytesToString(slice[i]), 10, 64)
				if err != nil {
					break
				}
				total += parseValue
			}
			return total, nil
		}
	}
}

var cgroupMemoryParserFunc = func(reader *bufio.Reader) (interface{}, error) {
	for {
		lineBytes, _, err := reader.ReadLine()
		if err != nil {
			return uint64(0), err
		}

		if bytes.HasPrefix(lineBytes, rssPrefix) {
			lineBytes = bytes.TrimSpace(lineBytes[len(rssPrefix):])
			return strconv.ParseUint(strutil.BytesToString(lineBytes), 10, 64)
		}
	}
}

// ParserFunc func that parser content of reader to uint64
type ParserFunc func(reader *bufio.Reader) (interface{}, error)

// NewParser creates new Parser with file path and ParserFunc
func NewParser(path string, parserFunc ParserFunc) (*Parser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Parser{
		f:      f,
		reader: bufio.NewReader(nil),
		parser: parserFunc,
	}, nil
}

type nopCloser struct {
	io.ReadSeeker
}

// Close does nothing. It wraps io.ReadSeeker to io.ReadSeekCloser
func (nopCloser) Close() error { return nil }

// NewReadSeekerParser creates new Parser with io.ReadSeeker and ParserFunc
func NewReadSeekerParser(reader io.ReadSeeker, parserFunc ParserFunc) *Parser {
	return &Parser{
		f:      nopCloser{reader},
		reader: bufio.NewReader(nil),
		parser: parserFunc,
	}
}

// Parser aims to parse file content that updated frequently (such as cgroup file) with high performance.
// It opens file only once and seek to start every time before read.
// NOTICE: Parser is not thread safe
type Parser struct {
	reader *bufio.Reader
	f      io.ReadSeekCloser
	parser ParserFunc
}

// Close closes file to parse
func (p *Parser) Close() error {
	p.reader.Reset(nil)
	return p.f.Close()
}

// Read resets reader to the start of the file and parses it.
// This method is not thread safe
func (p *Parser) Read() (interface{}, error) {
	_, err := p.f.Seek(0, io.SeekStart)
	if err != nil {
		return uint64(0), err
	}
	p.reader.Reset(p.f)
	return p.parser(p.reader)
}
