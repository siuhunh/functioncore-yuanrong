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
	"io"
	"os"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestNewReadSeekerParser(t *testing.T) {
	tests := []struct {
		name     string
		content  []byte
		parser   ParserFunc
		hasError bool
		expected uint64
	}{
		{
			name:     "parse cgroup cpu",
			content:  []byte("22299540985388\n"),
			parser:   cgroupCPUParserFunc,
			expected: 22299540985388,
		},
		{
			name:     "parse cgroup cpu error",
			content:  []byte("cpu1234\n"),
			parser:   cgroupCPUParserFunc,
			hasError: true,
		},
		{
			name:     "parse cgroup cpu error nil content",
			content:  nil,
			parser:   cgroupCPUParserFunc,
			hasError: true,
		},
		{
			name: "parse system cpu",
			content: []byte(`cpu  5780067 1681 2646772 128805915 297607 618355 556468 0 0 0
cpu0 469119 76 177059 7812682 27157 49194 129575 0 0 0
cpu1 322142 195 142022 8110571 12824 26876 69154 0 0 0
cpu2 470441 85 178526 7900292 27494 48920 39433 0 0 0
cpu3 316565 189 142730 8162210 13452 26679 22773 0 0 0
cpu4 459355 80 176236 7916929 25231 49931 34672 0 0 0
cpu5 318449 190 142327 8167630 12096 26906 16533 0 0 0
cpu6 465011 89 179408 7907609 26154 49038 37196 0 0 0
cpu7 343302 184 144611 8135769 12422 27203 20443 0 0 0
cpu8 396382 41 182999 7964931 24674 50941 30899 0 0 0
cpu9 263259 98 157896 8203376 8993 27069 16007 0 0 0
cpu10 388169 43 183953 7978206 21625 50665 29197 0 0 0
cpu11 263461 108 158949 8203086 9028 27189 15104 0 0 0
cpu12 386860 43 183617 7976278 22802 50729 31620 0 0 0
cpu13 265805 121 159735 8200066 9669 27116 15077 0 0 0
cpu14 386661 43 184164 7974042 23518 51694 30883 0 0 0
cpu15 265079 88 152532 8192230 20462 28196 17896 0 0 0
intr 2372718057 57 9 0 0 0 40 0 0 0 0 8717 49 15 0 0 0 0 0 0 0 0 0 0 0 0 0 24 3506190 0 2057418 30 1660203 25 1669371 22 2216227 33 0 2609974 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
ctxt 3724705417
btime 1680055284
processes 10144359
procs_running 1
procs_blocked 0
softirq 927618000 0 392545892 3328 109690633 6644760 0 2368 181614088 346 237116585`),
			parser:   systemCPUParserFunc,
			expected: 138706865,
		},
		{
			name:     "parse system cpu error no such line",
			content:  []byte(`5780067 1681 2646772 128805915 297607 618355 556468 0 0 0`),
			parser:   systemCPUParserFunc,
			hasError: true,
		},
		{
			name:     "parse system cpu, digit error",
			content:  []byte(`cpu  test800671681 2646772 128805915 297607 618355 556468 0 0 0`),
			parser:   systemCPUParserFunc,
			expected: 0,
		},
		{
			name: "parse cgroup memory",
			content: []byte(`cache 10150707200
rss 880640
rss_huge 0
shmem 0
mapped_file 946176
dirty 135168
writeback 270336
swap 0
pgpgin 3158595
pgpgout 680215
pgfault 992277
pgmajfault 0
inactive_anon 0
active_anon 0
inactive_file 8343023616
active_file 1808744448
unevictable 0
hierarchical_memory_limit 9223372036854771712
hierarchical_memsw_limit 9223372036854771712
total_cache 21492334592
total_rss 9384980480
total_rss_huge 5515509760
total_shmem 654385152
total_mapped_file 2744586240
total_dirty 8110080
total_writeback 2027520
total_swap 0
total_pgpgin 1336448421
total_pgpgout 1354048239
total_pgfault 1405894809
total_pgmajfault 50622
total_inactive_anon 199806976
total_active_anon 8360579072
total_inactive_file 19150966784
total_active_file 3246854144
total_unevictable 0`),
			parser:   cgroupMemoryParserFunc,
			expected: 880640,
		},
		{
			name:     "parse cgroup memory no such line",
			content:  []byte(`880640`),
			parser:   cgroupMemoryParserFunc,
			hasError: true,
		},
		{
			name:     "parse proc memory",
			content:  []byte("53473 4841 438 7 0 130 0"),
			parser:   procMemoryParserFunc,
			expected: 4841,
		},
		{
			name:     "parse proc memory read nil",
			content:  nil,
			parser:   procMemoryParserFunc,
			hasError: true,
		},
		{
			name:     "parse proc memory no field",
			content:  []byte("53473"),
			parser:   procMemoryParserFunc,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewReadSeekerParser(bytes.NewReader(tt.content), tt.parser)
			data, err := parser.Read()
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tt.expected, data)
			}
			assert.Nil(t, parser.Close())
		})
	}
}

func TestParser_ContentChange(t *testing.T) {
	fileName := "parser_tmp"
	f, err := os.Create(fileName)
	if err != nil {
		t.Error("failed to create file")
		return
	}
	defer os.Remove(fileName)
	defer f.Close()

	parser, err := NewParser(fileName, cgroupCPUParserFunc)
	if err != nil {
		t.Error("failed to create parser")
		return
	}
	defer parser.Close()

	f.Write([]byte("123"))
	data, _ := parser.Read()
	assert.Equal(t, uint64(123), data)

	f.Write([]byte("456"))
	data, _ = parser.Read()
	assert.Equal(t, uint64(123456), data)

	f.Truncate(0)
	f.Seek(0, io.SeekStart)
	f.Write([]byte("456"))
	data, _ = parser.Read()
	assert.Equal(t, uint64(456), data)
}

func TestNewNetParser(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			"test new net paeser",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNetParser()
			if (err != nil) != tt.wantErr {
				t.Errorf("failed to new net parser")
			}
		})
	}
}

func TestNewFileSystemParser(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			"test new net paeser",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFileSystemParser()
			if (err != nil) != tt.wantErr {
				t.Errorf("failed to new file system parser")
			}
		})
	}
}

func TestNewSystemCPUParser(t *testing.T) {
	_, err := NewSystemCPUParser()
	assert.Equal(t, true, err == nil)
}

func TestNewCGroupCPUParser(t *testing.T) {
	_, err := NewCGroupCPUParser()
	assert.Equal(t, true, err == nil)
}

func TestNewProcMemoryParser(t *testing.T) {
	_, err := NewProcMemoryParser("error pid")
	assert.Equal(t, true, err != nil)
}

func TestNewCGroupMemoryParserWithPath(t *testing.T) {
	_, err := NewCGroupMemoryParserWithPath("error path")
	assert.Equal(t, true, err != nil)
}

func TestNetParserFunc(t *testing.T) {
	convey.Convey("TestNetParserFunc ok", t, func() {
		reader := &bufio.Reader{}
		defer gomonkey.ApplyMethod(reader, "ReadLine", func(b *bufio.Reader) (line []byte, isPrefix bool, err error) {
			return []byte("eth0:1 2 3 4 5 6 7 8 9 10:xxx"), false, nil
		}).Reset()
		netDataIf, err := netParserFunc(reader)
		convey.So(err, convey.ShouldBeNil)
		netData, ok := netDataIf.(*NetData)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(netData.ReceiveBytes, convey.ShouldEqual, 1)
		convey.So(netData.ReceivePackage, convey.ShouldEqual, 2)
		convey.So(netData.TransmitBytes, convey.ShouldEqual, 9)
		convey.So(netData.TransmitPackage, convey.ShouldEqual, 10)
	})
	convey.Convey("TestNetParserFunc eof error", t, func() {
		reader := &bufio.Reader{}
		defer gomonkey.ApplyMethod(reader, "ReadLine", func(b *bufio.Reader) (line []byte, isPrefix bool, err error) {
			return []byte("eth0:1 2 3 4 5 6 7 8 9 10:xxx"), false, io.EOF
		}).Reset()
		_, err := netParserFunc(reader)
		convey.So(strings.Contains(err.Error(), "failed to get net data of"), convey.ShouldBeTrue)
	})
	convey.Convey("TestNetParserFunc eof error", t, func() {
		reader := &bufio.Reader{}
		defer gomonkey.ApplyMethod(reader, "ReadLine", func(b *bufio.Reader) (line []byte, isPrefix bool, err error) {
			return []byte("eth0:1 2 3 4 5 6:xxx"), false, nil
		}).Reset()
		_, err := netParserFunc(reader)
		convey.So(strings.Contains(err.Error(), "failed to parse net data because len not right"), convey.ShouldBeTrue)
	})
}

func TestFileSystemParserFunc(t *testing.T) {
	convey.Convey("TestFileSystemParserFunc ok", t, func() {
		reader := &bufio.Reader{}
		defer gomonkey.ApplyMethodSeq(reader, "ReadLine", []gomonkey.OutputCell{
			{
				Values: []interface{}{[]byte("0:1 2 3 4 5 6 7 8 9 10:xxx"), false, nil},
				Times:  0,
			},
			{
				Values: []interface{}{[]byte("eth0:1 2 3 4 5 6 7 8 9 10:xxx"), false, io.EOF},
				Times:  1,
			},
			{
				Values: []interface{}{[]byte("eth0:1 2 3 4 5 6 7 8 9 10:xxx"), false, io.EOF},
				Times:  2,
			},
		}).Reset()
		ioStatEntryIf, err := fileSystemParserFunc(reader)
		convey.So(err, convey.ShouldBeNil)
		ioStatEntry, ok := ioStatEntryIf.(BlkioStatEntrys)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(ioStatEntry[0].Major, convey.ShouldEqual, 0)
		convey.So(ioStatEntry[0].Minor, convey.ShouldEqual, 1)
		convey.So(ioStatEntry[0].Value, convey.ShouldEqual, 2)
	})
}
