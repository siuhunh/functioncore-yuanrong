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
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
)

func buildTestFile() string {
	path, _ := os.Getwd()
	if strings.Contains(path, "\\") {
		path = path + "\\test.json"
	} else {
		path = path + "/test.json"
	}

	return path
}

func TestInitFileWatcher(t *testing.T) {
	convey.Convey("TestInitFileWatcher", t, func() {
		stopCh := make(chan struct{}, 1)
		watcher, err := CreateFileWatcher(stopCh)
		convey.So(err, convey.ShouldBeNil)

		filename := buildTestFile()
		handler, _ := os.Create(filename)
		defer func() {
			handler.Close()
			os.Remove(filename)
		}()
		callbackChan := make(chan int, 5)
		watcher.RegisterCallback("", nil)
		watcher.RegisterCallback(filename, func(filename string, t OpType) {
			callbackChan <- 1
		})

		os.WriteFile(filename, []byte{'a'}, os.ModePerm)
		res := <-callbackChan
		convey.So(res, convey.ShouldBeGreaterThan, 0)
		time.Sleep(5 * time.Millisecond)
		close(stopCh)
	})
}

func TestInitFileWatcherWithInvalidCreator(t *testing.T) {
	convey.Convey("TestInitFileWatcherWithInvalidCreator", t, func() {
		defer SetCreator(createDefaultFileWatcher)
		SetCreator(func(stopCh <-chan struct{}) (FileWatcher, error) {
			return nil, errors.New("error")
		})

		stopCh := make(chan struct{}, 1)
		_, err := CreateFileWatcher(stopCh)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestFileWatcher_Start(t *testing.T) {
	convey.Convey("TestFileWatcher_Start", t, func() {
		stopCh := make(chan struct{}, 1)
		watcher, _ := CreateFileWatcher(stopCh)
		filename := "./TestFileWatcher_Start.tmp"
		f, _ := os.Create(filename)
		f.Close()
		tmp := hashRetry
		hashRetry = 1
		defer func() {
			hashRetry = tmp
		}()
		callbackChan := make(chan int, 1)
		watcher.RegisterCallback(filename, func(filename string, t OpType) {
			callbackChan <- 1
		})
		os.Remove(filename)
		time.AfterFunc(3*time.Second, func() {
			close(callbackChan)
		})
		convey.So(<-callbackChan, convey.ShouldEqual, 1)
		time.Sleep(50 * time.Millisecond)
		close(stopCh)
	})
}
