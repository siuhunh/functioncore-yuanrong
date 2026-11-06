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

package diskusage

import (
	"context"
	"errors"
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"functioncore/pkg/common/constants"
	"functioncore/test/utils"
)

const timeout = 500 * time.Millisecond

func Test_New(t *testing.T) {
	dir, err := ioutil.TempDir("", "*")
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "def"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "ignore"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "ignore", "ggg"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "def", "ghi"), os.ModePerm)
	require.NoError(t, err)
	err = ioutil.WriteFile(filepath.Join(dir, "abc", "aaa"), []byte("a"), os.ModePerm)
	require.NoError(t, err)

	hook := func(event fsnotify.Event) {
		fmt.Println(event)
	}

	monitor, err := New(dir, AddProcessHooks(hook), Ignore(filepath.Join(dir, "abc", "ignore")))
	require.NoError(t, err)
	time.Sleep(timeout)
	assert.Equal(t, int64(16385), monitor.Size())

	err = ioutil.WriteFile(filepath.Join(dir, "abc", "aaa"), []byte("bbb"), os.ModePerm)
	require.NoError(t, err)
	time.Sleep(timeout)
	assert.Equal(t, int64(16387), monitor.Size())

	// generate an fsnotify overflow error
	select {
	case monitor.watcher.Errors <- fsnotify.ErrEventOverflow:
	default:
		t.Errorf("failed to generate an fsnotify overflow error")
	}

	time.Sleep(timeout)

	err = os.RemoveAll(filepath.Join(dir, "abc"))
	require.NoError(t, err)
	time.Sleep(timeout)
	assert.Equal(t, int64(4096), monitor.Size())
}

func Test_cacheUsageMax(t *testing.T) {
	directory := &directory{}
	cache := &cache{
		directory: *directory,
	}
	m := &Monitor{
		cache: cache,
	}
	m.cacheUsageMax()
}

func TestNewRuntimeMonitor(t *testing.T) {
	convey.Convey("test NewRuntimeMonitor", t, func() {
		hook := func(event fsnotify.Event) {}
		convey.Convey("case1: success", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
					return &fsnotify.Watcher{}, nil
				}),
				gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "path", nil
				}),
				gomonkey.ApplyFunc(os.Lstat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),
				gomonkey.ApplyFunc((*fsnotify.Watcher).Add, func(_ *fsnotify.Watcher, name string) error {
					return nil
				}),
			})
			defer patches.ResetAll()
			stopCh := make(<-chan struct{})
			_, err := NewRuntimeMonitor([]string{"aaa"}, stopCh, AddProcessHooks(hook))
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case2: monitor dir is empty", func() {
			stopCh := make(<-chan struct{})
			_, err := NewRuntimeMonitor(nil, stopCh, AddProcessHooks(hook))
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to new monitor, monitor dir is empty")
		})
		convey.Convey("case3: failed to create watcher", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
					return nil, errors.New("failed to create watcher")
				}),
			})
			defer patches.ResetAll()
			stopCh := make(<-chan struct{})
			_, err := NewRuntimeMonitor([]string{"aaa"}, stopCh, AddProcessHooks(hook))
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to create watcher")
		})
		convey.Convey("case4: failed to absolute rootDir", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
					return &fsnotify.Watcher{}, nil
				}),
				gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", errors.New("failed to absolute rootDir")
				}),
			})
			defer patches.ResetAll()
			stopCh := make(<-chan struct{})
			_, err := NewRuntimeMonitor([]string{"aaa"}, stopCh, AddProcessHooks(hook))
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to absolute rootDir")
		})
		convey.Convey("case5: failed to stat dir", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
					return &fsnotify.Watcher{}, nil
				}),
				gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "path", nil
				}),
				gomonkey.ApplyFunc(os.Lstat, func(name string) (os.FileInfo, error) {
					return nil, errors.New("failed to stat dir")
				}),
			})
			defer patches.ResetAll()
			stopCh := make(<-chan struct{})
			_, err := NewRuntimeMonitor([]string{"aaa"}, stopCh, AddProcessHooks(hook))
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to stat dir")
		})
		convey.Convey("case6: failed to watch dir", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
					return &fsnotify.Watcher{}, nil
				}),
				gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "path", nil
				}),
				gomonkey.ApplyFunc(os.Lstat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),
				gomonkey.ApplyFunc((*fsnotify.Watcher).Add, func(_ *fsnotify.Watcher, name string) error {
					return errors.New("failed to watch dir")
				}),
			})
			defer patches.ResetAll()
			stopCh := make(<-chan struct{})
			_, err := NewRuntimeMonitor([]string{"aaa"}, stopCh, AddProcessHooks(hook))
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to watch dir")
		})
	})
}

func Test_NewCustomImageMonitor(t *testing.T) {
	dir, err := ioutil.TempDir("", "*")
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "def"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "ignore"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "ignore", "ggg"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "def", "ghi"), os.ModePerm)
	require.NoError(t, err)
	err = ioutil.WriteFile(filepath.Join(dir, "abc", "aaa"), []byte("a"), os.ModePerm)
	require.NoError(t, err)

	hook := func(event fsnotify.Event) {
		fmt.Println(event)
	}

	monitor, err := NewCustomImageMonitor(dir, AddProcessHooks(hook), Ignore(filepath.Join(dir, "abc", "ignore")))
	require.NoError(t, err)
	time.Sleep(timeout)
	assert.Equal(t, int64(16385), monitor.Size())
	assert.Equal(t, int64(5), monitor.FirstInode())

	err = ioutil.WriteFile(filepath.Join(dir, "abc", "aaa"), []byte("bbb"), os.ModePerm)
	require.NoError(t, err)
	time.Sleep(timeout)
	assert.Equal(t, int64(16387), monitor.Size())

	// generate an fsnotify overflow error
	select {
	case monitor.watcher.Errors <- fsnotify.ErrEventOverflow:
	default:
		t.Errorf("failed to generate an fsnotify overflow error")
	}

	time.Sleep(timeout)

	err = os.RemoveAll(filepath.Join(dir, "abc"))
	require.NoError(t, err)
	time.Sleep(timeout)
	assert.Equal(t, int64(4096), monitor.Size())
}

func TestNewError(t *testing.T) {
	convey.Convey("Test creates a new Monitor error", t, func() {
		patch := gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
			return "", errors.New("err")
		})
		defer patch.Reset()
		userCodeMonitor, err := New("ppp")
		convey.So(userCodeMonitor, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test creates a new Monitor error 2 ", t, func() {
		userCodeMonitor, err := New("ppp")
		convey.So(userCodeMonitor, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test creates a new Monitor error 3", t, func() {
		patch := gomonkey.ApplyFunc(os.Lstat, func(name string) (os.FileInfo, error) {
			return nil, nil
		})
		patch2 := gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
			return nil, errors.New("err")
		})
		defer patch.Reset()
		defer patch2.Reset()

		userCodeMonitor, err := New("ppp")
		convey.So(userCodeMonitor, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test creates a new Monitor error 3", t, func() {
		patch := gomonkey.ApplyFunc(os.Lstat, func(name string) (os.FileInfo, error) {
			return nil, nil
		})
		patch2 := gomonkey.ApplyFunc(AddProcessHooks, func(hooks ...ProcessHook) Opt {
			return func(m *Monitor) error {
				m.processHooks = append(m.processHooks, hooks...)
				return errors.New("err")
			}
		})
		defer patch.Reset()
		defer patch2.Reset()
		hook := func(event fsnotify.Event) {
			return
		}

		userCodeMonitor, err := New("ppp", AddProcessHooks(hook))
		convey.So(userCodeMonitor, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test creates a new Monitor error 4", t, func() {
		flag := 0
		patch := gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
			flag = 1
			return nil, errors.New("err")
		})
		defer patch.Reset()
		m := Monitor{}
		m.reinitialize()
		convey.So(flag, convey.ShouldEqual, 1)
	})
}
func TestMonitor(t *testing.T) {
	watcher, _ := fsnotify.NewWatcher()
	monitor1 := &Monitor{
		watcher: watcher,
		cache:   &cache{rootDir: "./dir"},
		stopCh:  make(chan struct{}),
		rootDir: "./dir",
	}
	go monitor1.start(true)
	monitor1.Close()
	assert.NotEmpty(t, monitor1)
}
func TestSegmenter(t *testing.T) {
	_, i := segmenter("aa", 0)
	assert.Equal(t, i, -1)
}

func TestInodeCheck(t *testing.T) {
	convey.Convey("test inodeCheck", t, func() {
		convey.Convey("case1: success", func() {
			monitor := Monitor{
				stopCh: make(chan struct{}),
			}
			curInodeUsageCallCounts := 0
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc((*Monitor).curInodeUsage, func(_ *Monitor) {
					curInodeUsageCallCounts++
				}),
			})
			defer patches.ResetAll()
			go func() {
				time.Sleep(60 * time.Millisecond)
				close(monitor.stopCh)
			}()
			monitor.inodeCheck()
			convey.So(curInodeUsageCallCounts, convey.ShouldEqual, 1)
		})
	})
}

func Test_FirstWithTimeOut(t *testing.T) {
	dir, err := ioutil.TempDir("", "*")
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "def"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "ignore"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "ignore", "ggg"), os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(filepath.Join(dir, "abc", "def", "ghi"), os.ModePerm)
	require.NoError(t, err)
	err = ioutil.WriteFile(filepath.Join(dir, "abc", "aaa"), []byte("a"), os.ModePerm)
	require.NoError(t, err)

	monitor, err := newMonitor(dir)
	require.NoError(t, err)

	patch := gomonkey.ApplyMethod(reflect.TypeOf(&fsnotify.Watcher{}), "Add", func(w *fsnotify.Watcher, name string) error {
		time.Sleep(1 * time.Second)
		return nil
	})
	defer patch.Reset()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()
	monitor.firstWithTimeOut(ctx)
	require.Equal(t, monitor.firstSize, int64(constants.ExpansionUserDataMaxSize))
	require.Equal(t, monitor.firstInode, int64(constants.ExpansionUserInodesMaxSize))
}

func Test_NewCustomImageMonitor_Timeout(t *testing.T) {
	convey.Convey("Test_NewCustomImageMonitor_Timeout", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc((*Monitor).firstInodeUsageWithCustomContainer, func(m *Monitor) {
				time.Sleep(11 * time.Second)
				m.firstInode = constants.ExpansionUserInodesMaxSize
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		dir, err := ioutil.TempDir("", "*")
		require.NoError(t, err)
		err = os.Mkdir(filepath.Join(dir, "abc"), os.ModePerm)
		require.NoError(t, err)
		err = os.Mkdir(filepath.Join(dir, "abc", "ignore"), os.ModePerm)
		require.NoError(t, err)
		err = os.Mkdir(filepath.Join(dir, "abc", "def"), os.ModePerm)
		require.NoError(t, err)
		err = os.Mkdir(filepath.Join(dir, "abc", "ignore", "ggg"), os.ModePerm)
		require.NoError(t, err)
		err = ioutil.WriteFile(filepath.Join(dir, "abc", "aaa"), []byte("a"), os.ModePerm)
		require.NoError(t, err)
		err = os.Mkdir(filepath.Join(dir, "abc", "def", "ghi"), os.ModePerm)
		require.NoError(t, err)

		hk := func(event fsnotify.Event) {
			fmt.Println(event)
		}

		mitro, err := NewCustomImageMonitor(dir, AddProcessHooks(hk), Ignore(filepath.Join(dir, "abc", "ignore")))
		require.NoError(t, err)
		assert.Equal(t, int64(constants.ExpansionUserDataMaxSize), mitro.firstSize)

		err = ioutil.WriteFile(filepath.Join(dir, "abc", "aaa"), []byte("bbb"), os.ModePerm)
		require.NoError(t, err)
		time.Sleep(timeout)
		assert.Equal(t, int64(4096), mitro.Size())
		assert.Equal(t, int64(constants.ExpansionUserDataMaxSize), mitro.firstSize)
	})
}

func Test_Close(t *testing.T) {
	monitor := Monitor{
		stopCh: make(chan struct{}),
	}
	for i := 0; i < 10; i++ {
		go monitor.Close()
	}
	time.Sleep(1 * time.Second)
	assert.Nil(t, monitor.stopCh)
}
