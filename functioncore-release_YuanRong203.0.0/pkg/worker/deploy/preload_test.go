/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package deploy deploys Layers and functions
package deploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"syscall"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	workerutils "functioncore/pkg/worker/utils"
)

type mockDirEntry struct {
	name  string
	isDir bool
}

func (m mockDirEntry) Name() string {
	return m.name
}

func (m mockDirEntry) IsDir() bool {
	return m.isDir
}

func (m mockDirEntry) Type() fs.FileMode {
	return 0
}

func (m mockDirEntry) Info() (fs.FileInfo, error) {
	return nil, nil
}

func TestRemovePreLoadInstanceFiles(t *testing.T) {
	tests := []struct {
		name         string
		readDirEntry []os.DirEntry
		readErr      error
		wantErr      error
		wantRevision string
	}{
		{
			name:         "success",
			readDirEntry: []os.DirEntry{mockDirEntry{name: "current.1"}},
			wantRevision: "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := gomonkey.ApplyFunc(os.ReadDir, func(name string) ([]os.DirEntry, error) {
				return tt.readDirEntry, tt.readErr
			})
			defer patches.Reset()
			revision, err := getCurrentRevision("path")
			if tt.wantErr != nil {
				assert.NotNil(t, err)
				return
			}
			assert.Equal(t, tt.wantRevision, revision)
		})
	}
}

func TestSetPreloadEnable(t *testing.T) {
	t.Run("header is nil", func(t *testing.T) {
		SetPreloadEnable(nil)
		assert.False(t, functionEnablePreload)
		assert.False(t, layerEnablePreload)
	})
	t.Run("function is enable preload", func(t *testing.T) {
		header := http.Header{}
		header.Set(constants.HeaderPreloadFunctionEnable, "true")
		SetPreloadEnable(header)
		defer func() {
			functionEnablePreload = false
			layerEnablePreload = false
		}()
		assert.True(t, functionEnablePreload)
		assert.False(t, layerEnablePreload)
	})
	t.Run("layer is enable preload", func(t *testing.T) {
		header := http.Header{}
		header.Set(constants.HeaderPreloadLayerEnable, "true")
		SetPreloadEnable(header)
		defer func() {
			functionEnablePreload = false
			layerEnablePreload = false
		}()
		assert.False(t, functionEnablePreload)
		assert.True(t, layerEnablePreload)
	})
}

type MockResponseWriter struct {
	header http.Header
}

func (m MockResponseWriter) Header() http.Header {
	return m.header
}

func (m MockResponseWriter) Write(_ []byte) (int, error) {
	return 0, nil
}

func (m MockResponseWriter) WriteHeader(_ int) {

}

func TestSetPreloadResult(t *testing.T) {
	t.Run("not enable", func(t *testing.T) {
		w := MockResponseWriter{}
		SetPreloadResult(w)
		assert.Equal(t, "", w.Header().Get(constants.HeaderPreloadFunctionResult))
		assert.Equal(t, "", w.Header().Get(constants.HeaderPreloadLayerResult))
	})
	t.Run("has error", func(t *testing.T) {
		functionEnablePreload = true
		layerEnablePreload = true
		functionPreloadErrorMessage = "function preload error"
		layerPreloadResultArray = []types.LayerPreloadResult{{LayerVersionName: "testlayer", Result: "xxxx"}}
		w := MockResponseWriter{header: make(http.Header)}
		SetPreloadResult(w)
		defer func() {
			functionEnablePreload = false
			layerEnablePreload = false
			functionPreloadErrorMessage = ""
			layerPreloadResultArray = nil
		}()
		data, _ := json.Marshal(types.LayerPreloadResultArray{Results: layerPreloadResultArray})
		assert.Equal(t, "function preload error", w.Header().Get(constants.HeaderPreloadFunctionResult))
		assert.Equal(t, string(data), w.Header().Get(constants.HeaderPreloadLayerResult))
	})
}

func TestDeployFunctionByPreload(t *testing.T) {
	preloadURN := &urnutils.BaseURN{
		Name:     "0@service@preload",
		Version:  "latest",
		TenantID: "preload",
	}
	urnutils.SetSeparator("@")
	t.Run("not enable", func(t *testing.T) {
		err := deployFunctionByPreload("/xxxx", "123", "")
		assert.EqualError(t, err, "not enable to deploy function by preload")
	})
	t.Run("failed to checkFunctionPreloadRequirements", func(t *testing.T) {
		functionEnablePreload = true
		workerutils.LocalFuncURN = preloadURN
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkFunctionPreloadRequirements, func(funcPreloadPath, sha512 string) (string, error) {
			return "", errors.New("failed")
		})
		defer func() {
			functionEnablePreload = false
			workerutils.LocalFuncURN = &urnutils.BaseURN{}
			patch.Reset()
		}()
		err := deployFunctionByPreload("/xxxx", "123", "")
		assert.EqualError(t, err, "failed")
	})
	t.Run("failed to mountAndLinkPreloadFile", func(t *testing.T) {
		functionEnablePreload = true
		workerutils.LocalFuncURN = preloadURN
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkFunctionPreloadRequirements, func(funcPreloadPath, sha512 string) (string, error) {
			return "", nil
		})
		patch.ApplyFunc(createInstanceUseFile, func(preloadDir string) error {
			return nil
		})
		patch.ApplyFunc(mountAndLinkPreloadFile, func(mountSource string, mountBindPath string, linkPath string) error {
			return errors.New("failed")
		})
		defer func() {
			functionEnablePreload = false
			workerutils.LocalFuncURN = &urnutils.BaseURN{}
			patch.Reset()
		}()
		err := deployFunctionByPreload("/xxxx", "123", "")
		assert.EqualError(t, err, "failed")
	})
	t.Run("failed to createInstanceUseFile", func(t *testing.T) {
		functionEnablePreload = true
		workerutils.LocalFuncURN = preloadURN
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkFunctionPreloadRequirements, func(funcPreloadPath, sha512 string) (string, error) {
			return "", nil
		})
		patch.ApplyFunc(createInstanceUseFile, func(preloadDir string) error {
			return errors.New("failed")
		})
		defer func() {
			functionEnablePreload = false
			workerutils.LocalFuncURN = &urnutils.BaseURN{}
			patch.Reset()
		}()
		err := deployFunctionByPreload("/xxxx", "123", "")
		assert.EqualError(t, err, "failed")
	})
	t.Run("success", func(t *testing.T) {
		functionEnablePreload = true
		workerutils.LocalFuncURN = preloadURN
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkFunctionPreloadRequirements, func(funcPreloadPath, sha512 string) (string, error) {
			return "", nil
		})
		patch.ApplyFunc(mountAndLinkPreloadFile, func(mountSource string, mountBindPath string, linkPath string) error {
			return nil
		})
		patch.ApplyFunc(createInstanceUseFile, func(preloadDir string) error {
			return nil
		})
		defer func() {
			functionEnablePreload = false
			workerutils.LocalFuncURN = &urnutils.BaseURN{}
			patch.Reset()
		}()
		err := deployFunctionByPreload("/xxxx", "123", "")
		assert.NoError(t, err, "failed")
	})
}

func TestCreateInstanceUseFile(t *testing.T) {
	t.Run("failed to open file", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return nil, errors.New("failed")
		})
		defer patch.Reset()
		err := createInstanceUseFile("/xxx")
		assert.EqualError(t, err, "failed")
	})
	t.Run("success", func(t *testing.T) {
		deferClearInstanceFiles = []string{}
		patch := gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return &os.File{}, nil
		})
		podName = "podName"
		defer func() {
			patch.Reset()
			deferClearInstanceFiles = []string{}
			podName = ""
		}()
		err := createInstanceUseFile("/xxx")
		assert.NoError(t, err)
		assert.Equal(t, "/xxx/instances/podName", deferClearInstanceFiles[0])
	})
}

func TestGetCurrentRevision(t *testing.T) {
	t.Run("failed ReadDir", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(os.ReadDir, func(name string) ([]os.DirEntry, error) {
			return nil, errors.New("failed")
		})
		defer patch.Reset()
		_, err := getCurrentRevision("/xxx")
		assert.EqualError(t, err, "failed")
	})
	t.Run("no currentRevisionFile", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(os.ReadDir, func(name string) ([]os.DirEntry, error) {
			return []os.DirEntry{mockDirEntry{name: "curr"}}, nil
		})
		defer patch.Reset()
		_, err := getCurrentRevision("/xxx")
		assert.EqualError(t, err, "current revision file is not found")
	})
	t.Run("current revision file is not the format", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(os.ReadDir, func(name string) ([]os.DirEntry, error) {
			return []os.DirEntry{mockDirEntry{name: "current1"}}, nil
		})
		defer patch.Reset()
		_, err := getCurrentRevision("/xxx")
		assert.EqualError(t, err, "current revision file current1 is not the format (current.n)")
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(os.ReadDir, func(name string) ([]os.DirEntry, error) {
			return []os.DirEntry{mockDirEntry{name: "file1"}, mockDirEntry{name: "current.1"}}, nil
		})
		defer patch.Reset()
		revision, err := getCurrentRevision("/xxx")
		assert.NoError(t, err)
		assert.Equal(t, "1", revision)
	})
}

func TestCheckSha512(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return &os.File{}, nil
		})
		patch.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
			return []byte("123"), nil
		})
		defer patch.Reset()
		err := checkSha512("/test", "123")
		assert.NoError(t, err)
	})
	t.Run("read sha512 is not equal", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return &os.File{}, nil
		})
		patch.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
			return []byte("456"), nil
		})
		defer patch.Reset()
		err := checkSha512("/test", "123")
		assert.EqualError(t, err, "preload sha512 file not equal")
	})
}

func TestLinkPreloadDependencies(t *testing.T) {
	t.Run("nodejs", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(linkFiles, func(dependenciesPath, mountPath string) error {
			assert.Equal(t, "/mount/node_modules", mountPath)
			assert.Equal(t, "/opt/function/code/node_modules/", dependenciesPath)
			return nil
		})
		defer patch.Reset()
		err := linkPreloadDependencies("nodejs12.13", "/mount", "/opt/function/code")
		assert.NoError(t, err)
	})
	t.Run("java", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(linkFiles, func(dependenciesPath, mountPath string) error {
			assert.Equal(t, "/mount", mountPath)
			assert.Equal(t, "/opt/function/code/lib/", dependenciesPath)
			return nil
		})
		defer patch.Reset()
		err := linkPreloadDependencies("java8", "/mount", "/opt/function/code")
		assert.NoError(t, err)
	})
	t.Run("python", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(linkFiles, func(dependenciesPath, mountPath string) error {
			assert.Equal(t, "/mount", mountPath)
			assert.Equal(t, "/opt/function/code/", dependenciesPath)
			return nil
		})
		defer patch.Reset()
		err := linkPreloadDependencies("python3.9", "/mount", "/opt/function/code")
		assert.NoError(t, err)
	})
}

func TestDeployLayerByPreload(t *testing.T) {
	t.Run("not enable", func(t *testing.T) {
		err := deployLayerByPreload(&types.Layer{Name: "layer", Version: "1"}, "123", "/opt/function/code")
		assert.EqualError(t, err, "not enable to deploy layer by preload")
	})
	t.Run("failed to checkLayerPreloadRequirements", func(t *testing.T) {
		layerEnablePreload = true
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkLayerPreloadRequirements, func(layerPreloadPath string) error {
			return errors.New("failed")
		})
		defer func() {
			layerEnablePreload = false
			patch.Reset()
		}()
		err := deployLayerByPreload(&types.Layer{Name: "layer", Version: "1"}, "123", "/opt/function/code")
		assert.EqualError(t, err, "failed")
	})
	t.Run("failed to mountAndLinkPreloadFile", func(t *testing.T) {
		layerEnablePreload = true
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkLayerPreloadRequirements, func(layerPreloadPath string) error {
			return nil
		})
		patch.ApplyFunc(createInstanceUseFile, func(preloadDir string) error {
			return nil
		})
		patch.ApplyFunc(mountAndLinkLayerPreloadFile, func(layerPreloadSource, mountBindPath, workerPath, language string) error {
			return errors.New("failed")
		})
		defer func() {
			layerEnablePreload = false
			patch.Reset()
		}()
		err := deployLayerByPreload(&types.Layer{Name: "layer", Version: "1"}, "123", "/opt/function/code")
		assert.EqualError(t, err, "failed")
	})
	t.Run("failed to createInstanceUseFile", func(t *testing.T) {
		layerEnablePreload = true
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkLayerPreloadRequirements, func(layerPreloadPath string) error {
			return nil
		})
		patch.ApplyFunc(createInstanceUseFile, func(preloadDir string) error {
			return errors.New("failed")
		})
		defer func() {
			layerEnablePreload = false
			patch.Reset()
		}()
		err := deployLayerByPreload(&types.Layer{Name: "layer", Version: "1"}, "123", "/opt/function/code")
		assert.EqualError(t, err, "failed")
	})
	t.Run("success", func(t *testing.T) {
		layerEnablePreload = true
		patch := gomonkey.ApplyFunc(exists, func(dir string) bool { return true })
		patch.ApplyFunc(checkLayerPreloadRequirements, func(layerPreloadPath string) error {
			return nil
		})
		patch.ApplyFunc(mountAndLinkLayerPreloadFile, func(layerPreloadSource, mountBindPath, workerPath, language string) error {
			return nil
		})
		patch.ApplyFunc(createInstanceUseFile, func(preloadDir string) error {
			return nil
		})
		defer func() {
			layerEnablePreload = false
			patch.Reset()
		}()
		err := deployLayerByPreload(&types.Layer{Name: "layer", Version: "1"}, "123", "/opt/function/code")
		assert.NoError(t, err)
	})
}

func TestLinkFiles(t *testing.T) {
	t.Run("src not exists", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return false
		})
		defer p.Reset()
		err := linkFiles("/dts", "/src")
		assert.EqualError(t, err, fmt.Sprintf("src path (%s) does not exist", "/src"))
	})
	t.Run("failed to create dts path", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return true
		})
		p.ApplyFunc(createIfNotExist, func(dependenciesPath string) error {
			return errors.New("failed")
		})
		defer p.Reset()
		err := linkFiles("/dts", "/src")
		assert.EqualError(t, err, "failed")
	})
	t.Run("failed to ReadDir", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return true
		})
		p.ApplyFunc(createIfNotExist, func(dependenciesPath string) error {
			return nil
		})
		p.ApplyFunc(os.ReadDir, func(dirname string) ([]os.DirEntry, error) {
			return nil, errors.New("failed read dir")
		})
		defer p.Reset()
		err := linkFiles("/dts", "/src")
		assert.EqualError(t, err, "failed read dir")
	})
	t.Run("failed to create link file", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFuncSeq(exists, []gomonkey.OutputCell{
			{Values: gomonkey.Params{true}},
			{Values: gomonkey.Params{false}},
		})
		p.ApplyFunc(createIfNotExist, func(dependenciesPath string) error {
			return nil
		})
		p.ApplyFunc(os.ReadDir, func(dirname string) ([]os.DirEntry, error) {
			return []os.DirEntry{mockDirEntry{}}, nil
		})
		p.ApplyFunc(os.Symlink, func(oldname, newname string) error {
			return errors.New("failed to link")
		})
		defer p.Reset()
		err := linkFiles("/dts", "/src")
		assert.EqualError(t, err, "failed to link")
	})
	t.Run("success", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFuncSeq(exists, []gomonkey.OutputCell{
			{Values: gomonkey.Params{true}},
			{Values: gomonkey.Params{false}},
		})
		p.ApplyFunc(createIfNotExist, func(dependenciesPath string) error {
			return nil
		})
		p.ApplyFunc(os.ReadDir, func(dirname string) ([]os.DirEntry, error) {
			return []os.DirEntry{mockDirEntry{}}, nil
		})
		p.ApplyFunc(os.Symlink, func(oldname, newname string) error {
			return nil
		})
		defer p.Reset()
		err := linkFiles("/dts", "/src")
		assert.NoError(t, err)
	})
}

func TestCheckFunctionPreloadRequirements(t *testing.T) {
	t.Run("preload path not exists", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return false
		})
		defer p.Reset()
		_, err := checkFunctionPreloadRequirements("/preload", "123")
		assert.EqualError(t, err, fmt.Sprintf("function preload path %s not exists", "/preload"))
	})
	t.Run("failed to get CurrentRevision", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return true
		})
		p.ApplyFunc(getCurrentRevision, func(path string) (string, error) {
			return "", errors.New("error revision")
		})
		defer p.Reset()
		_, err := checkFunctionPreloadRequirements("/preload", "123")
		assert.EqualError(t, err, fmt.Sprintf("failed to get preload function current revision in dir %s, err %s", "/preload", "error revision"))
	})
	t.Run("failed to check preload success file", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return true
		})
		p.ApplyFunc(getCurrentRevision, func(path string) (string, error) {
			return "1", nil
		})
		p.ApplyFunc(checkPreloadSuccessFile, func(path string) error {
			return errors.New("error success file")
		})
		defer p.Reset()
		_, err := checkFunctionPreloadRequirements("/preload", "123")
		assert.EqualError(t, err, fmt.Sprintf("failed to check preload success file, err %s", "error success file"))
	})
	t.Run("failed to check sha512", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return true
		})
		p.ApplyFunc(getCurrentRevision, func(path string) (string, error) {
			return "1", nil
		})
		p.ApplyFunc(checkPreloadSuccessFile, func(path string) error {
			return nil
		})
		p.ApplyFunc(checkSha512, func(path, sha512 string) error {
			return errors.New("error sha512")
		})
		defer p.Reset()
		_, err := checkFunctionPreloadRequirements("/preload", "123")
		assert.EqualError(t, err, fmt.Sprintf("failed to check preload sha512, err %s", "error sha512"))
	})
	t.Run("success", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(exists, func(dir string) bool {
			return true
		})
		p.ApplyFunc(getCurrentRevision, func(path string) (string, error) {
			return "1", nil
		})
		p.ApplyFunc(checkPreloadSuccessFile, func(path string) error {
			return nil
		})
		p.ApplyFunc(checkSha512, func(path, sha512 string) error {
			return nil
		})
		defer p.Reset()
		path, err := checkFunctionPreloadRequirements("/preload", "123")
		assert.NoError(t, err)
		assert.Equal(t, "/preload/revisions/1", path)
	})
}

func TestCheckPreloadSuccessFile(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(exists, func(dir string) bool { return true })
	defer p.Reset()
	err := checkPreloadSuccessFile("/xxxx")
	assert.NoError(t, err)
}

func TestMount(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(syscall.Mount, func(source string, target string, fstype string, flags uintptr, data string) (err error) {
		return nil
	})
	defer p.Reset()
	err := mount("/src", "/target")
	assert.NoError(t, err)
}

func TestCheckLayerPreloadRequirements(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(exists, func(dir string) bool { return true })
	p.ApplyFunc(checkPreloadSuccessFile, func(path string) error { return nil })
	defer p.Reset()
	err := checkLayerPreloadRequirements("/preload/layer")
	assert.NoError(t, err)
}

func TestRetryToUnmount(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(syscall.Unmount, func(target string, flags int) (err error) { return nil })
	defer p.Reset()
	err := retryToUnmount("/xxxx")
	assert.NoError(t, err)
}

func TestMountAndLinkLayerPreloadFile(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error { return nil })
	p.ApplyFunc(mount, func(source, target string) error { return nil })
	p.ApplyFunc(linkPreloadDependencies, func(language, mountPath, workerPath string) error { return nil })
	defer p.Reset()
	err := mountAndLinkLayerPreloadFile("layerPreloadSource", "mountBindPath", "workerPath", "language")
	assert.NoError(t, err)
}

func TestMountAndLinkPreloadFile(t *testing.T) {
	t.Run("failed to create mount dir", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return errors.New("failed mkdirAll")
		})
		defer p.Reset()
		err := mountAndLinkPreloadFile("/preload-source", "/preload-bind", "/opt/function/code/", "java8")
		assert.EqualError(t, err, "failed mkdirAll")
	})
	t.Run("failed to create mount bind", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return nil
		})
		p.ApplyFunc(mount, func(source, target string) error {
			return errors.New("failed mount")
		})
		defer p.Reset()
		err := mountAndLinkPreloadFile("/preload-source", "/preload-bind", "/opt/function/code/", "java8")
		assert.EqualError(t, err, "failed mount")
	})
	t.Run("failed to create link file", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return nil
		})
		p.ApplyFunc(mount, func(source, target string) error {
			return nil
		})
		p.ApplyFunc(linkFiles, func(dtsPath, srcPath string) error {
			return errors.New("failed link")
		})
		defer p.Reset()
		err := mountAndLinkPreloadFile("/preload-source", "/preload-bind", "/opt/function/code/", "java8")
		assert.EqualError(t, err, "failed link")
	})
	t.Run("java8 linkSpecialFile failed", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return nil
		})
		p.ApplyFunc(mount, func(source, target string) error {
			return nil
		})
		p.ApplyFunc(linkFiles, func(dtsPath, srcPath string) error {
			return nil
		})
		p.ApplyFunc(linkSpecialFile, func(linkPath string, mountBindPath string, language string) error {
			return errors.New("failed link special file")
		})
		defer p.Reset()
		err := mountAndLinkPreloadFile("/preload-source", "/preload-bind", "/opt/function/code/", "java8")
		assert.EqualError(t, err, "failed link special file")
	})
	t.Run("java8 linkSpecialFile success", func(t *testing.T) {
		p := gomonkey.NewPatches()
		p.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return nil
		})
		p.ApplyFunc(mount, func(source, target string) error {
			return nil
		})
		p.ApplyFunc(linkFiles, func(dtsPath, srcPath string) error {
			return nil
		})
		p.ApplyFunc(linkSpecialFile, func(linkPath string, mountBindPath string, language string) error {
			return nil
		})
		defer p.Reset()
		err := mountAndLinkPreloadFile("/preload-source", "/preload-bind", "/opt/function/code/", "java8")
		assert.NoError(t, err)
	})
}
