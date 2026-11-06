package usergroup

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/runtime-manager/utils"
	mockutils "functioncore/test/utils"
)

func Test_checkID(t *testing.T) {
	type args struct {
		uid int
		gid int
	}
	tests := []struct {
		name    string
		args    args
		want    int
		want1   int
		wantErr bool
	}{
		{
			name:    `defultID:uid=0,gid=0`,
			args:    args{0, 0},
			want:    constants.DefaultRuntimeUID,
			want1:   constants.DefaultRuntimeGID,
			wantErr: false,
		},
		{
			name:    `wrongUID:uid=-2,gid=0`,
			args:    args{-2, 0},
			want:    0,
			want1:   0,
			wantErr: true,
		},
		{
			name:    `randomId:uid=42,gid=10`,
			args:    args{42, 10},
			want:    42,
			want1:   10,
			wantErr: false,
		},
		{
			name:    `wrongGID:uid=-2,gid=0`,
			args:    args{42, -2},
			want:    0,
			want1:   0,
			wantErr: true,
		},
	}
	patch := gomonkey.ApplyFunc(CreateMountUser, func(int, int) error { return nil })
	defer patch.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := CheckID(tt.args.uid, tt.args.gid)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want1, got1)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func Test_exist(t *testing.T) {
	item := "case0"
	arr1 := []string{"case1", "case2", "case0"}
	arr2 := []string{"case3", "case4", "case5"}
	assert.Equal(t, true, exist(arr1, item))
	assert.Equal(t, false, exist(arr2, item))
}

func Test_fileToLines(t *testing.T) {
	res, err := fileToLines("/etc/group")
	assert.Equal(t, true, len(res) > 0)
	assert.Equal(t, false, err != nil)
}

func Test_createUserIfNotExist(t *testing.T) {
	patch := gomonkey.ApplyFunc(utils.AppendToFile, func(fileName string, line string) error { return nil })
	defer patch.Reset()
	rootUID := "0"
	UnknownUId := "-1"
	rootGID := "0"
	uName1, _ := createUserIfNotExist(rootUID, rootGID)
	uName2, _ := createUserIfNotExist(UnknownUId, rootGID)
	assert.Equal(t, "root", uName1)
	assert.Equal(t, "user"+UnknownUId, uName2)
}

func Test_createGroupIfNotExist(t *testing.T) {
	patch := gomonkey.ApplyFunc(utils.AppendToFile, func(fileName string, line string) error { return nil })
	defer patch.Reset()
	rootGID := "0"
	UnknownGId := "-1"
	err := createGroupIfNotExist(rootGID)
	assert.Equal(t, nil, err)
	err = createGroupIfNotExist(UnknownGId)
	assert.Equal(t, nil, err)
}

func Test_createMountUser(t *testing.T) {
	outputs := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil}, Times: 2},
		{Values: gomonkey.Params{errors.New("mock error")}, Times: 1},
		{Values: gomonkey.Params{nil}, Times: 1},
		{Values: gomonkey.Params{errors.New("mock error")}, Times: 1},
	}
	defer gomonkey.ApplyFuncSeq(addUserToGroup, outputs).Reset()
	rootUID, rootGID := 0, 0
	assert.Nil(t, CreateMountUser(rootUID, rootGID))
	assert.NotNil(t, CreateMountUser(rootUID, rootGID))
	assert.NotNil(t, CreateMountUser(rootUID, rootGID))

	convey.Convey("Test_createMountUser", t, func() {
		convey.Convey("Test_createMountUser_failed_create_user", func() {
			patch := gomonkey.ApplyFunc(createUserIfNotExist, func(uidStr, gidStr string) (string, error) {
				return "", fmt.Errorf("create user error")
			})
			defer patch.Reset()
			err := CreateMountUser(rootUID, rootGID)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("Test_createMountUser_failed_create_group", func() {
			patch := gomonkey.ApplyFunc(createUserIfNotExist, func(uidStr, gidStr string) (string, error) {
				return "", nil
			})
			patch.ApplyFunc(createGroupIfNotExist, func(gidStr string) error {
				return fmt.Errorf("create group error")
			})
			defer patch.Reset()
			err := CreateMountUser(rootUID, rootGID)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func Test_addUserToGroup(t *testing.T) {
	convey.Convey("Test_addUserToGroup", t, func() {
		convey.Convey("Test_addUserToGroup_getFileError", func() {
			patch := gomonkey.ApplyFunc(fileToLines, func(fileName string) ([]string, error) {
				return nil, fmt.Errorf("get file error")
			})
			defer patch.Reset()
			err := addUserToGroup("", "")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("Test_addUserToGroup_writeFileError", func() {
			defer gomonkey.ApplyFunc(fileToLines, func(fileName string) ([]string, error) {
				return []string{"1:1:1:1", "1:2:3:", "1:2:3:4"}, nil
			}).ApplyFunc(ioutil.WriteFile, func(filename string, data []byte, perm fs.FileMode) error {
				return fmt.Errorf("writeFile error")
			}).Reset()
			err := addUserToGroup("3", "5")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("Test_addUserToGroup_success", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(fileToLines, func(fileName string) ([]string, error) {
					return []string{"1:1:1:1", "1:2:3:", "1:2:3:4"}, nil
				}),
				gomonkey.ApplyFunc(ioutil.WriteFile, func(filename string, data []byte, perm fs.FileMode) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := addUserToGroup("3", "5")
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestChownRuntimeLogFile(t *testing.T) {
	convey.Convey("Test_ChownRuntimeLogFile", t, func() {
		convey.Convey("Test_ChownRuntimeLogFile_file_not_exist", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, os.ErrNotExist
				}),
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return errors.New("no way into this method")
				}),
			})
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := ChownRuntimeLogFile(-1, -1)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("Test_ChownRuntimeLogFile_stat_error", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, os.ErrExist
				}),
				gomonkey.ApplyFunc(os.Chown, func(name string, uid, gid int) error {
					if strings.HasSuffix(name, "nodejs-runtime.log") ||
						strings.HasSuffix(name, "python-runtime.log") ||
						strings.HasSuffix(name, "java-runtime.log") ||
						strings.HasSuffix(name, "cangjie-runtime.log") {
						return errors.New("Test error")
					}
					return fmt.Errorf("file %s not in logFiels", name)
				}),
			})
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := ChownRuntimeLogFile(-1, -1)
			convey.So(err.Error(), convey.ShouldEqual, "Test error")
		})
		convey.Convey("Test_ChownRuntimeLogFile_chown_ok", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),
				gomonkey.ApplyFunc(os.Chown, func(name string, uid, gid int) error {
					if strings.HasSuffix(name, "nodejs-runtime.log") ||
						strings.HasSuffix(name, "python-runtime.log") ||
						strings.HasSuffix(name, "java-runtime.log") ||
						strings.HasSuffix(name, "cangjie-runtime.log") {
						return nil
					}
					return fmt.Errorf("file %s not in logFiels", name)
				}),
			})
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := ChownRuntimeLogFile(-1, -1)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}
