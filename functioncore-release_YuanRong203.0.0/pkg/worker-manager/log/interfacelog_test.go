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

package log

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/logger"
	mockutils "functioncore/test/utils"
)

func TestCreateInterLogger(t *testing.T) {
	tests := []struct {
		name        string
		logPath     string
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{name: "case_TestCreateInterLogger",
			logPath: "./",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyGlobalVar(enableSnapShotManager, false),
				})
				return patches
			},
			wantErr: false,
		},
		{name: "case_TestCreateInterLogger with snapshot-manager",
			logPath: "./",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyGlobalVar(enableSnapShotManager, true),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CreateInterLogger(tt.logPath); (err != nil) != tt.wantErr {
				t.Errorf("CreateInterLogger() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	type args struct {
		i InterfaceLog
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		isNil       bool
	}{
		{
			name: "case_TestWrite",
			args: args{
				InterfaceLog{
					"1",
					"3",
					"name",
					"alias",
					"ver",
					"5",
					"abc",
					"efg",
					"TestWrite",
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			isNil: false,
		},
		{
			name: "test write method with interface log is nil",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(logger.NewInterfaceLogger, func(logPath, fileName string,
						cfg logger.InterfaceEncoderConfig) (*logger.InterfaceLogger, error) {
						return nil, errors.New("mock error")
					}),
				})
				return patches
			},
			isNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interfaceLog = nil
			patches := tt.patchesFunc()
			Write(tt.args.i)
			if (interfaceLog == nil) != tt.isNil {
				t.Errorf("CreateInterLogger() interfaceLog = %v, wantNil %v", interfaceLog, tt.isNil)
			}
			patches.ResetAll()
		})
	}
}
