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

package version

import (
	"testing"

	"github.com/agiledragon/gomonkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"functioncore/pkg/cli/build"
	"functioncore/pkg/cli/pkg/test"
	"functioncore/pkg/cli/utils/colorprint"
)

func TestInitVersionCMD(t *testing.T) {
	testCases := []struct {
		name      string
		version   string
		buildTime string
		wantStd   string
	}{
		{
			name:      "get version info",
			version:   `1.0.0`,
			buildTime: `20210218`,
			wantStd: "CLI version: " +
				colorprint.GetPrintString("1.0.0", colorprint.KeywordColor) +
				", build time: " + colorprint.GetPrintString("20210218", colorprint.KeywordColor) + ".\n",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			p := gomonkey.ApplyGlobalVar(&build.Version, tt.version)
			p.ApplyGlobalVar(&build.Date, tt.buildTime)
			defer p.Reset()

			cio, mio := test.MockCmdIO()
			cmd := InitVersionCMD(cio)

			cmd.SetArgs([]string{})
			_, err := cmd.ExecuteC()
			require.NoError(t, err)
			assert.Equal(t, tt.wantStd, mio.Out.String())
		})
	}
}
