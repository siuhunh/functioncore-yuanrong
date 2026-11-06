/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package alarm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	tests := []struct {
		alarm Type

		alarmName      string
		cause          string
		severity       string
		needSuppressed bool
	}{
		{
			alarm: CAEMetaEtcdConnection,

			alarmName:      caeMetaEtcdConnectionAlarmName,
			cause:          etcdErrCause,
			severity:       ImportantSeverity,
			needSuppressed: true,
		},
		{
			alarm: CAEMetaETCDConnectionAbnormalContinuously,

			alarmName:      caeMetaEtcdConnectionAbnormalContinuouslyAlarmName,
			cause:          etcdErrCause,
			severity:       CriticalSeverity,
			needSuppressed: false,
		},
		{
			alarm: MetaEtcdProxyConnection,

			alarmName:      metaEtcdProxyConnectionAlarmName,
			cause:          etcdErrCause,
			severity:       ImportantSeverity,
			needSuppressed: true,
		},
		{
			alarm: MetaEtcdProxyConnectionAbnormalContinuously,

			alarmName:      metaEtcdProxyConnectionAbnormalContinuouslyAlarmName,
			cause:          etcdErrCause,
			severity:       CriticalSeverity,
			needSuppressed: false,
		},
		{
			alarm: CAEMetaEtcdProxyConnection,

			alarmName:      caeMetaEtcdProxyConnectionAlarmName,
			cause:          etcdErrCause,
			severity:       ImportantSeverity,
			needSuppressed: true,
		},
		{
			alarm: CAEMetaEtcdProxyConnectionAbnormalContinuously,

			alarmName:      caeMetaEtcdProxyConnectionAbnormalContinuouslyAlarmName,
			cause:          etcdErrCause,
			severity:       CriticalSeverity,
			needSuppressed: false,
		},
	}

	for _, tt := range tests {
		t.Run(strconv.Itoa(int(tt.alarm)), func(t *testing.T) {
			assert.Equal(t, tt.cause, tt.alarm.Cause())
			assert.Equal(t, tt.alarmName, tt.alarm.AlarmName())
			assert.Equal(t, tt.severity, tt.alarm.Severity())
			assert.Equal(t, tt.needSuppressed, tt.alarm.NeedSuppressed())
		})
	}
}
