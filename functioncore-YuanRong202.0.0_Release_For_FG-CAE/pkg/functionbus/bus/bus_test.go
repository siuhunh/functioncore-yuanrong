/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package bus

import (
	"testing"

	"functioncore/pkg/functionbus/common"
)

func TestNewBus(t *testing.T) {
	type args struct {
		busType   common.BusType
		busOption common.BusOption
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"test busType is ChannelBusType",
			args{
				busType:   common.ChannelBusType,
				busOption: common.BusOption{},
			},
			true,
		},
		{
			"test busType is TcpType",
			args{
				busType:   common.TCPBusType,
				busOption: common.BusOption{},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewBus(tt.args.busType, tt.args.busOption)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

		})
	}
}
