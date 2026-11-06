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
package runtime

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/worker/config"
)

type creator struct{}

func (a *creator) NewRuntime(configuration *config.Configuration, handlers Handlers) (Runtime, error) {
	return nil, nil
}

func (a *creator) Prestart(c *config.RuntimeConfig) error {
	return nil
}

func TestReg_NewRuntime(t *testing.T) {
	type fields struct {
		creators map[string]Creator
	}
	type args struct {
		configuration *config.Configuration
		handlers      Handlers
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    LegacyRuntime
		wantErr bool
	}{
		{
			name: "case1",
			fields: fields{
				creators: map[string]Creator{},
			},
			args: args{
				configuration: &config.Configuration{},
				handlers:      Handlers{},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "case2",
			fields: fields{
				creators: map[string]Creator{
					"fusion": &creator{},
				},
			},
			args: args{
				configuration: &config.Configuration{
					RuntimeConfig: config.RuntimeConfig{
						Type: "fusion",
					},
				},
				handlers: Handlers{},
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reg{
				creators: tt.fields.creators,
			}
			got, err := r.NewRuntime(tt.args.configuration.Type, tt.args.configuration, tt.args.handlers)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRuntime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewRuntime() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReg_Prestart(t *testing.T) {
	type fields struct {
		creators map[string]Creator
	}
	type args struct {
		c *config.RuntimeConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "case1",
			fields: fields{
				creators: map[string]Creator{},
			},
			args: args{
				c: &config.RuntimeConfig{
					Type: "fusion",
				},
			},
			wantErr: true,
		},
		{
			name: "case2",
			fields: fields{
				creators: map[string]Creator{
					"fusion": &creator{},
				},
			},
			args: args{
				c: &config.RuntimeConfig{
					Type: "fusion",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reg{
				creators: tt.fields.creators,
			}
			if err := r.Prestart(tt.args.c.Type, tt.args.c); (err != nil) != tt.wantErr {
				t.Errorf("Prestart() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReg_GetCreator(t *testing.T) {
	type fields struct {
		creators map[string]Creator
	}
	type args struct {
		kind string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Creator
	}{
		{
			name: "case1",
			fields: fields{
				creators: map[string]Creator{
					"fusion": &creator{},
				},
			},
			args: args{
				kind: "fusion",
			},
			want: &creator{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reg{
				creators: tt.fields.creators,
			}
			if got := r.GetCreator(tt.args.kind); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCreator() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReg_Register(t *testing.T) {
	type fields struct {
		creators map[string]Creator
	}
	type args struct {
		kind    string
		creator Creator
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				creators: map[string]Creator{
					"fusion": &creator{},
				},
			},
			args: args{
				kind:    "fusion",
				creator: &creator{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reg{
				creators: tt.fields.creators,
			}
			r.Register(tt.args.kind, tt.args.creator)
			assert.Equal(t, r.creators["fusion"], &creator{})
		})
	}
}
