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
	"container/list"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	buscommon "functioncore/pkg/functionbus/common"
	"functioncore/test/utils"
)

func Test_futureTracer_increaseRequestCount(t *testing.T) {
	caller := "aaa"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name: "case1",
			fields: fields{
				traces: make(map[string]*trace),
				mux:    sync.Mutex{},
			},
			args: args{},
			want: 1,
		},
		{
			name: "case2",
			fields: fields{
				traces: map[string]*trace{
					caller: {},
				},
				mux: sync.Mutex{},
			},
			args: args{
				caller: caller,
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			if got := f.increaseRequestCount(tt.args.caller); got != tt.want {
				t.Errorf("increaseRequestCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_futureTracer_decreaseRequestCount(t *testing.T) {
	caller := "aaa"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {},
				},
				mux: sync.Mutex{},
			},
			args: args{
				caller: caller,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			if got := f.decreaseRequestCount(tt.args.caller); got != tt.want {
				t.Errorf("decreaseRequestCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_futureTracer_getRequestCount(t *testing.T) {
	caller := "aaa"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {},
				},
				mux: sync.Mutex{},
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			if got := f.getRequestCount(); got != tt.want {
				t.Errorf("getRequestCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_futureTracer_getRecoverProperty(t *testing.T) {
	caller := "aaa"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   buscommon.RecoverProperty
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records: list.New(),
						m:       map[string]*list.Element{},
					},
				},
				mux: sync.Mutex{},
			},
			want: buscommon.RecoverProperty{
				Requests: map[string]*buscommon.RecoverTraceRecord{
					caller: {
						Records: []string{},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			if got := f.getRecoverProperty(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRecoverProperty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_futureTracer_add(t *testing.T) {
	list1 := &list.List{}
	caller := "aaa"
	callee := "bbb"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
		callee string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records: &list.List{},
						m:       map[string]*list.Element{},
					},
				},
				mux: sync.Mutex{},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(list1), "PushBack",
						func(l *list.List, v interface{}) *list.Element {
							return &list.Element{}
						}),
				})
				return patches
			},
		},
		{
			name: "case2",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records: &list.List{},
						m: map[string]*list.Element{
							callee: {},
						},
					},
				},
				mux: sync.Mutex{},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			f.add(caller, callee)
			assert.NotNil(t, f.traces[caller].m[callee])
			patches.ResetAll()
		})
	}
}

func Test_futureTracer_add1(t *testing.T) {
	caller := "aaa"
	callee := "bbb"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
		callee string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{},
				mux:    sync.Mutex{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			f.add(caller, callee)
			assert.Nil(t, f.traces[caller])
		})
	}
}

func Test_futureTracer_remove(t *testing.T) {
	caller := "aaa"
	callee := "bbb"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
		callee string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records: &list.List{},
						m: map[string]*list.Element{
							callee: {},
						},
					},
				},
				mux: sync.Mutex{},
			},
		},
		{
			name: "case2",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records: &list.List{},
						m:       map[string]*list.Element{},
					},
				},
				mux: sync.Mutex{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			f.remove(caller, callee)
			assert.NotNil(t, f.traces[caller])
		})
	}
}

func Test_futureTracer_remove1(t *testing.T) {
	caller := "aaa"
	callee := "bbb"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
		callee string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{},
				mux:    sync.Mutex{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			f.remove(caller, callee)
			assert.Nil(t, f.traces[caller])
		})
	}
}

func Test_futureTracer_cancel(t *testing.T) {
	caller := "aaa"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
		want1  bool
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records: &list.List{},
						m:       map[string]*list.Element{},
					},
				},
				mux: sync.Mutex{},
			},
			args:  args{caller},
			want:  []string{},
			want1: false,
		},
		{
			name: "case2",
			fields: fields{
				traces: map[string]*trace{},
				mux:    sync.Mutex{},
			},
			args:  args{caller},
			want:  nil,
			want1: true,
		},
		{
			name: "case3",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records:  &list.List{},
						m:        map[string]*list.Element{},
						canceled: true,
					},
				},
				mux: sync.Mutex{},
			},
			args:  args{caller},
			want:  nil,
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			got, got1 := f.cancel(tt.args.caller)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cancel() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("cancel() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_futureTracer_isCanceled(t *testing.T) {
	caller := "aaa"
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		caller string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records:  &list.List{},
						m:        map[string]*list.Element{},
						canceled: true,
					},
				},
				mux: sync.Mutex{},
			},
			args:    args{caller},
			want:    true,
			wantErr: false,
		},
		{
			name: "case2",
			fields: fields{
				traces: map[string]*trace{},
				mux:    sync.Mutex{},
			},
			args:    args{caller},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			got, err := f.isCanceled(tt.args.caller)
			if (err != nil) != tt.wantErr {
				t.Errorf("isCanceled() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isCanceled() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_futureTracer_getRecordsWithLock(t *testing.T) {
	caller := "aaa"
	list1 := list.New()
	list1.PushBack("bbb")
	list1.PushBack("ccc")
	list2 := list.New()
	list2.PushBack(uint32(111))
	type fields struct {
		traces map[string]*trace
		mux    sync.Mutex
	}
	type args struct {
		t *trace
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{
			name: "case1",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records:  list1,
						m:        map[string]*list.Element{},
						canceled: true,
					},
				},
				mux: sync.Mutex{},
			},
			args: args{
				&trace{
					records: list1,
				},
			},
			want: []string{"bbb", "ccc"},
		},
		{
			name: "case2",
			fields: fields{
				traces: map[string]*trace{
					caller: {
						records:  list2,
						m:        map[string]*list.Element{},
						canceled: true,
					},
				},
				mux: sync.Mutex{},
			},
			args: args{
				&trace{
					records: list2,
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &futureTracer{
				traces: tt.fields.traces,
				mux:    tt.fields.mux,
			}
			if got := f.getRecordsWithLock(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRecordsWithLock() = %v, want %v", got, tt.want)
			}
		})
	}
}
