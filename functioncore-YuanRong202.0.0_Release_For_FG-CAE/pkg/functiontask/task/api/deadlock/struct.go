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

package deadlock

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/functiontask/task/common"
)

// StartRequest defines request for start deadlock checker.
type StartRequest = StartRequestPb

// StartResponse defines response for start deadlock checker.
type StartResponse = StartResponsePb

// Marshal marshal StartResponse
func (resp *StartResponse) Marshal() ([]byte, error) {
	return proto.Marshal(resp)
}

// Unmarshal unmarshal StartResponse
func (resp *StartResponse) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, resp)
}

// SetCode sets `Code` field of `StartResponse`.
func (resp *StartResponse) SetCode(code int32) {
	resp.Code = code
}

// SetMessage sets `Message` field of `StartResponse`.
func (resp *StartResponse) SetMessage(msg string) {
	resp.Message = msg
}

// Fields returns log fields.
func (resp *StartResponse) Fields() []zap.Field {
	return []zap.Field{zap.String("deadlockID", resp.DeadlockID)}
}

// EndRequest defines request for end deadlock checker.
type EndRequest = EndRequestPb

// EndResponse defines response for end deadlock checker.
type EndResponse = EndResponsePb

// Marshal marshal EndResponse
func (resp *EndResponse) Marshal() ([]byte, error) {
	return proto.Marshal(resp)
}

// Unmarshal unmarshal EndResponse
func (resp *EndResponse) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, resp)
}

// SetCode sets `Code` field of `EndResponse`.
func (resp *EndResponse) SetCode(code int32) {
	resp.Code = code
}

// SetMessage sets `Message` field of `EndResponse`.
func (resp *EndResponse) SetMessage(msg string) {
	resp.Message = msg
}

// Fields returns log fields.
func (resp *EndResponse) Fields() []zap.Field {
	return []zap.Field{zap.String("state", resp.State)}
}

// ReqIDList represents the request id list.
type ReqIDList = ReqIDListPb

// ConvertLists2Array converts []*deadlock.ReqIDList to []common.ReqIDList
func ConvertLists2Array(lists []*ReqIDList) []common.ReqIDList {
	reqIDLists := make([]common.ReqIDList, len(lists))
	for i, v := range lists {
		if v == nil {
			continue
		}
		reqIDLists[i] = (*v).ReqIDs
	}
	return reqIDLists
}

// ConvertLists2Struct converts []common.ReqIDList to []*deadlock.ReqIDList
func ConvertLists2Struct(lists []common.ReqIDList) []*ReqIDList {
	reqIDLists := make([]*ReqIDList, len(lists))
	for i, v := range lists {
		reqIDLists[i] = &ReqIDList{
			ReqIDs: v,
		}
	}
	return reqIDLists
}
