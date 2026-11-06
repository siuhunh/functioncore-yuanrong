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

package invoke

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Request defines request for invoke.
type Request struct {
	RequestPb
	create bool
}

// SetStateID sets stateID.
func (req *Request) SetStateID(stateID string) {
	req.StateID = stateID
}

// GetCreateState returns true if invoke by key.
func (req *Request) GetCreateState() bool {
	return req.create
}

// GetPayload returns payload of the request.
func (req *Request) GetPayload() string {
	return req.Payload
}

// GetReqFutureID returns reqFutureID of the request.
func (req *Request) GetReqFutureID() string {
	return ""
}

// Response defines response for invoke.
type Response = ResponsePb

// Marshal marshal Response
func (resp *Response) Marshal() ([]byte, error) {
	return proto.Marshal(resp)
}

// Unmarshal unmarshal Response
func (resp *Response) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, resp)
}

// SetCode sets `Code` field of `Response`.
func (resp *Response) SetCode(code int32) {
	resp.Code = code
}

// SetMessage sets `Message` field of `Response`.
func (resp *Response) SetMessage(msg string) {
	resp.Message = msg
}

// Fields returns log fields.
func (resp *Response) Fields() []zap.Field {
	return []zap.Field{zap.String("stateID", resp.StateID), zap.String("futureID", resp.FutureID)}
}

// TaskRequest defines function task request for invoke.
type TaskRequest = TaskRequestPb

// TaskResponse defines function task response for invoke.
type TaskResponse = TaskResponsePb

// Marshal marshal TaskResponse
func (resp *TaskResponse) Marshal() ([]byte, error) {
	return proto.Marshal(resp)
}

// Unmarshal unmarshal TaskResponse
func (resp *TaskResponse) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, resp)
}

// SetCode sets `Code` field of `TaskResponse`.
func (resp *TaskResponse) SetCode(code int32) {
	resp.Code = code
}

// SetMessage sets `Message` field of `TaskResponse`.
func (resp *TaskResponse) SetMessage(msg string) {
	resp.Message = msg
}

// Fields returns log fields.
func (resp *TaskResponse) Fields() []zap.Field {
	return nil
}

// TaskCrateStateQueueRequest defines function task request for crate state queue.
type TaskCrateStateQueueRequest = TaskCrateStateQueueRequestPb

// OnCompleteRequest defines request for on complete.
type OnCompleteRequest struct {
	OnCompleteRequestPb
	create bool
}

// SetStateID sets stateID.
func (req *OnCompleteRequest) SetStateID(stateID string) {
	req.StateID = stateID
}

// GetCreateState returns true if invoke by key.
func (req *OnCompleteRequest) GetCreateState() bool {
	return req.create
}

// GetPayload returns payload of the request.
func (req *OnCompleteRequest) GetPayload() string {
	return ""
}

// GetReqFutureID returns reqFutureID of the request.
func (req *OnCompleteRequest) GetReqFutureID() string {
	return req.FutureID
}
