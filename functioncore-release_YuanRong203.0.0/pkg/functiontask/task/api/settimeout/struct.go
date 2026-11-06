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

package settimeout

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Request defines request for set timeout.
type Request = RequestPb

// Response defines response for set timeout.
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
	return []zap.Field{zap.String("futureID", resp.FutureID)}
}
