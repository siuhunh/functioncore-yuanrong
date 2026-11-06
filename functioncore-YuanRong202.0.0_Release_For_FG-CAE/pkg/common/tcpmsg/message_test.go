/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package tcpmsg

import (
	"bytes"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func Test_GetMsgType(t *testing.T) {
	convey.Convey("GetMsgType", t, func() {
		msg := &Message{
			Header: Header{
				MsgType: StopProcess,
			},
		}
		msgType := msg.GetMsgType()
		convey.So(msgType, convey.ShouldEqual, StopProcess)
	})
}

func Test_Encode(t *testing.T) {
	convey.Convey("Encode", t, func() {
		convey.Convey("failed", func() {
			msg := &Message{
				Header: Header{
					MsgType: StopProcess,
				},
			}
			data, err := Encode(msg)
			convey.So(data, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("successful", func() {
			msg := &Message{
				Header: Header{
					MsgType:  StopProcess,
					PacketID: newPacketID(),
				},
				Body: []byte("test"),
			}
			data, err := Encode(msg)
			convey.So(data, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_Decode(t *testing.T) {
	convey.Convey("Decode", t, func() {
		convey.Convey("failed case 1", func() {
			var buf bytes.Buffer
			buf.Write([]byte("test"))
			data, err := Decode(&buf)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(data, convey.ShouldBeNil)
		})

		convey.Convey("failed case 2", func() {
			var buf bytes.Buffer
			buf.Write([]byte("0123456789012345678901234567890123456789012"))
			data, err := Decode(&buf)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(data, convey.ShouldBeNil)
		})

		convey.Convey("successful case", func() {
			msg := &Message{
				Header: Header{
					MagicNumber: 0x08,
					Version:     0x01,
					MsgType:     StopProcess,
					BodyLen:     10,
					PacketID:    newPacketID(),
				},
				Body: []byte("0123456789"),
			}

			data, err := Encode(msg)
			convey.So(data, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)

			var buf bytes.Buffer
			buf.Write(data)
			decodeData, err := Decode(&buf)
			convey.So(decodeData, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}
