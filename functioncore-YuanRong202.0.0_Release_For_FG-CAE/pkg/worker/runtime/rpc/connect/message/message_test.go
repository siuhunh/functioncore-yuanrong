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

// Package message define message used in socket connection
package message

import (
	"bytes"
	"fmt"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestOverflow(t *testing.T) {
	// if modify maxPkgLen, please prevent integer overflow
	if maxPkgLen+headerLen < 0 || maxPkgLen+maxPkgLen+headerLen < 0 {
		t.Errorf("package length in encode maybe overflow")
	}
}

func TestSetMsg(t *testing.T) {
	Convey("test set message", t, func() {
		Convey("test set message success", func() {
			err := setMsg(&Message{
				MetaData: make([]byte, maxPkgLen),
				Payload: make([]byte, maxPkgLen),
			})
			So(err, ShouldEqual, nil)
		})
		Convey("test set message fail", func() {
			err := setMsg(&Message{
				MetaData: make([]byte, maxPkgLen + 1),
				Payload: make([]byte, maxPkgLen + 1),
			})
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestCheckMsg(t *testing.T) {
	Convey("test check message", t, func() {
		Convey("test check message success", func() {
			err := checkMsg(&Message{
				Header: Header{
					magicNumber: magicNumber,
					PacketID: string(make([]byte, packetIDLen)),
				},
			})
			So(err, ShouldEqual, nil)
		})
		Convey("test check message magicNumber fail", func() {
			err := checkMsg(&Message{
				Header: Header{
					magicNumber: magicNumber + 1,
					PacketID: string(make([]byte, packetIDLen)),
				},
			})
			So(err, ShouldNotEqual, nil)
		})
		Convey("test check message PacketID fail", func() {
			err := checkMsg(&Message{
				Header: Header{
					magicNumber: magicNumber,
					PacketID: string(make([]byte, packetIDLen + 1)),
				},
			})
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestEncode(t *testing.T) {
	header := Header{
		PacketID: uuid.New().String(), // must be 36 bytes string. Or runtime will malfunction.
	}
	metaData := []byte("metaData")
	payload := []byte("payload")
	msg := &Message{
		Header:   header,
		MetaData: metaData,
		Payload: payload,
	}
	Convey("test encode function", t, func() {
		Convey("test set message error", func() {
			patch := ApplyFunc(setMsg, func(message *Message) error {
				return fmt.Errorf("test set message error")
			})
			defer patch.Reset()
			err := Encode(msg, &bytes.Buffer{})
			So(err, ShouldNotEqual, nil)
		})
		Convey("test check message error", func() {
			patch := ApplyFunc(checkMsg, func(message *Message) error {
				return fmt.Errorf("test check message error")
			})
			defer patch.Reset()
			err := Encode(msg, &bytes.Buffer{})
			So(err, ShouldNotEqual, nil)
		})
		Convey("test message header error", func() {
			patch := ApplyFunc(setMsg, func(message *Message) error {
				message.Header.metaDataLen = maxPkgLen / 2
				message.Header.payloadLen = maxPkgLen / 2
				return nil
			})
			defer patch.Reset()
			err := Encode(msg, &bytes.Buffer{})
			So(err, ShouldNotEqual, nil)
		})
		Convey("test message pkgLen error", func() {
			patch := ApplyFunc(setMsg, func(message *Message) error {
				message.Header.metaDataLen = maxPkgLen + 1
				return nil
			})
			defer patch.Reset()
			err := Encode(msg, &bytes.Buffer{})
			So(err, ShouldNotEqual, nil)
		})
		Convey("test message encode success", func() {
			err := Encode(msg, &bytes.Buffer{})
			So(err, ShouldEqual, nil)
		})
	})
}

func TestDecode(t *testing.T) {
	header := Header{
		PacketID: uuid.New().String(), // must be 36 bytes string. Or runtime will malfunction.
	}
	metaData := []byte("metaData")
	payload := []byte("payload")
	msg := &Message{
		Header:   header,
		MetaData: metaData,
		Payload: payload,
	}
	Convey("test decode function", t, func() {
		Convey("test message decode success", func() {
			buf := &bytes.Buffer{}
			err := Encode(msg, buf)
			So(err, ShouldEqual, nil)

			decoded := &Message{}
			err = Decode(decoded, buf)
			So(err, ShouldEqual, nil)
			So(string(decoded.Payload), ShouldEqual, string(payload))
		})
	})
}