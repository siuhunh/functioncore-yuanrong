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

package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/google/uuid"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/pool"
	"functioncore/pkg/functionbus/proxyerrors"
)

func TestBaseMessage(t *testing.T) {
	packetID := uuid.New().String()
	req := NewMessage()
	req.Header = MsgHeader{
		MagicNumber:   0x08,
		Version:       0x01,
		MsgType:       0,
		ErrCode:       0,
		CompressType:  0,
		SerializeType: 0,
		PacketID:      packetID,
	}

	req.Header.MetaData = map[string]string{
		"name": "country",
	}

	payload := `{
                "A": 10,
                "B": 20,
        }
        `
	req.Payload = []byte(payload)

	encoded := Encode(req)

	var buf bytes.Buffer
	buf.Write(encoded)

	msg := NewMessage()
	err1 := Decode(msg, &buf)
	if err1 != nil {
		t.Fatal(err1)
	}

	if msg.Header.MagicNumber != 0x08 {
		t.Errorf("expect 0x08 but got %x", msg.Header.MagicNumber)
	}

	if msg.Header.Version != 0x01 {
		t.Errorf("expected 0x01 but got %x", msg.Header.Version)
	}

	if msg.Header.MsgType != 0 {
		t.Errorf("expected 0 but got %d", msg.Header.MsgType)
	}

	if msg.Header.ErrCode != 0 {
		t.Errorf("expected 0 but got %d", msg.Header.ErrCode)
	}

	if msg.Header.CompressType != 0 {
		t.Errorf("expected 0 but got %d", msg.Header.CompressType)
	}

	if msg.Header.SerializeType != 0 {
		t.Errorf("expected 0 but got %d", msg.Header.SerializeType)
	}

	if msg.Header.PacketID != packetID {
		t.Errorf("get error packetID %s", msg.Header.PacketID)
	}

	for k, v := range msg.Header.MetaData {
		if k != "name" {
			t.Errorf("expected 'name' but got %s", k)
		}

		if v != "country" {
			t.Errorf("expected 'country' but got %s", v)
		}
	}

	if reflect.DeepEqual(msg.Payload, req.Payload) == false {
		t.Errorf("expected req.Payload but got %v", msg.Payload)
	}
}

func TestBaseMsgs(t *testing.T) {
	req := NewMessage()
	packetID := uuid.New().String()
	req.Header = MsgHeader{
		MagicNumber:   0x08,
		Version:       0x01,
		ErrCode:       0,
		CompressType:  0,
		SerializeType: 0,
		PacketID:      packetID,
	}

	req.Header.MetaData = map[string]string{
		"name": "country",
	}

	payload := `{
                "A": 10,
                "B": 20,
        }
        `
	req.Payload = []byte(payload)

	encoded := Encode(req)

	var buf bytes.Buffer
	buf.Write(encoded)

	msg := NewMessage()
	err := Decode(msg, &buf)
	if msg == nil || err != nil {
		t.Errorf("decode fail: %s", err)
	}
}

func TestNewHeartbeat(t *testing.T) {
	msg := &Message{}
	msg.Header.PacketID = uuid.New().String()

	encoded := Encode(msg)
	data := make([]byte, 2*len(encoded), 2*len(encoded))
	copy(data[0:len(encoded)], encoded)
	copy(data[len(encoded):], encoded)
	var buf bytes.Buffer
	buf.Write(encoded)
	fmt.Println("1 encode", encoded)
	fmt.Println("2 encode", data)
	buf.Write(data)
	fmt.Println("buf ", buf)
	rsp := NewMessage()
	err := Decode(rsp, &buf)
	if rsp == nil || err != nil {
		t.Errorf("decode fail: %s", err)
	}

	rsp = NewMessage()
	err = Decode(rsp, &buf)
	if rsp == nil || err != nil {
		t.Errorf("decode fail: %s", err)
	}
}

func TestPacketIDIsEmpty(t *testing.T) {
	convey.Convey("TestPacketIDIsEmpty", t, func() {
		convey.Convey("long MetaDataLen", func() {
			req := NewMessage()
			req.Header = MsgHeader{
				MagicNumber:   0x08,
				Version:       0x01,
				MsgType:       0,
				ErrCode:       0,
				CompressType:  0,
				SerializeType: 0,
				PacketID:      "",
			}

			req.Header.MetaData = map[string]string{
				"name": "country",
			}

			payload := `{
               "A": 10,
               "B": 20,
        	}
        	`
			req.Payload = []byte(payload)

			var buf bytes.Buffer
			NewEncode(&buf, req)
			err := Decode(NewMessage(), &buf)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, "length of decoding is too long MetaDataLen")
		})
		convey.Convey("packet id decode error", func() {
			req := NewMessage()
			req.Header = MsgHeader{
				MagicNumber:   0x08,
				Version:       0x01,
				MsgType:       0,
				ErrCode:       0,
				CompressType:  0,
				SerializeType: 0,
				PacketID:      "",
			}

			req.Header.MetaData = map[string]string{
				"name": "country",
			}

			req.Payload = make([]byte, 40, 40)
			req.Payload[38] = 'a'
			raw := req.Payload
			var buf bytes.Buffer
			NewEncode(&buf, req)
			msg := NewMessage()
			err := Decode(msg, &buf)
			if err != nil {
				t.Fatal(err)
			}
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(raw, convey.ShouldNotEqual, msg.Payload)
		})
	})

}

func TestSingleModel(t *testing.T) {
	convey.Convey("TestSingleModel", t, func() {
		convey.Convey("test NewHeartBeat", func() {
			var heartBeats1, heartBeats2 []byte
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				heartBeats1 = NewHeartBeat()
				wg.Done()
			}()
			go func() {
				heartBeats2 = NewHeartBeat()
				wg.Done()
			}()
			wg.Wait()
			convey.So(string(heartBeats1), convey.ShouldEqual, string(heartBeats2))
		})
		convey.Convey("test NewACK", func() {
			var ack1, ack2 []byte
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				ack1 = NewACK()
				wg.Done()
			}()
			go func() {
				ack2 = NewACK()
				wg.Done()
			}()
			wg.Wait()
			convey.So(string(ack1), convey.ShouldEqual, string(ack2))
		})
	})
}

func TestEncode(t *testing.T) {
	req := NewMessage()
	packetID := uuid.New().String()
	req.Header = MsgHeader{
		MagicNumber:   0x08,
		Version:       0x01,
		ErrCode:       0,
		CompressType:  0,
		SerializeType: 0,
		PacketID:      packetID,
	}

	req.Header.MetaData = map[string]string{
		"name": "country",
	}
	req.Payload = []byte("abcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrst")

	var buf bytes.Buffer
	NewEncode(&buf, req)

	msg := NewMessage()
	err := Decode(msg, &buf)
	if msg == nil || err != nil {
		t.Errorf("decode fail: %s", err)
	}
	fmt.Println("payload ", string(msg.Payload))
}

func BenchmarkEncode(b *testing.B) {
	req := NewMessage()
	packetID := uuid.New().String()
	req.Header = MsgHeader{
		MagicNumber:   0x08,
		Version:       0x01,
		ErrCode:       0,
		CompressType:  0,
		SerializeType: 0,
		PacketID:      packetID,
	}

	req.Header.MetaData = map[string]string{
		"name": "country",
	}
	req.Payload = []byte("abcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrst")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(req)
	}
}

func BenchmarkNewEncode(b *testing.B) {
	req := NewMessage()
	packetID := uuid.New().String()
	req.Header = MsgHeader{
		MagicNumber:   0x08,
		Version:       0x01,
		ErrCode:       0,
		CompressType:  0,
		SerializeType: 0,
		PacketID:      packetID,
	}

	req.Header.MetaData = map[string]string{
		"name": "country",
	}
	buf := pool.BuffPool.Get()
	req.Payload = []byte("abcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrst")
	bp.Put(make([]byte, intLen, intLen))
	bp.Put(make([]byte, intLen, intLen))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewEncode(buf, req)
		buf.Reset()
	}
}

func TestDecode(t *testing.T) {
	convey.Convey("TestDecode", t, func() {
		convey.Convey("test err 1", func() {
			req := NewMessage()
			packetID := uuid.New().String()
			req.Header = MsgHeader{
				MagicNumber:   0x08,
				Version:       0x01,
				ErrCode:       0,
				CompressType:  0,
				SerializeType: 0,
				PacketID:      packetID,
			}
			req.Header.MetaData = map[string]string{
				"name": "country",
			}
			req.Payload = []byte("abcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrst")

			patch := gomonkey.ApplyFunc(monitor.StatefulFunctionReqMemSeparator, func(isStateful bool, memoryUsed uint64) bool {
				return false
			})
			patch.ApplyFunc(io.CopyN, func(dst io.Writer, src io.Reader, n int64) (written int64, err error) {
				return 1, nil
			})
			defer patch.Reset()

			var buf bytes.Buffer
			NewEncode(&buf, req)

			msg := NewMessage()
			err := Decode(msg, &buf)
			if err != nil || msg.Header.ErrCode != proxyerrors.HeavyLoadCode {
				t.Errorf("decode fail: %s", err)
			}
		})
		convey.Convey("test err 2", func() {
			req := NewMessage()
			packetID := uuid.New().String()
			req.Header = MsgHeader{
				MagicNumber:   0x08,
				Version:       0x01,
				ErrCode:       0,
				CompressType:  0,
				SerializeType: 0,
				PacketID:      packetID,
			}
			req.Header.MetaData = map[string]string{
				"name": "country",
			}
			req.Payload = []byte("abcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrstabcdefghijklmnopqrst")

			patch := gomonkey.ApplyFunc(monitor.StatefulFunctionReqMemSeparator, func(isStateful bool, memoryUsed uint64) bool {
				return false
			})
			patch.ApplyFunc(io.CopyN, func(dst io.Writer, src io.Reader, n int64) (written int64, err error) {
				return 1, errors.New("CopyN error")
			})
			defer patch.Reset()

			var buf bytes.Buffer
			NewEncode(&buf, req)

			msg := NewMessage()
			err := Decode(msg, &buf)
			convey.ShouldNotBeNil(err)
		})
	})
}

func Test_determineStatefulMsg(t *testing.T) {
	var meta map[string]string
	if determineStatefulMsg(meta) {
		t.Errorf("this is error")
	}
	meta = make(map[string]string)
	meta[StatefulMsgKey] = StatefulMsgKey
	if !determineStatefulMsg(meta) {
		t.Errorf("this is error")
	}
	delete(meta, StatefulMsgKey)
	meta["testKey"] = StatefulMsgKey
	if determineStatefulMsg(meta) {
		t.Errorf("this is error")
	}
}
