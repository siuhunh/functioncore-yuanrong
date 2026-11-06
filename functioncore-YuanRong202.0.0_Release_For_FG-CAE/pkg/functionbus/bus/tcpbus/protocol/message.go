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

// Package protocol encode and decode message
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/pool"
	"functioncore/pkg/functionbus/proxyerrors"
)

// BusMagicNum is magic number
const (
	BusMagicNum byte = 0x08
	BusVersion  byte = 0x01

	uint32Len = 4
	intLen    = 4
	byteLen   = 1

	metaDataInitSize = 4
	fixedLen         = 53
	packIDLen        = 36

	maxPkgLen = 1024 * 1024 * 500 // maxlength 500MB
	// StatefulMsgKey -
	StatefulMsgKey = "statefulFunc"
)

// MessageType is the message type
type MessageType byte

// MessageRequest message type is request
const (
	CallReqMsg MessageType = iota
	CallRspMsg
	HeartBeat
	ExitSignal
	AuthCheckMsg
	ACK
	ReportRspMsg
	CtrlRuntimeMsg
)

// CompressType is compress type
type CompressType byte

// CompressNone compress is none
const (
	CompressNone CompressType = iota
)

var (
	responseMemoryEvaluator = 1.0
)

var heartBeat = struct {
	sync.Once
	msgData []byte
}{}

var ack = struct {
	sync.Once
	msgData []byte
}{}

// MsgHeader is the header of message
type MsgHeader struct {
	MagicNumber   byte
	Version       byte
	MsgType       MessageType
	CompressType  byte   // reserved field, message compress is not yet supported
	SerializeType byte   // reserved field, payload need not serialize
	PacketID      string // uuid 36 byte
	ErrCode       uint32
	MetaDataLen   uint32
	PayloadLen    uint32
	MetaData      map[string]string
}

// Message contains header and payload
type Message struct {
	Header          MsgHeader
	Payload         []byte
	CffHTTPResponse bool
}

// Reset -
func (m *Message) Reset() {
	m.Header = MsgHeader{}
	m.Header.MagicNumber = BusMagicNum
	m.Header.Version = BusVersion
	m.Payload = m.Payload[:0]
}

// SetRequestMemoryEvaluator set responseMemoryEvaluator
func SetRequestMemoryEvaluator(evaluator float64) {
	responseMemoryEvaluator = evaluator
}

// NewHeartBeat create new heartbeat message
func NewHeartBeat() []byte {
	heartBeat.Do(func() {
		msg := &Message{}
		msg.Header.MagicNumber = BusMagicNum
		msg.Header.Version = BusVersion
		msg.Header.MsgType = HeartBeat
		heartBeat.msgData = Encode(msg)
	})
	return heartBeat.msgData
}

// NewACK create new ACK message
func NewACK() []byte {
	ack.Do(func() {
		msg := &Message{}
		msg.Header.MagicNumber = BusMagicNum
		msg.Header.Version = BusVersion
		msg.Header.MsgType = ACK
		ack.msgData = Encode(msg)
	})
	return ack.msgData
}

// NewMessage create new request message
func NewMessage() *Message {
	msg := &Message{}
	msg.Header.MagicNumber = BusMagicNum
	msg.Header.Version = BusVersion
	return msg
}

func newEncodeMetadata(buf *bytes.Buffer, metaData map[string]string) {
	if len(metaData) == 0 {
		return
	}

	tmp := bp.Get()
	// convert map's len and body to byte slice
	for k, v := range metaData {
		binary.BigEndian.PutUint32(tmp, uint32(len(k)))
		buf.Write(tmp)
		buf.WriteString(k)
		binary.BigEndian.PutUint32(tmp, uint32(len(v)))
		buf.Write(tmp)
		buf.WriteString(v)
	}
	bp.Put(tmp)
}

func newPreProEnCodeMsg(msg *Message, buf *bytes.Buffer) {
	newEncodeMetadata(buf, msg.Header.MetaData)
	msg.Header.Version = BusVersion
	msg.Header.MagicNumber = BusMagicNum
	msg.Header.MetaDataLen = (uint32)(buf.Len())
	msg.Header.PayloadLen = (uint32)(len(msg.Payload))
	if buf.Len() > maxPkgLen {
		msg.Header.MetaDataLen = maxPkgLen
	}
	if len(msg.Payload) > maxPkgLen {
		msg.Header.PayloadLen = maxPkgLen
	}
	return
}

// NewEncode encode Message to buffer
func NewEncode(data *bytes.Buffer, msg *Message) {
	metaData := pool.BuffPool.Get()
	newPreProEnCodeMsg(msg, metaData)

	data.WriteByte(msg.Header.MagicNumber)
	data.WriteByte(msg.Header.Version)
	data.WriteByte(byte(msg.Header.MsgType))
	data.WriteByte(msg.Header.CompressType)
	data.WriteByte(msg.Header.SerializeType)
	data.WriteString(msg.Header.PacketID)

	tmp := bp.Get()
	binary.BigEndian.PutUint32(tmp, msg.Header.ErrCode)
	data.Write(tmp)
	binary.BigEndian.PutUint32(tmp, msg.Header.MetaDataLen)
	data.Write(tmp)
	binary.BigEndian.PutUint32(tmp, msg.Header.PayloadLen)
	data.Write(tmp)
	bp.Put(tmp)

	metaData.WriteTo(data)
	pool.BuffPool.Put(metaData)

	data.Write(msg.Payload)
	msg.Payload = nil
}

// Encode Message to binary.
func Encode(msg *Message) []byte {
	totalLen, metaData := preProEnCodeMsg(msg)
	data := make([]byte, totalLen, totalLen)

	copy(data[0:byteLen], []byte{msg.Header.MagicNumber})
	offset := byteLen
	copy(data[offset:offset+byteLen], []byte{msg.Header.Version})
	offset = offset + byteLen
	copy(data[offset:offset+byteLen], []byte{byte(msg.Header.MsgType)})
	offset = offset + byteLen
	copy(data[offset:offset+byteLen], []byte{msg.Header.CompressType})
	offset = offset + byteLen
	copy(data[offset:offset+byteLen], []byte{msg.Header.SerializeType})
	offset = offset + byteLen
	copy(data[offset:offset+packIDLen], msg.Header.PacketID)
	offset = offset + packIDLen
	binary.BigEndian.PutUint32(data[offset:offset+uint32Len], msg.Header.ErrCode)
	offset = offset + uint32Len
	binary.BigEndian.PutUint32(data[offset:offset+uint32Len], msg.Header.MetaDataLen)
	offset = offset + uint32Len
	binary.BigEndian.PutUint32(data[offset:offset+uint32Len], msg.Header.PayloadLen)
	offset = offset + uint32Len
	offset1 := uint32(offset)
	copy(data[offset1:offset1+msg.Header.MetaDataLen], metaData)

	offset1 = offset1 + msg.Header.MetaDataLen

	copy(data[offset1:offset1+msg.Header.PayloadLen], msg.Payload)
	return data
}

func preProEnCodeMsg(msg *Message) (uint32, []byte) {
	metaData := encodeMetadata(msg.Header.MetaData)
	msg.Header.Version = BusVersion
	msg.Header.MagicNumber = BusMagicNum
	msg.Header.MetaDataLen = (uint32)(len(metaData))
	msg.Header.PayloadLen = (uint32)(len(msg.Payload))
	// truncate message if message is too long
	if len(metaData) > maxPkgLen {
		msg.Header.MetaDataLen = maxPkgLen
	}
	if len(msg.Payload) > maxPkgLen {
		msg.Header.PayloadLen = maxPkgLen
	}
	totalLen := msgLen(msg)
	return totalLen, metaData
}

func msgLen(msg *Message) uint32 {
	totalLen := fixedLen + msg.Header.MetaDataLen + msg.Header.PayloadLen
	return totalLen
}

func encodeMetadata(metaData map[string]string) []byte {
	if len(metaData) == 0 {
		return []byte{}
	}

	buf := new(bytes.Buffer)
	var length = make([]byte, intLen, intLen)
	// convert map's len and body to byte slice
	for k, v := range metaData {
		binary.BigEndian.PutUint32(length, uint32(len(k)))
		buf.Write(length)
		buf.WriteString(k)
		binary.BigEndian.PutUint32(length, uint32(len(v)))
		buf.Write(length)
		buf.WriteString(v)
	}

	return buf.Bytes()
}

// Decode binary to Message.
// Magic number represents corresponding offset or data size.
func Decode(msg *Message, r io.Reader) error {
	var err error
	var readBytes []byte
	if readBytes, err = decodeMsg(r, fixedLen); err != nil {
		return err
	}
	if err = preprocess(msg, readBytes); err != nil {
		return err
	}
	if readBytes, err = decodeMsg(r, msg.Header.MetaDataLen); err != nil {
		return errors.New(err.Error() + " MetaDataLen")
	}

	if msg.Header.MetaData, err = decodeMetadata(msg.Header.MetaDataLen, []byte(readBytes)); err != nil {
		return fmt.Errorf(err.Error() + "decode metadata error")
	}

	if msg.Header.PayloadLen != 0 {
		memoryUsed := uint64(responseMemoryEvaluator * float64(msg.Header.PayloadLen))
		var isStatefulFunc bool
		if determineStatefulMsg(msg.Header.MetaData) || msg.Header.MsgType == HeartBeat ||
			msg.Header.MsgType == ACK || msg.Header.MsgType == AuthCheckMsg || msg.Header.MsgType == CallRspMsg {
			isStatefulFunc = true
		}
		if !monitor.StatefulFunctionReqMemSeparator(isStatefulFunc, memoryUsed) {
			log.GetLogger().Warnf("server memory overloaded! PayloadLen %d", msg.Header.PayloadLen)
			if _, err := io.CopyN(ioutil.Discard, r, int64(msg.Header.PayloadLen)); err != nil {
				log.GetLogger().Errorf("failed to discard message, %s", err.Error())
				return err
			}
			msg.Header.ErrCode = proxyerrors.HeavyLoadCode
			msg.Payload = []byte(proxyerrors.HeavyLoadErr)
			return nil
		}
	}

	if readBytes, err = decodeMsg(r, msg.Header.PayloadLen); err != nil {
		return err
	}
	msg.Payload = readBytes

	return nil
}

// determineStatefulMsg -
func determineStatefulMsg(metadata map[string]string) bool {
	if len(metadata) == 0 {
		return false
	}
	return metadata[StatefulMsgKey] != ""
}

// preprocess process the fixed-length header bytes
func preprocess(msg *Message, readBytes []byte) error {
	if len(readBytes) != fixedLen {
		return fmt.Errorf("fixed-length headers decode failed")
	}
	msg.Header.MagicNumber = readBytes[0]
	if msg.Header.MagicNumber != BusMagicNum {
		return errors.New("magic number mismatch:" + string(msg.Header.MagicNumber))
	}
	offset := byteLen
	msg.Header.Version = readBytes[offset]
	offset = offset + byteLen
	msg.Header.MsgType = MessageType(readBytes[offset])
	offset = offset + byteLen
	msg.Header.CompressType = readBytes[offset]
	offset = offset + byteLen
	msg.Header.SerializeType = readBytes[offset]
	offset = offset + byteLen
	msg.Header.PacketID = string(readBytes[offset : offset+packIDLen])
	offset = offset + packIDLen
	msg.Header.ErrCode = binary.BigEndian.Uint32(readBytes[offset : offset+uint32Len])
	offset = offset + uint32Len
	msg.Header.MetaDataLen = binary.BigEndian.Uint32(readBytes[offset : offset+uint32Len])
	offset = offset + uint32Len
	msg.Header.PayloadLen = binary.BigEndian.Uint32(readBytes[offset : offset+uint32Len])
	return nil
}

// decode Message by Specify length
func decodeMsg(r io.Reader, length uint32) ([]byte, error) {
	if length > maxPkgLen {
		return nil, errors.New("length of decoding is too long")
	}
	readBytes := make([]byte, length, length)
	_, err := io.ReadFull(r, readBytes)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errors.New(err.Error() + " decode error")
	}
	return readBytes, nil
}

func decodeMetadata(len uint32, data []byte) (map[string]string, error) {
	if len <= 0 {
		return nil, nil
	}
	result := make(map[string]string, metaDataInitSize)
	pos := uint32(0)
	for pos < len {
		// parse key
		keyLen := binary.BigEndian.Uint32(data[pos : pos+uint32Len])
		pos = pos + uint32Len
		if pos+keyLen+uint32Len > len {
			return result, fmt.Errorf("key or value are missing")
		}

		key := byteSlice2String(data[pos : pos+keyLen])
		pos = pos + keyLen

		// parse value
		valLen := binary.BigEndian.Uint32(data[pos : pos+uint32Len])
		pos = pos + uint32Len
		if pos+valLen > len {
			return result, fmt.Errorf("key or value are missing")
		}

		value := byteSlice2String(data[pos : pos+valLen])
		pos = pos + valLen
		result[key] = value
	}

	return result, nil
}

func byteSlice2String(b []byte) string {
	return string(b)
}
