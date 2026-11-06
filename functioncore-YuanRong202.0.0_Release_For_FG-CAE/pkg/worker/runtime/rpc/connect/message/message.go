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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	headerLen   = 47 // fixed header length
	packetIDLen = 36 // fixed packetID length, create by uuid
	byteLen     = 1
	uint32Len   = 4
	magicNumber = 0x01
	maxPkgLen   = 1024 * 1024 * 128 // 128M
)

// PacketType four packet types
type PacketType byte

const (
	// Req request type
	Req PacketType = iota
	// NormalResp normal response type
	NormalResp PacketType = 1
	// ErrorResp error response type
	ErrorResp PacketType = 2
	// Post post message type
	Post PacketType = 3
	// DisconnectResp response type returned when client disconnected from server
	DisconnectResp PacketType = 4
)

// Header the header of msg, has fixed length 48 byte
type Header struct {
	magicNumber byte       // 1 byte
	Version     byte       // 1 byte
	PacketType  PacketType // 1 byte
	metaDataLen uint32     // 4 byte
	payloadLen  uint32     // 4 byte
	PacketID    string     // 36 byte
}

// Message Message has Header and Payload
type Message struct {
	Header   Header
	MetaData []byte
	Payload  []byte
}

// Encode encode msg to []byte
func Encode(msg *Message, buf *bytes.Buffer) error {
	if err := setMsg(msg); err != nil {
		return err
	}
	if err := checkMsg(msg); err != nil {
		return err
	}
	// the length of each sum element is limited to prevent integer overflow
	if msg.Header.metaDataLen > maxPkgLen || msg.Header.payloadLen > maxPkgLen {
		return fmt.Errorf("send length is large than 128M")
	}

	pkgLen := headerLen + msg.Header.metaDataLen + msg.Header.payloadLen

	if pkgLen > maxPkgLen {
		return fmt.Errorf("send length is large than 128M")
	}

	buf.Reset()
	buf.Grow(int(pkgLen))

	if _, err := buf.Write(
		[]byte{msg.Header.magicNumber, msg.Header.Version, byte(msg.Header.PacketType)}); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.metaDataLen); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.payloadLen); err != nil {
		return err
	}
	if _, err := buf.WriteString(msg.Header.PacketID); err != nil {
		return err
	}
	if _, err := buf.Write(msg.MetaData); err != nil {
		return err
	}
	if _, err := buf.Write(msg.Payload); err != nil {
		return err
	}

	return nil
}

func setMsg(msg *Message) error {
	msg.Header.magicNumber = magicNumber
	metaDataLen := len(msg.MetaData)
	payloadLen := len(msg.Payload)
	// prevent integer overflow
	if metaDataLen > maxPkgLen || payloadLen > maxPkgLen {
		return fmt.Errorf("send length is large than 128M")
	}
	msg.Header.metaDataLen = uint32(metaDataLen)
	msg.Header.payloadLen = uint32(payloadLen)
	return nil
}

func checkMsg(msg *Message) error {
	if msg.Header.magicNumber != magicNumber {
		return fmt.Errorf("magicNumber is %v, should is %v", msg.Header.magicNumber, magicNumber)
	}
	if len(msg.Header.PacketID) != packetIDLen {
		return fmt.Errorf("packetID length is %d ,  should is %d", len(msg.Header.PacketID), packetIDLen)
	}
	return nil
}

// Decode decode []byte to msg
func Decode(msg *Message, r io.Reader) error {
	err := msg.Header.decode(r)
	if err != nil {
		return err
	}

	metaDataBytes := make([]byte, msg.Header.metaDataLen, msg.Header.metaDataLen)
	n, err := io.ReadFull(r, metaDataBytes)
	if err != nil {
		return err
	}
	if n != int(msg.Header.metaDataLen) {
		return fmt.Errorf("decode message payload error")
	}
	msg.MetaData = metaDataBytes

	payLoadBytes := make([]byte, msg.Header.payloadLen, msg.Header.payloadLen)
	n, err = io.ReadFull(r, payLoadBytes)
	if err != nil {
		return err
	}
	if n != int(msg.Header.payloadLen) {
		return fmt.Errorf("decode message metatData error")
	}
	msg.Payload = payLoadBytes

	err = checkMsg(msg)
	if err != nil {
		return err
	}
	return nil
}

var headerPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, headerLen, headerLen)
	},
}

func (h *Header) decode(r io.Reader) error {
	headerBytesIf := headerPool.Get()
	headerBytes, ok := headerBytesIf.([]byte)
	if !ok {
		return errors.New("failed to assert")
	}
	defer headerPool.Put(headerBytes)

	// read headerLen bytes
	n, err := io.ReadFull(r, headerBytes)
	if err != nil {
		return err
	}
	if n != headerLen {
		return fmt.Errorf("decode header error, headerLen %d, should %d", n, headerLen)
	}
	if headerBytes[0] != magicNumber {
		return fmt.Errorf("decode message get error magicNumber: %d", headerBytes[0])
	}

	h.magicNumber = headerBytes[0]
	offset := byteLen
	h.Version = headerBytes[offset]
	offset = offset + byteLen
	h.PacketType = PacketType(headerBytes[offset])
	offset = offset + byteLen
	h.metaDataLen = binary.BigEndian.Uint32(headerBytes[offset : offset+uint32Len])
	offset = offset + uint32Len
	h.payloadLen = binary.BigEndian.Uint32(headerBytes[offset : offset+uint32Len])
	offset = offset + uint32Len
	h.PacketID = string(headerBytes[offset : offset+packetIDLen])
	return nil
}
