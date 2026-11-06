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

// Package tcpmsg is tcp msg struct and protocol
package tcpmsg

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MessageType -
type MessageType byte

const (
	headerLen            = packetIDLen + uint32Len + 3
	packetIDLen          = uuidLen + uint64Len
	uuidLen              = 16
	uint64Len            = 8
	defaultMagicNum byte = 0x01
	defaultVersion  byte = 0x01
	maxPkgLen            = 1024 * 1024 * 128 // 128M
	byteLen              = 1
	uint32Len            = 4
)

const (
	// StopProcess stop process of runtime
	StopProcess MessageType = iota
	// InitProcess init process of runtime
	InitProcess
	// ReportLog report std log collection to worker
	ReportLog
	// LogFinished report std log collection finished, triggered by FinishRequest
	LogFinished
	// RequestFinished request finished information from worker
	RequestFinished
	// ReportMemory report memory used
	ReportMemory
	// GotRuntimeException get exception from runtime manager
	GotRuntimeException
	// StopRuntimeManager stop runtime-manager process
	StopRuntimeManager
	// ReportRequest report request in runtime
	ReportRequest
	// PauseRuntimeProcess pause runtime process
	PauseRuntimeProcess
	// RecoverRuntimeProcess recover runtime process
	RecoverRuntimeProcess
	// KillRuntimeProcess -
	KillRuntimeProcess
	// RuntimeProcessExit notifies the worker the current runtime process has exited.
	RuntimeProcessExit
	// RestoreDead restore dead
	RestoreDead
	// RestartRuntimeProcess tells runtime-manager to restart the runtime process
	RestartRuntimeProcess
	// ResetDomainName set domainName and IP in hosts file.
	ResetDomainName
	// FastFreezeCheckpoint checkpoint current runtime process by fastfreeze
	FastFreezeCheckpoint
	// ReportCPU report memory used
	ReportCPU
	// CollectCPUUsed collect memory used
	CollectCPUUsed
	// InitExtensionProcess start extension process
	InitExtensionProcess
	// ExtensionProcessExit notifies the current extension process has exited.
	ExtensionProcessExit
	// ExecPreStopCommand -
	ExecPreStopCommand
	// ReportCaeMetrics collect net data
	ReportCaeMetrics
	// TermCAEProcess terminate cae process
	TermCAEProcess
	// ResetDNSConfig reset dns config
	ResetDNSConfig
	// ExecuteTenantCommands -
	ExecuteTenantCommands
)

const (
	// MessageWithNoResponse send message and no need to response
	MessageWithNoResponse = -1
	// MessageWithSuccessResponse send success message
	MessageWithSuccessResponse = 0
	// RuntimeRestarted function runtime restarted code
	RuntimeRestarted = 1
	// MessageWithTermError terminate cae process error
	MessageWithTermError = 2
	// MessageWithTermTimeout terminate cae process time out
	MessageWithTermTimeout = 3
	// MessageWithError error need to handle
	MessageWithError = 4
)

// RuntimeProcessStatus -
type RuntimeProcessStatus int

const (
	// RuntimeProcessStatusInit -
	RuntimeProcessStatusInit RuntimeProcessStatus = -2
	// RuntimeProcessStatusException -
	RuntimeProcessStatusException RuntimeProcessStatus = -1
	// RuntimeProcessStatusRunning -
	RuntimeProcessStatusRunning RuntimeProcessStatus = 0
	// RuntimeProcessStatusReconnect -
	RuntimeProcessStatusReconnect RuntimeProcessStatus = 1
	// RuntimeProcessStatusRecover -
	RuntimeProcessStatusRecover RuntimeProcessStatus = 18
	// RuntimeProcessStatusPaused -
	RuntimeProcessStatusPaused RuntimeProcessStatus = 19
	// RuntimeProcessStatusFinalize -
	RuntimeProcessStatusFinalize RuntimeProcessStatus = 20
	// RuntimeProcessStatusReconnectFailed -
	RuntimeProcessStatusReconnectFailed RuntimeProcessStatus = 21
)

// RuntimeRSP runtime response info
type RuntimeRSP struct {
	Port      int32
	RuntimeID string
	Result    string
}

// RuntimeManagerRSP runtime manager response
type RuntimeManagerRSP struct {
	Err      string
	Code     int
	Response RuntimeRSP
}

// Header -
type Header struct {
	MagicNumber byte        // 1 byte
	Version     byte        // 1 byte
	MsgType     MessageType // 1 byte
	BodyLen     uint32      // 4 byte
	PacketID    string      // 24 byte
}

// Message -
type Message struct {
	Header Header
	Body   []byte
}

// GetMsgType get msg type
func (m *Message) GetMsgType() MessageType {
	return m.Header.MsgType
}

// Encode encode message to bytes
func Encode(msg *Message) ([]byte, error) {
	// set header
	msg.Header.MagicNumber = defaultMagicNum
	msg.Header.Version = defaultVersion
	msg.Header.BodyLen = uint32(len(msg.Body))
	// check message
	if err := checkMsg(msg); err != nil {
		return nil, err
	}
	pkgLen := headerLen + msg.Header.BodyLen

	if pkgLen < 0 {
		return nil, fmt.Errorf("sended length is less than 0")
	}
	if pkgLen > maxPkgLen {
		return nil, fmt.Errorf("sended length is large than 128M")
	}
	data := make([]byte, pkgLen, pkgLen)

	copy(data[0:byteLen], []byte{msg.Header.MagicNumber})
	offset := uint32(byteLen)
	copy(data[offset:offset+byteLen], []byte{msg.Header.Version})
	offset = offset + byteLen
	copy(data[offset:offset+byteLen], []byte{byte(msg.Header.MsgType)})
	offset = offset + byteLen
	binary.BigEndian.PutUint32(data[offset:offset+uint32Len], uint32(msg.Header.BodyLen))
	offset = offset + uint32Len
	copy(data[offset:offset+packetIDLen], []byte(msg.Header.PacketID))
	offset = offset + packetIDLen
	copy(data[offset:pkgLen], msg.Body)
	return data, nil
}

// Decode decode bytes to message
func Decode(r io.Reader) (*Message, error) {
	msg := &Message{}
	if err := msg.Header.decode(r); err != nil {
		return nil, err
	}
	bodyBytes := make([]byte, msg.Header.BodyLen, msg.Header.BodyLen)
	n, err := io.ReadFull(r, bodyBytes)
	if err != nil {
		return nil, err
	}
	if n != int(msg.Header.BodyLen) {
		return nil, fmt.Errorf("decode message body error")
	}
	msg.Body = bodyBytes

	err = checkMsg(msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func checkMsg(msg *Message) error {
	if len(msg.Header.PacketID) != packetIDLen {
		return fmt.Errorf("packetID length is %d, should is %d", len(msg.Header.PacketID), packetIDLen)
	}
	return nil
}

func (h *Header) decode(r io.Reader) error {
	headerBytes := make([]byte, headerLen, headerLen)
	// read headerLen bytes
	n, err := io.ReadFull(r, headerBytes)
	if err != nil {
		return err
	}
	if n != headerLen {
		return fmt.Errorf("decode header error, headerLen %d, should %d", n, headerLen)
	}
	if headerBytes[0] != defaultMagicNum {
		return fmt.Errorf("decode message get error magicNumber: %d", headerBytes[0])
	}
	h.MagicNumber = headerBytes[0]
	offset := byteLen
	h.Version = headerBytes[offset]
	offset = offset + byteLen
	h.MsgType = MessageType(headerBytes[offset])
	offset = offset + byteLen
	h.BodyLen = binary.BigEndian.Uint32(headerBytes[offset : offset+uint32Len])
	offset = offset + uint32Len
	h.PacketID = string(headerBytes[offset : offset+packetIDLen])
	return nil
}
