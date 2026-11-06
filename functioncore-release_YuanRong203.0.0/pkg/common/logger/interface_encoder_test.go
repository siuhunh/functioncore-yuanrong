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

package logger

import (
	"math"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/constants"
)

// TestNewInterfaceEncoder Test New Interface Encoder
func TestNewInterfaceEncoder(t *testing.T) {
	cfg := InterfaceEncoderConfig{
		ModuleName: "FunctionWorker",
		HTTPMethod: "POST",
		ModuleFrom: "FrontendInvoke",
		TenantID:   "tenant2",
		FuncName:   "myFunction",
		FuncVer:    "latest",
	}

	encoder := NewInterfaceEncoder(cfg, false)

	priority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.InfoLevel
	})

	sink := zapcore.Lock(os.Stdout)
	core := zapcore.NewCore(encoder, sink, priority)
	logger := zap.New(core)

	logger.Info("e1b71add-cb24-4ef8-93eb-af8d3ceb74e8|0|success|1")

	clone := encoder.Clone()
	assert.NotEmpty(t, clone)
	encoder.AddBool("3", true)
	err := encoder.AddArray("4", nil)
	assert.NotEmpty(t, err)
	err = encoder.AddObject("4", nil)
	assert.NotEmpty(t, err)
	encoder.AddBinary("4", []byte{})
	encoder.AddComplex128("4", complex(1, 2))
	encoder.AddComplex64("4", complex(1, 2))
	encoder.AddDuration("4", time.Second)
	encoder.AddByteString("4", []byte{})
	encoder.AddFloat64("2", 3.14)
	encoder.AddFloat32("2", 3.14)
	encoder.AddInt("1", 1)
	encoder.AddInt8("1", 1)
	encoder.AddInt16("1", 1)
	encoder.AddInt32("1", 1)
	encoder.AddInt64("1", 1)
	encoder.AddString("5", "12")
	encoder.AddString("5", "1 2")
	encoder.AddTime("6", time.Time{})
	encoder.AddUint("1", uint(1))
	encoder.AddUint8("1", uint8(10))
	encoder.AddUint16("1", uint16(100))
	encoder.AddUint32("1", uint32(1000))
	encoder.AddUint64("1", uint64(1000))
	b := make([]int, 1)
	encoder.AddUintptr("12", uintptr(unsafe.Pointer(&b[0])))
	encoder.OpenNamespace("3")
	err = encoder.AddReflected("3", 1)
	assert.Empty(t, err)
}

func Test_interfaceEncoder_Append(t *testing.T) {
	encoderConfig := InterfaceEncoderConfig{
		ModuleName: "FunctionWorker",
		HTTPMethod: "POST",
		ModuleFrom: "FrontendInvoke",
		TenantID:   "tenant2",
		FuncName:   "myFunction",
		FuncVer:    "latest",
	}
	encoder := &interfaceEncoder{
		InterfaceEncoderConfig: &encoderConfig,
		buf:                    _bufferPool.Get(),
		spaced:                 false,
		podName:                os.Getenv(constants.HostNameEnvKey),
	}
	encoder.AppendInt16(1)
	encoder.AppendUint32(2)
	encoder.AppendByteString([]byte("abc"))
	encoder.AppendFloat32(3)
	encoder.appendFloat(math.Inf(1), 10)
	encoder.appendFloat(math.Inf(-1), 10)
	encoder.AppendComplex64(1 + 2i)
	encoder.AppendUintptr(uintptr(1))
	encoder.AppendUint8(1)
	encoder.AppendInt32(2)
	encoder.AppendUint16(3)
	encoder.AppendUint(4)
	encoder.AppendInt8(0)
	encoder.AppendInt(5)
	encoder.AppendInt32(7)
	encoder.AppendBool(false)
	assert.NotEmpty(t, encoder.buf.Len())
}
