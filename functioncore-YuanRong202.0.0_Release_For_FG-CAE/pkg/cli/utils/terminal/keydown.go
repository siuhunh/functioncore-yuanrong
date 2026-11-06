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

// Package terminal console interaction mode
package terminal

import "functioncore/pkg/cli/pkg/log"

const modeControlKeyMinLength = 3

var keyDownList = map[byte]uint16{
	50: keyIns,
	51: keyDel,
	53: keyPgup,
	54: keyPgdn,
	65: keyUp,
	66: keyDown,
	67: keyRight,
	68: keyLeft,
	70: keyEnd,
	72: keyHome,
}

func judgeInput(str []byte) inputEvent {
	inputResult := inputEvent{
		event:    eventWord,
		inputKey: keyUnknown,
	}
	// This is a visible character in ASCII
	if len(str) == 1 && str[0] >= 32 && str[0] <= 126 {
		inputResult.word = str
		inputResult.event = eventWord
		inputResult.inputKey = keyWord
		return inputResult
	}
	switch str[0] {
	case 3:
		inputResult.event = eventExit
		inputResult.inputKey = keyCtrlC
	case 8:
		inputResult.event = eventCommand
		inputResult.inputKey = keyBackspace
	case 9:
		inputResult.event = eventCommand
		inputResult.inputKey = keyTab
	case 10:
		inputResult.event = eventExit
		inputResult.inputKey = keyEnter
	case 13:
		inputResult.event = eventExit
		inputResult.inputKey = keyEnter
	case 27:
		return getInputCommandType(str)
	default:
		log.GetLogger().Errorf("unknown input type: %d", str[0])
	}
	return inputResult
}

func getInputCommandType(str []byte) inputEvent {
	if len(str) == 1 {
		return inputEvent{
			event:    eventExit,
			inputKey: keyESC,
		}
	}
	switch str[1] {
	case 91:
		return inputEvent{
			word:     str,
			event:    eventCommand,
			inputKey: getInputModeControlKey(str),
		}
	default:
		log.GetLogger().Errorf("unknown input command type: %d", str[1])
	}
	return inputEvent{
		event:    eventWord,
		inputKey: keyUnknown,
	}
}

func getInputModeControlKey(str []byte) uint16 {
	if len(str) < modeControlKeyMinLength {
		return keyUnknown
	}
	controlKey, ok := keyDownList[str[2]]
	if !ok {
		return keyUnknown
	}
	return controlKey
}
