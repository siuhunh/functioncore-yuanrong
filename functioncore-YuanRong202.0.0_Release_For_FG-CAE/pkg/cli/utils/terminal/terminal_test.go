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
package terminal

import (
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/ssh/terminal"

	"functioncore/pkg/cli/pkg/test"
	"functioncore/pkg/cli/utils"
)

func TestInputWordToTerminal(t *testing.T) {
	convey.Convey("TestInputWordToTerminal", t, func() {
		_, tio := test.MockCmdIO()
		system = &cmdIO{
			In:  tio.In,
			Out: tio.Out,
		}
		go inputWordToTerminal()
		inputChannel <- inputEvent{
			inputKey: keyUnknown,
			event:    0,
		}

		inputChannel <- inputEvent{
			inputKey: keyWord,
			word:     []byte("1"),
			event:    0,
		}

		inputChannel <- inputEvent{
			inputKey: keyRight,
			event:    1,
		}

		inputChannel <- inputEvent{
			inputKey: keyEnter,
			event:    2,
		}
		assert.Equal(t, "\x1b[0G1\x1b[0G1", tio.Out.String())
	})
}

func TestGetUserInput(t *testing.T) {
	convey.Convey("TestGetUserInput", t, func() {
		_, tio := test.MockCmdIO()
		system = &cmdIO{
			In:  tio.In,
			Out: tio.Out,
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(terminal.MakeRaw, func(fd int) (*terminal.State, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(terminal.Restore, func(fd int, state *terminal.State) error {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		tio.In.Write([]byte("a"))
		go getUserInput()
		res := <-inputChannel
		assert.Equal(t, []byte("a"), res.word)
		convey.So(res.inputKey, convey.ShouldEqual, keyWord)
	})
}

func TestInputCommand(t *testing.T) {
	convey.Convey("TestInputCommand", t, func() {
		_, tio := test.MockCmdIO()
		system = &cmdIO{
			In:  tio.In,
			Out: tio.Out,
		}
		patch := gomonkey.ApplyFunc(utils.GetResourcePath, func() (string, error) {
			currentDir, _ := os.Getwd()
			return currentDir, nil
		})
		defer patch.Reset()
		resetHistory()
		testWord := inputWord{
			wordStr:  "123456",
			word:     []byte("123456"),
			length:   6,
			position: 6,
		}
		testWord.executeInputCommand(inputEvent{
			inputKey: keyRight,
		})
		convey.So(testWord.position, convey.ShouldEqual, 6)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyLeft,
		})
		convey.So(testWord.position, convey.ShouldEqual, 5)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyHome,
		})
		convey.So(testWord.position, convey.ShouldEqual, 0)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyLeft,
		})
		convey.So(testWord.position, convey.ShouldEqual, 0)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyBackspace,
		})
		assert.Equal(t, []byte("123456"), testWord.word)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyDel,
		})
		assert.Equal(t, []byte("23456"), testWord.word)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyRight,
		})
		convey.So(testWord.position, convey.ShouldEqual, 1)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyEnd,
		})
		convey.So(testWord.position, convey.ShouldEqual, 5)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyBackspace,
		})
		assert.Equal(t, []byte("2345"), testWord.word)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyDel,
		})
		assert.Equal(t, []byte("2345"), testWord.word)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyUp,
		})
		convey.So(testWord.word, convey.ShouldBeEmpty)

		testWord.executeInputCommand(inputEvent{
			inputKey: keyDown,
		})
		convey.So(testWord.word, convey.ShouldBeEmpty)
		resetHistory()
	})
}
