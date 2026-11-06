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

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh/terminal"

	"functioncore/pkg/cli/pkg/log"
)

type inputEvent struct {
	word     []byte
	inputKey uint16
	event    int
}

type inputWord struct {
	wordStr  string
	word     []byte
	length   int
	position int
}

type cmdIO struct {
	In  io.Reader
	Out io.Writer
}

var (
	inputChannel = make(chan inputEvent)
	frontSpace   = "0"
	word         = inputWord{}
	system       = &cmdIO{}
)

// ScanLine read a line from r and print to w in real time
func ScanLine(r io.Reader, w io.Writer, size int) string {
	system = &cmdIO{
		In:  r,
		Out: w,
	}
	if runtime.GOOS == "windows" {
		return scanByWindows()
	}
	return scanByLinux(size + 1)
}

func scanByWindows() string {
	var line string

	scanner := bufio.NewScanner(system.In)
	if scanner.Scan() {
		line = strings.TrimSpace(scanner.Text())
	}

	return line
}

func scanByLinux(size int) string {
	go func() {
		if err := recover(); err != nil {
			log.GetLogger().Errorf("scanLine panic, err: %s", fmt.Sprintf("%#v", err))
			os.Exit(1)
		}
	}()
	frontSpace = strconv.Itoa(size)
	loadHistory()
	go getUserInput()
	str := inputWordToTerminal()
	cleanInput()
	return strings.TrimSpace(str)
}

func inputWordToTerminal() string {
	inputResult := ""
	for {
		needPrint := true
		select {
		case input, ok := <-inputChannel:
			if !ok {
				needPrint = false
				break
			}
			if input.inputKey == keyUnknown {
				needPrint = false
				continue
			}
			switch input.event {
			case eventWord:
				word.appendInputString(input)
			case eventCommand:
				word.executeInputCommand(input)
			case eventExit:
				inputResult = word.inputExitCommand(input)
				needPrint = false
			default:
			}
		}
		if needPrint {
			// move cursor to initial position to overwrite print
			fmt.Fprint(system.Out, "\x1b["+frontSpace+"G", word.wordStr)
			if word.length-word.position != 0 {
				// reset cursor position
				fmt.Fprintf(system.Out, "\u001B[%dD", word.length-word.position)
			}
		} else {
			break
		}
	}
	_, err := fmt.Fprintln(system.Out)
	if err != nil {
		return ""
	}
	return inputResult
}

func getUserInput() {
	// fd 0 is stdin
	state, err := terminal.MakeRaw(0)
	if err != nil {
		log.GetLogger().Errorf("failed to set stdin to raw: %s", err.Error())
		return
	}
	defer func() {
		if err := terminal.Restore(0, state); err != nil {
			log.GetLogger().Errorf("failed to restore terminal: %s", err.Error())
			return
		}
	}()

	in := bufio.NewReader(system.In)
	str := make([]byte, 128)
	for {
		size, err := in.Read(str)
		if err != nil {
			_, err := fmt.Fprintln(system.Out, "stdin:", err.Error())
			if err != nil {
				return
			}
			break
		}
		inputType := judgeInput(str[:size])
		inputChannel <- inputType
		if inputType.event == eventExit {
			return
		}
	}
}

func (w *inputWord) appendInputString(input inputEvent) {
	temp := make([]byte, 0, w.length+len(input.word))
	// save the character before the cursor
	temp = append(temp, w.word[:w.position]...)
	// insert the input character at the cursor
	temp = append(temp, input.word...)
	w.position++
	// insert character after cursor
	temp = append(temp, w.word[w.position-1:]...)
	w.word = temp
	w.length = len(temp)
	w.wordStr = string(w.word)
}

func (w *inputWord) executeInputCommand(input inputEvent) {
	w.length = len(w.word)
	w.wordStr = string(w.word)
	commandDrivenMap := map[uint16]func(){
		keyLeft:      w.moveLeft,
		keyRight:     w.moveRight,
		keyUp:        w.getUpRecord,
		keyDown:      w.getDownRecord,
		keyHome:      w.moveFarLeft,
		keyEnd:       w.moveFarRight,
		keyBackspace: w.deletePreviousWord,
		keyDel:       w.deleteNextWord,
	}
	commandFunc, ok := commandDrivenMap[input.inputKey]
	if !ok {
		return
	}
	commandFunc()
}

func cleanInput() {
	word = inputWord{}
}

func (w *inputWord) moveLeft() {
	if w.position <= 0 {
		return
	}
	w.position--
}

func (w *inputWord) moveRight() {
	if w.position >= w.length {
		return
	}
	w.position++
}

func (w *inputWord) getUpRecord() {
	record := getLastHistory()
	w.getRecord(record)
}

func (w *inputWord) getDownRecord() {
	record := getNextHistory()
	w.getRecord(record)
}

func (w *inputWord) getRecord(record history) {
	showStr := fillBlank(string(record.word), w.length-record.length)
	w.word = record.word
	w.length = len(showStr)
	w.position = record.length
	w.wordStr = showStr
}

// fillBlank If the length of this character is less than that of the last character,
// a blank character needs to be added
func fillBlank(showStr string, blankSize int) string {
	for i := 0; i < blankSize; i++ {
		showStr = showStr + " "
	}
	return showStr
}

func (w *inputWord) deletePreviousWord() {
	if w.position <= 0 {
		return
	}
	w.position--
	w.deleteWord(w.position)
}

func (w *inputWord) deleteNextWord() {
	if w.position >= w.length {
		return
	}
	w.deleteWord(w.position)
}

func (w *inputWord) deleteWord(position int) {
	if position+1 <= w.length {
		w.word = append(w.word[:position], w.word[position+1:]...)
	} else {
		w.word = w.word[:position]
	}
	w.wordStr = fillBlank(string(w.word), 1)
	w.length = len(w.wordStr)
}

func (w *inputWord) inputExitCommand(input inputEvent) (inputResult string) {
	if input.inputKey == keyEnter {
		saveHistoryCMD(word.word)
		inputResult = string(word.word)
	} else if input.inputKey == keyCtrlC {
		_, err := fmt.Fprintln(system.Out)
		if err != nil {
			return ""
		}
		SaveHistoryCMDToFile()
		os.Exit(0)
	}
	return inputResult
}

func (w *inputWord) moveFarLeft() {
	w.position = 0
}

func (w *inputWord) moveFarRight() {
	w.position = w.length
}
