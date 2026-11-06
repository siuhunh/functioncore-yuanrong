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
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"functioncore/pkg/cli/pkg/log"
	"functioncore/pkg/cli/utils"
)

type history struct {
	word   []byte
	length int
}

const (
	historyMaxLimit = 100
	historyFileName = ".history"
)

var (
	historyList      []history
	newlyHistoryList []history
	historyPos       int
	historyOnce      sync.Once
)

func loadHistory() {
	historyOnce.Do(func() {
		historyPath, err := getHistoryPath()
		if err != nil {
			log.GetLogger().Errorf("failed to get history path: %s", err.Error())
			return
		}
		data, err := getHistoryFileData(historyPath)
		if err != nil {
			log.GetLogger().Errorf("failed to read history file")
			return
		}
		historyList = readCmdHistory(data)
		historyPos = len(historyList)
	})
}

func saveHistoryCMD(word []byte) {
	if len(word) == 0 {
		return
	}
	historyWord := history{
		word:   word,
		length: len(word),
	}
	for key, word := range historyList {
		if bytes.Equal(word.word, historyWord.word) {
			if key+1 < len(historyList) {
				historyList = append(historyList[:key], historyList[key+1:]...)
			} else {
				historyList = historyList[:key]
			}
			break
		}
	}
	historyList = append(historyList, historyWord)
	newlyHistoryList = append(newlyHistoryList, historyWord)
	if len(historyList) > historyMaxLimit {
		historyList = historyList[1:]
	}
	historyPos = len(historyList)
}

func getLastHistory() history {
	if len(historyList) == 0 {
		return history{
			word:   []byte(""),
			length: 0,
		}
	}
	if historyPos < 1 {
		historyPos = 1
		return historyList[0]
	}
	historyPos--
	return historyList[historyPos]
}

func getNextHistory() history {
	if len(historyList) == 0 {
		return history{
			word:   []byte(""),
			length: 0,
		}
	}
	length := len(historyList)
	if historyPos >= length-1 {
		historyPos = length - 1
		return historyList[length-1]
	}
	historyPos++
	return historyList[historyPos]
}

// SaveHistoryCMDToFile save user input
func SaveHistoryCMDToFile() {
	historyPath, err := getHistoryPath()
	if err != nil {
		log.GetLogger().Errorf("failed to get history path: %s", err.Error())
		return
	}

	data, err := getHistoryFileData(historyPath)
	if err != nil {
		log.GetLogger().Errorf("failed to open history file: %s", err.Error())
		return
	}

	file, err := os.OpenFile(historyPath, os.O_WRONLY|os.O_SYNC|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		log.GetLogger().Errorf("failed to open history file")
		return
	}
	defer file.Close()
	err = saveToFile(file, data)
	if err != nil {
		log.GetLogger().Errorf("failed to write history to file: %s", err.Error())
		return
	}
}

func getHistoryPath() (string, error) {
	resourcePath, err := utils.GetResourcePath()
	if err != nil {
		return "", err
	}
	historyPath := path.Join(resourcePath, historyFileName)
	return historyPath, nil
}

func resetHistory() {
	historyPath, err := getHistoryPath()
	if err != nil {
		log.GetLogger().Errorf("failed to get history path: %s", err.Error())
		return
	}
	historyList = []history{}
	err = os.Remove(historyPath)
	if err != nil {
		log.GetLogger().Errorf("failed to clear history")
		return
	}
}

func readCmdHistory(data []byte) []history {
	historyData := bytes.Split(data, []byte("\n"))
	result := make([]history, 0, len(historyData))
	for _, datum := range historyData {
		if len(datum) == 0 {
			continue
		}
		result = append(result, history{
			word:   datum,
			length: len(datum),
		})
	}
	return result
}

func saveToFile(file *os.File, data []byte) error {
	var err error
	fileData := readCmdHistory(data)
	saveMap := make(map[string]bool, len(fileData)+len(newlyHistoryList))
	resultList := make([]string, 0, historyMaxLimit)
	for i := len(newlyHistoryList) - 1; i >= 0; i-- {
		str := string(newlyHistoryList[i].word)
		if saveMap[str] {
			continue
		}
		resultList = append(resultList, str)
		saveMap[str] = true
	}

	for i := len(fileData) - 1; i >= 0; i-- {
		str := string(fileData[i].word)
		if saveMap[str] {
			continue
		}
		resultList = append(resultList, str)
		saveMap[str] = true
	}

	if len(resultList) > historyMaxLimit {
		resultList = resultList[:historyMaxLimit]
	}

	bufferedWriter := bufio.NewWriter(file)
	for i := len(resultList) - 1; i >= 0; i-- {
		_, err = bufferedWriter.Write(
			append([]byte(resultList[i]), '\n'),
		)
		if err != nil {
			return err
		}
	}

	return bufferedWriter.Flush()
}

func getHistoryFileData(historyPath string) ([]byte, error) {
	file, err := os.OpenFile(historyPath, os.O_RDONLY|os.O_SYNC|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return nil, errors.New("failed to open history file")
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, errors.New("failed to read history file")
	}
	return data, nil
}
