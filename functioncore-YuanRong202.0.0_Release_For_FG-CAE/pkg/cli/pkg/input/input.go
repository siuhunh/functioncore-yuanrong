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

// Package input user input in batches
package input

import (
	"fmt"
	"regexp"
	"strings"
)

// ExplodeMultipleInput Split by Space
func ExplodeMultipleInput(values string) []string {
	if values == "" {
		return []string{}
	}
	reg := regexp.MustCompile("\\s+")
	values = reg.ReplaceAllString(strings.Trim(values, " "), " ")
	inputArr := strings.Split(values, " ")
	return inputArr
}

// MatchMultipleStrings regular matching of multiple strings
func MatchMultipleStrings(strArr []string, regular string) []string {
	if len(regular) <= 0 {
		return nil
	}
	var regularStr strings.Builder
	regularStr.WriteString("^")
	for _, str := range regular {
		if str == '*' || str == '?' {
			regularStr.Write([]byte{'.', byte(str)})
			continue
		}
		regularStr.WriteByte(byte(str))
	}
	regularStr.WriteString("$")
	result := make([]string, 0, len(strArr))
	for _, value := range strArr {
		isMatch, err := regexp.MatchString(regularStr.String(), value)
		if err == nil && isMatch {
			result = append(result, value)
		}
	}

	return result
}

// CheckInputNum check user input
func CheckInputNum(inputValues map[string]string) error {
	for inputType, values := range inputValues {
		inputArr := ExplodeMultipleInput(values)
		if len(inputArr) != 1 {
			return fmt.Errorf("the number of %s must be one", inputType)
		}
	}
	return nil
}
