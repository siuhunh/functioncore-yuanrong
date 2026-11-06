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

package template

import (
	"fmt"
	"io"
	"strconv"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/verify"
	"functioncore/pkg/cli/template/entity"
	"functioncore/pkg/cli/template/repo"
	"functioncore/pkg/cli/utils/colorprint"
	"functioncore/pkg/cli/utils/terminal"
)

// Selector template selector
type Selector struct {
	Interactive bool
	Source      string
	Type        string
	Runtime     string
	Selected    string
	W           io.Writer
	R           io.Reader
}

// Option option func of Selector
type Option func(*Selector)

// GetTemplateInfo get function template info
func GetTemplateInfo(w io.Writer, r io.Reader, projectPath, runtime string) (string, entity.TemplateInfo, error) {
	functionName := getFirstFuncName(w, r, projectPath)
	templateSelector := NewSelector(constant.NonInteractive, repo.Local, constant.Function, runtime,
		Selected(constant.EmptyFunction))
	templateInfo, err := templateSelector.GetTemplate()
	if err != nil {
		return functionName, entity.TemplateInfo{}, err
	}
	return functionName, templateInfo, nil
}

func getFirstFuncName(w io.Writer, r io.Reader, projectPath string) string {
	var functionName string

	showStr := "Your First Function Name: "
	fmt.Fprintln(w)
	colorprint.PrintInteractive(w, showStr)
	if line := terminal.ScanLine(r, w, len(showStr)); line != "" {
		functionName = line
	}
	if functionName == "" {
		return getFirstFuncName(w, r, projectPath)
	}
	if !verify.MatchFunctionName(functionName) {
		colorprint.PrintfColorWord(w, "First Function Name is incorrect, %s.", colorprint.ErrorColor,
			verify.ErrFunctionNameFormat)
		return getFirstFuncName(w, r, projectPath)
	}
	return functionName
}

// SelectRuntime select runtime from runtimeEnum
func SelectRuntime(w io.Writer, r io.Reader, runtimeEnum []string) string {
	num := 1
	var err error

	fmt.Fprintf(w, "\nWhich runtime would you like to use? \n")
	for i, runtime := range runtimeEnum {
		fmt.Fprintf(w, "\t %d- %s \n", i+1, runtime)
	}
	showStr := fmt.Sprintf("Runtime [%d]: ", num)
	colorprint.PrintInteractive(w, showStr)
	if line := terminal.ScanLine(r, w, len(showStr)); line != "" {
		num, err = strconv.Atoi(line)
		if err != nil {
			colorprint.PrintfColorWord(w, "The runtime must in %s.", colorprint.ErrorColor, config.RuntimeEnum)
			return SelectRuntime(w, r, runtimeEnum)
		}
	}

	if num <= 0 || num > len(config.RuntimeEnum) {
		colorprint.PrintfColorWord(w, "The runtime must in %s.", colorprint.ErrorColor, config.RuntimeEnum)
		return SelectRuntime(w, r, runtimeEnum)
	}
	return runtimeEnum[num-1]
}

// NewSelector new template selector
func NewSelector(interactive bool, source, templateType, runtime string, options ...Option) *Selector {
	selector := &Selector{
		Interactive: interactive,
		Source:      source,
		Type:        templateType,
		Runtime:     runtime,
	}

	for _, option := range options {
		option(selector)
	}

	return selector
}

// Selected selected template name
func Selected(Selected string) Option {
	return func(s *Selector) {
		s.Selected = Selected
	}
}

// Writer io writer
func Writer(w io.Writer) Option {
	return func(s *Selector) {
		s.W = w
	}
}

// Reader io reader
func Reader(r io.Reader) Option {
	return func(s *Selector) {
		s.R = r
	}
}
