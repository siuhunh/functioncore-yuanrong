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

// Package create is create cmd of CLI
package create

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/create"
	"functioncore/pkg/cli/pkg/verify"
	"functioncore/pkg/cli/template"
	"functioncore/pkg/cli/template/repo"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/cli/utils/colorprint"
	"functioncore/pkg/cli/utils/terminal"
)

const (
	// TemplateSourceNum -
	TemplateSourceNum = "1"
)

type options struct {
	cmdIO        *cmdio.CmdIO
	runtime      string
	template     string
	functionName string
}

var opts = &options{}

var cmd = &cobra.Command{
	Use:   "create",
	Short: "used to create a function project",
	Long:  fmt.Sprintf(`run the "%s create" command to create a function project.`, constant.CliName),
	Example: utils.RawFormat(fmt.Sprintf(`
    Create a function in interactive mode:
    $ %s create

    Create a function in non-interactive mode:
    $ %s create --runtime Python3.6 --template hello-world --functionName hello-world
`, constant.CliName, constant.CliName)),
	Args: cobra.NoArgs,
	Run:  createFunction,
}

// InitCreateCMD init create cmd
func InitCreateCMD(cio *cmdio.CmdIO) *cobra.Command {
	opts.cmdIO = cio

	cmd.Flags().StringVarP(&opts.runtime, "runtime", "r", "", fmt.Sprintf("runtime of %s",
		constant.PlatformName))
	cmd.Flags().StringVarP(&opts.template, "template", "t", "",
		fmt.Sprintf("%s function template name . Example:--template=hello-world", constant.PlatformName))
	cmd.Flags().StringVarP(&opts.functionName, "functionName", "f", "", "function name")

	return cmd
}

func createFunction(cmd *cobra.Command, args []string) {
	var err error
	// create function
	if opts.functionName != "" && opts.runtime != "" && opts.template != "" {
		err = nonInteractive(opts)
	} else {
		err = interactive(opts.cmdIO.Out, opts.cmdIO.In)
	}
	if err != nil {
		colorprint.PrintFail(opts.cmdIO.ErrOut, "Failed to function project, ", err.Error(), ".")
		return
	}
	colorprint.PrintSuccess(opts.cmdIO.Out, "Succeeded", " to create a function project!")
}

func nonInteractive(opts *options) error {
	if err := verify.CheckFunctionName(opts.functionName); err != nil {
		return err
	}
	if _, err := create.GetNewProjectPath(opts.functionName); err != nil {
		return err
	}
	if err := verify.CheckRuntime(opts.runtime); err != nil {
		return err
	}
	templateSelector := template.NewSelector(constant.NonInteractive, repo.Local, constant.Service, opts.runtime,
		template.Selected(opts.template))
	templateInfo, err := templateSelector.GetTemplate()
	if err != nil {
		return fmt.Errorf("an error occurred while get template path : %s", err)
	}
	if err = create.Function(opts.cmdIO.Out, templateInfo.TemplatePath, opts.functionName); err != nil {
		return fmt.Errorf("an error occurred while generate project : %s", err)
	}
	colorMap := []colorprint.StringColorInfo{
		{Str: "Name: ", Color: colorprint.Colorless},
		{Str: "\nRuntime: ", Color: colorprint.Colorless},
		{Str: opts.runtime, Color: colorprint.KeywordColor},
		{Str: "\nTemplate: ", Color: colorprint.Colorless},
		{Str: opts.template, Color: colorprint.KeywordColor},
		{Str: "\nOutputDirectory: .\n", Color: colorprint.Colorless},
	}
	colorprint.PrintKeywords(opts.cmdIO.Out, colorMap)
	return nil
}

func interactive(w io.Writer, r io.Reader) error {
	sourceNum := selectTemplateSource(w, r)
	if sourceNum == TemplateSourceNum {
		runtime := template.SelectRuntime(w, r, config.RuntimeEnum)
		projectName := enterProjectName(w, r)
		templateSelector := template.NewSelector(constant.Interactive, repo.Local, constant.Service, runtime,
			template.Writer(opts.cmdIO.Out), template.Reader(opts.cmdIO.In))
		templateInfo, err := templateSelector.GetTemplate()
		if err != nil {
			return fmt.Errorf("an error occurred while select template : %s", err)
		}
		err = create.Function(w, templateInfo.TemplatePath, projectName)
		if err != nil {
			return fmt.Errorf("an error occurred while generate project : %s", err)
		}
		colorMap := []colorprint.StringColorInfo{
			{Str: "Name: ", Color: colorprint.Colorless},
			{Str: projectName, Color: colorprint.KeywordColor},
			{Str: "\nRuntime: ", Color: colorprint.Colorless},
			{Str: runtime, Color: colorprint.KeywordColor},
			{Str: "\nTemplate: ", Color: colorprint.Colorless},
			{Str: templateInfo.TemplateName, Color: colorprint.KeywordColor},
			{Str: "\nOutputDirectory: .\n", Color: colorprint.Colorless},
		}
		colorprint.PrintKeywords(opts.cmdIO.Out, colorMap)
		return nil
	}
	return errors.New("the template source choice only 1")
}

func enterProjectName(w io.Writer, r io.Reader) string {
	projectName := "hello-world"
	showStr := "Function Name[hello-world]: "
	_, err := fmt.Fprintln(w)
	if err != nil {
		return ""
	}
	colorprint.PrintInteractive(w, showStr)
	if line := terminal.ScanLine(r, w, len(showStr)); line != "" {
		projectName = line
	}
	if _, err := create.GetNewProjectPath(projectName); err != nil {
		colorprint.PrintFail(w, "", err.Error(), "")
		return enterProjectName(w, r)
	}
	_, err = fmt.Fprintln(w)
	if err != nil {
		return ""
	}
	return projectName
}

func selectTemplateSource(w io.Writer, r io.Reader) string {
	num := TemplateSourceNum
	fmt.Fprintf(w, "Which template source would you like to use ? \n")
	fmt.Fprintf(w, "\t"+TemplateSourceNum+" - %s Quick Start Template ? \n", constant.PlatformName)
	showStr := fmt.Sprintf("Choice [%s]: ", num)
	colorprint.PrintInteractive(w, showStr)
	if line := terminal.ScanLine(r, w, len(showStr)); line != "" {
		num = line
	}
	if num != TemplateSourceNum {
		colorprint.PrintFail(w, "", "The template source choice only 1.", "\n")
		return selectTemplateSource(w, r)
	}
	_, err := fmt.Fprintln(w)
	if err != nil {
		return ""
	}
	return num
}
