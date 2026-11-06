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

// Package delete is delete cmd of CLI
package delete

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/function"
	"functioncore/pkg/cli/pkg/input"
	"functioncore/pkg/cli/pkg/verify"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/cli/utils/colorprint"
	"functioncore/pkg/cli/utils/terminal"
)

// Options delete options
type Options struct {
	CmdIO        *cmdio.CmdIO
	client       client.Client
	tenantID     string
	ServiceName  string
	FunctionName string
	Version      string
	Alias        string
	force        bool
}

const (
	targetServiceStr         = "service"
	targetFunctionStr        = "function"
	targetFunctionVersionStr = "functionVersion"
	maxMultiParameter        = 50
	multiParameter           = 1
	regular                  = 2
)

var opts = &Options{}

var cmd = &cobra.Command{
	Use:   "delete",
	Short: "used to delete a service project",
	Long:  fmt.Sprintf(`run the "%s delete" command to delete a service project.`, constant.CliName),
	Example: utils.RawFormat(fmt.Sprintf(`
    Delete the default service project:
    $ %s delete

    Delete a service by specifying the service name:
    $ %s delete -s serviceName

    Delete multiple services by specifying the service name:
    $ %s delete -s "serviceNameA serviceNameB"

    Delete the regular matching services of specifying the service name:
    $ %s delete -s "serviceName*"

    Delete a function of the current service:
    $ %s delete -f functionName

    Delete a function of the specified service:
    $ %s delete -f functionName -s serviceName

    Delete multiple functions of the specified service:
    $ %s delete -f "functionNameA functionNameB"  -s serviceName

    Delete the regular matching functions of the specified service:
    $ %s delete -f "functionName*"  -s serviceName

    Delete the $latest version of the function:
    $ %s delete -f functionName -v '$latest'

    Delete a version of the function:
    $ %s delete -f functionName -v versionNum

    Delete multiple versions of the function:
    $ %s delete -v "versionNum1 versionNum2" -f functionName

    Delete the regular matching versions of the function:
    $ %s delete -v "versionNum*" -f functionName
	`, constant.CliName, constant.CliName, constant.CliName, constant.CliName, constant.CliName,
		constant.CliName, constant.CliName, constant.CliName, constant.CliName, constant.CliName,
		constant.CliName, constant.CliName)),
	Args: utils.NoArgs,
	Run:  delete,
}

// InitCMD init the delete cmd
func InitCMD(c client.Client, cio *cmdio.CmdIO) *cobra.Command {
	opts.CmdIO = cio
	opts.client = c
	opts.tenantID = config.FConfig.CLIConfig.TenantID

	cmd.Flags().StringVarP(&opts.ServiceName, "serviceName", "s", constant.DefaultService, constant.ServiceNameUsage)
	cmd.Flags().StringVarP(&opts.FunctionName, "functionName", "f", "", "function name")
	cmd.Flags().StringVarP(&opts.Version, "version", "v", "", "function version")
	cmd.Flags().BoolVarP(&opts.force, "force", "", false, "forcible deletion flag")

	return cmd
}

func delete(cmd *cobra.Command, args []string) {
	defer terminal.SaveHistoryCMDToFile()

	target := getTarget()
	deletionTarget, err := getDeletionTarget(target)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut, "Failed to delete, ", err.Error(), ".\n")
	}
	deletionTarget.delete()
}

var deletionTargets = make(map[string]deletionTarget, 4)

func getDeletionTarget(target string) (deletionTarget, error) {
	deletionTargets[targetServiceStr] = new(targetService)
	deletionTargets[targetFunctionStr] = new(targetFunction)
	deletionTargets[targetFunctionVersionStr] = new(targetFunctionVersion)
	deletionTarget, ok := deletionTargets[target]
	if !ok {
		return nil, errors.New("the deletionTarget to be deleted does not exist")
	}
	return deletionTarget, nil
}

var reConfirmations = make(map[string]string, 4)

func getReConfirmation(target string, deleteAllType bool) string {
	if deleteAllType {
		reConfirmations[targetServiceStr] = "Whether to delete ALL the service ?(y/n): "
		reConfirmations[targetFunctionStr] = "Whether to delete ALL the function ?(y/n): "
		reConfirmations[targetFunctionVersionStr] =
			fmt.Sprintf("Whether to delete ALL version of the function %s ?(y/n):", opts.FunctionName)
	} else {
		reConfirmations[targetServiceStr] = "Whether to delete the service "
		reConfirmations[targetFunctionStr] = "Whether to delete the function "
		reConfirmations[targetFunctionVersionStr] = fmt.Sprintf("Whether to delete the function %s version ",
			opts.FunctionName)
	}
	reConfirmation, ok := reConfirmations[target]
	if !ok {
		return ""
	}
	return reConfirmation
}

func getTarget() string {
	if opts.FunctionName == "" {
		return targetServiceStr
	}
	if opts.Version != "" {
		return targetFunctionVersionStr
	}
	return targetFunctionStr
}

type deletionTarget interface {
	delete()
	getMatched(string) []string
	judgeRegular(string) bool
}

type targetService struct{}

func (s targetService) delete() {
	serviceInputs, deleteAllType, err := getMultipleInput(s, opts.ServiceName)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the service %s project , ", opts.ServiceName),
			err.Error(), ".\n")
		return
	}

	if !function.Force(opts.CmdIO, opts.force, serviceInputs,
		getReConfirmation(targetServiceStr, deleteAllType), deleteAllType) {
		return
	}

	failCount := 0
	for _, serviceName := range serviceInputs {
		if err := DelService(serviceName); err != nil {
			failCount++
			colorprint.PrintFail(opts.CmdIO.ErrOut,
				fmt.Sprintf("Failed to delete the service %s project, ", serviceName),
				err.Error(), ".\n")
			continue
		}
		colorprint.PrintSuccess(opts.CmdIO.Out, "Succeeded",
			fmt.Sprintf(" to delete the service %s project!", serviceName))
	}
	if len(serviceInputs) == 1 {
		return
	}
	if failCount > 0 {
		colorprint.PrintFail(opts.CmdIO.ErrOut, "Failed to delete the service , ",
			fmt.Sprintf("fail count %d", failCount), ".\n")
		return
	}
	colorprint.PrintSuccess(opts.CmdIO.Out, "Succeeded",
		fmt.Sprintf(" to delete the service %s project!", opts.ServiceName))
}

func (s targetService) getMatched(regularString string) []string {
	services, err := function.QueryServices(opts.client, opts.tenantID)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the service %s project, ", opts.ServiceName), err.Error(), "\n")
		return nil
	}
	return input.MatchMultipleStrings(services, regularString)
}

func (s targetService) judgeRegular(str string) bool {
	return !verify.MatchServiceName(str)
}

// DelService delete service
func DelService(serviceName string) error {
	functions, err := function.QueryFunctions(opts.client, opts.tenantID, serviceName)
	if err != nil {
		return err
	}
	if len(functions) == 0 {
		return errors.New("which is not existed")
	}
	failCount := deleteFunctions(serviceName, functions)
	if failCount > 0 {
		return fmt.Errorf("fail count %d", failCount)
	}
	return nil
}

func deleteFunctions(serviceName string, functions []string) int {
	failCount := 0
	for _, functionName := range functions {
		err := function.Delete(opts.client, opts.tenantID, serviceName, functionName)
		if err != nil {
			failCount++
			colorprint.PrintFail(opts.CmdIO.ErrOut,
				fmt.Sprintf("Failed to delete the function %s, ", functionName), err.Error(), ".\n")
			continue
		}
		colorprint.PrintSuccess(opts.CmdIO.Out, "Succeeded",
			fmt.Sprintf(" to delete the function %s!", functionName))
	}
	return failCount
}

type targetFunction struct{}

func (s targetFunction) delete() {
	inputValues := map[string]string{
		targetServiceStr: opts.ServiceName,
	}
	err := input.CheckInputNum(inputValues)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the function %s, ", opts.FunctionName), err.Error(), ".\n")
		return
	}
	functionInputs, deleteAllType, err := getMultipleInput(s, opts.FunctionName)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the function %s, ", opts.FunctionName), err.Error(), ".\n")
		return
	}
	if !function.Force(opts.CmdIO, opts.force, functionInputs,
		getReConfirmation(targetFunctionStr, deleteAllType), deleteAllType) {
		return
	}

	failCount := deleteFunctions(opts.ServiceName, functionInputs)
	if len(functionInputs) == 1 {
		return
	}
	if failCount > 0 {
		colorprint.PrintFail(opts.CmdIO.ErrOut, "Failed to delete the function , ",
			fmt.Sprintf("fail count %d", failCount), ".\n")
		return
	}
	colorprint.PrintSuccess(opts.CmdIO.Out, "Succeeded",
		fmt.Sprintf(" to delete the function %s !", opts.FunctionName))
}

func (s targetFunction) getMatched(regularString string) []string {
	functions, err := function.QueryFunctions(opts.client, opts.tenantID, opts.ServiceName)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the function %s , ", opts.FunctionName), err.Error(), "\n")
		return nil
	}
	if len(functions) == 0 {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the function %s , ", opts.FunctionName),
			"can not find function", "\n")
		return nil
	}
	return input.MatchMultipleStrings(functions, regularString)
}

func (s targetFunction) judgeRegular(str string) bool {
	return !verify.MatchFunctionName(str)
}

type targetFunctionVersion struct{}

func (s targetFunctionVersion) delete() {
	inputValues := map[string]string{targetServiceStr: opts.ServiceName, targetFunctionStr: opts.FunctionName}
	err := input.CheckInputNum(inputValues)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the function %s, ", opts.FunctionName), err.Error(), ".\n")
		return
	}
	versionInputs, deleteAllType, err := getMultipleInput(s, opts.Version)
	failWord := fmt.Sprintf("Failed to delete the function %s version %s, ", opts.FunctionName, opts.Version)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut, failWord, err.Error(), ".\n")
		return
	}
	if !function.Force(opts.CmdIO, opts.force, versionInputs,
		getReConfirmation(targetFunctionVersionStr, deleteAllType), deleteAllType) {
		return
	}
	if deleteAllType {
		s.deleteAllVersion()
		return
	}
	failCount := 0
	for _, versionNum := range versionInputs {
		if len(versionInputs) > 1 && versionNum == constant.LatestVersion {
			continue
		}
		err = function.DeleteByVersion(opts.client, opts.tenantID, opts.ServiceName, opts.FunctionName, versionNum)
		if err != nil {
			failCount++
			str := fmt.Sprintf("Failed to delete the function %s version %s, ", opts.FunctionName, versionNum)
			colorprint.PrintFail(opts.CmdIO.ErrOut, str, err.Error(), ".\n")
			continue
		}
		colorprint.PrintSuccess(opts.CmdIO.Out, "Succeeded",
			fmt.Sprintf(" to delete the function %s version %s !", opts.FunctionName, versionNum))
	}
	if len(versionInputs) == 1 {
		return
	}
	if failCount > 0 {
		colorprint.PrintFail(opts.CmdIO.ErrOut, failWord, fmt.Sprintf("fail count %d", failCount), ".\n")
		return
	}
	colorprint.PrintSuccess(opts.CmdIO.Out, "Succeeded",
		fmt.Sprintf(" to delete the function %s version %s !", opts.FunctionName, opts.Version))
}

func (s targetFunctionVersion) deleteAllVersion() {
	err := function.Delete(opts.client, opts.tenantID, opts.ServiceName, opts.FunctionName)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete ALL version of the function %s, ", opts.FunctionName),
			err.Error(), ".\n")
		return
	}
	colorprint.PrintSuccess(opts.CmdIO.Out, "Succeeded",
		fmt.Sprintf(" to delete All version of the function %s!", opts.FunctionName))
}

func (s targetFunctionVersion) getMatched(regularString string) []string {
	versions, err := function.QueryVersionList(opts.client, opts.tenantID, opts.ServiceName, opts.FunctionName)
	if err != nil {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the version %s , ", opts.Version), err.Error(), "\n")
		return nil
	}
	if len(versions) == 0 {
		colorprint.PrintFail(opts.CmdIO.ErrOut,
			fmt.Sprintf("Failed to delete the version %s , ", opts.Version),
			"can not find version", "\n")
		return nil
	}
	return input.MatchMultipleStrings(versions, regularString)
}

func (s targetFunctionVersion) judgeRegular(str string) bool {
	return !verify.MatchVersion(str)
}

func getMultipleInput(s deletionTarget, multipleInputs string) ([]string, bool, error) {
	inputType, inputs, err := JudgeMultipleInput(s, multipleInputs)
	deleteAllType := false
	if err != nil {
		return nil, false, err
	}
	if inputType == regular {
		if inputs[0] == "*" {
			deleteAllType = true
		}
		inputs = s.getMatched(inputs[0])
		if inputs == nil || len(inputs) == 0 {
			return nil, false, fmt.Errorf("can not find")
		}
	} else if len(inputs) > maxMultiParameter {
		return nil, false, fmt.Errorf("maximum continuous input of %d values", maxMultiParameter)
	}
	return inputs, deleteAllType, nil
}

// JudgeMultipleInput When the user inputs, it is judged whether it is multi-parameter or regular matching
func JudgeMultipleInput(s deletionTarget, values string) (int, []string, error) {
	inputArr := input.ExplodeMultipleInput(values)
	if len(inputArr) == 0 {
		return 0, nil, errors.New("the input value is empty")
	}
	if len(inputArr) == 1 && s.judgeRegular(inputArr[0]) {
		return regular, inputArr, nil
	}
	for _, value := range inputArr {
		if s.judgeRegular(value) {
			return 0, nil, errors.New("regular expressions are not supported in multi-parameter mode")
		}
	}
	return multiParameter, inputArr, nil
}
