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

package invoke

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/function"
	"functioncore/pkg/cli/utils"
)

type options struct {
	cmdIO          *cmdio.CmdIO
	client         client.Client
	tenantID       string
	serviceName    string
	functionName   string
	eventJSON      string
	eventPath      string
	version        string
	alias          string
	concurrency    int
	remain         bool
	stateKey       string
	instanceMemory string
	headerStr      string
	tailLog        bool
}

const microToMilliSecond = 1000
const defaultResponseHeadersSize = 16
const thousandTimes = 1000

var (
	opts          = &options{}
	wg            sync.WaitGroup
	mutex         sync.Mutex
	isBreak       bool
	maxDuration   = float64(0)
	minDuration   = math.MaxFloat64
	totalDuration = float64(0)
	durations     []float64
	maxMen        = float64(0)
	resultCode    = 0
)

var cmd = &cobra.Command{
	Use:   "invoke FUNCTION",
	Short: "used to invoke function",
	Long: fmt.Sprintf(`run the "%s invoke" command to invoke a function of function.`,
		constant.CliName),
	Example: utils.RawFormat(fmt.Sprintf(`
    Invoke a function of the current function:
    $ %s invoke functionname
    
    Invoke a function and specify function name:
    $ %s invoke functionname -s serviceName

    Invoke a function with event json(windows:'{\"key\":\"value\"}'):
    $ %s invoke functionname -e '{"key":"value"}'

    Invoke a function with event file:
    $ %s invoke functionname -p path/to/event.json

    Invoke a function and specify version 1:
    $ %s invoke functionname -v 1
	`, constant.CliName, constant.CliName, constant.CliName,
		constant.CliName, constant.CliName)),
	Args: cobra.ExactArgs(1),
	Run:  invoke,
}

// InitInvokeCMD init cmd of invoke
func InitInvokeCMD(c client.Client, cio *cmdio.CmdIO) *cobra.Command {
	opts.cmdIO = cio
	opts.client = c
	opts.tenantID = config.FConfig.CLIConfig.TenantID

	cmd.Flags().StringVarP(&opts.serviceName, "serviceName", "s", constant.DefaultService,
		constant.ServiceNameUsage)
	cmd.Flags().StringVarP(&opts.eventJSON, "event", "e", "",
		"the event json string that you providing to your inputed function")
	cmd.Flags().StringVarP(&opts.eventPath, "eventPath", "p", "",
		"the event json file that you providing to your inputed function")
	cmd.Flags().StringVarP(&opts.version, "version", "v", constant.LatestVersion, "function version")
	cmd.Flags().IntVarP(&opts.concurrency, "concurrency", "c", 1, "The default value is 1, "+
		fmt.Sprintf("and the maximum value is %d.", fhttp.DefaultMaxConnsPerHost))
	cmd.Flags().BoolVarP(&opts.remain, "remain", "r", false, "remain")
	cmd.Flags().StringVarP(&opts.instanceMemory, "instanceMemory", "m", "", "function instance memory")
	cmd.Flags().StringVarP(&opts.headerStr, "header", "d", "{}", "invoke header")
	cmd.Flags().BoolVarP(&opts.tailLog, "tail", "t", true, "tail invoke log")

	return cmd
}

func invoke(cmd *cobra.Command, args []string) {
	functionName := args[0]
	invokeInfo, err := getInvokeInfo(functionName)
	if err != nil {
		fmt.Printf("Failed to invoke the function %s, %s.\n", functionName, err)
		os.Exit(1)
		return
	}

	if opts.remain {
		fmt.Printf("Start to remain invoke.\n")
		for i := 1; ; i++ {
			CallAndPrintResult(invokeInfo, opts.client, i)
			if isBreak {
				break
			}
			if i%thousandTimes == 0 {
				writeDataToFile(invokeInfo.FunctionName)
				cleanCollectData()
			}
		}
		fmt.Printf("Finish to remain invoke.\n")
	}
	if opts.concurrency > fhttp.DefaultMaxConnsPerHost {
		fmt.Printf("Failed to invoke the function %s, the concurrency should be less than %d.\n", functionName,
			fhttp.DefaultMaxConnsPerHost)
		os.Exit(1)
		return
	}
	fmt.Printf("Start to invoke, concurrency[%d].\n", opts.concurrency)
	for i := 1; i <= opts.concurrency; i++ {
		wg.Add(1)
		go wgCallAndPrintResult(invokeInfo, opts.client, i)
	}
	wg.Wait()
	fmt.Printf("Finish to invoke, concurrency[%d] maxDuration[%8.4f] minDuration[%8.4f].\n",
		opts.concurrency, maxDuration, minDuration)
	os.Exit(resultCode)
}

func cleanCollectData() {
	maxDuration = float64(0)
	minDuration = math.MaxFloat64
	totalDuration = float64(0)
	durations = []float64{}
	maxMen = float64(0)
}

func writeDataToFile(name string) {
	f, err := os.OpenFile(name+"-remain.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		fmt.Println("failed to open remain.log")
		return
	}
	defer f.Close()
	sort.Float64s(durations)
	if len(durations) < thousandTimes {
		return
	}
	medianDuration := (durations[thousandTimes/2-1] + durations[thousandTimes/2]) / 2
	message := fmt.Sprintf("remain invoke 1000 times, "+
		"maxDuration[%8.4fms] minDuration[%8.4fms] averageDuration[%8.4fms] "+
		"medianDuration[%8.4fms] minMemort[%8.4fMB]\n",
		maxDuration, minDuration, totalDuration/thousandTimes, medianDuration, maxMen)
	_, err = f.WriteString(message)
	if err != nil {
		fmt.Printf("failed to write remain invoke data to %s-remain.txt, %s \n", name, err.Error())
		return
	}
}

func wgCallAndPrintResult(invokeInfo function.InvokeInfo, c client.Client, index int) {
	CallAndPrintResult(invokeInfo, c, index)
	println("-------------------------------------------")
	wg.Done()
}

// CallAndPrintResult call function and print the invoke result
func CallAndPrintResult(invokeInfo function.InvokeInfo, c client.Client, index int) (int, string) {
	fmt.Printf("[%d] Started to invoke the function: %s\nversion/alias: %s\n\n",
		index, invokeInfo.FunctionName, strings.Replace(invokeInfo.Qualifier, "!", "", 1))
	var err error
	start := time.Now()
	resp, err := function.Invoke(c, invokeInfo)
	invokeLatency := int(time.Now().Sub(start)/time.Microsecond) / microToMilliSecond
	invokeUseTime := fmt.Sprintf("invoke use time: %d ms\n", invokeLatency)
	defer func() {
		if err != nil {
			isBreak = true
		}
	}()
	if err != nil {
		fmt.Printf("Failed to invoke the function %s, %s.\n", invokeInfo.FunctionName, err)
		resultCode = 1
		return fhttp.StatusInternalServerError, ""
	}
	invokeResponse := fmt.Sprintf("%s", resp.Body())
	statusCode := resp.StatusCode()
	headers := getResponseHeaders(resp)
	defer func() {
		if statusCode != fhttp.StatusOK {
			isBreak = true
			resultCode = getCodeFromInvokeResponse(invokeResponse)
		}
	}()
	if err := printInvokeResponse(invokeUseTime, invokeResponse, headers, index); err != nil {
		return statusCode, ""
	}

	return statusCode, invokeResponse
}

func printInvokeResponse(invokeUseTime, invokeResponse string, headers map[string]string, index int) error {
	var prints []string
	prints = append(prints, invokeUseTime)

	result, err := printInvokeResult(invokeResponse, index)
	if err != nil {
		resultCode = 1
		fmt.Printf("[%d] Failed to print the invocation result, %s.\n", index, err)
		return err
	}
	prints = append(prints, result)

	summary := printInvokeSummary(headers["X-Invoke-Summary"], index)
	collectInvokeDuration(headers["X-Invoke-Summary"])
	prints = append(prints, summary)

	logs, err := printInvokeLogs(headers["X-Log-Result"], index)
	if err != nil {
		resultCode = 1
		fmt.Printf("[%d] Failed to print the invocation log, %s.\n", index, err)
		return err
	}
	prints = append(prints, logs)

	for _, printContent := range prints {
		fmt.Printf(printContent)
	}
	return nil
}

// Response -
type Response struct {
	Code int `json:"code"`
}

func getResponseHeaders(response *fhttp.Response) map[string]string {
	headers := make(map[string]string, defaultResponseHeadersSize)
	response.Header.VisitAll(func(key, value []byte) {
		headers[string(key)] = string(value)
	})
	return headers
}

func getCodeFromInvokeResponse(response string) int {
	r := &Response{}
	if err := json.Unmarshal([]byte(response), r); err != nil {
		fmt.Println("failed to Unmarshal summary", err)
		return 1
	}
	return r.Code
}

func collectInvokeDuration(summary string) {
	s := &Summary{}
	if err := json.Unmarshal([]byte(summary), s); err != nil {
		fmt.Println("failed to Unmarshal summary", err)
	}
	mutex.Lock()
	maxDuration = max(maxDuration, s.Duration)
	minDuration = min(minDuration, s.Duration)
	totalDuration += s.Duration
	durations = append(durations, s.Duration)
	maxMen = max(maxMen, s.MemoryUsed)
	mutex.Unlock()
}

func max(a float64, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a float64, b float64) float64 {
	if a > b {
		return b
	}
	return a
}

// Summary invoke function summary
type Summary struct {
	Duration   float64 `json:"duration"`
	MemoryUsed float64 `json:"memoryUsed"`
}

func getInvokeInfo(functionName string) (function.InvokeInfo, error) {
	var invokeInfo function.InvokeInfo

	eventByte, err := GetEventBytes(opts.eventJSON, opts.eventPath)
	if err != nil {
		return invokeInfo, err
	}

	qualifier := opts.version
	if opts.alias != "" {
		qualifier = "!" + opts.alias
	}
	headers, err := getInvokeHeader(opts.headerStr)
	if err != nil {
		return invokeInfo, err
	}

	invokeInfo = function.InvokeInfo{
		ServiceName:    opts.serviceName,
		FunctionName:   functionName,
		Event:          eventByte,
		Qualifier:      qualifier,
		StateKey:       opts.stateKey,
		TenantID:       opts.tenantID,
		InstanceMemory: opts.instanceMemory,
		Headers:        headers,
		TailLog:        opts.tailLog,
	}
	return invokeInfo, nil
}

func getInvokeHeader(str string) (map[string]string, error) {
	headers := make(map[string]string, 1)
	if err := json.Unmarshal([]byte(str), &headers); err != nil {
		return nil, err
	}
	return headers, nil
}

func printInvokeResult(invokeResponse string, index int) (string, error) {
	if json.Valid([]byte(invokeResponse)) {
		var formatResponse bytes.Buffer
		if err := json.Indent(&formatResponse,
			[]byte(invokeResponse), "", "    "); err != nil {
			return "", err
		}
		invokeResponse = formatResponse.String()
	}

	return fmt.Sprintf("[%d]The invocation result: %s\n\n", index, invokeResponse), nil
}

func printInvokeSummary(summary string, index int) string {
	return fmt.Sprintf("[%d] The invocation summary: %s\n\n", index, summary)
}

func printInvokeLogs(logs string, index int) (string, error) {
	data, err := base64.StdEncoding.DecodeString(logs)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("[%d] The invocation log: \n%s\n\n", index, string(data)), nil
}
