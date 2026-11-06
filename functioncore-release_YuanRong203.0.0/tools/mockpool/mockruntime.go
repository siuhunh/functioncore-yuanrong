package main

import (
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"functioncore/tools/mockpool/mockruntime"
)

// MockRuntimeCmd -
type MockRuntimeCmd struct {
	port                   int
	invokeResponse         string
	invokeResponseFilePath string
	invokeDuration         time.Duration
}

// NewMockRuntimeCmd -
func NewMockRuntimeCmd() *MockRuntimeCmd {
	return &MockRuntimeCmd{}
}

// ApplyCobra -
func (m *MockRuntimeCmd) ApplyCobra(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.IntVar(
		&m.port,
		"mock-runtime-port",
		30000,
		"mock runtime端口",
	)
	flags.StringVar(
		&m.invokeResponse,
		"mock-runtime-invoke-response",
		"",
		"mock runtime invoke返回消息",
	)
	flags.StringVar(
		&m.invokeResponseFilePath,
		"mock-runtime-invoke-response-file-path",
		"",
		"mock runtime invoke返回消息文件, 覆盖mock-runtime-invoke-response",
	)
	flags.DurationVar(
		&m.invokeDuration,
		"mock-runtime-invoke-duration",
		0,
		"mock runtime invoke时长",
	)
}

// Run -
func (m *MockRuntimeCmd) Run() error {
	var opts []mockruntime.Opt
	if m.invokeResponseFilePath != "" {
		content, err := os.ReadFile(m.invokeResponseFilePath)
		if err != nil {
			return errors.Wrap(err, "read file")
		}
		opts = append(opts, mockruntime.WithInvokeResponse(content))
	} else if m.invokeResponse != "" {
		opts = append(opts, mockruntime.WithInvokeResponse([]byte(m.invokeResponse)))
	}
	if m.invokeDuration != 0 {
		opts = append(opts, mockruntime.WithInvokeDuration(m.invokeDuration))
	}

	rt := mockruntime.NewMockRuntime(m.port, opts...)
	return rt.Run()
}

// GenerateArgs -
func (m *MockRuntimeCmd) GenerateArgs() []string {
	var res []string
	if m.port != 0 {
		res = append(res, "--mock-runtime-port="+strconv.Itoa(m.port))
	}
	if m.invokeResponse != "" {
		res = append(res, "--mock-runtime-invoke-response="+m.invokeResponse)
	}
	if m.invokeResponseFilePath != "" {
		res = append(res, "--mock-runtime-invoke-response-file-path="+m.invokeResponseFilePath)
	}
	if m.invokeDuration != 0 {
		res = append(res, "--mock-runtime-invoke-duration="+m.invokeDuration.String())
	}
	return res
}
