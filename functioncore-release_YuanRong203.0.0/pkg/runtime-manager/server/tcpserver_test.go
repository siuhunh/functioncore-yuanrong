package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/oom"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types"
	"functioncore/pkg/runtime-manager/process"
)

func Test_StartListening(t *testing.T) {
	var err error
	patch := gomonkey.ApplyFunc(tcpmsg.ListenSocketServer, func(
		address string,
		handlerFunc func(msg *tcpmsg.Message) (int, tcpmsg.RuntimeRSP, error),
		newConnFunc func(conn *tcpmsg.SocketConn)) error {
		return err
	})
	defer patch.Reset()
	assert.Equal(t, false, StartListening("127.0.0.1") != nil)
	err = errors.New("ListenSocketServer failed")
	assert.Equal(t, true, StartListening("127.0.0.1") != nil)
}

func Test_requestHandler(t *testing.T) {
	process.Manager.EnableCPUWatchLoop = true
	msg := &tcpmsg.Message{Header: tcpmsg.Header{}}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(process.StopRuntimeProcess, func(msg *tcpmsg.Message) (int, error) { return 0, nil }),
		gomonkey.ApplyMethod(reflect.TypeOf(process.Manager.ConnectionWithWorker), "SendMessageWithNoResp",
			func(*tcpmsg.SocketConn, *tcpmsg.Message) error {
				return nil
			}),
		gomonkey.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return false }),
		gomonkey.ApplyFunc(process.InitRuntimeProcess, func(reqBody *runtimemanagerrequest.InitRequestBody) (int,
			error) {
			return 0, nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	process.Manager.CurrentRuntime = &process.RuntimeProcess{}
	msgs := []tcpmsg.MessageType{tcpmsg.StopProcess, tcpmsg.RequestFinished, tcpmsg.GotRuntimeException,
		tcpmsg.StopRuntimeManager, tcpmsg.PauseRuntimeProcess, tcpmsg.ResetDomainName, tcpmsg.InitProcess,
		tcpmsg.InitProcess, tcpmsg.RecoverRuntimeProcess, tcpmsg.RestartRuntimeProcess, tcpmsg.RecoverRuntimeProcess,
		tcpmsg.ResetDNSConfig}
	for _, msgType := range msgs {
		msg.Header.MsgType = msgType
		wantCode := tcpmsg.MessageWithNoResponse
		code, _, _ := requestHandler(msg)
		switch msgType {
		case tcpmsg.StopProcess:
			wantCode = 0
		case tcpmsg.GotRuntimeException:
			wantCode = 0
		case tcpmsg.StopRuntimeManager:
			process.ExitSignalOnNoRequest = make(chan struct{})
		case tcpmsg.PauseRuntimeProcess:
			wantCode = 0
		case tcpmsg.RecoverRuntimeProcess:
			wantCode = 0
		case tcpmsg.InitProcess:
			wantCode = 0
		case tcpmsg.ResetDomainName:
			wantCode = 0
		case tcpmsg.RestartRuntimeProcess:
			wantCode = 0
		case tcpmsg.FastFreezeCheckpoint:
			wantCode = 0
		default:
			wantCode = tcpmsg.MessageWithNoResponse
		}
		t.Logf("mess %v", msgType)
		assert.Equal(t, wantCode, code)
	}
}

func Test_exit(t *testing.T) {
	convey.Convey("Test_exit", t, func() {
		exit()
		// exit no longer closed ExistSignal since DTS2024101122458
		convey.So(process.ExitSignal, convey.ShouldNotBeNil)
		process.ExitSignal = make(chan struct{})
	})
}

func TestExecPreStopCommand(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(process.ExecPreStopCommand, func() (string, error) { return "", nil }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		code, resp, err := execPreStopCommand()
		assert.NotNil(t, resp)
		assert.Equal(t, code, 0)
		assert.NoError(t, err)
	})
	t.Run("some error", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(process.ExecPreStopCommand, func() (string, error) { return "", errors.New("some error") }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		code, resp, err := execPreStopCommand()
		assert.NotNil(t, resp)
		assert.Equal(t, code, 0)
		assert.Error(t, err)
	})
}

func TestHandleTenantCommands(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(executeSingleCommand, func(ctx context.Context, translatedCommand string) error {
				return nil
			}),
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Output, func(cmd *exec.Cmd) ([]byte, error) {
				return []byte("1245"), nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		tenantCommandValuesMap := map[string]string{}
		tenantCommandValuesMap["pid"] = "ls"
		tenantCommandStruct := types.TenantCommandStruct{
			TaskID: "123456",
			Command: []string{"${runtimePath}/java ${dependencyPath}/ls ${pid}", "ll ${dependencyPath}",
				"ll ${runtimePath}"},
			Timeout:                300,
			TenantCommandValuesMap: tenantCommandValuesMap,
		}

		bodyValue, err := json.Marshal(tenantCommandStruct)
		if err != nil {
			assert.Error(t, errors.New("marshal tenantCommandStruct fail"))
			return
		}

		msg := &tcpmsg.Message{
			Body: bodyValue,
		}
		code, err := handleTenantCommands(msg)
		assert.Equal(t, code, tcpmsg.MessageWithSuccessResponse)
		assert.Equal(t, err, nil)
	})
	t.Run("Unmarshal failed", func(t *testing.T) {
		msg := &tcpmsg.Message{
			Body: []byte("test error"),
		}
		code, err := handleTenantCommands(msg)
		assert.Equal(t, code, tcpmsg.MessageWithError)
		assert.Equal(t, err.Error(), "invalid character 'e' in literal true (expecting 'r')")
	})
	t.Run("${value} not in valueMap", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(executeSingleCommand, func(ctx context.Context, translatedCommand string) error {
				return nil
			}),
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Output, func(cmd *exec.Cmd) ([]byte, error) {
				return []byte("1245"), nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		tenantCommandValuesMap := map[string]string{}
		tenantCommandValuesMap["pid"] = "ls"
		tenantCommandStruct := types.TenantCommandStruct{
			TaskID: "123456",
			Command: []string{"${test}/java",
				"ll ${runtimePath}"},
			Timeout:                300,
			TenantCommandValuesMap: tenantCommandValuesMap,
		}

		bodyValue, err := json.Marshal(tenantCommandStruct)
		if err != nil {
			assert.Error(t, errors.New("marshal tenantCommandStruct fail"))
			return
		}

		msg := &tcpmsg.Message{
			Body: bodyValue,
		}
		code, err := handleTenantCommands(msg)
		assert.Equal(t, code, tcpmsg.MessageWithError)
		assert.Equal(t, err.Error(), "variable test not found in map")
	})
	t.Run("get ${value} fail", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(executeSingleCommand, func(ctx context.Context, translatedCommand string) error {
				return nil
			}),
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Output, func(cmd *exec.Cmd) ([]byte, error) {
				return []byte(""), errors.New("execute fail")
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		tenantCommandValuesMap := map[string]string{}
		tenantCommandValuesMap["pid"] = "ls"
		tenantCommandStruct := types.TenantCommandStruct{
			TaskID: "123456",
			Command: []string{"${pid}/java",
				"ll ${runtimePath}"},
			Timeout:                300,
			TenantCommandValuesMap: tenantCommandValuesMap,
		}

		bodyValue, err := json.Marshal(tenantCommandStruct)
		if err != nil {
			assert.Error(t, errors.New("marshal tenantCommandStruct fail"))
			return
		}

		msg := &tcpmsg.Message{
			Body: bodyValue,
		}
		code, err := handleTenantCommands(msg)
		assert.Equal(t, code, tcpmsg.MessageWithError)
		assert.Equal(t, err.Error(), "failed to get value for variable pid: execute fail")
	})
	t.Run("execute command error", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(executeSingleCommand, func(ctx context.Context, translatedCommand string) error {
				return errors.New("java Not Found")
			}),
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Output, func(cmd *exec.Cmd) ([]byte, error) {
				return []byte("1234"), nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		tenantCommandValuesMap := map[string]string{}
		tenantCommandValuesMap["pid"] = "ls"
		tenantCommandStruct := types.TenantCommandStruct{
			TaskID: "123456",
			Command: []string{"java ${pid}",
				"ll ${runtimePath}"},
			Timeout:                300,
			TenantCommandValuesMap: tenantCommandValuesMap,
		}

		bodyValue, err := json.Marshal(tenantCommandStruct)
		if err != nil {
			assert.Error(t, errors.New("marshal tenantCommandStruct fail"))
			return
		}

		msg := &tcpmsg.Message{
			Body: bodyValue,
		}
		code, err := handleTenantCommands(msg)
		assert.Equal(t, code, tcpmsg.MessageWithError)
		assert.Equal(t, err.Error(), "execute command: java 1234 fail, err: java Not Found")
	})
}

func TestTranslateSingleCommand(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Output, func(cmd *exec.Cmd) ([]byte, error) {
				return []byte("1234"), nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		tenantCommandValuesMap := map[string]string{}
		tenantCommandValuesMap["pid"] = "ls"
		command := "${runtimePath}/java ${dependencyPath}/ls ${dependencyPath}/prof ${pid}"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		translatedCommand, err := translateSingleCommand(ctx, command, tenantCommandValuesMap)
		assert.Equal(t, translatedCommand,
			"/opt/function/runtime/java /opt/function/dependencies/ls /opt/function/dependencies/prof 1234")
		assert.Equal(t, err, nil)
	})
}

func TestExecuteSingleCommand(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).StderrPipe, func(cmd *exec.Cmd) (io.ReadCloser, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Start, func(cmd *exec.Cmd) error {
				return nil
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Scan, func(s *bufio.Scanner) bool {
				return true
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Text, func(s *bufio.Scanner) string {
				return "absafdkfjkkkkkieeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeekdicia"
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Err, func(s *bufio.Scanner) error {
				return nil
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Wait, func(cmd *exec.Cmd) error {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		command := "ll"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		err := executeSingleCommand(ctx, command)
		assert.Equal(t, err, nil)
	})
	t.Run("err output not success", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).StderrPipe, func(cmd *exec.Cmd) (io.ReadCloser, error) {
				return nil, errors.New("error test")
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Start, func(cmd *exec.Cmd) error {
				return nil
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Scan, func(s *bufio.Scanner) bool {
				return true
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Text, func(s *bufio.Scanner) string {
				return "abdsajgicjjjjjjjjjjjjjjjjjjjasfe"
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Err, func(s *bufio.Scanner) error {
				return errors.New("output not finished")
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Wait, func(cmd *exec.Cmd) error {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		command := "ll"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		err := executeSingleCommand(ctx, command)
		assert.Equal(t, err, nil)
	})
	t.Run("start command fail", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Start, func(cmd *exec.Cmd) error {
				return errors.New("start command fail")
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		command := "ll"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		err := executeSingleCommand(ctx, command)
		assert.Equal(t, err.Error(), "start command fail")
	})
	t.Run("execute command ail", func(t *testing.T) {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(exec.CommandContext, func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc((*exec.Cmd).StderrPipe, func(cmd *exec.Cmd) (io.ReadCloser, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Start, func(cmd *exec.Cmd) error {
				return nil
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Scan, func(s *bufio.Scanner) bool {
				return true
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Text, func(s *bufio.Scanner) string {
				return "absafdkfjkkkkkieeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeekdicia"
			}),
			gomonkey.ApplyFunc((*bufio.Scanner).Err, func(s *bufio.Scanner) error {
				return nil
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Wait, func(cmd *exec.Cmd) error {
				return errors.New("execute command fail")
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		command := "ll"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		err := executeSingleCommand(ctx, command)
		assert.Equal(t, err.Error(), "execute command fail")
	})
}

func Test_getInitRequestBody(t *testing.T) {
	req := &tcpmsg.Message{}

	reqBody := &runtimemanagerrequest.InitRequestBody{
		IsMcpFunction: false,
	}
	reqMsg := []byte("")

	reqMsg, err := json.Marshal(reqBody)
	if err != nil {
		reqMsg = []byte("")
	}

	req.Body = reqMsg

	_, err = getInitRequestBody(req)
	if err != nil {
		t.Errorf("failed to test getInitRequestBody")
	}

}
