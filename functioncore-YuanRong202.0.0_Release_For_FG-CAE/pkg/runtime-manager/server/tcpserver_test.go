package server

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/oom"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/tcpmsg"
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
