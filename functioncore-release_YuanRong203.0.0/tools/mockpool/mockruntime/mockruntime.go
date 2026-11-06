package mockruntime

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/connection"
	"functioncore/pkg/worker/runtime/rpc"
	"functioncore/pkg/worker/runtime/rpc/connect/message"
	"functioncore/pkg/worker/runtime/rpc/pb"
)

// Opt -
type Opt func(*MockRuntime)

// WithInvokeResponse -
func WithInvokeResponse(b []byte) Opt {
	return func(mr *MockRuntime) {
		mr.invokeResponse = b
	}
}

// WithInvokeDuration -
func WithInvokeDuration(t time.Duration) Opt {
	return func(mr *MockRuntime) {
		mr.invokeDuration = t
	}
}

// MockRuntime -
type MockRuntime struct {
	port           int
	invokeResponse []byte
	invokeDuration time.Duration
}

// NewMockRuntime -
func NewMockRuntime(port int, opts ...Opt) *MockRuntime {
	mr := &MockRuntime{
		port:           port,
		invokeResponse: []byte("{}"),
	}
	for _, o := range opts {
		o(mr)
	}
	return mr
}

// Run -
func (m *MockRuntime) Run() error {
	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(m.port))
	if err != nil {
		return errors.Wrapf(err, "net listen port: %d", m.port)
	}

	msgPool := &sync.Pool{
		New: func() interface{} {
			return &message.Message{}
		},
	}
	bufPool := &sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}

	process := func(conn net.Conn) {
		for {
			msg := msgPool.Get().(*message.Message)
			err := message.Decode(msg, conn)
			if err != nil {
				panic(err)
			}

			w := func(msg *message.Message) {
				buf := bufPool.Get().(*bytes.Buffer)
				defer bufPool.Put(buf)
				err := message.Encode(msg, buf)
				if err != nil {
					panic(err)
				}
				_, err = conn.Write(buf.Bytes())
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to write: %s\n", err)
				}
				msgPool.Put(msg)
			}

			go m.handle(msg, w)
		}
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			errors.Wrap(err, "net accept")
		}
		go process(connection.NewBufferedConn(conn))
	}
}

func (m *MockRuntime) handle(msg *message.Message, w func(*message.Message)) {
	switch msg.MetaData[0] {
	case rpc.LoadRequestType:
		m.handleLoad(msg, w)
	case rpc.InitializerRequestType:
		m.handleInit(msg, w)
	case rpc.InvokeRequestType:
		m.handleInvoke(msg, w)
	}
}

func (m *MockRuntime) handleLoad(msg *message.Message, w func(*message.Message)) {
	resp := &pb.LoadResponse{}
	b, err := proto.Marshal(resp)
	if err != nil {
		panic(err)
	}

	msg.Header.PacketType = message.NormalResp
	msg.Payload = b
	w(msg)
}

func (m *MockRuntime) handleInit(msg *message.Message, w func(*message.Message)) {
	resp := &pb.InitializerResponse{}
	b, err := proto.Marshal(resp)
	if err != nil {
		panic(err)
	}

	msg.Header.PacketType = message.NormalResp
	msg.Payload = b
	w(msg)
}

func (m *MockRuntime) handleInvoke(msg *message.Message, w func(*message.Message)) {
	if m.invokeDuration > 0 {
		time.Sleep(m.invokeDuration)
	}

	resp := &pb.InvokeResponse{
		Result: m.invokeResponse,
	}
	b, err := proto.Marshal(resp)
	if err != nil {
		panic(err)
	}

	msg.Header.PacketType = message.NormalResp
	msg.Payload = b
	w(msg)
}
