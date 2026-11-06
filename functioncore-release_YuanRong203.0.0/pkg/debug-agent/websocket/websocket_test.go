/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package websocket operates websocket server and socket client
package websocket

import (
	"context"
	"errors"
	"net"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gorilla/websocket"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/tls"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/debug-agent/config"
	"functioncore/test/utils"
)

func TestBuildStreamOption(t *testing.T) {
	convey.Convey("BuildStreamOption should return correct StreamOption", t, func() {
		conf := &config.DebugAgentConfig{
			ServerConfig: config.ServerConfig{
				HTTPSConfig: tls.InternalHTTPSConfig{
					HTTPSEnable: false,
				},
				Port: 8802,
			},
		}
		expected := &commonwebsocket.StreamOption{
			UpstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "127.0.0.1:8801",
			},
			DownstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "127.0.0.1:8800",
			},
		}
		actual := BuildStreamOption(conf)
		convey.So(actual, convey.ShouldResemble, expected)
	})
}

func createStreamOption() *commonwebsocket.StreamOption {
	opt := &commonwebsocket.StreamOption{
		UpstreamOption: &commonwebsocket.SingleStreamOption{
			URL: "127.0.0.1:8801",
		},
		DownstreamOption: &commonwebsocket.SingleStreamOption{
			URL: "127.0.0.1:8800",
		},
	}
	return opt
}

func TestCreateWssConnPair(t *testing.T) {
	opt := createStreamOption()
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(startWssServer, func(_ *commonwebsocket.SingleStreamOption, _ <-chan struct{}) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: failed to create websocket connect",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(startWssServer, func(_ *commonwebsocket.SingleStreamOption, _ <-chan struct{}) error {
						return errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		stopCh := make(chan struct{})
		patches := tt.patchesFunc()
		err := CreateConnPair(opt, stopCh)
		if (err != nil) != tt.wantErr {
			t.Errorf("CreateConnPair error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
		close(stopCh)
	}
}

func Test_socketConnect(t *testing.T) {
	url := "127.0.0.1:8800"
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(net.Dial, func(network, address string) (net.Conn, error) {
						return &net.TCPConn{}, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: failed to dial",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(net.Dial, func(network, address string) (net.Conn, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		_, err := socketConnect(url)
		if (err != nil) != tt.wantErr {
			t.Errorf("socketConnect error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
	}
}

func Test_startWssServer(t *testing.T) {
	upstreamOpt := createStreamOption().UpstreamOption
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServe", func(_ *http.Server) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: failed to start server",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServe", func(_ *http.Server) error {
						return errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		stopCh := make(chan struct{})
		patches := tt.patchesFunc()
		err := startWssServer(upstreamOpt, stopCh)
		if (err != nil) != tt.wantErr {
			t.Errorf("startWssServer error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
		close(stopCh)
	}
}

func Test_checkJDWPHandshakeMessage(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
		data    []byte
	}{
		{
			name:    "case1: success",
			wantErr: false,
			data:    []byte(jdwpHandshakeMessage),
		},
		{
			name:    "case2: not jdwp handshake message",
			wantErr: true,
			data:    []byte("a"),
		},
	}
	for _, tt := range tests {
		err := checkJDWPHandshakeMessage(tt.data)
		if (err != nil) != tt.wantErr {
			t.Errorf("checkJDWPHandshakeMessage = %v, wantErr %v", err, tt.wantErr)
			return
		}
	}
}

func Test_connectionPair_handleMessage(t *testing.T) {
	convey.Convey("test handleMessage", t, func() {
		pair := createConnectionPair()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(pair), "forwardMessageFromUpToDown", func(_ *connectionPair) {
				pair.receivedFirstMsg.Do(func() {
					pair.receivedFirstMsg.wg.Done()
				})
				return
			}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(pair), "forwardMessageFromDownToUp", func(_ *connectionPair) {
				return
			}),
		})
		defer patches.ResetAll()
		handleMessageFunc := func(p *connectionPair) string {
			p.handleMessage()
			return "OK"
		}
		result := handleMessageFunc(pair)
		convey.So(result, convey.ShouldEqual, "OK")
	})
}

func Test_Continuously_DEBUG(t *testing.T) {
	convey.Convey("test first DEBUG", t, func() {
		firstPair := createConnectionPair()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(firstPair), "forwardMessageFromUpToDown",
				func(_ *connectionPair) {
					firstPair.receivedFirstMsg.Do(func() {
						firstPair.receivedFirstMsg.wg.Done()
					})
					return
				}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(firstPair), "forwardMessageFromDownToUp",
				func(_ *connectionPair) {
					return
				}),
		})
		defer patches.ResetAll()
		handleMessageFunc := func(pair *connectionPair) string {
			pair.handleMessage()
			return "OK"
		}
		result := handleMessageFunc(firstPair)
		convey.So(result, convey.ShouldEqual, "OK")
	})
	convey.Convey("test second DEBUG", t, func() {
		secondPair := createConnectionPair()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(secondPair), "forwardMessageFromUpToDown",
				func(_ *connectionPair) {
					secondPair.receivedFirstMsg.Do(func() {
						secondPair.receivedFirstMsg.wg.Done()
					})
					return
				}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(secondPair), "forwardMessageFromDownToUp",
				func(_ *connectionPair) {
					return
				}),
		})
		defer patches.ResetAll()
		handleMessageFunc := func(pair *connectionPair) string {
			pair.handleMessage()
			return "OK"
		}
		result := handleMessageFunc(secondPair)
		convey.So(result, convey.ShouldEqual, "OK")
	})
}

func Test_socketConnection_close(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		isClosed    uint32
	}{
		{
			name: "case1: connection is not closed",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Close", func(_ *net.TCPConn) error {
						return nil
					}),
				})
				return patches
			},
			isClosed: uint32(0),
		},
		{
			name: "case1: connection has been closed",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			isClosed: uint32(1),
		},
		{
			name: "case1: failed to close socket connection",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Close", func(_ *net.TCPConn) error {
						return errors.New("error")
					}),
				})
				return patches
			},
			isClosed: uint32(0),
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		pair := createConnectionPair()
		pair.downstream.isClosed = tt.isClosed
		pair.downstream.close()
		isClosed := atomic.LoadUint32(&pair.downstream.isClosed)
		if isClosed != uint32(1) {
			t.Errorf("isClosed = %d, want = %d", isClosed, uint32(1))
			return
		}
		patches.ResetAll()
	}
}

func createConnectionPair() *connectionPair {
	upstreamConn := &commonwebsocket.WssConnection{
		Endpoint:    "127.0.0.1:8801",
		Connection:  &websocket.Conn{},
		ForwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
		CloseChan:   make(chan struct{}),
		IsClosed:    uint32(0),
	}
	downstreamConn := &socketConnection{
		endpoint:    "127.0.0.1:8800",
		connection:  &net.TCPConn{},
		forwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
		closeChan:   make(chan struct{}),
		isClosed:    uint32(0),
	}
	pair := &connectionPair{
		upstream:   upstreamConn,
		downstream: downstreamConn,
	}
	return pair
}

func Test_connectionPair_forwardMessageFromDownToUp(t *testing.T) {
	convey.Convey("test forwardMessageFromDownToUp", t, func(c convey.C) {
		pair := createConnectionPair()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(pair.upstream), "TrySendMessage", func(_ *commonwebsocket.WssConnection, _ commonwebsocket.WssMessage) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(pair.upstream), "Close", func(_ *commonwebsocket.WssConnection) {
				return
			}),
			gomonkey.ApplyPrivateMethod(pair.downstream, "close", func(_ *socketConnection) {
				return
			}),
			gomonkey.ApplyPrivateMethod(pair, "readMessageFromSocket", func(_ *connectionPair) error {
				return nil
			}),
		})
		done := make(chan bool)
		go func(c convey.C) {
			pair.forwardMessageFromDownToUp()
			done <- true
		}(c)
		time.Sleep(10 * time.Millisecond)
		defer patches.ResetAll()
		isDone, _ := <-done
		c.So(isDone, convey.ShouldBeTrue)
		close(pair.upstream.CloseChan)
		close(pair.downstream.closeChan)
		time.Sleep(10 * time.Millisecond)
	})
}

func Test_connectionPair_forwardMessageFromUpToDown(t *testing.T) {
	convey.Convey("test forwardMessageFromDownToUp", t, func(c convey.C) {
		pair := createConnectionPair()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(pair.upstream), "Close", func(_ *commonwebsocket.WssConnection) {
				return
			}),
			gomonkey.ApplyPrivateMethod(pair.downstream, "close", func(_ *socketConnection) {
				return
			}),
			gomonkey.ApplyPrivateMethod(pair.downstream, "trySendSocketMessage", func(_ *socketConnection, _ []byte) error {
				return nil
			}),
			gomonkey.ApplyPrivateMethod(pair, "readMessageFromWebsocket", func(_ *connectionPair) error {
				return nil
			}),
		})
		done := make(chan bool)
		go func(c convey.C) {
			pair.forwardMessageFromUpToDown()
			done <- true
		}(c)
		time.Sleep(10 * time.Millisecond)
		defer patches.ResetAll()
		isDone, _ := <-done
		c.So(isDone, convey.ShouldBeTrue)
		close(pair.upstream.CloseChan)
		close(pair.downstream.closeChan)
		time.Sleep(10 * time.Millisecond)
	})
}

func Test_socketConnection_trySendSocketMessage(t *testing.T) {
	conn := createConnectionPair().downstream
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
		isClosed    uint32
	}{
		{
			name: "case1: success",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Write", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, nil
					}),
				})
				return patches
			},
			wantErr:  false,
			isClosed: uint32(0),
		},
		{
			name: "case2: connection is closed",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			wantErr:  true,
			isClosed: uint32(1),
		},
		{
			name: "case3: failed to send",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Write", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, errors.New("error")
					}),
				})
				return patches
			},
			wantErr:  true,
			isClosed: uint32(0),
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		conn.isClosed = tt.isClosed
		err := conn.trySendSocketMessage([]byte("a"))
		if (err != nil) != tt.wantErr {
			t.Errorf("trySendSocketMessage = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
	}
}

func Test_connectionPair_readMessageFromWebsocket(t *testing.T) {
	pair := createConnectionPair()
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success, not jdwp handshake message",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "ReadMessage", func(_ *websocket.Conn) (int, []byte, error) {
						return websocket.BinaryMessage, []byte("a"), nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: success, jdwp handshake message",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "ReadMessage", func(_ *websocket.Conn) (int, []byte, error) {
						return websocket.BinaryMessage, []byte(jdwpHandshakeMessage), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.upstream), "TrySendMessage", func(_ *commonwebsocket.WssConnection, _ commonwebsocket.WssMessage) error {
						return nil
					}),
					gomonkey.ApplyFunc(socketConnect, func(_ string) (net.Conn, error) {
						return &net.TCPConn{}, nil
					}),
					gomonkey.ApplyFunc(handshakeWithRuntime, func(_ net.Conn) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case3: failed to send jdwp handshake message",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "ReadMessage", func(_ *websocket.Conn) (int, []byte, error) {
						return websocket.BinaryMessage, []byte(jdwpHandshakeMessage), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.upstream), "TrySendMessage", func(_ *commonwebsocket.WssConnection, _ commonwebsocket.WssMessage) error {
						return errors.New("error")
					}),
					gomonkey.ApplyFunc(socketConnect, func(_ string) (net.Conn, error) {
						return &net.TCPConn{}, nil
					}),
					gomonkey.ApplyFunc(handshakeWithRuntime, func(_ net.Conn) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case4: failed to read message",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "ReadMessage", func(_ *websocket.Conn) (int, []byte, error) {
						return 0, nil, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case5: failed to connect to runtime",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "ReadMessage", func(_ *websocket.Conn) (int, []byte, error) {
						return websocket.BinaryMessage, []byte(jdwpHandshakeMessage), nil
					}),
					gomonkey.ApplyFunc(socketConnect, func(_ string) (net.Conn, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case6: failed to handshake with runtime",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "ReadMessage", func(_ *websocket.Conn) (int, []byte, error) {
						return websocket.BinaryMessage, []byte(jdwpHandshakeMessage), nil
					}),
					gomonkey.ApplyFunc(socketConnect, func(_ string) (net.Conn, error) {
						return &net.TCPConn{}, nil
					}),
					gomonkey.ApplyFunc(handshakeWithRuntime, func(_ net.Conn) error {
						return errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		err := pair.readMessageFromWebsocket()
		if (err != nil) != tt.wantErr {
			t.Errorf("readMessageFromWebsocket = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
	}
}

func Test_connectionPair_readMessageFromSocket(t *testing.T) {
	pair := createConnectionPair()
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Read", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: failed to read message",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Read", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		err := pair.readMessageFromSocket()
		if (err != nil) != tt.wantErr {
			t.Errorf("readMessageFromSocket = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
	}
}

func Test_connectionPair_release(t *testing.T) {
	convey.Convey("test release", t, func(c convey.C) {
		pair := createConnectionPair()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Close", func(_ *net.TCPConn) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "Close", func(_ *websocket.Conn) error {
				return nil
			}),
		})
		pair.release()
		convey.So(pair.upstream.IsClosed, convey.ShouldEqual, uint32(1))
		convey.So(pair.downstream.isClosed, convey.ShouldEqual, uint32(1))
		defer patches.ResetAll()
	})
}

func Test_handshakeWithRuntime(t *testing.T) {
	conn := &net.TCPConn{}
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Write", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Read", func(_ *net.TCPConn, b []byte) (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyFunc(checkJDWPHandshakeMessage, func(_ []byte) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: failed to write",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Write", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case3: failed to read",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Write", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Read", func(_ *net.TCPConn, b []byte) (int, error) {
						return 0, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case4: message received from runtime is not jdwp handshake message",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Write", func(_ *net.TCPConn, _ []byte) (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&net.TCPConn{}), "Read", func(_ *net.TCPConn, b []byte) (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyFunc(checkJDWPHandshakeMessage, func(_ []byte) error {
						return errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		err := handshakeWithRuntime(conn)
		if (err != nil) != tt.wantErr {
			t.Errorf("handshakeWithRuntime = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
	}
}

func Test_startWssServerStop(t *testing.T) {
	convey.Convey("test startWssServer", t, func() {
		convey.Convey("case1: test stopCh", func() {
			stop := false
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServe", func(_ *http.Server) error {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "Shutdown", func(_ *http.Server, _ context.Context) error {
					stop = true
					return nil
				}),
			})
			defer patches.ResetAll()

			upstreamOpt := createStreamOption().UpstreamOption
			stopCh := make(chan struct{})
			err := startWssServer(upstreamOpt, stopCh)
			convey.So(err, convey.ShouldBeNil)
			convey.So(stop, convey.ShouldBeFalse)
			time.Sleep(100 * time.Millisecond)
			close(stopCh)
			time.Sleep(100 * time.Millisecond)
			convey.So(stop, convey.ShouldBeTrue)
		})
	})
}
