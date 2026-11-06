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

package network

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"math/big"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
	mockutils "functioncore/test/utils"
)

func TestNetWork_NeedChange(t *testing.T) {
	nw := &NetWork{}
	if nw.NeedChange(nw.NATConfigures) {
		t.Error("failed to test no NATConfigures")
	}
	nw.NATConfigures = append(nw.NATConfigures, commontypes.NATConfigure{})

	if !nw.NeedChange(nw.NATConfigures) {
		t.Error("failed to test 1 NATConfigure")
	}

	nw.NATConfigures = append(nw.NATConfigures, commontypes.NATConfigure{})

	if !nw.NeedChange(nw.NATConfigures) {
		t.Error("failed to test 2 NATConfigures")
	}
}

func TestNetWork_Change(t *testing.T) {
	convey.Convey("test network change", t, func() {
		nw := &NetWork{}
		nw.IPV6Enable = true
		natList := make(map[string][]string)
		natList["exclude"] = make([]string, 0)
		natConfig := commontypes.NATConfigure{
			HostVMCidr:     "",
			PatContainerIP: "127.0.0.1",
			PatGateway:     "127.0.0.1",
			PatPodName:     "",
			TenantCidr:     "",
			NatSubnetList:  natList,
		}
		convey.Convey("test configNumber cause index outbounds", func() {
			err := nw.Change(0, nw.NATConfigures, funcTunnelName, false, nil)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("test command iptables", func() {
			nw.NATConfigures = append(nw.NATConfigures, natConfig)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						ServerConfig: *&config.ServerConfig{
							HTTPSConfig: *&tls.InternalHTTPSConfig{
								HTTPSEnable: false,
							},
						},
					}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
					_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
					return errors.New("do http health check failed")
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					// ignore
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := nw.Change(0, nw.NATConfigures, funcTunnelName, false, nil)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("failed to check Connectivity", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(ExecuteShellCommandWithCombinedOutput,
					func(name string, arg ...string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc((*NetWork).checkConnectivity, func(_ *NetWork, ip string, subMetaDigest string, ipv6 bool) error {
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						ServerConfig: *&config.ServerConfig{
							HTTPSConfig: *&tls.InternalHTTPSConfig{
								HTTPSEnable: false,
							},
						},
					}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
					_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
					return nil
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					// ignore
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			nw.NATConfigures = append(nw.NATConfigures, natConfig)
			err := nw.Change(0, nw.NATConfigures, funcTunnelName, false, nil)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("test subnet list is empty", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(ExecuteShellCommandWithCombinedOutput,
					func(name string, arg ...string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc((*NetWork).checkConnectivity, func(_ *NetWork, ip string, subMetaDigest string, ipv6 bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						ServerConfig: *&config.ServerConfig{
							HTTPSConfig: *&tls.InternalHTTPSConfig{
								HTTPSEnable: false,
							},
						},
					}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
					_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
					return nil
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					// ignore
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			tmpNatConfig := commontypes.NATConfigure{
				HostVMCidr:     "",
				PatContainerIP: "127.0.0.1",
				PatGateway:     "127.0.0.1",
				PatPodName:     "",
				TenantCidr:     "",
				NatSubnetList:  make(map[string][]string),
			}
			nw.NATConfigures = append(nw.NATConfigures, tmpNatConfig)
			err := nw.Change(0, nw.NATConfigures, funcTunnelName, false, nil)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("test change get net interface failed", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(ExecuteShellCommandWithCombinedOutput,
					func(name string, arg ...string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc((*NetWork).checkConnectivity, func(_ *NetWork, ip string, subMetaDigest string, ipv6 bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						ServerConfig: *&config.ServerConfig{
							HTTPSConfig: *&tls.InternalHTTPSConfig{
								HTTPSEnable: false,
							},
						},
					}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
					_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
					return nil
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					// ignore
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			nw.NATConfigures = append(nw.NATConfigures, natConfig)
			err := nw.Change(0, nw.NATConfigures, funcTunnelName, false, nil)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("test change ok", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					return &exec.Cmd{}
				}),
				gomonkey.ApplyFunc((*exec.Cmd).Output, func(_ *exec.Cmd) ([]byte, error) {
					return []byte("mock"), nil
				}),
				gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(ExecuteShellCommandWithCombinedOutput,
					func(name string, arg ...string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(GetIntfnameAndGatewayByIPAddr,
					func(name string) (string, string, error) {
						return "", "", nil
					}),
				gomonkey.ApplyFunc((*NetWork).checkConnectivity, func(_ *NetWork, ip string, subMetaDigest string, ipv6 bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						ServerConfig: *&config.ServerConfig{
							HTTPSConfig: *&tls.InternalHTTPSConfig{
								HTTPSEnable: false,
							},
						},
					}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
					_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
					return nil
				}),
				gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
					// ignore
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			nw.NATConfigures = append(nw.NATConfigures, natConfig)
			err := nw.Change(0, nw.NATConfigures, funcTunnelName, false, nil)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestNetWork_CheckNATHealth(t *testing.T) {
	nw := &NetWork{}
	subMetaDigest := "fake_sub_meta_digest"
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
			return &config.Configuration{
				ServerConfig: *&config.ServerConfig{
					HTTPSConfig: *&tls.InternalHTTPSConfig{
						HTTPSEnable: false,
					},
				},
			}, nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
			_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
			return errors.New("do http health check failed")
		}),
		gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
			// ignore
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	if nw.CheckNATHealth(context.Background(), "1.1.1.1", "1.1.1.1", subMetaDigest) == nil {
		t.Error("failed to test ping invalid ip")
	}
	ctxWithTimeout, _ := context.WithTimeout(context.Background(), 1*time.Second)
	if nw.CheckNATHealth(ctxWithTimeout, "1.1.1.1", "1.1.1.1", subMetaDigest) != nil {
		t.Error("failed to test ping invalid ip")
	}
}

func TestNetWork_Reset(t *testing.T) {
	p := gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
		return nil
	})
	defer p.Reset()
	nw := &NetWork{}
	nw.Reset("")

	p = gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
		return errors.New("exec command error")
	})
	nw.Reset("")
}

func TestNetWork_checkConnectivity(t *testing.T) {
	convey.Convey("test checkConnectivity 1", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return &config.Configuration{
					ServerConfig: *&config.ServerConfig{
						HTTPSConfig: *&tls.InternalHTTPSConfig{
							HTTPSEnable: false,
						},
					},
				}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
				_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
				return errors.New("do http health check failed")
			}),
			gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
				// ignore
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ip := "127.0.0.1"
		subMetaDigest := "fake_sub_meta_digest"
		n := &NetWork{}
		err := n.checkConnectivity(ip, subMetaDigest, false)
		convey.So(err.Error(), convey.ShouldEqual, "failed to do http health check '127.0.0.1'")
	})
	convey.Convey("test checkConnectivity 2", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return &config.Configuration{
					ServerConfig: *&config.ServerConfig{
						HTTPSConfig: *&tls.InternalHTTPSConfig{
							HTTPSEnable: false,
						},
					},
				}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
				_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ip := "127.0.0.1:80"
		subMetaDigest := "fake_sub_meta_digest"
		n := &NetWork{}
		err := n.checkConnectivity(ip, subMetaDigest, false)
		fmt.Println(err)
		convey.So(err, convey.ShouldEqual, nil)
	})
}

func TestNetWork_buildExcludeCmd(t *testing.T) {
	convey.Convey("buildExcludeCmd test", t, func() {
		excludeSubnetList := make([]string, 0)
		excludeSubnetList = append(excludeSubnetList, "127.0.0.1:8080")
		excludeSubnetList = append(excludeSubnetList, "127.0.0.1:8090")
		cmdList := make([]*cmdPair, 0)
		intfName := "test"
		containerGateway := ""
		cmdList = buildExcludeCmd(excludeSubnetList, cmdList, intfName, containerGateway)
		convey.So(cmdList[0].del.cmdline, convey.ShouldEqual, "ip route del 127.0.0.1:8080 dev test")
		convey.So(cmdList[1].del.cmdline, convey.ShouldEqual, "ip route del 127.0.0.1:8090 dev test")
	})
}

func TestNetWork_buildIPTunnelCmd(t *testing.T) {
	convey.Convey("test buildIPTunnelCmd", t, func() {
		cmdList := make([]*cmdPair, 0)
		tunnelName := "testTunnel"
		funcContainerIP := "127.0.0.1"
		patContainerIP := "127.0.0.2"
		cmdList = buildIPTunnelCmd(cmdList, tunnelName, funcContainerIP, patContainerIP)
		convey.So(cmdList[0].add.cmdline, convey.ShouldEqual, "ip tunnel add testTunnel mode ipip local 127.0.0.1 remote 127.0.0.2")
		convey.So(cmdList[1].add.cmdline, convey.ShouldEqual, "ip link set testTunnel up")
	})
}
func Test_getIntfnameAndGatewayByIPAddr(t *testing.T) {
	ip := "127.0.0.1"
	tests := []struct {
		name    string
		Setup   func(t *testing.T)
		wantErr string
	}{
		{
			name:    "case1",
			wantErr: "",
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(GetIntfNameAndNetmaskOfIp, func(ip string) (string, string, error) {
					return "", "", errors.New("")
				})
				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
		{
			name:    "case2",
			wantErr: "",
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(GetIntfNameAndNetmaskOfIp, func(ip string) (string, string, error) {
					return "", "", nil
				})
				patches1 := gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(cmdline string) (int, string, error) {
						return 0, "", errors.New("")
					})
				t.Cleanup(func() {
					patches.Reset()
					patches1.Reset()
				})
			},
		},
		{
			name:    "case3",
			wantErr: "",
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(GetIntfNameAndNetmaskOfIp, func(ip string) (string, string, error) {
					return "", "", nil
				})
				patches1 := gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(cmdline string) (int, string, error) {
						return 1, "", nil
					})
				t.Cleanup(func() {
					patches.Reset()
					patches1.Reset()
				})
			},
		},
		{
			name:    "case4",
			wantErr: "",
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(GetIntfNameAndNetmaskOfIp, func(ip string) (string, string, error) {
					return "", "", nil
				})
				patches1 := gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(cmdline string) (int, string, error) {
						return 0, "adcvad", nil
					})
				t.Cleanup(func() {
					patches.Reset()
					patches1.Reset()
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			_, gateway, err := GetIntfnameAndGatewayByIPAddr(ip)
			if err != nil && !strings.EqualFold(gateway, "") {
				assert.EqualError(t, err, tt.wantErr, " Not as expectedg")
			}
		})
	}
}

func Test_checkPatContainerIP(t *testing.T) {
	n := &NetWork{}
	n.IPV6Enable = true
	n.Ping6Hash = "123"
	patContainerIP := ""
	ipv6PatContainerIP := ""
	subMetaDigest := "fake_sub_meta_digest"
	tests := []struct {
		name  string
		Setup func(t *testing.T)
	}{
		{
			name: "test checkPatContainerIP",
			Setup: func(t *testing.T) {
				patches := []*gomonkey.Patches{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
						return &config.Configuration{
							ServerConfig: *&config.ServerConfig{
								HTTPSConfig: *&tls.InternalHTTPSConfig{
									HTTPSEnable: false,
								},
							},
						}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(_ *fasthttp.Client,
						_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
						return nil
					}),
					gomonkey.ApplyFunc(utils.GetHash, func(path string) (hashValue string) {
						return "123"
					}),
					gomonkey.ApplyFunc((*exec.Cmd).Run, func(c *exec.Cmd) error {
						return nil
					}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
						// ignore
					}),
				}
				t.Cleanup(func() {
					for idx := range patches {
						patches[idx].Reset()
					}
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			err := n.checkPatContainerIP(patContainerIP, ipv6PatContainerIP, subMetaDigest)
			if err != nil {
				t.Error("Not as expected, err: " + err.Error())
			}
		})
	}
}

func Test_execCommandLine(t *testing.T) {
	command := &command{
		failurable: false,
	}

	tests := []struct {
		name  string
		Setup func(t *testing.T)
	}{
		{
			name: "test execCommandLine",
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(cmdline string) (int, string, error) {
						return 1, "", nil
					})
				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			err := execCommandLine(command)
			if err == nil {
				assert.EqualError(t, err, " Not as expectedg")
			}
		})
	}
	assert.Equal(t, execCommandLine(nil).Error(), "command is nil")
}

func Test_ChangeNetworkToStreamHandler(t *testing.T) {
	p := gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
		return errors.New("wrong")
	})
	defer p.Reset()
	streamHandlerHostCidr := "10.0.0.0/16"
	streamHandlerServicePort := "26939"
	err := ChangeNetworkToStreamHandler(streamHandlerHostCidr, streamHandlerServicePort)
	assert.Equal(t, "wrong", err.Error())

	patch := gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
		return nil
	})
	defer patch.Reset()
	err = ChangeNetworkToStreamHandler(streamHandlerHostCidr, streamHandlerServicePort)
	assert.Equal(t, nil, err)
}

func TestVpcGracefulShutdown(t *testing.T) {
	isMocked := false
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
		isMocked    bool
	}{
		{
			name: "test with error",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
						isMocked = true
						return errors.New("mock exec command line error")
					}),
				})
				return patches
			},
			isMocked: true,
		},
	}
	for _, tt := range tests {
		isMocked = false
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			gracefulShutdownCmdList = []*command{
				{
					cmdline:    "ls -lh",
					failurable: true,
				},
			}
			VpcGracefulShutdown()
			if isMocked != tt.isMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
			}
		})
		patches.ResetAll()
	}
}

func Test_buildConnectionToCacheManagerCmd(t *testing.T) {
	type args struct {
		cmdList          []*cmdPair
		containerGateway string
		intfName         string
	}
	tests := []struct {
		name   string
		ipAddr string
		args   args
		want   []*cmdPair
	}{
		{
			name:   "test with illegal ip address",
			ipAddr: "illegal ip address",
			args: args{
				cmdList: make([]*cmdPair, 0),
			},
			want: make([]*cmdPair, 0),
		},
		{
			name:   "test with legal ip address",
			ipAddr: "192.168.1.1",
			args: args{
				cmdList:          make([]*cmdPair, 0),
				containerGateway: "10.0.0.1",
				intfName:         "mockIntfName",
			},
			want: []*cmdPair{
				{
					add: &command{cmdline: fmt.Sprintf("ip route add %s via %s dev %s",
						"192.168.1.1", "10.0.0.1", "mockIntfName")},
				},
			},
		},
	}
	for _, tt := range tests {
		os.Setenv("CACHE_MANAGER_SERVICE_HOST", tt.ipAddr)
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, buildConnectionToCacheManagerCmd(tt.args.cmdList, tt.args.containerGateway, tt.args.intfName), "buildConnectionToCacheManagerCmd(%v, %v, %v)", tt.args.cmdList, tt.args.containerGateway, tt.args.intfName)
		})
	}
}

func TestNetWork_Derange(t *testing.T) {
	NATConfigures := []commontypes.NATConfigure{{PatPodName: "pat_1"}, {PatPodName: "pat_2"}}
	EndpointVpcNATConfigure := []commontypes.NATConfigure{{PatPodName: "pat_3"}, {PatPodName: "pat_4"}}
	type fields struct {
		NATConfigures           []commontypes.NATConfigure
		EndpointVpcNATConfigure []commontypes.NATConfigure
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockutils.PatchesFunc
		want1       []commontypes.NATConfigure
		want2       []commontypes.NATConfigure
	}{
		{
			name: "case01: no need to derange",
			fields: fields{
				NATConfigures:           NATConfigures,
				EndpointVpcNATConfigure: EndpointVpcNATConfigure,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(rand.Int,
						func(rand io.Reader, max *big.Int) (n *big.Int, err error) {
							return big.NewInt(11), nil
						}),
				})
				return patches
			},
			want1: []commontypes.NATConfigure{{PatPodName: "pat_1"}, {PatPodName: "pat_2"}},
			want2: []commontypes.NATConfigure{{PatPodName: "pat_3"}, {PatPodName: "pat_4"}},
		}, {
			name: "case02: need to derange",
			fields: fields{
				NATConfigures:           NATConfigures,
				EndpointVpcNATConfigure: EndpointVpcNATConfigure,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(rand.Int,
						func(rand io.Reader, max *big.Int) (n *big.Int, err error) {
							return big.NewInt(10), nil
						}),
				})
				return patches
			},
			want1: []commontypes.NATConfigure{{PatPodName: "pat_2"}, {PatPodName: "pat_1"}},
			want2: []commontypes.NATConfigure{{PatPodName: "pat_4"}, {PatPodName: "pat_3"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			n := &NetWork{
				NATConfigures:           tt.fields.NATConfigures,
				EndpointVpcNATConfigure: tt.fields.EndpointVpcNATConfigure,
			}
			n.Derange()
			assert.Equalf(t, tt.want1, n.NATConfigures, "test error")
			assert.Equalf(t, tt.want2, n.EndpointVpcNATConfigure, "test error")
			patches.ResetAll()
		})
	}
}

func Test_buildApigRouteCmd(t *testing.T) {
	type args struct {
		apigRoutes       commontypes.RouteInfoData
		intfName         string
		containerGateway string
	}
	tests := []struct {
		name      string
		cmdLength int
		arg       args
	}{
		{
			name: "ApigRouteEnable is disable",
			arg: args{
				apigRoutes: commontypes.RouteInfoData{
					ApigRouteEnable: false,
				},
			},
			cmdLength: 0,
		},
		{
			name: "ApigRoutes is empty",
			arg: args{
				apigRoutes: commontypes.RouteInfoData{
					ApigRouteEnable: true,
					ApigRoutes:      []string{},
				},
			},
			cmdLength: 0,
		},
		{
			name: "succeed to buildApigRouteCmd",
			arg: args{
				apigRoutes: commontypes.RouteInfoData{
					ApigRouteEnable: true,
					ApigRoutes:      []string{"192.168.0.1", "192.168.0.2"},
				},
			},
			cmdLength: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := buildApigRouteCmd(tt.arg.apigRoutes, tt.arg.intfName, tt.arg.containerGateway)
			if len(data) != tt.cmdLength {
				t.Error("Not as expectedg")
			}
		})
	}
}

func TestNetWork_addRouteInfo(t *testing.T) {
	convey.Convey("succeed to addRouteInfo", t, func() {
		nw := &NetWork{
			RouteInfo: commontypes.RouteInfoData{
				ApigRouteEnable: false,
				ApigRoutes:      []string{"192.168.0.1"},
			},
		}
		err := nw.addRouteInfo("test", "test")
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_execAddRouteCmd(t *testing.T) {
	convey.Convey("test execAddRouteCmd", t, func() {
		convey.Convey("failed to execCommandLine", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
					return errors.New("test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cmdList := make([]*cmdPair, 0)
			cmdList = append(cmdList, &cmdPair{
				add: &command{
					cmdline: "ls",
				},
				del: &command{
					cmdline: "ls",
				},
			})
			cmdList = append(cmdList, &cmdPair{
				add: &command{
					cmdline: "ls",
				},
				del: &command{
					cmdline: "ls",
				},
			})
			err := execAddRouteCmd(cmdList)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("cmdList add or del is nil", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
					return errors.New("test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cmdList := make([]*cmdPair, 0)
			cmdList = append(cmdList, &cmdPair{
				add: &command{
					cmdline: "ls",
				},
				del: nil,
			})
			err := execAddRouteCmd(cmdList)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("succeed to exec add route cmd", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cmdList := make([]*cmdPair, 0)
			cmdList = append(cmdList, &cmdPair{
				add: &command{
					cmdline: "ls",
				},
				del: &command{
					cmdline: "ls",
				},
			})
			err := execAddRouteCmd(cmdList)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_buildPodRouteForNetworkModelVPC(t *testing.T) {
	defer func() {
		os.Unsetenv("POD_CIDR")
	}()

	inftName := "fakeEth"
	gateway := "192.0.0.7"

	tests := []struct {
		name     string
		podCidr  string
		excepted []*cmdPair
	}{
		{
			name:     "no pod cidr config",
			podCidr:  "",
			excepted: nil,
		},
		{
			name:    "has pod cidr config",
			podCidr: "10.0.0.1/16",
			excepted: []*cmdPair{
				{
					add: &command{cmdline: "ip route add 10.0.0.1/16 via 192.0.0.7 dev fakeEth"},
					del: &command{cmdline: "ip route del 10.0.0.1/16 dev fakeEth", failurable: true},
				},
				{
					add: &command{cmdline: "iptables -D OUTPUT -d 10.0.0.1/16 -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW -j DROP", failurable: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("POD_CIDR", tt.podCidr)
			ret := buildPodRouteForNetworkModelVPC(nil, inftName, gateway)
			fmt.Printf("actual :%+v\n", ret)
			fmt.Printf("except :%+v", tt.excepted)
			assert.Equal(t, ret, tt.excepted, tt.name)
		})
	}
}

func Test_execChangeNetCmd(t *testing.T) {
	type args struct {
		cmdList []*cmdPair
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test cmd add is nil",
			args: args{
				cmdList: []*cmdPair{
					{
						add: nil,
						del: nil,
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
		{
			name: "test  index == len(cmdList)-1 && cmd.del != nil",
			args: args{
				cmdList: []*cmdPair{
					{
						add: &command{},
						del: &command{},
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(execCommandLine, func(command *command) error {
						return errors.New("mock execCommandLine error")
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			tt.wantErr(t, execChangeNetCmd(tt.args.cmdList), fmt.Sprintf("execChangeNetCmd(%v)", tt.args.cmdList))
		})
	}
}

func Test_resetWorkerResolveConf(t *testing.T) {
	type args struct {
		n *NetWork
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test ioutil.ReadFile error",
			args: args{
				n: &NetWork{},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			name: "test open file error",
			args: args{
				n: &NetWork{},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						str := "nameserver 169.254.20.10"
						return []byte(str), nil
					}),
					gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			name: "test rollback error",
			args: args{
				n: &NetWork{SafeDNSAddr: "127.0.0.1"},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						str := "nameserver 169.254.20.10"
						return []byte(str), nil
					}),
					gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
						return &os.File{}, nil
					}),
					gomonkey.ApplyFunc((*os.File).Truncate, func(_ *os.File, _ int64) error {
						return nil
					}),
					gomonkey.ApplyFunc((*os.File).Seek, func(f *os.File, offset int64, whence int) (ret int64, err error) {
						return 0, nil
					}),
					gomonkey.ApplyFunc(fmt.Fprintln, func(w io.Writer, a ...any) (int, error) {
						return 0, errors.New("error")
					}),
					gomonkey.ApplyFunc(ioutil.WriteFile, func(filename string, data []byte, perm fs.FileMode) error {
						return errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			name: "test rollback success",
			args: args{
				n: &NetWork{SafeDNSAddr: "127.0.0.1"},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						str := "nameserver 169.254.20.10"
						return []byte(str), nil
					}),
					gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
						return &os.File{}, nil
					}),
					gomonkey.ApplyFunc((*os.File).Truncate, func(_ *os.File, _ int64) error {
						return nil
					}),
					gomonkey.ApplyFunc((*os.File).Seek, func(f *os.File, offset int64, whence int) (ret int64, err error) {
						return 0, nil
					}),
					gomonkey.ApplyFunc(fmt.Fprintln, func(w io.Writer, a ...any) (int, error) {
						return 0, errors.New("error")
					}),
					gomonkey.ApplyFunc(ioutil.WriteFile, func(filename string, data []byte, perm fs.FileMode) error {
						return nil
					}),
				})
				return patches
			},
		},
		{
			name: "test clear",
			args: args{
				n: &NetWork{SafeDNSAddr: "127.0.0.1"},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						str := "nameserver 169.254.20.10"
						return []byte(str), nil
					}),
					gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
						return &os.File{}, nil
					}),
					gomonkey.ApplyFunc((*os.File).Truncate, func(_ *os.File, _ int64) error {
						return nil
					}),
					gomonkey.ApplyFunc((*os.File).Seek, func(f *os.File, offset int64, whence int) (ret int64, err error) {
						return 0, nil
					}),
					gomonkey.ApplyFunc(fmt.Fprintln, func(w io.Writer, a ...any) (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyFunc(ioutil.WriteFile, func(filename string, data []byte, perm fs.FileMode) error {
						return nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			tt.args.n.resetWorkerResolveConf()
		})
	}
}

func TestChangeNetworkToDesPort(t *testing.T) {
	p := gomonkey.ApplyFuncSeq(execCommandLine, []gomonkey.OutputCell{
		{Values: gomonkey.Params{errors.New("some error")}, Times: 1},
	})
	defer p.Reset()
	err := ChangeNetworkToDesPort("8080")
	assert.NotNil(t, err)
}

func Test_AddExcludeSubnetRouteToUploadSnapshot(t *testing.T) {
	type args struct {
		n *NetWork
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "is safety false",
			args: args{n: &NetWork{IsSafety: false}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
						return "", "", errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			name: "GetIntfnameAndGatewayByIPAddr error ",
			args: args{n: &NetWork{IsSafety: true}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
						return "", "", errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			name: "GetIntfnameAndGatewayByIPAddr error ",
			args: args{n: &NetWork{IsSafety: true, CurrentNATConfigure: commontypes.NATConfigure{NatSubnetList: map[string][]string{"exclude": {"10.0.0.1", "10.0.0.2"}}}}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
						return "etc", "127.0.0.1", nil
					}),
					gomonkey.ApplyFunc((*exec.Cmd).Run, func(_ *exec.Cmd) error {
						return errors.New("error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			tt.args.n.AddExcludeSubnetRouteToUploadSnapshot()
		})
	}
}

func Test_ChangeNetworkForSafeVpc(t *testing.T) {
	type args struct {
		n *NetWork
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test",
			args: args{n: &NetWork{CurrentNATConfigure: commontypes.NATConfigure{NatSubnetList: map[string][]string{"exclude": {"10.0.0.1", "10.0.0.2"}}}}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getNameServerList, func() []string {
						return []string{"127.0.0.1"}
					}),
					gomonkey.ApplyFunc((*NetWork).resetWorkerResolveConf, func(_ *NetWork) {}),
					gomonkey.ApplyFunc((*exec.Cmd).Run, func(_ *exec.Cmd) error {
						return errors.New("error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			tt.args.n.NatInitializeChan = make(chan struct{}, 1)
			tt.args.n.CodeDeployChan = make(chan struct{}, 1)
			tt.args.n.NatInitializeChan <- struct{}{}
			tt.args.n.CodeDeployChan <- struct{}{}
			tt.args.n.ChangeNetworkForSafeVpc()
		})
	}
}

func TestNetWork_ChangePublicVpc(t *testing.T) {
	convey.Convey("test public network change", t, func() {
		nw := &NetWork{}
		nw.IPV6Enable = true
		natList := make(map[string][]string)
		natList["exclude"] = make([]string, 0)
		natConfig := commontypes.NATConfigure{
			HostVMCidr:     "",
			PatContainerIP: "127.0.0.1",
			PatGateway:     "127.0.0.1",
			PatPodName:     "",
			TenantCidr:     "",
			NatSubnetList:  natList,
		}
		convey.Convey("test configNumber cause index outbounds", func() {
			err := nw.ChangePublicVpc(0, nw.PublicVpcNATConfigure, nil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test command iptables", func() {
			nw.PublicVpcNATConfigure = append(nw.PublicVpcNATConfigure, natConfig)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := nw.ChangePublicVpc(0, nw.PublicVpcNATConfigure, nil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("failed to check Connectivity", func() {
			nw.PublicVpcNATConfigure = append(nw.PublicVpcNATConfigure, natConfig)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(ExecuteShellCommandWithCombinedOutput,
					func(name string, arg ...string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc((*NetWork).checkConnectivity, func(_ *NetWork, ip string, subMetaDigest string, ipv6 bool) error {
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						ServerConfig: *&config.ServerConfig{
							HTTPSConfig: *&tls.InternalHTTPSConfig{
								HTTPSEnable: false,
							},
						},
					}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			nw.NATConfigures = append(nw.NATConfigures, natConfig)
			err := nw.ChangePublicVpc(0, nw.PublicVpcNATConfigure, nil)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("succeed to get net work info", func() {
			nw.PublicVpcNATConfigure = append(nw.PublicVpcNATConfigure, natConfig)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(os.Getenv, func(s string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(ExecuteShellCommandWithCombinedOutput,
					func(name string, arg ...string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", nil
					}),
				gomonkey.ApplyFunc(GetIntfnameAndGatewayByIPAddr,
					func(name string) (string, string, error) {
						return "", "", nil
					}),
				gomonkey.ApplyFunc((*NetWork).checkConnectivity, func(_ *NetWork, ip string, subMetaDigest string, ipv6 bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						ServerConfig: *&config.ServerConfig{
							HTTPSConfig: *&tls.InternalHTTPSConfig{
								HTTPSEnable: false,
							},
						},
					}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			nw.NATConfigures = append(nw.NATConfigures, natConfig)
			err := nw.ChangePublicVpc(0, nw.PublicVpcNATConfigure, nil)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_getExtendedInfoRouteCmd(t *testing.T) {
	convey.Convey("test getExtendedInfoRouteCmd", t, func() {
		convey.Convey("case1: success", func() {
			cmd := getExtendedInfoRouteCmd("127.0.0.1/8", "127.0.0.1", "aaa")
			convey.So(cmd, convey.ShouldNotBeNil)
			convey.So(cmd.del.cmdline, convey.ShouldEqual, "ip route del 127.0.0.1/8 dev aaa")
			convey.So(cmd.add.cmdline, convey.ShouldEqual, "ip route add 127.0.0.1/8 via 127.0.0.1 dev aaa onlink")
		})
		convey.Convey("case2: extendedInfo is nil", func() {
			cmd := getExtendedInfoRouteCmd("", "127.0.0.1", "aaa")
			convey.So(cmd, convey.ShouldBeNil)
		})
		convey.Convey("case3: extendedInfo is invalid", func() {
			cmd := getExtendedInfoRouteCmd("error", "127.0.0.1", "aaa")
			convey.So(cmd, convey.ShouldBeNil)
		})
	})
}
