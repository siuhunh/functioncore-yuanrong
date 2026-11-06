/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/fsnotify/fsnotify"
	"github.com/smartystreets/goconvey/convey"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/obs"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/deploy"
	"functioncore/pkg/worker/invoke"
	"functioncore/pkg/worker/mount"
	"functioncore/pkg/worker/network"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/fusion"
	"functioncore/pkg/worker/snapshot"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
	"functioncore/pkg/worker/websocket"
	mockUtils "functioncore/test/utils"
)

type fakeDeploy struct{}

func (d *fakeDeploy) Deploy(conf *config.Configuration,
	getCodeFinishCh chan struct{}, language string) (*deploy.DurationInfo, error) {
	return &deploy.DurationInfo{DeployFuncTime: 1 * time.Millisecond}, nil
}

type fakeBigCodeDeploy struct{}

func (d *fakeBigCodeDeploy) Deploy(conf *config.Configuration, getCodeFinishCh chan struct{}) (*deploy.DurationInfo, error) {
	time.Sleep(1 * time.Second)
	return &deploy.DurationInfo{DeployFuncTime: 1 * time.Millisecond}, nil
}

func (d *fakeDeploy) DeploySnapshot(conf *config.Configuration, ch chan struct{},
	language string) (*deploy.DurationInfo, error) {
	return &deploy.DurationInfo{DeployFuncTime: 1 * time.Millisecond}, nil
}

func (d *fakeDeploy) UploadSnapshot(conf *config.Configuration, bucketID, ObjectID string) (int64, string, error) {
	return 0, "", nil
}

func (d *fakeDeploy) DownloadSnapshotWithRetry(conf *config.Configuration, snapshotConfig config.SnapshotConfig,
	language string, traceID string) (bool, error) {
	return false, nil
}

func (d *fakeDeploy) DeleteSnapshot(conf *config.Configuration, bucketID, ObjectID string,
	needDeleteImageTag []string) error {
	return nil
}

func (d *fakeDeploy) SetCacheManagerServiceIP(conf *config.Configuration) {
	return
}

func (d *fakeDeploy) UploadDumpFile(conf *config.Configuration, dumpFileDir string) error {
	return nil
}

type fakeDeployErr struct{}

func (d *fakeDeployErr) Deploy(conf *config.Configuration,
	getCodeFinishCh chan struct{}, language string) (*deploy.DurationInfo, error) {
	return nil, errors.New("failed to deploy")
}

func (d *fakeDeployErr) DeploySnapshot(conf *config.Configuration,
	ch chan struct{}, language string) (*deploy.DurationInfo, error) {
	return nil, errors.New("failed to deploy snapshot")
}

func (d *fakeDeployErr) UploadSnapshot(conf *config.Configuration, bucketID, ObjectID string) (int64, string, error) {
	return 0, "", errors.New("failed to upload snapshot")
}

func (d *fakeDeployErr) DownloadSnapshotWithRetry(conf *config.Configuration, snapshotConfig config.SnapshotConfig,
	language string, traceID string) (bool, error) {
	return false, nil
}

func (d *fakeDeployErr) DeleteSnapshot(conf *config.Configuration, bucketID, ObjectID string,
	needDeleteImageTag []string) error {
	return errors.New("delete snapshot error")
}

func (d *fakeDeployErr) SetCacheManagerServiceIP(conf *config.Configuration) {
	return
}

func (d *fakeDeployErr) UploadDumpFile(conf *config.Configuration, dumpFileDir string) error {
	return errors.New("error")
}

func Test_changeNetWorkEx(t *testing.T) {
	fakeDeployObject := fakeDeploy{}
	server := Server{conf: &config.Configuration{}, deployer: &fakeDeployObject, invokeServer: &invoke.Server{DeployCodeFinishedChan: make(chan struct{})}}
	close(server.invokeServer.DeployCodeFinishedChan)
	Convey("Test_changeNetWorkEx ", t, func() {
		Convey("test change network failed", func() {
			defer ApplyFunc(changeNetWork, func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
				return errors.New("failed to change net")
			}).Reset()
			netWorkCh := make(chan error, 1)
			getCodeFinishCh := make(chan struct{}, 1)
			getCodeFinishCh <- struct{}{}
			server.changeNetWorkEx(context.Background(), &network.NetWork{
				PublicVpcNATConfigure: []commontypes.NATConfigure{{}},
			}, netWorkCh, getCodeFinishCh)
			select {
			case err := <-netWorkCh:
				So(err, ShouldNotBeNil)
			}
		})
		Convey("test change network success", func() {
			defer ApplyFunc(changeNetWork, func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
				return nil
			}).Reset()
			netWorkCh := make(chan error, 1)
			getCodeFinishCh := make(chan struct{}, 1)
			getCodeFinishCh <- struct{}{}
			server.changeNetWorkEx(context.Background(), &network.NetWork{}, netWorkCh, getCodeFinishCh)
			select {
			case err := <-netWorkCh:
				So(err, ShouldBeNil)
			}
		})
	})
}

func Test_server_deploy(t *testing.T) {
	convey.Convey("Test_server_deploy RunningModeRestore usePoolPod ok", t, func() {
		deployer := &fakeDeploy{}
		server := Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					IsStreamEnable: true, Language: "go",
					RunningMode: constants.RunningModeRestore,
					UsePoolPod:  true,
				},
			},
			deployer: deployer,
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(deployer), "DeploySnapshot",
				func(d *fakeDeploy, conf *config.Configuration, ch chan struct{}, language string) (*deploy.DurationInfo, error) {
					return &deploy.DurationInfo{}, nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(deployer), "Deploy",
				func(d *fakeDeploy, conf *config.Configuration, getCodeFinishCh chan struct{},
					language string) (*deploy.DurationInfo, error) {
					return &deploy.DurationInfo{}, nil
				}),
			gomonkey.ApplyFunc(utils.RestoreCPULimit, func() error {
				return nil
			}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(&server), "changeNetworkToStreamHandler",
				func(s *Server, deployMessageChan chan *deploy.Message, d *deploy.DurationInfo) bool {
					return true
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		msgChan := make(chan *deploy.Message, 10)
		server.deploy(msgChan, make(chan struct{}), "go")
		msg, _ := <-msgChan
		convey.So(msg.Err, convey.ShouldBeNil)
	})
	convey.Convey("Test_server_deploy RunningModeRestore usePoolPod snapshot ok code nok", t, func() {
		deployer := &fakeDeploy{}
		server := Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					IsStreamEnable: true, Language: "go",
					RunningMode: constants.RunningModeRestore,
					UsePoolPod:  true,
				},
			},
			deployer: deployer,
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(deployer), "DeploySnapshot",
				func(d *fakeDeploy, conf *config.Configuration, ch chan struct{}, language string) (*deploy.DurationInfo, error) {
					return &deploy.DurationInfo{}, nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(deployer), "Deploy",
				func(d *fakeDeploy, conf *config.Configuration, getCodeFinishCh chan struct{},
					language string) (*deploy.DurationInfo, error) {
					return &deploy.DurationInfo{}, errors.New("deploy failed")
				}),
			gomonkey.ApplyFunc(utils.RestoreCPULimit, func() error {
				return nil
			}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(&server), "changeNetworkToStreamHandler",
				func(s *Server, deployMessageChan chan *deploy.Message, d *deploy.DurationInfo) bool {
					return true
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		msgChan := make(chan *deploy.Message, 10)
		server.deploy(msgChan, make(chan struct{}), "go")
		msg, _ := <-msgChan
		convey.So(msg.Err, convey.ShouldBeNil)
	})
	convey.Convey("Test_server_deploy RunningModeRestore usePoolPod snapshot nok code ok", t, func() {
		deployer := &fakeDeploy{}
		server := Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					IsStreamEnable: true, Language: "go",
					RunningMode: constants.RunningModeRestore,
					UsePoolPod:  true,
				},
			},
			deployer: deployer,
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(deployer), "DeploySnapshot",
				func(d *fakeDeploy, conf *config.Configuration, ch chan struct{}, language string) (*deploy.DurationInfo, error) {
					return nil, errors.New("DeploySnapshot failed")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(deployer), "Deploy",
				func(d *fakeDeploy, conf *config.Configuration, getCodeFinishCh chan struct{},
					language string) (*deploy.DurationInfo, error) {
					return &deploy.DurationInfo{}, nil
				}),
			gomonkey.ApplyFunc(utils.RestoreCPULimit, func() error {
				return nil
			}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(&server), "changeNetworkToStreamHandler",
				func(s *Server, deployMessageChan chan *deploy.Message, d *deploy.DurationInfo) bool {
					return true
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		msgChan := make(chan *deploy.Message, 10)
		server.deploy(msgChan, make(chan struct{}), "go")
		msg, _ := <-msgChan
		convey.So(msg.Err, convey.ShouldBeNil)
	})
}

func Test_deploy(t *testing.T) {
	patches := gomonkey.NewPatches()

	{
		// case 1 deploy failed
		defer patches.Reset()

		fakeDeployObjectErr := fakeDeployErr{}
		serverErr := Server{conf: &config.Configuration{}, deployer: &fakeDeployObjectErr}

		deployMesErr := make(chan *deploy.Message, 1)
		go serverErr.deploy(deployMesErr, nil, "")

		select {
		case deployMessage, _ := <-deployMesErr:
			if deployMessage.Err.Error() != errors.New("deploy function error: failed to deploy").Error() {
				t.Errorf("deployMessage Err: %v not equal to deploy Err: '%v'", deployMessage.Err,
					errors.New("deploy function error: failed to deploy"))
			}
		}
		patches.Reset()
	}

	{
		// case 2
		fakeDeployObject1 := fakeDeploy{}
		server1 := Server{conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{IsStreamEnable: true, Language: "go"}}, deployer: &fakeDeployObject1}
		patches := ApplyFunc(utils.RestoreCPULimit, func() error {
			return nil
		})
		patches.ApplyFunc(network.ChangeNetworkToStreamHandler,
			func(streamHandlerHostCidr string, streamHandlerServicePort string) error {
				return errors.New("failed to ChangeNetworkToStreamHandler")
			})
		defer patches.Reset()

		deployMes1 := make(chan *deploy.Message, 1)
		server1.deploy(deployMes1, nil, "")

		select {
		case deployMessage, _ := <-deployMes1:
			if deployMessage.Err.Error() != errors.New("failed to ChangeNetworkToStreamHandler").Error() {
				t.Errorf("deployMessage Err: %v not equal to ChangeNetWork Err: %v", deployMessage.Err,
					errors.New("failed to ChangeNetworkToStreamHandler"))
			}
		}
		defer patches.Reset()
	}
	{
		// case 3
		fakeDeployObject1 := fakeDeploy{}
		server1 := Server{conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{StreamResponse: true, Language: "go"}}, deployer: &fakeDeployObject1}
		patches := ApplyFunc(utils.RestoreCPULimit, func() error {
			return nil
		})
		patches.ApplyFunc(network.ChangeNetworkToStreamHandler,
			func(streamHandlerHostCidr string, streamHandlerServicePort string) error {
				return nil
			})
		patches.ApplyFunc(network.ChangeNetworkToDesPort,
			func(streamHandlerServicePort string) error {
				return errors.New("failed to ChangeNetworkToStreamHandler")
			})
		defer patches.Reset()

		deployMes1 := make(chan *deploy.Message, 1)
		server1.deploy(deployMes1, nil, "")

		select {
		case deployMessage, _ := <-deployMes1:
			if deployMessage.Err.Error() != errors.New("failed to ChangeNetworkToStreamHandler").Error() {
				t.Errorf("deployMessage Err: %v not equal to ChangeNetWork Err: %v", deployMessage.Err,
					errors.New("failed to ChangeNetworkToStreamHandler"))
			}
		}
		defer patches.Reset()
	}

	{
		// case 4
		fakeDeployObject := fakeDeploy{}
		server := Server{conf: &config.Configuration{}, deployer: &fakeDeployObject}
		patches := ApplyFunc(utils.RestoreCPULimit, func() error {
			return nil
		})
		defer patches.Reset()

		deployMes := make(chan *deploy.Message, 1)
		server.deploy(deployMes, nil, "")

		select {
		case deployMessage, _ := <-deployMes:
			if deployMessage.Err != nil {
				t.Errorf("deployMessage Err: %v not equal to nil", deployMessage.Err)
			}
		}
		patches.Reset()
	}
	{
		// case 4 deploy cae
		defer patches.Reset()

		fakeDeployObjectErr := fakeDeployErr{}
		server := Server{conf: &config.Configuration{}, deployer: &fakeDeployObjectErr}

		patches := ApplyFunc(utils.GetBusinessType, func() string {
			return constants.BusinessTypeCAE
		})

		deployMes := make(chan *deploy.Message, 1)
		server.deploy(deployMes, nil, "")

		select {
		case deployMessage, _ := <-deployMes:
			if deployMessage.Err != nil {
				t.Errorf("deployMessage Err: %v not equal to nil", deployMessage.Err)
			}
		}
		patches.Reset()
	}
}

func TestChangeNAT(t *testing.T) {
	net := &network.NetWork{
		NATConfigures:           nil,
		EndpointVpcNATConfigure: nil,
		Replaceable:             false,
		EndpointVpcReplaceable:  false,
		VPCReservedCidr:         "",
		SvcCidr:                 "256.256.256.256",
	}
	Convey("TestChangeNAT without error", t, func() {
		Convey("test change NAT success", func() {
			patch := ApplyFunc(changeVpcNAT, func(n *network.NetWork, param checkNATParam, getCodeFinishCh chan struct{}) error {
				return nil
			})
			defer patch.Reset()
			err := changeNAT(net, 0, nil)
			So(err, ShouldEqual, nil)
		})
	})

	net = &network.NetWork{
		NATConfigures:           []commontypes.NATConfigure{{}},
		EndpointVpcNATConfigure: []commontypes.NATConfigure{{}},
		Replaceable:             false,
		EndpointVpcReplaceable:  false,
		VPCReservedCidr:         "",
	}
	Convey("TestChangeNAT both error", t, func() {
		Convey("test change NAT success", func() {
			patch := ApplyFunc(changeVpcNAT, func(n *network.NetWork, param checkNATParam, getCodeFinishCh chan struct{}) error {
				return errors.New("failed to changeVpcNAT")
			})
			defer patch.Reset()
			err := changeNAT(net, 0, nil)
			if err == nil {
				t.Errorf("changeNAT err is nil")
			}
		})
	})

	net = &network.NetWork{
		NATConfigures:           []commontypes.NATConfigure{{}},
		EndpointVpcNATConfigure: []commontypes.NATConfigure{{}},
		Replaceable:             false,
		EndpointVpcReplaceable:  false,
		VPCReservedCidr:         "",
	}
	Convey("TestChangeNAT both success", t, func() {
		Convey("test change NAT success", func() {
			defer ApplyFunc(changeVpcNAT, func(n *network.NetWork, param checkNATParam, getCodeFinishCh chan struct{}) error {
				return nil
			}).Reset()
			err := changeNAT(net, 0, nil)
			So(err, ShouldEqual, nil)
		})
	})

	net = &network.NetWork{
		NATConfigures:           []commontypes.NATConfigure{{}},
		EndpointVpcNATConfigure: nil,
		Replaceable:             false,
		EndpointVpcReplaceable:  false,
		VPCReservedCidr:         "",
	}
	Convey("TestChangeNAT NATConfigures error", t, func() {
		Convey("test change NAT success", func() {
			patch := ApplyFunc(changeVpcNAT, func(n *network.NetWork, param checkNATParam, getCodeFinishCh chan struct{}) error {
				return errors.New("failed to changeVpcNAT")
			})
			defer patch.Reset()
			err := changeNAT(net, 0, nil)
			if err == nil {
				t.Errorf("changeNAT err is nil")
			}
		})
	})

	net = &network.NetWork{
		NATConfigures:           nil,
		EndpointVpcNATConfigure: []commontypes.NATConfigure{{}},
		Replaceable:             false,
		EndpointVpcReplaceable:  false,
		VPCReservedCidr:         "",
	}
	Convey("TestChangeNAT EndpointVpcNATConfigure error", t, func() {
		Convey("test change NAT success", func() {
			patch := ApplyFunc(changeVpcNAT, func(n *network.NetWork, param checkNATParam, getCodeFinishCh chan struct{}) error {
				return errors.New("failed to changeVpcNAT")
			})
			defer patch.Reset()
			err := changeNAT(net, 0, nil)
			if err == nil {
				t.Errorf("changeNAT err is nil")
			}
		})
	})
}

func TestChangeNetWork(t *testing.T) {
	net := &network.NetWork{
		NATConfigures:           nil,
		EndpointVpcNATConfigure: nil,
		Replaceable:             false,
		EndpointVpcReplaceable:  false,
		VPCReservedCidr:         "",
	}
	Convey("TestChangeNetWork", t, func() {
		Convey("test change network success", func() {
			patch := ApplyFunc(changeNAT, func(n *network.NetWork, configNumber int, getCodeFinishCh chan struct{}) error {
				return nil
			})
			defer patch.Reset()
			err := changeNetWork(context.Background(), net, nil)
			So(err, ShouldEqual, nil)
		})
	})
}

func TestChangeVpcNAT(t *testing.T) {
	param := checkNATParam{
		configure: []types.NATConfigure{
			{
				PatPodName: "mock-pat-pod-1",
			},
			{
				PatPodName: "mock-pat-pod-2",
			},
		},
		configNumber: 0,
		replaceable:  true,
		tunnelName:   "",
		needEndpoint: false,
	}
	Convey("TestChangeVpcNAT 1", t, func() {
		Convey("test change vpc success", func() {
			net := &network.NetWork{
				NATConfigures:           nil,
				EndpointVpcNATConfigure: nil,
				Replaceable:             false,
				EndpointVpcReplaceable:  false,
				VPCReservedCidr:         "",
			}
			patch := ApplyFunc((*network.NetWork).Change, func(n *network.NetWork, configNumber int,
				configures []commontypes.NATConfigure, tunnelName string, needEndpoint bool, getCodeFinishCh chan struct{}) error {
				return nil
			})
			defer patch.Reset()
			err := changeVpcNAT(net, param, nil)
			So(err, ShouldEqual, nil)
			param.replaceable = false
			err = changeVpcNAT(net, param, nil)
			So(err, ShouldEqual, nil)
		})
	})
	Convey("TestChangeVpcNAT 2", t, func() {
		net := &network.NetWork{
			NATConfigures:           nil,
			EndpointVpcNATConfigure: nil,
			Replaceable:             false,
			EndpointVpcReplaceable:  false,
			VPCReservedCidr:         "",
		}
		Convey("test change vpc error", func() {
			patch := ApplyFunc((*network.NetWork).Change, func(n *network.NetWork, configNumber int,
				configures []commontypes.NATConfigure, tunnelName string, needEndpoint bool, getCodeFinishCh chan struct{}) error {
				return errors.New("failed to change")
			})
			defer patch.Reset()
			param.replaceable = true
			err := changeVpcNAT(net, param, nil)
			if err == nil {
				t.Errorf("changeVpcNAT err is nil")
			}
			param.replaceable = false
			err = changeVpcNAT(net, param, nil)
			if err == nil {
				t.Errorf("changeVpcNAT err is nil")
			}
		})
	})
}

func TestServer_cancelAndCreateNewContext(t *testing.T) {
	type fields struct {
		ctx          context.Context
		cancelFunc   context.CancelFunc
		conf         *config.Configuration
		httpServer   *http.Server
		invokeServer *invoke.Server
		deployer     deploy.Deployer
		finalizers   []func() error
	}
	mockCtx, mockCancelFunc := context.WithCancel(context.Background())
	tests := []struct {
		name    string
		fields  fields
		wantNil bool
	}{
		{
			name:    "test with context is nil",
			fields:  fields{},
			wantNil: false,
		},
		{
			name: "test with context is not nil",
			fields: fields{
				ctx:        mockCtx,
				cancelFunc: mockCancelFunc,
			},
			wantNil: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				pingCtx:        tt.fields.ctx,
				pingCancelFunc: tt.fields.cancelFunc,
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			if got := s.cancelAndCreateNewContext(); (got != nil) == tt.wantNil {
				t.Errorf("cancelAndCreateNewContext() = %v is nil, want %v", got, tt.wantNil)
			}
		})
	}
}

func TestServer_updateConfigure_NAT(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		changedNet *network.NetWork
		traceID    string
	}
	net := &network.NetWork{
		NATConfigures: []commontypes.NATConfigure{
			{
				ContainerCidr:  "",
				HostVMCidr:     "192.168.0.0/16",
				PatContainerIP: "10.0.0.116",
				PatVMIP:        "192.168.91.213",
				PatPortIP:      "",
				PatMacAddr:     "",
				PatGateway:     "172.16.0.1",
				PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
				TenantCidr:     "172.16.0.0/24",
				NatSubnetList: map[string][]string{
					"exclude": {"100.64.0.0/10"},
					"include": {"172.16.0.0/24"},
				},
			},
		},
		PublicVpcNATConfigure: []commontypes.NATConfigure{
			{
				ContainerCidr:  "",
				HostVMCidr:     "192.168.0.0/16",
				PatContainerIP: "10.0.0.116",
				PatVMIP:        "192.168.91.213",
				PatPortIP:      "",
				PatMacAddr:     "",
				PatGateway:     "172.16.0.1",
				PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
			},
			{
				ContainerCidr:  "",
				HostVMCidr:     "192.168.0.0/17",
				PatContainerIP: "10.0.0.117",
				PatVMIP:        "192.168.91.214",
				PatPortIP:      "",
				PatMacAddr:     "",
				PatGateway:     "172.16.0.1",
				PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
			},
		},
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        bool
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 normal update scenario",
			fields: fields{
				net:  net,
				conf: &config.Configuration{IsSafety: true},
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					NATConfigures: []commontypes.NATConfigure{
						// ContainerCidr: HostVMCidr:192.168.0.0/16
						// PatContainerIP:10.0.0.116 PatVMIP:192.168.91.213 PatPortIP:
						// PatMacAddr: PatGateway:172.16.0.1 PatPodName:cff-pat-pod-deb2c51a-2f13-4398-bc6
						// TenantCidr:172.16.0.0/24
						// NatSubnetList:map[exclude:[100.64.0.0/10] include:[172.16.0.0/24]]
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    true,
							IsNewCreated: false,
						},
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
					},
				},
			},
			want:    true,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
					ApplyFunc((*Server).resetWorkerDNSRoute, func(_ *Server, patPodIP string) {}),
				})
				return patches
			},
		}, {
			name: "case02 unkown scenario",
			fields: fields{
				net:  net,
				conf: &config.Configuration{},
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					NATConfigures: []commontypes.NATConfigure{
						// ContainerCidr: HostVMCidr:192.168.0.0/16
						// PatContainerIP:10.0.0.116 PatVMIP:192.168.91.213 PatPortIP:
						// PatMacAddr: PatGateway:172.16.0.1 PatPodName:cff-pat-pod-deb2c51a-2f13-4398-bc6
						// TenantCidr:172.16.0.0/24
						// NatSubnetList:map[exclude:[100.64.0.0/10] include:[172.16.0.0/24]]
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    true,
							IsNewCreated: true,
						},
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: false,
						},
					},
				},
			},
			want:    false,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
				})
				return patches
			},
		}, {
			name: "case03 normal replace scenario",
			fields: fields{
				net:  net,
				conf: &config.Configuration{},
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					NATConfigures: []commontypes.NATConfigure{
						// ContainerCidr: HostVMCidr:192.168.0.0/16
						// PatContainerIP:10.0.0.116 PatVMIP:192.168.91.213 PatPortIP:
						// PatMacAddr: PatGateway:172.16.0.1 PatPodName:cff-pat-pod-deb2c51a-2f13-4398-bc6
						// TenantCidr:172.16.0.0/24
						// NatSubnetList:map[exclude:[100.64.0.0/10] include:[172.16.0.0/24]]
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc8",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
					},
				},
			},
			want:    true,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
				})
				return patches
			},
		},
		{
			name: "case04 safety vpc setting",
			fields: fields{
				conf: &config.Configuration{IsSafety: true},
				net:  net,
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					NATConfigures: []commontypes.NATConfigure{
						// ContainerCidr: HostVMCidr:192.168.0.0/16
						// PatContainerIP:10.0.0.116 PatVMIP:192.168.91.213 PatPortIP:
						// PatMacAddr: PatGateway:172.16.0.1 PatPodName:cff-pat-pod-deb2c51a-2f13-4398-bc6
						// TenantCidr:172.16.0.0/24
						// NatSubnetList:map[exclude:[100.64.0.0/10] include:[172.16.0.0/24]]
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc8",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
					},
				},
			},
			want:    true,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
				})
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*Server).resetWorkerDNSRoute,
						func(_ *Server, patPodIP string) {}),
				})
				return patches
			},
		},

		{
			name: "case05 update public network 1",
			fields: fields{
				net:  net,
				conf: &config.Configuration{},
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					PublicVpcNATConfigure: []commontypes.NATConfigure{
						{
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
							IsDeleted:      false,
							IsNewCreated:   true,
						},
						{
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							IsDeleted:      false,
							IsNewCreated:   true,
						},
					},
				},
			},
			want:    true,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
					ApplyFunc((*Server).resetWorkerDNSRoute, func(_ *Server, patPodIP string) {}),
				})
				return patches
			},
		},

		{
			name: "case06 update public network 2",
			fields: fields{
				net:  net,
				conf: &config.Configuration{},
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					PublicVpcNATConfigure: []commontypes.NATConfigure{
						{
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
							IsDeleted:      true,
							IsNewCreated:   false,
						},
						{
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							IsDeleted:      true,
							IsNewCreated:   false,
						},
					},
				},
			},
			want:    true,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
					ApplyFunc((*Server).resetWorkerDNSRoute, func(_ *Server, patPodIP string) {}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			patches := tt.patchesFunc()
			got, err := s.updateNATConfigure(tt.args.changedNet, tt.args.traceID)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateNATConfigure() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("updateNATConfigure() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestServer_updateEndpointNATConfigures_Endpoint(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		changedNet *network.NetWork
		traceID    string
	}
	net := &network.NetWork{
		EndpointVpcNATConfigure: []commontypes.NATConfigure{
			{
				ContainerCidr:  "",
				HostVMCidr:     "192.168.0.0/16",
				PatContainerIP: "10.0.0.116",
				PatVMIP:        "192.168.91.213",
				PatPortIP:      "",
				PatMacAddr:     "",
				PatGateway:     "172.16.0.1",
				PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
				TenantCidr:     "172.16.0.0/24",
				NatSubnetList: map[string][]string{
					"exclude": {"100.64.0.0/10"},
					"include": {"172.16.0.0/24"},
				},
			},
		},
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        bool
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 normal update scenario",
			fields: fields{
				net:  net,
				conf: &config.Configuration{},
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					EndpointVpcNATConfigure: []commontypes.NATConfigure{
						// ContainerCidr: HostVMCidr:192.168.0.0/16
						// PatContainerIP:10.0.0.116 PatVMIP:192.168.91.213 PatPortIP:
						// PatMacAddr: PatGateway:172.16.0.1 PatPodName:cff-pat-pod-deb2c51a-2f13-4398-bc6
						// TenantCidr:172.16.0.0/24
						// NatSubnetList:map[exclude:[100.64.0.0/10] include:[172.16.0.0/24]]
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    true,
							IsNewCreated: false,
						},
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
					},
				},
			},
			want:    true,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
				})
				return patches
			},
		}, {
			name: "case02 normal replace scenario",
			fields: fields{
				net:  net,
				conf: &config.Configuration{},
			},
			args: args{
				traceID: "mock-traceID-normal",
				changedNet: &network.NetWork{
					EndpointVpcNATConfigure: []commontypes.NATConfigure{
						// ContainerCidr: HostVMCidr:192.168.0.0/16
						// PatContainerIP:10.0.0.116 PatVMIP:192.168.91.213 PatPortIP:
						// PatMacAddr: PatGateway:172.16.0.1 PatPodName:cff-pat-pod-deb2c51a-2f13-4398-bc6
						// TenantCidr:172.16.0.0/24
						// NatSubnetList:map[exclude:[100.64.0.0/10] include:[172.16.0.0/24]]
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.116",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc8",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
						{
							ContainerCidr:  "",
							HostVMCidr:     "192.168.0.0/16",
							PatContainerIP: "10.0.0.117",
							PatVMIP:        "192.168.91.213",
							PatPortIP:      "",
							PatMacAddr:     "",
							PatGateway:     "172.16.0.1",
							PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
							TenantCidr:     "172.16.0.0/24",
							NatSubnetList: map[string][]string{
								"exclude": {"100.64.0.0/10"},
								"include": {"172.16.0.0/24"},
							},
							IsDeleted:    false,
							IsNewCreated: true,
						},
					},
				},
			},
			want:    true,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			patches := tt.patchesFunc()
			got, err := s.updateNATConfigure(tt.args.changedNet, tt.args.traceID)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateNATConfigure() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("updateNATConfigure() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_checkNAT(t *testing.T) {
	type args struct {
		ctx         context.Context
		n           *network.NetWork
		param       checkNATParam
		expectedErr bool
	}
	var checkNATErr error
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{name: "case1 param replaceablle true",
			args: args{param: checkNATParam{replaceable: true}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(handleCheckReplaceable, func(ctx context.Context, n *network.NetWork, param checkNATParam) {
						return
					}),
				})
				return patches
			}},
		{name: "case2 param replaceablle false",
			args: args{param: checkNATParam{replaceable: false}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{name: "case3 param replaceablle false, CheckNatHealth error",
			args: args{param: checkNATParam{replaceable: false, configure: []types.NATConfigure{{}}}, expectedErr: true},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*network.NetWork).CheckNATHealth,
						func(_ *network.NetWork, ctx context.Context, vpcContainerIP string,
							vpcGatewayIP string, subMetaDigest string) error {
							checkNATErr = errors.New("failed to CheckNATHealth")
							return errors.New("failed to CheckNATHealth")
						}),
					ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp,
						func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
							return nil
						}),
				})
				return patches
			}},
		{name: "case4 param replaceablle false, CheckNatHealth success",
			args: args{param: checkNATParam{replaceable: false, configure: []types.NATConfigure{{IPV6PatGateway: "mock_gateway"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*network.NetWork).CheckNATHealth,
						func(_ *network.NetWork, ctx context.Context, vpcContainerIP string,
							vpcGatewayIP string, subMetaDigest string) error {
							checkNATErr = nil
							return nil
						}),
					ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp,
						func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
							return nil
						}),
				})
				return patches
			}},
	}
	patchesTemp := gomonkey.NewPatches()
	patchesTemp.ApplyGlobalVar(&runtime.ProcessExitSignal, make(chan struct{}))
	patchesTemp.ApplyGlobalVar(&runtime.ProcessExitError, snerror.New(161908, "test"))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			checkNAT(tt.args.ctx, tt.args.n, tt.args.param)
			patches.ResetAll()
			if tt.args.expectedErr {
				assert.Error(t, checkNATErr)
			} else {
				assert.Nil(t, checkNATErr)
			}
		})
	}
}

func TestServer_loadFunction(t *testing.T) {
	jsonBytes, _ := json.Marshal(types.FunctionSpecRequest{})
	funcReqOfLogPolicy := types.FunctionSpecRequest{
		Message: types.FunctionMessage{
			Function: types.FunctionSpec{
				LogPolicy: types.LogPolicy{
					LogPaths: []string{"path"},
				},
			},
		},
	}
	jsonBytesOfLogPolicy, _ := json.Marshal(funcReqOfLogPolicy)
	type fields struct {
		conf                    *config.Configuration
		httpServer              *http.Server
		invokeServer            *invoke.Server
		net                     *network.NetWork
		pingCtx                 context.Context
		pingCancelFunc          context.CancelFunc
		pingCtxMutex            sync.Mutex
		deployer                deploy.Deployer
		finalizers              []func() error
		customContainerPreError error
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		statusCode  int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 load reserved instance checkAuth failed",
			fields{conf: &config.Configuration{DeploymentConfig: config.DeploymentConfig{}}}, args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							return errors.New("failed to AuthCheckLocally")
						}),
				})
				return patches
			}},
		{"case2 load reserved instance, repeated loading the function", fields{conf: &config.Configuration{}},
			args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							status.Set(status.Initializing)
							return nil
						}),
				})
				return patches
			}},
		{"case3 load instance, deployAndLoad error", fields{conf: &config.Configuration{}}, args{
			w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/load", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				return req
			}(),
		}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						status.Set(status.Waiting)
						return nil
					}),
				ApplyFunc(ioutil.ReadAll,
					func(r io.Reader) ([]byte, error) {
						return nil, errors.New("failed to readall")
					}),
			})
			return patches
		}},
		{"case4 load instance, deployAndLoad error CustomContainerLanguage ",
			fields{conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
				invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}, customContainerPreError: errors.New(failedGetRuntimeContainerID)}, args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytes))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							status.Set(status.Waiting)
							return nil
						}),
					ApplyFunc((*urnutils.BaseURN).ParseFrom,
						func(_ *urnutils.BaseURN, urn string) error {
							return nil
						}),
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return nil, errors.New("failed to changeNetWork")
						}),
					ApplyFunc(setRuntimeMessage,
						func(rtCfg *config.Configuration, msg *types.FunctionSpecRequest) {
							return
						}),
				})
				return patches
			}},
		{"case5 load instance, LoadAndServe error CustomContainerLanguage ",
			fields{conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
				invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}}, args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytes))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							status.Set(status.Waiting)
							return nil
						}),
					ApplyFunc((*urnutils.BaseURN).ParseFrom,
						func(_ *urnutils.BaseURN, urn string) error {
							return nil
						}),
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
					ApplyFunc(setRuntimeMessage,
						func(rtCfg *config.Configuration, msg *types.FunctionSpecRequest) {
							return
						}),
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					ApplyFunc((*Server).mountVolume,
						func(_ *Server, cli engine.ContainerClient) error {
							return nil
						}),
					ApplyFunc((*invoke.Server).LoadAndServe,
						func(_ *invoke.Server, ctx context.Context, conf *config.Configuration,
							deployMessageChan <-chan *deploy.Message, netWorkCh chan error) (*deploy.DurationInfo, error) {
							return nil, errors.New("failed to LoadAndServe")
						}),
					ApplyFunc(network.ChangeNetworkToDesPort,
						func(port string) error {
							return nil
						}),
				})
				return patches
			}},
		{"case6 load instance, recordLoaded error",
			fields{conf: &config.Configuration{}, invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}}, args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytes))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							status.Set(status.Waiting)
							return nil
						}),
					ApplyFunc((*urnutils.BaseURN).ParseFrom,
						func(_ *urnutils.BaseURN, urn string) error {
							return nil
						}),
					ApplyFunc((*invoke.Server).LoadAndServe,
						func(_ *invoke.Server, ctx context.Context, conf *config.Configuration,
							deployMessageChan <-chan *deploy.Message, netWorkCh chan error) (*deploy.DurationInfo, error) {
							return &deploy.DurationInfo{DeployFuncTime: 1 * time.Millisecond,
								DeployLayerTime: 1 * time.Millisecond}, nil
						}),
					ApplyFunc(os.Create,
						func(name string) (*os.File, error) {
							return nil, errors.New("failed to create file")
						}),
				})
				return patches
			}},
		{"case7 load cae instance",
			fields{conf: &config.Configuration{DeploymentConfig: config.DeploymentConfig{}}}, args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader([]byte{}))
					req.Header.Set(constants.FunctionLifecycleType, constants.PersistenceFunction)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"case8 load instance, deployAndLoad error, write log config file success",
			fields{conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
				invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}, customContainerPreError: errors.New(failedGetRuntimeContainerID)}, args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytesOfLogPolicy))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							status.Set(status.Waiting)
							return nil
						}),
					ApplyFunc((*urnutils.BaseURN).ParseFrom,
						func(_ *urnutils.BaseURN, urn string) error {
							return nil
						}),
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return nil, errors.New("failed to changeNetWork")
						}),
					ApplyFunc((*Server).writeLogConfigFile, func() error {
						return nil
					}),
				})
				return patches
			}},
		{"case9 load instance, deployAndLoad error, write log config file error",
			fields{conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
				invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}, customContainerPreError: errors.New(failedGetRuntimeContainerID)}, args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytesOfLogPolicy))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}(),
			}, http.StatusInternalServerError, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							status.Set(status.Waiting)
							return nil
						}),
					ApplyFunc((*urnutils.BaseURN).ParseFrom,
						func(_ *urnutils.BaseURN, urn string) error {
							return nil
						}),
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return nil, errors.New("failed to changeNetWork")
						}),
					ApplyFunc((*Server).writeLogConfigFile, func() error {
						return errors.New("error")
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			patches := tt.patchesFunc()
			s.loadFunction(tt.args.w, tt.args.r)

			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			time.Sleep(5 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestServer_loadFunctionWithRepeatedLoad(t *testing.T) {
	s := &Server{
		conf:         &config.Configuration{DeploymentConfig: config.DeploymentConfig{}},
		invokeServer: &invoke.Server{},
		deployer:     &fakeDeployErr{},
	}
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		ApplyFunc(localauth.AuthCheckLocally,
			func(ak string, sk string, requestSign string, timestamp string, duration int) error {
				status.Set(status.Waiting)
				return nil
			}),
		ApplyFunc((*urnutils.BaseURN).ParseFrom,
			func(_ *urnutils.BaseURN, urn string) error {
				return nil
			}),
		ApplyFunc(changeNetWork,
			func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
				return nil
			}),
		ApplyFunc(setRuntimeMessage,
			func(rtCfg *config.Configuration, msg *types.FunctionSpecRequest) {
				return
			}),
		ApplyFunc(engine.NewContainerClient,
			func() (engine.ContainerClient, error) {
				return &engine.DockerClient{}, nil
			}),
		ApplyFunc((*Server).mountVolume,
			func(_ *Server, cli engine.ContainerClient) error {
				return nil
			}),
		ApplyFunc((*invoke.Server).LoadAndServe,
			func(_ *invoke.Server, ctx context.Context, conf *config.Configuration,
				deployMessageChan <-chan *deploy.Message, netWorkCh chan error) (*deploy.DurationInfo, error) {
				return &deploy.DurationInfo{}, nil
			}),

		ApplyFunc((*runtime.Metrics).IdleCheckSuicide,
			func(_ *runtime.Metrics) {
				return
			}),
		ApplyFunc((*Server).dumpFileNotify,
			func(_ *Server) {
				return
			}),
	})
	defer patches.ResetAll()
	specReq := types.FunctionSpecRequest{}
	data, err := json.Marshal(specReq)
	if err != nil {
		t.Errorf("failed to marshal, err: %v", err.Error())
	}
	w := httptest.NewRecorder()
	r := func() *http.Request {
		req := httptest.NewRequest("POST", "/load", bytes.NewReader(data))
		return req
	}()
	status.Set(status.Waiting)
	s.loadFunction(w, r)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("resp %v", resp)
	}
	tmpResp := httptest.NewRecorder()
	tmpReq := func() *http.Request {
		req := httptest.NewRequest("POST", "/load", bytes.NewReader(data))
		return req
	}()
	s.loadFunction(tmpResp, tmpReq)
	repeatedResp := tmpResp.Result()
	if repeatedResp.StatusCode != http.StatusInternalServerError {
		t.Errorf("StatusCode is err, StatusCode %d", repeatedResp.StatusCode)
	}
	defer repeatedResp.Body.Close()
	resp = tmpResp.Result()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("resp %v", resp)
	}
	badResp := snerror.BadResponse{}
	if err = json.Unmarshal(respBody, &badResp); err != nil {
		t.Errorf("failed to unmarshal, err:%s", err.Error())
	}
	if badResp.Message != "repeated loading" {
		t.Errorf("repeated load case is falied:wq")
	}
}

func TestNewServer(t *testing.T) {
	type args struct {
		conf *config.Configuration
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to new server",
			args{conf: &config.Configuration{
				ServerConfig: config.ServerConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{HTTPSEnable: true},
				}}}, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(os.Getenv,
						func(key string) string {
							return "192.168.0.1"
						}),
					ApplyFunc(tls.InitTLSConfig,
						func(tlsProtocols string, tlsCiphers string) (err error) {
							return nil
						}),
					ApplyFunc(invoke.InitServer,
						func(conf *config.Configuration, serverAddr string) (*invoke.Server, error) {
							return &invoke.Server{}, nil
						}),
					ApplyFunc(deploy.NewDeployer,
						func(c *config.Configuration) (deploy.Deployer, error) {
							return &fakeDeploy{}, nil
						}),
				})
				return patches
			}},
		{"case2 ParseIP error",
			args{conf: &config.Configuration{
				ServerConfig: config.ServerConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{HTTPSEnable: true},
				}}}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
					ApplyFunc(tls.InitTLSConfig,
						func(tlsProtocols string, tlsCiphers string) (err error) {
							return errors.New("failed to InitTLSConfig")
						}),
				})
				return patches
			}},
		{"case3 InitTLSConfig error",
			args{conf: &config.Configuration{
				ServerConfig: config.ServerConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{HTTPSEnable: true},
				}}}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
					ApplyFunc(tls.InitTLSConfig, func(tlsProtocols string, tlsCiphers string) error {
						return errors.New("failed to InitTLSConfig")
					}),
				})
				return patches
			}},
		{"case4 InitServer error",
			args{conf: &config.Configuration{
				ServerConfig: config.ServerConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{HTTPSEnable: true},
				}}}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(os.Getenv,
						func(key string) string {
							return "192.168.0.1"
						}),
					ApplyFunc(tls.InitTLSConfig,
						func(tlsProtocols string, tlsCiphers string) (err error) {
							return nil
						}),
					ApplyFunc(invoke.InitServer,
						func(conf *config.Configuration, serverAddr string) (*invoke.Server, error) {
							return nil, errors.New("failed to InitServer")
						}),
				})
				return patches
			}},
		{"case5 NewDeployer error",
			args{conf: &config.Configuration{
				ServerConfig: config.ServerConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{HTTPSEnable: true},
				}}}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(os.Getenv,
						func(key string) string {
							return "192.168.0.1"
						}),
					ApplyFunc(tls.InitTLSConfig,
						func(tlsProtocols string, tlsCiphers string) (err error) {
							return nil
						}),
					ApplyFunc(invoke.InitServer,
						func(conf *config.Configuration, serverAddr string) (*invoke.Server, error) {
							return &invoke.Server{}, nil
						}),
					ApplyFunc(deploy.NewDeployer,
						func(c *config.Configuration) (deploy.Deployer, error) {
							return nil, errors.New("failed to NewDeployer")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := NewServer(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestWriteRestoreDeadResponse(t *testing.T) {
	Convey("TestWriteRestoreDeadResponse, err is nil", t, func() {
		var s int
		writer := mockUtils.MockResponseWriter{}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(writer), "WriteHeader", func(_ http.ResponseWriter, statusCode int) {
				s = statusCode
			}),
		})
		defer patches.ResetAll()

		writeRestoreDeadResponse(writer, nil)
		convey.ShouldEqual(s, http.StatusOK)
	})
	Convey("TestWriteRestoreDeadResponse, err is not nil", t, func() {
		var s int
		writer := mockUtils.MockResponseWriter{}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(writer), "WriteHeader", func(_ http.ResponseWriter, statusCode int) {
				s = statusCode
			}),
		})
		defer patches.ResetAll()

		writeRestoreDeadResponse(writer, errors.New("error"))
		convey.ShouldEqual(s, http.StatusInternalServerError)
	})
}

func TestServer_recordLoaded(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	tests := []struct {
		name        string
		fields      fields
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to recordLoaded",
			fields{conf: &config.Configuration{DeploymentConfig: config.DeploymentConfig{}}},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(os.Create,
						func(name string) (*os.File, error) {
							return &os.File{}, nil
						}),
					ApplyFunc((*os.File).Close,
						func(_ *os.File) error {
							return nil
						}),
				})
				return patches
			}},
		{"case2 file close failed",
			fields{conf: &config.Configuration{DeploymentConfig: config.DeploymentConfig{}}},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(os.Create,
						func(name string) (*os.File, error) {
							return &os.File{}, nil
						}),
					ApplyFunc((*os.File).Close,
						func(_ *os.File) error {
							return errors.New("failed to close file")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			if err := s.recordLoaded(); (err != nil) != tt.wantErr {
				t.Errorf("recordLoaded() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_setExtraEnv(t *testing.T) {
	type args struct {
		rtCfg *config.Configuration
		msg   *types.FunctionSpecRequest
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 setExtraEnv to end", args{
			rtCfg: &config.Configuration{},
			msg: &types.FunctionSpecRequest{
				Message: types.FunctionMessage{
					Function: types.FunctionSpec{
						EncryptedUserData: "abc",
						Environment:       "abc",
						Language:          "go"}}},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.Decrypt,
					func(src string) ([]byte, error) {
						res, _ := json.Marshal(map[string]string{"a": "b"})
						return res, nil
					}),
			})
			return patches
		},
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			setExtraEnv(tt.args.rtCfg, tt.args.msg)
			if tt.args.rtCfg.RuntimeConfig.ExtraUserData == nil {
				t.Errorf("setExtraEnv failed")
			}
			patches.ResetAll()
		})
	}
}

func TestServer_readRequest(t *testing.T) {
	jsonBytes, _ := json.Marshal("abc")
	jsonBytes1, _ := json.Marshal(types.FunctionSpecRequest{})
	caeTestReq := types.FunctionSpecRequest{
		Message: types.FunctionMessage{
			Function: types.FunctionSpec{
				BusinessType: constants.BusinessTypeCAE,
				CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{
					Endpoint: "testEndPoint",
				},
				Properties: map[string]string{"testKey": "testValue"},
			},
		},
	}
	jsonBytes4CAEObsClient, _ := json.Marshal(caeTestReq)
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        *network.NetWork
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 json Unmarshal error", fields{},
			args{httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytes))},
			nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case2 ValidateStruct error", fields{},
			args{httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytes1))},
			nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
		{"case3 cae test", fields{},
			args{httptest.NewRequest("POST", "/load", bytes.NewReader(jsonBytes4CAEObsClient))},
			nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
					ApplyFunc(deploy.NewDumpDeployer, func(c *config.Configuration, customS3Config commontypes.CustomUploadConfig) (deploy.Deployer, error) {
						return nil, errors.New("error")
					}),
					ApplyFunc((*Server).saveCAEFuncProperty, func(_ *Server, property map[string]string) {}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			got, err := s.readRequest(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("readRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readRequest() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_setConfigS3AkSk(t *testing.T) {
	Convey("test setConfigS3AkSk", t, func() {
		akSkTestReq := types.FunctionSpecRequest{
			Message: types.FunctionMessage{
				Deployment: types.FunctionDeploymentSpec{
					BucketID:      "BucketID",
					ObjectID:      "ObjectID",
					Access:        "Access",
					Secret:        "Secret",
					SecurityToken: "SecurityToken",
				},
			},
		}
		akSkTestReqByte, _ := json.Marshal(akSkTestReq)
		r := httptest.NewRequest("POST", "/load", bytes.NewReader(akSkTestReqByte))
		s := &Server{conf: &config.Configuration{}}
		err := s.setConfigS3AkSk(r)
		convey.So(err, ShouldBeNil)
		convey.So(s.conf.Access, convey.ShouldEqual, "Access")
		convey.So(s.conf.Secret, convey.ShouldEqual, "Secret")
		convey.So(s.conf.SecurityToken, convey.ShouldEqual, "SecurityToken")
	})
}

func Test_caeDumpFileUploadConfig(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		ApplyFunc(deploy.NewDumpDeployer, func(_ *config.Configuration, customS3Config commontypes.CustomUploadConfig) (deploy.Deployer, error) {
			return nil, errors.New("error")
		}),
	})
	s := &Server{conf: &config.Configuration{}}
	caeTestReq := types.FunctionSpecRequest{
		Message: types.FunctionMessage{
			Function: types.FunctionSpec{
				BusinessType: constants.BusinessTypeCAE,
				CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{
					Endpoint: "testEndPoint",
					FilePath: "",
				},
				Properties: map[string]string{"testKey": "testValue"},
			},
		},
	}
	s.caeDumpFileUploadConfig(caeTestReq)
	patches.ResetAll()
	if s.conf.CAECustomS3ConfigInfo.FilePath != "" {
		t.Errorf("s.conf.CAECustomS3ConfigInfo.FilePath error, actul: %s", s.conf.CAECustomS3ConfigInfo.FilePath)
	}

	// case2 build file path with all config
	caeTestReq = types.FunctionSpecRequest{
		Message: types.FunctionMessage{
			Function: types.FunctionSpec{
				BusinessType: constants.BusinessTypeCAE,
				CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{
					Endpoint: "testEndPoint",
					FilePath: "",
				},
				Properties: map[string]string{
					"application_id":   "testappid",
					"application_name": "testapp",
					"component_id":     "testcomponent_id",
					"component_name":   "testcomponent_name",
					"environment_id":   "testenvironment_id",
					"environment_name": "testenvironment_name"},
			},
		},
	}
	patches.Append(mockUtils.PatchSlice{
		ApplyFunc(deploy.NewDumpDeployer, func(_ *config.Configuration, customS3Config commontypes.CustomUploadConfig) (deploy.Deployer, error) {
			return nil, nil
		}),
		ApplyFunc(os.Getenv, func(key string) string {
			if key == "HOSTNAME" {
				return "host_name"
			}
			return ""
		}),
	})
	s.caeDumpFileUploadConfig(caeTestReq)
	patches.ResetAll()
	if s.conf.CAECustomS3ConfigInfo.FilePath != "testenvironment_name/testapp/testcomponent_name/host_name" {
		t.Errorf("s.conf.CAECustomS3ConfigInfo.FilePath error, actul: %s", s.conf.CAECustomS3ConfigInfo.FilePath)
	}

	// case3 build file path with some config
	caeTestReq = types.FunctionSpecRequest{
		Message: types.FunctionMessage{
			Function: types.FunctionSpec{
				BusinessType: constants.BusinessTypeCAE,
				CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{
					Endpoint: "testEndPoint",
					FilePath: "",
				},
				Properties: map[string]string{
					"application_id":   "testappid",
					"application_name": "testapp",
					"component_id":     "testcomponent_id",
					"environment_id":   "testenvironment_id",
					"environment_name": "testenvironment_name"},
			},
		},
	}
	patches.Append(mockUtils.PatchSlice{
		ApplyFunc(deploy.NewDumpDeployer, func(_ *config.Configuration, customS3Config commontypes.CustomUploadConfig) (deploy.Deployer, error) {
			return nil, nil
		}),
		ApplyFunc(os.Getenv, func(key string) string {
			if key == "HOSTNAME" {
				return "host_name"
			}
			return ""
		}),
	})
	s.caeDumpFileUploadConfig(caeTestReq)
	patches.ResetAll()
	if s.conf.CAECustomS3ConfigInfo.FilePath != "testenvironment_name/testapp/host_name" {
		t.Errorf("s.conf.CAECustomS3ConfigInfo.FilePath error, actul: %s", s.conf.CAECustomS3ConfigInfo.FilePath)
	}

	// case4 build file path with only hostname
	caeTestReq = types.FunctionSpecRequest{
		Message: types.FunctionMessage{
			Function: types.FunctionSpec{
				BusinessType: constants.BusinessTypeCAE,
				CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{
					Endpoint: "testEndPoint",
					FilePath: "",
				},
				Properties: map[string]string{},
			},
		},
	}
	patches.Append(mockUtils.PatchSlice{
		ApplyFunc(deploy.NewDumpDeployer, func(_ *config.Configuration, customS3Config commontypes.CustomUploadConfig) (deploy.Deployer, error) {
			return nil, nil
		}),
		ApplyFunc(os.Getenv, func(key string) string {
			if key == "HOSTNAME" {
				return "host_name"
			}
			return ""
		}),
	})
	s.caeDumpFileUploadConfig(caeTestReq)
	patches.ResetAll()
	if s.conf.CAECustomS3ConfigInfo.FilePath != "host_name" {
		t.Errorf("s.conf.CAECustomS3ConfigInfo.FilePath error, actul: %s", s.conf.CAECustomS3ConfigInfo.FilePath)
	}

}

func TestDumpFileUploadConfig(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		ApplyFunc(deploy.NewDumpDeployer, func(_ *config.Configuration, customS3Config commontypes.CustomUploadConfig) (deploy.Deployer, error) {
			return nil, nil
		}),
	})
	s := &Server{conf: &config.Configuration{}}
	caeTestReq := types.FunctionSpecRequest{
		Message: types.FunctionMessage{
			Function: types.FunctionSpec{
				BusinessType: constants.BusinessTypeCAE,
				CustomUploadConfig: types.CustomUploadConfig{CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{
					Endpoint: "testEndPoint",
					FilePath: "/test",
				}},
			},
		},
	}
	s.DumpFileUploadConfig(caeTestReq)
	patches.ResetAll()
	if s.conf.CustomUploadConfig.FilePath != "" {
		assert.Equal(t, "/test/"+"commandTask/"+"-", s.conf.CustomUploadConfig.FilePath)
	}
}

func TestServer_prepareCustomContainer(t *testing.T) {
	type fields struct {
		conf                       *config.Configuration
		httpServer                 *http.Server
		invokeServer               *invoke.Server
		net                        *network.NetWork
		pingCtx                    context.Context
		pingCancelFunc             context.CancelFunc
		pingCtxMutex               sync.Mutex
		deployer                   deploy.Deployer
		finalizers                 []func() error
		runtimeContainerID         string
		skipWaitRuntimeContainerID bool
	}
	type args struct {
		net *network.NetWork
	}
	conf := &config.Configuration{}
	conf.FuncMountConfig = types.FuncMountConfig{FuncMounts: []types.FuncMount{{MountType: "mock_MountType"}}}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 NewClientWithOpts error", fields{runtimeContainerID: "aaa", conf: conf}, args{net: &network.NetWork{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return nil, errors.New("failed to NewClientWithOpts")
						}),
					ApplyMethod(reflect.TypeOf(&Server{}), "GetCustomContainerInfo",
						func(_ *Server) error {
							return nil
						}),
				})
				return patches
			}},
		{"case2 mountVolume error", fields{runtimeContainerID: "aaa", conf: conf}, args{net: &network.NetWork{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					ApplyFunc((*Server).mountVolume,
						func(_ *Server, cli engine.ContainerClient) error {
							return errors.New("failed to mountVolume")
						}),
					ApplyMethod(reflect.TypeOf(&Server{}), "GetCustomContainerInfo",
						func(_ *Server) error {
							return nil
						}),
				})
				return patches
			}},
		{"case3 wait RuntimeContainerID error", fields{skipWaitRuntimeContainerID: true, conf: conf}, args{net: &network.NetWork{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return nil
						}),
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					ApplyMethod(reflect.TypeOf(&Server{}), "GetCustomContainerInfo",
						func(_ *Server) error {
							return nil
						}),
				})
				return patches
			}},
		{"case4 change net work fail", fields{conf: conf}, args{net: &network.NetWork{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(changeNetWork,
						func(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
							return errors.New("failed")
						}),
					ApplyMethod(reflect.TypeOf(&Server{}), "GetCustomContainerInfo",
						func(_ *Server) error {
							return nil
						}),
				})
				return patches
			}},
		{"case5 getCustomContainerInfo error", fields{conf: conf}, args{net: &network.NetWork{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&Server{}), "GetCustomContainerInfo",
						func(_ *Server) error {
							return errors.New("failed")
						}),
				})
				return patches
			}},
		{"case6 FuncMounts equal 0", fields{conf: &config.Configuration{}}, args{net: &network.NetWork{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&Server{}), "GetCustomContainerInfo",
						func(_ *Server) error {
							return errors.New("failed")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:                       tt.fields.conf,
				httpServer:                 tt.fields.httpServer,
				invokeServer:               tt.fields.invokeServer,
				net:                        tt.fields.net,
				pingCtx:                    tt.fields.pingCtx,
				pingCancelFunc:             tt.fields.pingCancelFunc,
				pingCtxMutex:               tt.fields.pingCtxMutex,
				deployer:                   tt.fields.deployer,
				finalizers:                 tt.fields.finalizers,
				runtimeContainerID:         tt.fields.runtimeContainerID,
				skipWaitRuntimeContainerID: tt.fields.skipWaitRuntimeContainerID,
			}
			s.prepareCustomContainer()
			patches.ResetAll()
		})
	}
}

func Test_defineExtraHeader(t *testing.T) {
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		want        map[string]string
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to defineExtraHeader ", args{func() *http.Request {
			req := httptest.NewRequest("POST", "/load", bytes.NewReader([]byte{}))
			req.Header.Set(constants.HeaderAccessKey, "test")
			req.Header.Set(constants.HeaderSecretKey, "test")
			req.Header.Set(constants.HeaderAuthToken, "test")
			req.Header.Set(constants.HeaderSecurityAccessKey, "testAK")
			req.Header.Set(constants.HeaderSecuritySecretKey, "testSK")
			req.Header.Set(constants.HeaderSecurityToken, "testSecurityToken")
			req.Header.Set(constants.HeaderIAMToken, "testIAMToken")
			return req
		}()},
			map[string]string{
				constants.HeaderAccessKey:         "test",
				constants.HeaderSecretKey:         "test",
				constants.HeaderAuthToken:         "test",
				constants.HeaderSecurityAccessKey: "testAK",
				constants.HeaderSecuritySecretKey: "testSK",
				constants.HeaderSecurityToken:     "testSecurityToken",
				constants.HeaderIAMToken:          "testIAMToken",
			}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.Decrypt,
						func(src string) ([]byte, error) {
							return []byte(src), nil
						}),
				})
				return patches
			}},
		{"case2 defineExtraHeader Decrypt error", args{func() *http.Request {
			req := httptest.NewRequest("POST", "/load", bytes.NewReader([]byte{}))
			req.Header.Set(constants.HeaderAccessKey, "test")
			req.Header.Set(constants.HeaderSecretKey, "test")
			req.Header.Set(constants.HeaderAuthToken, "test")
			req.Header.Set(constants.HeaderSecurityAccessKey, "testAK")
			req.Header.Set(constants.HeaderSecuritySecretKey, "testSK")
			req.Header.Set(constants.HeaderSecurityToken, "testSecurityToken")
			req.Header.Set(constants.HeaderIAMToken, "testIAMToken")
			return req
		}()},
			map[string]string{
				constants.HeaderAccessKey:         "",
				constants.HeaderSecretKey:         "",
				constants.HeaderAuthToken:         "",
				constants.HeaderSecurityAccessKey: "",
				constants.HeaderSecuritySecretKey: "",
				constants.HeaderSecurityToken:     "",
				constants.HeaderIAMToken:          "",
			}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.Decrypt,
						func(src string) ([]byte, error) {
							return nil, errors.New("failed to Decrypt")
						}),
				})
				return patches
			}},
		{"case3 defineExtraHeader without old iam ak sk", args{func() *http.Request {
			req := httptest.NewRequest("POST", "/load", bytes.NewReader([]byte{}))
			req.Header.Set(constants.HeaderSecurityAccessKey, "testAK")
			req.Header.Set(constants.HeaderSecuritySecretKey, "testSK")
			req.Header.Set(constants.HeaderSecurityToken, "testSecurityToken")
			return req
		}()},
			map[string]string{
				constants.HeaderAccessKey:         "",
				constants.HeaderSecretKey:         "",
				constants.HeaderAuthToken:         "",
				constants.HeaderSecurityAccessKey: "testAK",
				constants.HeaderSecuritySecretKey: "testSK",
				constants.HeaderSecurityToken:     "testSecurityToken",
				constants.HeaderIAMToken:          "",
			}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(localauth.Decrypt,
						func(src string) ([]byte, error) {
							return []byte(src), nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := defineExtraHeader(tt.args.r); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("defineExtraHeader() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_decryptHeaderToExtra(t *testing.T) {
	Convey("decryptHeaderToExtra", t, func() {
		var extra map[string]string
		decryptHeaderToExtra(nil, extra, "test")
		So(extra, ShouldEqual, nil)
	})
}

func Test_mountVolume(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		cli  engine.ContainerClient
		conf *config.Configuration
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 FuncMounts 0", fields{}, args{cli: &engine.DockerClient{}, conf: &config.Configuration{}}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
		{"case2 client info error", fields{}, args{cli: &engine.DockerClient{}, conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				FuncMountConfig: types.FuncMountConfig{FuncMounts: []types.FuncMount{{}}},
			}}},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(engine.ContainerClient.Info,
						func(_ engine.ContainerClient, ctx context.Context, containerID string) (engine.ContainerInfo, error) {
							return engine.ContainerInfo{}, errors.New("failed to get container info")
						}),
				})
				return patches
			}},
		{"case3 VolumesForFunction error", fields{}, args{cli: &engine.DockerClient{}, conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				FuncMountConfig: types.FuncMountConfig{FuncMounts: []types.FuncMount{{}}},
			}}},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(engine.ContainerClient.Info,
						func(_ engine.ContainerClient, ctx context.Context, containerID string) (engine.ContainerInfo, error) {
							return engine.ContainerInfo{}, nil
						}),
					ApplyFunc(mount.VolumesForFunction,
						func(mountConfig types.FuncMountConfig) error {
							return errors.New("failed to VolumesForFunction")
						}),
				})
				return patches
			}},
		{"case4 succeed to mountVolume", fields{}, args{cli: &engine.DockerClient{}, conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				RuntimeContainerID: "1",
				FuncMountConfig:    types.FuncMountConfig{FuncMounts: []types.FuncMount{{}}},
			}}},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&engine.DockerClient{}), "Info",
						func(_ *engine.DockerClient, ctx context.Context, containerID string) (engine.ContainerInfo, error) {
							return engine.ContainerInfo{}, nil
						}),
					ApplyFunc(mount.VolumesForFunction,
						func(mountConfig types.FuncMountConfig) error {
							return nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:       tt.args.conf,
				finalizers: tt.fields.finalizers,
			}
			if err := s.mountVolume(tt.args.cli); (err != nil) != tt.wantErr {
				t.Errorf("mountVolume() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_handleCheckReplaceable(t *testing.T) {
	type args struct {
		ctx   context.Context
		n     *network.NetWork
		param checkNATParam
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 configNumber more than configure length", args{param: checkNATParam{configNumber: 1}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"case2 CheckNATHealth return nil", args{param: checkNATParam{configure: []commontypes.NATConfigure{{}}}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*network.NetWork).CheckNATHealth,
						func(_ *network.NetWork, ctx context.Context, vpcContainerIP string,
							vpcGatewayIP string, subMetaDigest string) error {
							return nil
						}),
				})
				return patches
			}},
		{"case3 Change error", args{param: checkNATParam{configure: []commontypes.NATConfigure{{IPV6PatGateway: "mock"}, {IPV6PatGateway: "mock"}}}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*network.NetWork).CheckNATHealth,
						func(_ *network.NetWork, ctx context.Context, vpcContainerIP string,
							vpcGatewayIP string, subMetaDigest string) error {
							return errors.New("failed to CheckNATHealth")
						}),
					ApplyFunc((*network.NetWork).Change,
						func(_ *network.NetWork, configNumber int, configures []commontypes.NATConfigure,
							tunnelName string, needEndpoint bool, getCodeFinishCh chan struct{}) error {
							return errors.New("failed to Change")
						}),
					ApplyFunc(time.Sleep,
						func(d time.Duration) {
							return
						}),
					ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp,
						func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
							return nil
						}),
				})
				return patches
			}},
	}
	patchesTemp := gomonkey.NewPatches()
	patchesTemp.ApplyGlobalVar(&runtime.ProcessExitSignal, make(chan struct{}))
	patchesTemp.ApplyGlobalVar(&runtime.ProcessExitError, snerror.New(161908, "test"))
	patchesTemp.ApplyFunc(fusion.SendWorkerErr, func(runtimeErr runtime.Error) { return })
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handleCheckReplaceable(tt.args.ctx, tt.args.n, tt.args.param)
			patches.ResetAll()
		})
	}
}

func TestServer_reloadFunction(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 reloadFunction all error", fields{conf: &config.Configuration{},
			invokeServer: &invoke.Server{}, deployer: &fakeDeployErr{}}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc((*invoke.Server).LoadAndServe,
					func(_ *invoke.Server, ctx context.Context, conf *config.Configuration,
						deployMessageChan <-chan *deploy.Message, netWorkCh chan error) (*deploy.DurationInfo, error) {
						return nil, errors.New("failed to LoadAndServe")
					}),
				ApplyFunc(os.Exit,
					func(code int) {
						return
					}),
				ApplyFunc((*Server).Start,
					func(_ *Server) error {
						return errors.New("failed to Start")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status.Set(status.Waiting)
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			patches := tt.patchesFunc()
			s.reloadFunction()
			patches.ResetAll()
		})
	}
}

func TestServer_changePatPodIP(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		statusCode  int
	}{
		{"case1 checkAuth error", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return errors.New("failed to AuthCheckLocally")
					}),
			})
			return patches
		}, http.StatusInternalServerError},
		{"case2 parseNetWork error", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return nil
					}),
				ApplyFunc(parseNetWork,
					func(r *http.Request) (*network.NetWork, error) {
						return &network.NetWork{}, errors.New("failed to parseNetWork")
					}),
			})
			return patches
		}, http.StatusInternalServerError},
		{"case3 succeed to changePatPodIP", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return nil
					}),
				ApplyFunc(parseNetWork,
					func(r *http.Request) (*network.NetWork, error) {
						return &network.NetWork{}, nil
					}),
			})
			return patches
		}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			s.changePatPodIP(tt.args.w, tt.args.r)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func Test_parseNetWork(t *testing.T) {
	jsonBytes, _ := json.Marshal("abc")
	jsonBytes1, _ := json.Marshal(types.FunctionSpecRequest{})
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 json Unmarshal error",
			args{httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader([]byte{}))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return nil, errors.New("failed to ReadAll")
						}),
				})
				return patches
			}},
		{"case2 Unmarshal error",
			args{httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader(jsonBytes))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
		{"case3 succeed to parseNetWork",
			args{httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader(jsonBytes1))},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := parseNetWork(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseNetWork() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_persistConfig(t *testing.T) {
	type args struct {
		cfg *config.Configuration
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 yaml Marshal failed", args{&config.Configuration{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(yaml.Marshal,
						func(in interface{}) (out []byte, err error) {
							return nil, errors.New("failed to Marshal")
						}),
				})
				return patches
			}},
		{"case2 WriteFile failed", args{&config.Configuration{}}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(yaml.Marshal,
						func(in interface{}) (out []byte, err error) {
							return []byte{}, nil
						}),
					ApplyFunc(ioutil.WriteFile,
						func(filename string, data []byte, perm fs.FileMode) error {
							return errors.New("failed to WriteFile")
						}),
				})
				return patches
			}},
		{"case2 succeed to persistConfig", args{&config.Configuration{}}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(yaml.Marshal,
						func(in interface{}) (out []byte, err error) {
							return []byte{}, nil
						}),
					ApplyFunc(ioutil.WriteFile,
						func(filename string, data []byte, perm fs.FileMode) error {
							return nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := persistConfig(tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("persistConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestServer_GracefulShutdown(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1", fields{httpServer: &http.Server{}, invokeServer: &invoke.Server{},
			finalizers: []func() error{}},
			func() mockUtils.PatchSlice {
				invokeServerShutdown := false
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(context.WithTimeout,
						func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
							return context.WithDeadline(parent, time.Now().Add(1*time.Millisecond))
						}),
					ApplyFunc((*http.Server).Shutdown,
						func(_ *http.Server, ctx context.Context) error {
							assert.True(t, invokeServerShutdown)
							return errors.New("failed to Shutdown")
						}),
					ApplyFunc((*invoke.Server).GracefulShutdown,
						func(_ *invoke.Server) error {
							invokeServerShutdown = true
							return errors.New("failed to Shutdown")
						}),
					ApplyFunc(updateLogConfigFile, func(_ bool) {}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
				preStopCh:      make(chan struct{}),
			}
			s.GracefulShutdown()
			patches.ResetAll()
		})
	}
}

func TestServer_notifyFunction(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		statusCode  int
	}{
		{"case1 succeed to notifyFunction", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/change_pat_pod_ip", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return nil
					}),
			})
			return patches
		}, http.StatusOK},
		{"notify idle mode", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/notify", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				req.Header.Set(constants.HeaderIdleMode, "true")
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return nil
					}),
			})
			return patches
		}, http.StatusOK},
		{"notify billing switch true", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/notify", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				req.Header.Set(constants.HeaderBillingSwitch, "true")
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return nil
					}),
			})
			return patches
		}, http.StatusOK},
		{"notify billing switch false", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/notify", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				req.Header.Set(constants.HeaderBillingSwitch, "false")
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return nil
					}),
			})
			return patches
		}, http.StatusOK},
		{"notify billing switch unknown", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/notify", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
				req.Header.Set(constants.HeaderBillingSwitch, "unknown")
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(localauth.AuthCheckLocally,
					func(ak string, sk string, requestSign string, timestamp string, duration int) error {
						return nil
					}),
			})
			return patches
		}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			s.notifyFunction(tt.args.w, tt.args.r)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func TestIsUpgrading(t *testing.T) {
	s := &Server{
		conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				IsClusterUpgrading: "false",
				Language:           "java8",
			},
		},
		invokeServer: &invoke.Server{},
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	testArgs := &args{
		w: httptest.NewRecorder(),
		r: func() *http.Request {
			req := httptest.NewRequest("POST", "/update_agency_info", bytes.NewReader([]byte{}))
			req.Header.Set(constants.HeaderTraceID, "traceID")
			req.Header.Set(constants.HeaderAccessKey, "ak2")
			req.Header.Set(constants.HeaderSecretKey, "sk2")
			req.Header.Set(constants.HeaderSecretKey, "token2")
			req.Header.Set(constants.HeaderClusterUpgrading, "true")
			return req
		}(),
	}

	Convey("Is upgrading", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&invoke.Server{}), "HandleUpdateAgencyToRuntime", func(*invoke.Server) {
			return
		})
		defer patch.Reset()
		s.handleUpdateAgencyInfo(testArgs.w, testArgs.r)
		So(s.conf.IsClusterUpgrading, ShouldEqual, "true")
		resp := testArgs.w.(*httptest.ResponseRecorder).Result()
		So(resp.StatusCode, ShouldEqual, http.StatusOK)
	})
	Convey("Not java Function update", t, func() {
		s.conf.Language = "python3"
		s.handleUpdateAgencyInfo(testArgs.w, testArgs.r)
		resp := testArgs.w.(*httptest.ResponseRecorder).Result()
		So(resp.StatusCode, ShouldEqual, http.StatusOK)
	})
	Convey("java Function update", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&invoke.Server{}), "HandleUpdateAgencyToRuntime", func(*invoke.Server) {
			return
		})
		defer patch.Reset()
		s.conf.Language = "java17"
		s.handleUpdateAgencyInfo(testArgs.w, testArgs.r)
		resp := testArgs.w.(*httptest.ResponseRecorder).Result()
		So(resp.StatusCode, ShouldEqual, http.StatusOK)
	})
}

func TestServer_IsLoaded(t *testing.T) {
	Convey("IsLoaded", t, func() {
		Convey("config is nil", func() {
			s := &Server{
				conf: nil,
			}
			res := s.IsLoaded()
			So(res, ShouldBeFalse)
		})

		Convey("IsLoaded is false", func() {
			s := &Server{
				conf: &config.Configuration{},
			}
			res := s.IsLoaded()
			So(res, ShouldBeFalse)
		})

		Convey("IsLoaded is true", func() {
			s := &Server{
				conf: &config.Configuration{},
			}
			s.conf.IsLoaded = true
			res := s.IsLoaded()
			So(res, ShouldBeTrue)
		})

		Convey("IsLoaded is true, when it is a custom container", func() {
			s := &Server{
				conf: &config.Configuration{},
			}
			s.conf.Language = worker.CustomContainerLanguage
			res := s.IsLoaded()
			So(res, ShouldBeTrue)
		})
	})
}

func Test_checkNATConfigures(t *testing.T) {
	type args struct {
		changedNATConfigures []types.NATConfigure
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "case01 update scenario, one pat is deleted and the other is new created",
			args: args{changedNATConfigures: []types.NATConfigure{
				{
					ContainerCidr:  "",
					HostVMCidr:     "192.168.0.0/16",
					PatContainerIP: "10.0.0.116",
					PatVMIP:        "192.168.91.213",
					PatPortIP:      "",
					PatMacAddr:     "",
					PatGateway:     "172.16.0.1",
					PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
					TenantCidr:     "172.16.0.0/24",
					NatSubnetList: map[string][]string{
						"exclude": {"100.64.0.0/10"},
						"include": {"172.16.0.0/24"},
					},
					IsDeleted:    true,
					IsNewCreated: false,
				},
				{
					ContainerCidr:  "",
					HostVMCidr:     "192.168.0.0/16",
					PatContainerIP: "10.0.0.117",
					PatVMIP:        "192.168.91.213",
					PatPortIP:      "",
					PatMacAddr:     "",
					PatGateway:     "172.16.0.1",
					PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
					TenantCidr:     "172.16.0.0/24",
					NatSubnetList: map[string][]string{
						"exclude": {"100.64.0.0/10"},
						"include": {"172.16.0.0/24"},
					},
					IsDeleted:    false,
					IsNewCreated: true,
				},
			}},
			want:    false,
			wantErr: false,
		}, {
			name: "case02 replace scenario, two pats are new created",
			args: args{changedNATConfigures: []types.NATConfigure{
				{
					ContainerCidr:  "",
					HostVMCidr:     "192.168.0.0/16",
					PatContainerIP: "10.0.0.116",
					PatVMIP:        "192.168.91.213",
					PatPortIP:      "",
					PatMacAddr:     "",
					PatGateway:     "172.16.0.1",
					PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
					TenantCidr:     "172.16.0.0/24",
					NatSubnetList: map[string][]string{
						"exclude": {"100.64.0.0/10"},
						"include": {"172.16.0.0/24"},
					},
					IsDeleted:    false,
					IsNewCreated: true,
				},
				{
					ContainerCidr:  "",
					HostVMCidr:     "192.168.0.0/16",
					PatContainerIP: "10.0.0.117",
					PatVMIP:        "192.168.91.213",
					PatPortIP:      "",
					PatMacAddr:     "",
					PatGateway:     "172.16.0.1",
					PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
					TenantCidr:     "172.16.0.0/24",
					NatSubnetList: map[string][]string{
						"exclude": {"100.64.0.0/10"},
						"include": {"172.16.0.0/24"},
					},
					IsDeleted:    false,
					IsNewCreated: true,
				},
			}},
			want:    true,
			wantErr: false,
		}, {
			name: "case03 unkown scenario",
			args: args{changedNATConfigures: []types.NATConfigure{
				{
					ContainerCidr:  "",
					HostVMCidr:     "192.168.0.0/16",
					PatContainerIP: "10.0.0.116",
					PatVMIP:        "192.168.91.213",
					PatPortIP:      "",
					PatMacAddr:     "",
					PatGateway:     "172.16.0.1",
					PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc6",
					TenantCidr:     "172.16.0.0/24",
					NatSubnetList: map[string][]string{
						"exclude": {"100.64.0.0/10"},
						"include": {"172.16.0.0/24"},
					},
					IsDeleted:    false,
					IsNewCreated: false,
				},
				{
					ContainerCidr:  "",
					HostVMCidr:     "192.168.0.0/16",
					PatContainerIP: "10.0.0.117",
					PatVMIP:        "192.168.91.213",
					PatPortIP:      "",
					PatMacAddr:     "",
					PatGateway:     "172.16.0.1",
					PatPodName:     "cff-pat-pod-deb2c51a-2f13-4398-bc7",
					TenantCidr:     "172.16.0.0/24",
					NatSubnetList: map[string][]string{
						"exclude": {"100.64.0.0/10"},
						"include": {"172.16.0.0/24"},
					},
					IsDeleted:    true,
					IsNewCreated: true,
				},
			}},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkNATConfigures(tt.args.changedNATConfigures)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkNATConfigures() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("checkNATConfigures() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServer_checkpointFunction(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		statusCode  int
	}{
		{"case1 succeed to checkpointFunction", fields{conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
			invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}},
			args{w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("GET", "/checkpoint", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}()}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()

				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(status.Get, func() status.Status {
						return status.Ready
					}),
					ApplyFunc((*invoke.Server).CheckpointRuntimeProcess,
						func(_ *invoke.Server, deployer deploy.Deployer, traceID string) {
							return
						}),
					ApplyFunc((*network.NetWork).AddExcludeSubnetRouteToUploadSnapshot, func(_ *network.NetWork) {}),
				})
				return patches
			}, http.StatusOK},
		{"case2 failed to checkpointFunction Checkpointing", fields{conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
			invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}},
			args{w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("GET", "/checkpoint", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}()}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				status.Set(status.Ready)
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*invoke.Server).CheckpointRuntimeProcess,
						func(_ *invoke.Server, deployer deploy.Deployer, traceID string) {
							return
						}),
					ApplyFunc(status.Get, func() status.Status {
						return status.Checkpointing
					}),
					ApplyFunc((*network.NetWork).AddExcludeSubnetRouteToUploadSnapshot, func(_ *network.NetWork) {}),
				})
				return patches
			}, http.StatusOK},
		{"case3 failed to checkpointFunction GetSnapshotInfo failed", fields{conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
			invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}},
			args{w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("GET", "/checkpoint", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}()}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				status.Set(status.CheckpointReady)
				snapshotInfo := &types.SnapshotDeploymentSpec{ObjectID: "test"}
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*invoke.Server).CheckpointRuntimeProcess,
						func(_ *invoke.Server, deployer deploy.Deployer, traceID string) {
							return
						}),
					ApplyFunc(snapshot.GetSnapshotInfo, func() *types.SnapshotDeploymentSpec {
						return snapshotInfo
					}),
					ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte{}, errors.New("marshal error")
					}),
					ApplyFunc((*network.NetWork).AddExcludeSubnetRouteToUploadSnapshot, func(_ *network.NetWork) {}),
				})
				return patches
			}, http.StatusInternalServerError},
		{"case4 failed to checkpointFunction GetSnapshotInfo succeed", fields{conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
			invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}},
			args{w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("GET", "/checkpoint", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}()}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				status.Set(status.CheckpointReady)
				snapshotInfo := &types.SnapshotDeploymentSpec{ObjectID: "test"}
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*invoke.Server).CheckpointRuntimeProcess,
						func(_ *invoke.Server, deployer deploy.Deployer, traceID string) {
							return
						}),
					ApplyFunc(snapshot.GetSnapshotInfo, func() *types.SnapshotDeploymentSpec {
						return snapshotInfo
					}),
					ApplyFunc((*network.NetWork).AddExcludeSubnetRouteToUploadSnapshot, func(_ *network.NetWork) {}),
				})
				return patches
			}, http.StatusOK},
		{"case5 failed to checkpointFunction", fields{conf: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
			invokeServer: &invoke.Server{}, deployer: &fakeDeploy{}},
			args{w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("GET", "/checkpoint", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderPodApplier, constants.WorkerManagerApplier)
					return req
				}()}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				status.Set(status.CheckpointFailed)
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*network.NetWork).AddExcludeSubnetRouteToUploadSnapshot, func(_ *network.NetWork) {}),
				})
				return patches
			}, http.StatusInternalServerError},
	}
	snapshot.GetSnapshotInfo()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			s.checkpointFunction(tt.args.w, tt.args.r)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			time.Sleep(3 * time.Second)
			patches.ResetAll()
		})
	}
}

func TestServer_handleShutdown(t *testing.T) {
	type fields struct {
		conf            *config.Configuration
		httpServer      *http.Server
		invokeServer    *invoke.Server
		net             *network.NetWork
		pingCtx         context.Context
		pingCancelFunc  context.CancelFunc
		pingCtxMutex    sync.Mutex
		deployer        deploy.Deployer
		finalizers      []func() error
		terminationChan chan struct{}
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		statusCode int
		invokeNum  int
	}{
		{"case1", fields{terminationChan: make(chan struct{})}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/shutdown", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "traceID")
				return req
			}()}, http.StatusOK, 1},
		{"case2", fields{terminationChan: make(chan struct{})}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/shutdown", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "traceID")
				return req
			}()}, http.StatusOK, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				conf:            tt.fields.conf,
				httpServer:      tt.fields.httpServer,
				invokeServer:    tt.fields.invokeServer,
				net:             tt.fields.net,
				pingCtx:         tt.fields.pingCtx,
				pingCancelFunc:  tt.fields.pingCancelFunc,
				pingCtxMutex:    tt.fields.pingCtxMutex,
				deployer:        tt.fields.deployer,
				finalizers:      tt.fields.finalizers,
				terminationChan: tt.fields.terminationChan,
			}
			wg := &sync.WaitGroup{}
			for i := 0; i < tt.invokeNum; i++ {
				wg.Add(1)
				go func() {
					s.handleShutdown(tt.args.w, tt.args.r)
					resp := tt.args.w.(*httptest.ResponseRecorder).Result()
					if resp.StatusCode != tt.statusCode {
						t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
					}
					wg.Done()
				}()
			}
			wg.Wait()
		})
	}
}

func TestServer_normalMountVolume(t *testing.T) {
	type fields struct {
		conf                     *config.Configuration
		httpServer               *http.Server
		invokeServer             *invoke.Server
		net                      *network.NetWork
		pingCtx                  context.Context
		pingCancelFunc           context.CancelFunc
		pingCtxMutex             sync.Mutex
		pingHash                 string
		deployer                 deploy.Deployer
		finalizers               []func() error
		terminationChan          chan struct{}
		terminationChanCloseOnce sync.Once
	}

	var f fields
	var f1 fields
	conf := &config.Configuration{
		StartArgs:               config.StartArgs{},
		RuntimeConfig:           config.RuntimeConfig{},
		DeploymentConfig:        config.DeploymentConfig{},
		SnapshotConfig:          config.SnapshotConfig{},
		ServerConfig:            config.ServerConfig{},
		VPCConfig:               config.VPCConfig{},
		StorageConfig:           config.StorageConfig{},
		InvokeServer:            config.InvokeServer{},
		BackgroundProcessLimit:  config.BackgroundProcessLimit{},
		HeartbeatConfig:         config.HeartbeatConfig{},
		UserLogTag:              "",
		FunctionNameSeparator:   "",
		ReportTimeWindowsPeriod: 0,
		SyncRequestTimeout:      0,
		ReconnectTimes:          0,
	}
	conf1 := &config.Configuration{
		RuntimeConfig: config.RuntimeConfig{},
	}
	f1.conf = conf1
	funcMount := commontypes.FuncMount{
		MountType:      "MountType",
		MountResource:  "MountResource",
		MountSharePath: "MountSharePath",
		LocalMountPath: "LocalMountPath",
		Status:         "Status",
	}

	conf.RuntimeConfig.FuncMountConfig.FuncMounts = append(conf.RuntimeConfig.FuncMountConfig.FuncMounts, funcMount)
	f.conf = conf
	tests := []struct {
		name        string
		fields      fields
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 NewContainerClient error", f, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return nil, errors.New("failed to NewContainerClient")
						}),
				})
				return patches
			}},
		{"case2 mountVolume error", f, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					ApplyFunc((*Server).mountVolume,
						func(_ *Server, cli engine.ContainerClient) error {
							return errors.New("failed to mountVolume")
						}),
				})
				return patches
			}},
		{"case3 mountVolume success", f, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					ApplyFunc((*Server).mountVolume,
						func(_ *Server, cli engine.ContainerClient) error {
							return nil
						}),
				})
				return patches
			}},
		{"case4 FuncMounts empty", f1, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				conf:                     tt.fields.conf,
				httpServer:               tt.fields.httpServer,
				invokeServer:             tt.fields.invokeServer,
				net:                      tt.fields.net,
				pingCtx:                  tt.fields.pingCtx,
				pingCancelFunc:           tt.fields.pingCancelFunc,
				pingCtxMutex:             tt.fields.pingCtxMutex,
				pingHash:                 tt.fields.pingHash,
				deployer:                 tt.fields.deployer,
				finalizers:               tt.fields.finalizers,
				terminationChan:          tt.fields.terminationChan,
				terminationChanCloseOnce: tt.fields.terminationChanCloseOnce,
			}
			if err := s.normalMountVolume(); (err != nil) != tt.wantErr {
				t.Errorf("normalMountVolume() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_checkAuth(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "failed to check auth",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string, timestamp string, duration int) error {
					return errors.New("test")
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "succeed to check auth",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string, timestamp string, duration int) error {
					return nil
				})
				return patches
			},
			wantErr: true,
		},
	}
	s := &Server{
		conf: &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if flag := s.checkAuth(&http.Request{}); flag != tt.wantErr {
				t.Errorf("newStorageMonitor flag = %v, wantErr %v", flag, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestServer_handleUpdateAgencyInfo(t *testing.T) {
	type fields struct {
		conf            *config.Configuration
		httpServer      *http.Server
		invokeServer    *invoke.Server
		net             *network.NetWork
		pingCtx         context.Context
		pingCancelFunc  context.CancelFunc
		pingCtxMutex    sync.Mutex
		deployer        deploy.Deployer
		finalizers      []func() error
		terminationChan chan struct{}
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		statusCode int
		invokeNum  int
	}{
		{"case1", fields{invokeServer: &invoke.Server{}, conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{}}, terminationChan: make(chan struct{})}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/update_agency_info", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "traceID")
				req.Header.Set(constants.HeaderAccessKey, "ak1")
				req.Header.Set(constants.HeaderSecretKey, "sk1")
				req.Header.Set(constants.HeaderSecretKey, "token1")
				return req
			}()}, http.StatusOK, 1},
		{"case2", fields{invokeServer: &invoke.Server{}, conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{}}, terminationChan: make(chan struct{})}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/update_agency_info", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "traceID")
				req.Header.Set(constants.HeaderAccessKey, "ak2")
				req.Header.Set(constants.HeaderSecretKey, "sk2")
				req.Header.Set(constants.HeaderSecretKey, "token2")
				return req
			}()}, http.StatusOK, 10},
	}
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&invoke.Server{}), "HandleUpdateAgencyToRuntime", func(*invoke.Server) {
		return
	})
	defer patch.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				conf:            tt.fields.conf,
				httpServer:      tt.fields.httpServer,
				invokeServer:    tt.fields.invokeServer,
				net:             tt.fields.net,
				pingCtx:         tt.fields.pingCtx,
				pingCancelFunc:  tt.fields.pingCancelFunc,
				pingCtxMutex:    tt.fields.pingCtxMutex,
				deployer:        tt.fields.deployer,
				finalizers:      tt.fields.finalizers,
				terminationChan: tt.fields.terminationChan,
			}
			wg := &sync.WaitGroup{}
			for i := 0; i < tt.invokeNum; i++ {
				wg.Add(1)
				go func() {
					s.handleUpdateAgencyInfo(tt.args.w, tt.args.r)
					resp := tt.args.w.(*httptest.ResponseRecorder).Result()
					if resp.StatusCode != tt.statusCode {
						t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
					}
					wg.Done()
				}()
			}
			wg.Wait()
		})
	}
}

func TestIsLoaded(t *testing.T) {
	s := &Server{
		conf: &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				DeployDir: "/path/to/deploy/dir",
			},
		},
	}
	Convey("not exist", t, func() {
		patche := gomonkey.ApplyFunc(os.Stat, func(filename string) (os.FileInfo, error) {
			return nil, os.ErrNotExist
		})
		defer patche.Reset()
		loaded := s.isLoaded()
		So(loaded, ShouldBeFalse)
	})
}

func TestCheckHealth(t *testing.T) {
	s := &Server{
		conf: &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				DeployDir: "notexist",
			},
		},
	}
	loadedResult = true
	patche := gomonkey.ApplyFunc(os.Stat, func(filename string) (os.FileInfo, error) {
		return nil, nil
	})
	defer patche.Reset()
	testCases := []struct {
		name           string
		status         status.Status
		expectedStatus int
	}{
		{
			name:           "Status is not Waiting",
			status:         status.Ready,
			expectedStatus: http.StatusOK,
		}, {
			name:           "Status is Waiting",
			status:         status.Waiting,
			expectedStatus: http.StatusInternalServerError,
		},
	}
	for _, tc := range testCases {
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()

		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(status.Get, func() status.Status {
				return tc.status
			}),
		})
		s.checkHealth(w, req)
		assert.Equal(t, tc.expectedStatus, w.Code)
		patches.ResetAll()
	}
}

func TestServer_getCustomContainerInfo(t *testing.T) {
	type fields struct {
		conf     *config.Configuration
		net      *network.NetWork
		pingHash string
	}

	conf := &config.Configuration{}
	net := &network.NetWork{}
	var f fields

	f.pingHash = "mock_pingHash"
	f.conf = conf
	f.net = net
	var NATConfigures []types.NATConfigure
	NATConfiguresValue, _ := json.Marshal(NATConfigures)
	var EndpointVpcNATConfigure []types.NATConfigure
	EndpointVpcNATConfigureValue, _ := json.Marshal(EndpointVpcNATConfigure)
	var RouteInfo types.RouteInfoData
	RouteInfoValue, _ := json.Marshal(RouteInfo)
	var funcMountConfig types.FuncMountConfig
	funcMountConfigValue, _ := json.Marshal(funcMountConfig)

	tests := []struct {
		name        string
		fields      fields
		patchesFunc func() []*Patches
		wantErr     bool
	}{
		{
			name:   "case1",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
					return errors.New("failed")
				}))
				os.Setenv(constants.NATConfigures, string(NATConfiguresValue))
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case2",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return nil, nil
				}))
				os.Setenv(constants.NATConfigures, string(NATConfiguresValue))
				os.Setenv(constants.EndpointVpcNATConfigure, "aaa")
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case3",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return nil, nil
				}))
				os.Setenv(constants.NATConfigures, string(NATConfiguresValue))
				os.Setenv(constants.EndpointVpcNATConfigure, string(EndpointVpcNATConfigureValue))
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case4",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return nil, nil
				}))
				os.Setenv(constants.NATConfigures, string(NATConfiguresValue))
				os.Setenv(constants.EndpointVpcNATConfigure, string(EndpointVpcNATConfigureValue))
				os.Setenv(constants.RouteInfo, "aaa")
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case5",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return nil, nil
				}))
				os.Setenv(constants.NATConfigures, string(NATConfiguresValue))
				os.Setenv(constants.EndpointVpcNATConfigure, string(EndpointVpcNATConfigureValue))
				os.Setenv(constants.RouteInfo, string(RouteInfoValue))
				os.Setenv(constants.FuncMountConfig, "aaa")
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case5",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return nil, nil
				}))
				os.Setenv(constants.NATConfigures, string(NATConfiguresValue))
				os.Setenv(constants.EndpointVpcNATConfigure, string(EndpointVpcNATConfigureValue))
				os.Setenv(constants.RouteInfo, string(RouteInfoValue))
				os.Setenv(constants.FuncMountConfig, string(funcMountConfigValue))
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:     tt.fields.conf,
				net:      tt.fields.net,
				pingHash: tt.fields.pingHash,
			}
			err := s.GetCustomContainerInfo()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

func TestServer_writeLogConfigFile(t *testing.T) {
	type fields struct {
		conf     *config.Configuration
		net      *network.NetWork
		pingHash string
	}

	conf := &config.Configuration{
		RuntimeConfig: config.RuntimeConfig{
			LogPolicy: commontypes.LogPolicy{
				LogPaths:           []string{"/cae/log/log.log"},
				HostPath:           "/var/log",
				EnableMultiCollect: true,
				MultiPattern:       "pattern",
				LogStreamID:        "streamID",
				LogGroupID:         "groupID",
			},
			Properties:         map[string]string{"key": "value"},
			RuntimeContainerID: "containerId",
		},
	}

	os.Setenv("PodName", "test-pod")

	net := &network.NetWork{}
	var f fields

	f.pingHash = "mock_pingHash"
	f.conf = conf
	f.net = net

	var logConfig config.LogConfig

	tests := []struct {
		name        string
		fields      fields
		patchesFunc func() []*Patches
		wantErr     bool
	}{
		{
			name:   "case1 success",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return nil
				}))
				patches = append(patches, ApplyFunc(os.Create, func(name string) (*os.File, error) {
					return &os.File{}, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Write, func(file *os.File, b []byte) (int, error) {
					json.Unmarshal(b, &logConfig)
					return 0, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Chmod, func(file *os.File, mode os.FileMode) error {
					return nil
				}))
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case2 MkdirAll failed",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return errors.New("error")
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case3 Create failed",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return nil
				}))
				patches = append(patches, ApplyFunc(os.Create, func(name string) (*os.File, error) {
					return nil, errors.New("error")
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case4 Write failed",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return nil
				}))
				patches = append(patches, ApplyFunc(os.Create, func(name string) (*os.File, error) {
					return &os.File{}, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Write, func(file *os.File, b []byte) (int, error) {
					return 0, errors.New("error")
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case5 Marshal failed",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return nil
				}))
				patches = append(patches, ApplyFunc(os.Create, func(name string) (*os.File, error) {
					return &os.File{}, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Write, func(file *os.File, b []byte) (int, error) {
					return 0, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				patches = append(patches, ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return []byte{}, errors.New("error")
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case6 Chmod failed",
			fields: f,
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return nil
				}))
				patches = append(patches, ApplyFunc(os.Create, func(name string) (*os.File, error) {
					return &os.File{}, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Write, func(file *os.File, b []byte) (int, error) {
					return 0, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Chmod, func(file *os.File, mode os.FileMode) error {
					return errors.New("error")
				}))
				return patches
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:     tt.fields.conf,
				net:      tt.fields.net,
				pingHash: tt.fields.pingHash,
			}
			err := s.writeLogConfigFile()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, 2, len(logConfig.LogPath))
				assert.Equal(t, "userfunc/cae/log/log.log", logConfig.LogPath[0])
				assert.Equal(t, fmt.Sprintf("userfunc%s/*.log", constants.CAEMountUserLogPath), logConfig.LogPath[1])
				assert.Equal(t, "test-pod", logConfig.PodName)
				assert.Equal(t, true, logConfig.EnableMultiCollect)
				assert.Equal(t, "pattern", logConfig.MultiPattern)
				assert.Equal(t, "streamID", logConfig.LogStreamID)
				assert.Equal(t, "groupID", logConfig.LogGroupID)
				assert.Equal(t, "value", logConfig.Properties["key"])
				assert.Equal(t, "containerId", logConfig.ContainerID)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

func Test_parseWebsocket(t *testing.T) {
	jsonBytes, _ := json.Marshal("abc")
	jsonBytes1, _ := json.Marshal(schedulerapi.CreateWebSocketConnectionReq{})
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 io read error",
			args{httptest.NewRequest("POST", "/wss_connect", bytes.NewReader([]byte{}))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return nil, errors.New("failed to ReadAll")
						}),
				})
				return patches
			}},
		{"case2 Unmarshal error",
			args{httptest.NewRequest("POST", "/wss_connect", bytes.NewReader(jsonBytes))},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
		{"case3 Unmarshal successfully",
			args{httptest.NewRequest("POST", "/wss_connect", bytes.NewReader(jsonBytes1))},
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, errors.New("failed to ValidateStruct")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := parseWebsocket(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseWebsocket() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestServer_handleWssConnect(t *testing.T) {
	type fields struct {
		conf           *config.Configuration
		httpServer     *http.Server
		invokeServer   *invoke.Server
		net            *network.NetWork
		pingCtx        context.Context
		pingCancelFunc context.CancelFunc
		pingCtxMutex   sync.Mutex
		deployer       deploy.Deployer
		finalizers     []func() error
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		statusCode  int
	}{
		{
			name: "case1 parseWebsocket failed",
			fields: fields{
				conf: &config.Configuration{},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/wss_connect", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderTraceID, "A")
					return req
				}(),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(parseWebsocket, func(r *http.Request) (*schedulerapi.CreateWebSocketConnectionReq, error) {
						return &schedulerapi.CreateWebSocketConnectionReq{}, errors.New("failed to parseWebsocket")
					}),
					ApplyFunc(addRouteOfDispatcher, func(dispatcherIP string) error {
						return nil
					}),
					ApplyFunc((*websocket.WssManager).ChangeWebSocketIptables,
						func(_ *websocket.WssManager, connReq *schedulerapi.CreateWebSocketConnectionReq) error {
							return nil
						}),
				})
				return patches
			},
			statusCode: http.StatusInternalServerError,
		},
		{
			name: "case2 CreateWssConnPair failed",
			fields: fields{
				conf: &config.Configuration{},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/wss_connect", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderTraceID, "A")
					return req
				}(),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(parseWebsocket, func(r *http.Request) (*schedulerapi.CreateWebSocketConnectionReq, error) {
						return &schedulerapi.CreateWebSocketConnectionReq{}, nil
					}),
					ApplyFunc((*websocket.WssManager).CreateWssConnPair, func(_ *websocket.WssManager, traceID string, opt *commonwebsocket.StreamOption) (*websocket.WssConnectionPair,
						error) {
						return nil, errors.New("failed to create wss connection pair")
					}),
					ApplyFunc(addRouteOfDispatcher, func(dispatcherIP string) error {
						return nil
					}),
					ApplyFunc((*websocket.WssManager).ChangeWebSocketIptables,
						func(_ *websocket.WssManager, connReq *schedulerapi.CreateWebSocketConnectionReq) error {
							return nil
						}),
				})
				return patches
			},
			statusCode: http.StatusInternalServerError,
		},
		{
			name: "case3 wss connect ok",
			fields: fields{
				conf: &config.Configuration{}, finalizers: []func() error{},
			},
			args: args{w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/wss_connect", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderTraceID, "A")
					return req
				}()},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(parseWebsocket, func(r *http.Request) (*schedulerapi.CreateWebSocketConnectionReq, error) {
						return &schedulerapi.CreateWebSocketConnectionReq{}, nil
					}),
					ApplyFunc((*websocket.WssManager).CreateWssConnPair, func(_ *websocket.WssManager, traceID string, opt *commonwebsocket.StreamOption) (*websocket.WssConnectionPair,
						error) {
						return &websocket.WssConnectionPair{}, nil
					}),
					ApplyFunc((*websocket.WssConnectionPair).HandleMessage, func(_ *websocket.WssConnectionPair) {
						return
					}),
					ApplyFunc(addRouteOfDispatcher, func(dispatcherIP string) error {
						return nil
					}),
					ApplyFunc((*websocket.WssManager).ChangeWebSocketIptables,
						func(_ *websocket.WssManager, connReq *schedulerapi.CreateWebSocketConnectionReq) error {
							return nil
						}),
				})
				return patches
			},
			statusCode: http.StatusOK,
		},
		{
			name:   "test vpc network",
			fields: fields{conf: &config.Configuration{}, finalizers: []func() error{}},
			args: args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/wss_connect", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderTraceID, "A")
					return req
				}(),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(parseWebsocket, func(r *http.Request) (*schedulerapi.CreateWebSocketConnectionReq, error) {
						return &schedulerapi.CreateWebSocketConnectionReq{}, nil
					}),
					ApplyFunc((*websocket.WssManager).CreateWssConnPair, func(_ *websocket.WssManager, traceID string, opt *commonwebsocket.StreamOption) (*websocket.WssConnectionPair,
						error) {
						return &websocket.WssConnectionPair{}, nil
					}),
					ApplyFunc((*websocket.WssConnectionPair).HandleMessage, func(_ *websocket.WssConnectionPair) {
						return
					}),
					ApplyFunc(network.GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
						return "mockIntfName", "mockContainerIP", nil
					}),
					ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
						return utils.IptablesCmdSucceed, "mockResult", nil
					}),
					ApplyFunc((*websocket.WssManager).ChangeWebSocketIptables,
						func(_ *websocket.WssManager, connReq *schedulerapi.CreateWebSocketConnectionReq) error {
							return nil
						}),
				})
				return patches
			},
			statusCode: http.StatusOK,
		},
		{
			name:   "test vpc network add route get gateway ip error",
			fields: fields{conf: &config.Configuration{}, finalizers: []func() error{}},
			args: args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/wss_connect", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderTraceID, "A")
					return req
				}(),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(parseWebsocket, func(r *http.Request) (*schedulerapi.CreateWebSocketConnectionReq, error) {
						return &schedulerapi.CreateWebSocketConnectionReq{}, nil
					}),
					ApplyFunc(network.GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
						return "mockIntfName", "mockContainerIP", errors.New("mock get gateway ip error")
					}),
					ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
						return 1, "mockResult", nil
					}),
				})
				return patches
			},
			statusCode: http.StatusInternalServerError,
		},
		{
			name:   "test vpc network add route exec shell command error",
			fields: fields{conf: &config.Configuration{}, finalizers: []func() error{}},
			args: args{
				w: httptest.NewRecorder(),
				r: func() *http.Request {
					req := httptest.NewRequest("POST", "/wss_connect", bytes.NewReader([]byte{}))
					req.Header.Set(constants.HeaderTraceID, "A")
					return req
				}(),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(parseWebsocket, func(r *http.Request) (*schedulerapi.CreateWebSocketConnectionReq, error) {
						return &schedulerapi.CreateWebSocketConnectionReq{}, nil
					}),
					ApplyFunc(network.GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
						return "mockIntfName", "mockContainerIP", errors.New("mock get gateway ip error")
					}),
					ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
						return 1, "mockResult", errors.New("mock shell command error")
					}),
				})
				return patches
			},
			statusCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:           tt.fields.conf,
				httpServer:     tt.fields.httpServer,
				invokeServer:   tt.fields.invokeServer,
				net:            tt.fields.net,
				pingCtx:        tt.fields.pingCtx,
				pingCancelFunc: tt.fields.pingCancelFunc,
				pingCtxMutex:   tt.fields.pingCtxMutex,
				deployer:       tt.fields.deployer,
				finalizers:     tt.fields.finalizers,
			}
			s.handleWssConnect(tt.args.w, tt.args.r)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

type mockCli struct {
}

func (m *mockCli) Info(ctx context.Context, containerID string) (engine.ContainerInfo, error) {
	return engine.ContainerInfo{
		RootFsPath: mockUtils.MockLocalMountPrefix,
	}, nil
}
func (m *mockCli) Kill(ctx context.Context, containerID string) error {
	return nil
}
func (m *mockCli) Close() error {
	return nil
}

func TestServer_mountVolume(t *testing.T) {
	type fields struct {
		conf                       *config.Configuration
		httpServer                 *http.Server
		invokeServer               *invoke.Server
		net                        *network.NetWork
		pingCtx                    context.Context
		pingCancelFunc             context.CancelFunc
		pingCtxMutex               sync.Mutex
		pingHash                   string
		deployer                   deploy.Deployer
		finalizers                 []func() error
		terminationChan            chan struct{}
		terminationChanCloseOnce   sync.Once
		customDiskMgr              *diskusage.EvictionManager
		customContainerPreError    error
		customContainerPreOnce     sync.Once
		skipWaitRuntimeContainerID bool
		customContainerPreFinished bool
		runtimeContainerID         string
	}
	type args struct {
		cli engine.ContainerClient
	}
	cfg := &config.Configuration{}
	cfg.FuncMountConfig.FuncMounts = []types.FuncMount{
		{
			LocalMountPath: "mockLocalMountPath",
		},
	}
	mockCli := &mockCli{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test modify local mount",
			fields: fields{
				conf:       cfg,
				finalizers: make([]func() error, 0),
			},
			args: args{
				cli: mockCli,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(mount.VolumesForFunction, func(mountConfig types.FuncMountConfig) error {
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
			s := &Server{
				conf:                       tt.fields.conf,
				httpServer:                 tt.fields.httpServer,
				invokeServer:               tt.fields.invokeServer,
				net:                        tt.fields.net,
				pingCtx:                    tt.fields.pingCtx,
				pingCancelFunc:             tt.fields.pingCancelFunc,
				pingCtxMutex:               tt.fields.pingCtxMutex,
				pingHash:                   tt.fields.pingHash,
				deployer:                   tt.fields.deployer,
				finalizers:                 tt.fields.finalizers,
				terminationChan:            tt.fields.terminationChan,
				terminationChanCloseOnce:   tt.fields.terminationChanCloseOnce,
				customDiskMgr:              tt.fields.customDiskMgr,
				customContainerPreError:    tt.fields.customContainerPreError,
				customContainerPreOnce:     tt.fields.customContainerPreOnce,
				skipWaitRuntimeContainerID: tt.fields.skipWaitRuntimeContainerID,
				customContainerPreFinished: tt.fields.customContainerPreFinished,
				runtimeContainerID:         tt.fields.runtimeContainerID,
			}
			fmt.Println(s.conf.FuncMountConfig.FuncMounts)
			s.mountVolume(tt.args.cli)
			fmt.Println(s.conf.FuncMountConfig.FuncMounts)
			for i := range s.conf.FuncMountConfig.FuncMounts {
				localMountPath := s.conf.FuncMountConfig.FuncMounts[i].LocalMountPath
				if !strings.HasPrefix(localMountPath, mockUtils.MockLocalMountPrefix) {
					t.Errorf("expect has prefix %v but found %v", mockUtils.MockLocalMountPrefix, localMountPath)
				}
			}
		})
	}
}

func TestServer_setRuntimeMessage(t *testing.T) {
	Convey("Test_setRuntimeMessage ", t, func() {
		Convey("case1-1 : cae func with snapshot size is 0", func() {
			rtCfg := config.Configuration{}
			msg := types.FunctionSpecRequest{}
			msg.Message.SnapshotDeployment.SnapshotSize = 0
			msg.Message.Function.FuncSignature = "test"
			rtCfg.SnapshotConfig.EnableSnapshot = "enable"
			rtCfg.SnapshotConfig.IsSnapshotExpire = false
			defer ApplyFunc(utils.GetBusinessType, func() string {
				return constants.BusinessTypeCAE
			}).Reset()
			setRuntimeMessage(&rtCfg, &msg)
			So(rtCfg.RunningMode, ShouldEqual, "")
		})

		Convey("case1-2 : cae func with snapshot FuncSignature is diff", func() {
			rtCfg := config.Configuration{}
			msg := types.FunctionSpecRequest{}
			msg.Message.SnapshotDeployment.SnapshotSize = int64(200)
			msg.Message.Function.FuncSignature = "test"
			rtCfg.SnapshotConfig.EnableSnapshot = "enable"
			rtCfg.SnapshotConfig.IsSnapshotExpire = false
			defer ApplyFunc(utils.GetBusinessType, func() string {
				return constants.BusinessTypeCAE
			}).Reset()
			setRuntimeMessage(&rtCfg, &msg)
			So(rtCfg.RunningMode, ShouldEqual, constants.RunningModeRestore)
		})

		Convey("case1-3 : cae func with snapshot FuncSignature is the same", func() {
			rtCfg := config.Configuration{}
			msg := types.FunctionSpecRequest{}
			msg.Message.SnapshotDeployment.SnapshotSize = int64(200)
			msg.Message.Function.FuncSignature = "test1"
			rtCfg.SnapshotConfig.EnableSnapshot = "enable"
			rtCfg.SnapshotConfig.IsSnapshotExpire = false
			defer ApplyFunc(utils.GetBusinessType, func() string {
				return constants.BusinessTypeCAE
			}).Reset()
			setRuntimeMessage(&rtCfg, &msg)
			So(rtCfg.RunningMode, ShouldEqual, constants.RunningModeRestore)
		})

		Convey("case2 : faas func with snapshot", func() {
			rtCfg := config.Configuration{}
			msg := types.FunctionSpecRequest{}
			msg.Message.SnapshotDeployment.SnapshotSize = int64(200)
			rtCfg.SnapshotConfig.EnableSnapshot = "enable"
			rtCfg.SnapshotConfig.IsSnapshotExpire = false
			rtCfg.SnapshotConfig.SnapshotSize = 200
			setRuntimeMessage(&rtCfg, &msg)
			So(rtCfg.RunningMode, ShouldEqual, constants.RunningModeRestore)
		})

		Convey("case3 : cae func with faststart enable", func() {
			rtCfg := config.Configuration{}
			msg := types.FunctionSpecRequest{}
			msg.Message.SnapshotDeployment.FastStartEnable = true
			defer ApplyFunc(utils.GetBusinessType, func() string {
				return ""
			}).Reset()
			setRuntimeMessage(&rtCfg, &msg)
			So(rtCfg.FastStartEnable, ShouldBeFalse)
		})
	})
}

func TestServer_handleWssConnCount(t *testing.T) {
	type fields struct {
		conf         *config.Configuration
		httpServer   *http.Server
		invokeServer *invoke.Server
		deployer     deploy.Deployer
		finalizers   []func() error
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		statusCode  int
	}{
		{"case1 get count marshal failed", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/wss_count", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "A")
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return []byte{}, errors.New("marshal error")
				}),
			})
			return patches
		}, http.StatusInternalServerError},
		{"case2 get count successfully", fields{conf: &config.Configuration{}}, args{w: httptest.NewRecorder(),
			r: func() *http.Request {
				req := httptest.NewRequest("POST", "/wss_count", bytes.NewReader([]byte{}))
				req.Header.Set(constants.HeaderTraceID, "A")
				return req
			}()}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &Server{
				conf:         tt.fields.conf,
				httpServer:   tt.fields.httpServer,
				invokeServer: tt.fields.invokeServer,
				deployer:     tt.fields.deployer,
				finalizers:   tt.fields.finalizers,
			}
			s.handleWssConnCount(tt.args.w, tt.args.r)
			resp := tt.args.w.(*httptest.ResponseRecorder).Result()
			if resp.StatusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			patches.ResetAll()
		})
	}
}

func TestAllowSetCustomContainerPort(t *testing.T) {
	Convey("TestAllowSetCustomContainerPort ", t, func() {
		isAllow := allowSetCustomContainerPort(0)
		So(isAllow, ShouldBeFalse)
		isAllow = allowSetCustomContainerPort(8081)
		So(isAllow, ShouldBeFalse)
		isAllow = allowSetCustomContainerPort(9090)
		So(isAllow, ShouldBeTrue)
	})
}

func Test_addRouteOfDispatcher(t *testing.T) {
	tests := []struct {
		name        string
		args        string
		err         error
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1",
			args: "127.127.127.127",
			err:  nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(network.GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
						return "255.255.255.255", "eht0", nil
					}),
					gomonkey.ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
						return 0, "", nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			err := addRouteOfDispatcher(tt.args)
			if err != tt.err && err.Error() != tt.err.Error() {
				t.Errorf("failed to test func: addRouteOfDispatcher, actual = %v, want %v", err, tt.err)
			}
			patches.ResetAll()
		})
	}
}

func Test_updateLogConfigFile(t *testing.T) {
	logConfig := config.LogConfig{}
	var newLogConfig config.LogConfig
	data, _ := json.Marshal(logConfig)
	tests := []struct {
		name        string
		patchesFunc func() []*Patches
		wantErr     bool
	}{
		{
			name: "case1 success",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
					return data, nil
				}))
				patches = append(patches, ApplyFunc(os.OpenFile, func(_ string, _ int, _ os.FileMode) (*os.File, error) {
					return &os.File{}, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Write, func(file *os.File, b []byte) (int, error) {
					json.Unmarshal(b, &newLogConfig)
					return 0, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2 ioutil.ReadFile failed",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
					return nil, errors.New("error")
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name: "case3 json.Unmarshal failed",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
					return []byte("aaa"), nil
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name: "case4 os.OpenFile failed",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
					return data, nil
				}))
				patches = append(patches, ApplyFunc(os.OpenFile, func(_ string, _ int, _ os.FileMode) (*os.File, error) {
					return nil, errors.New("error")
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name: "case5 file.Write failed",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
					return data, nil
				}))
				patches = append(patches, ApplyFunc(os.OpenFile, func(_ string, _ int, _ os.FileMode) (*os.File, error) {
					return &os.File{}, nil
				}))
				patches = append(patches, ApplyFunc((*os.File).Write, func(file *os.File, b []byte) (int, error) {
					return 0, errors.New("error")
				}))
				patches = append(patches, ApplyFunc((*os.File).Close, func(file *os.File) error {
					return nil
				}))
				return patches
			},
			wantErr: true,
		},
		{
			name: "case6 json.Marshal failed",
			patchesFunc: func() []*Patches {
				patches := make([]*Patches, 0)
				patches = append(patches, ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
					return data, nil
				}))
				patches = append(patches, ApplyFunc(json.Marshal, func(_ any) ([]byte, error) {
					return nil, errors.New("error")
				}))
				return patches
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			updateLogConfigFile(true)
			if !tt.wantErr {
				assert.Equal(t, true, newLogConfig.PodDelete)
			}
			for _, v := range patches {
				v.Reset()
			}
		})
	}
}

func Test_safetyVpcCfgSet(t *testing.T) {
	h := make(http.Header)
	h.Set(constants.CacheManagerClusterID, "127.0.0.1")
	tests := []struct {
		name string
		r    *http.Request
		s    *Server
	}{
		{
			name: "case1 cacheManagerServiceIP is nil",
			r:    &http.Request{},
			s:    &Server{},
		},
		{
			name: "case2 server conf is nil",
			r:    &http.Request{Header: h},
			s:    &Server{},
		},
		{
			name: "case3 clear",
			r:    &http.Request{Header: h},
			s:    &Server{conf: &config.Configuration{}, net: &network.NetWork{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.safetyVpcCfgSet(tt.r)
		})
	}
}

func Test_resetWorkerDNSRoute(t *testing.T) {
	patches := ApplyFunc((*exec.Cmd).Run, func(_ *exec.Cmd) error {
		return errors.New("error")
	})
	defer patches.Reset()
	s := Server{conf: &config.Configuration{DNSResolveAddr: "  , 123, 127.0.0.1"}}
	s.resetWorkerDNSRoute("169.254.0.1")
}

func Test_CodeChanNatInitialChan(t *testing.T) {
	s := &Server{
		net: &network.NetWork{
			CodeDeployChan:    make(chan struct{}, 1),
			NatInitializeChan: make(chan struct{}, 1),
		},
		conf: &config.Configuration{
			IsSafety: true,
		},
	}
	s.SendToDeployCodeChan()
	s.SendToNatInitialChan()
	_, ok := <-s.net.CodeDeployChan
	if !ok {
		t.Errorf("error, not close")
	}
	_, ok = <-s.net.NatInitializeChan
	if !ok {
		t.Errorf("error, not close")
	}
	close(s.net.CodeDeployChan)
	close(s.net.NatInitializeChan)
}

func Test_uploadSnapshotWithRetry(t *testing.T) {
	convey.Convey("upload Snapshot", t, func() {
		convey.Convey("test upload snapshot succeed", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(deploy.UploadSnapshotToDS,
					func(bucketID, objectID string) (int64, string, error) {
						return 0, "", errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			flag, _ := uploadSnapshotWithRetry("", "", "")
			convey.So(flag, convey.ShouldBeTrue)
		})
	})

	convey.Convey("upload Snapshot", t, func() {
		convey.Convey("test upload snapshot succeed", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(deploy.UploadSnapshotToDS,
					func(bucketID, objectID string) (int64, string, error) {
						return 0, "", nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			flag, _ := uploadSnapshotWithRetry("", "", "")
			convey.So(flag, convey.ShouldBeFalse)
		})
	})
}

func Test_writeErrorMsg(t *testing.T) {
	w := mockUtils.MockResponseWriter{}
	errCode := 500
	errMsg := "error"
	traceID := "traceID001"
	writeErrorMsg(w, errCode, errMsg, traceID)
}

func TestServer_deployAndLoadCustomContainer(t *testing.T) {
	var (
		s     = &Server{}
		r     = &http.Request{}
		patch = gomonkey.NewPatches()
	)

	convey.Convey("test deployAndLoadCustomContainer", t, func() {
		convey.Convey("test failed to exec mount error", func() {
			mockAddCustomContainerFinalizer(patch, nil)
			s.customContainerPreError = errors.New("failed to get RuntimeContainerID")
			mockExecuteMount(patch, errors.New("executeMount error"))
			_, err := s.deployAndLoadCustomContainer(r)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "executeMount error")
		})

		convey.Convey("test custom container pre error", func() {
			mockAddCustomContainerFinalizer(patch, nil)
			s.customContainerPreError = errors.New("other error")
			_, err := s.deployAndLoadCustomContainer(r)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "other error")
		})

		convey.Convey("test failed to exec mount command", func() {
			mockAddCustomContainerFinalizer(patch, nil)
			s.customContainerPreError = nil
			mockNeedCustomContainerPreLoad(patch, true)
			_, err := s.deployAndLoadCustomContainer(r)
			convey.So(err, convey.ShouldNotBeNil)
			if snErr, ok := err.(snerror.SNError); ok {
				convey.So(snErr.Code(), convey.ShouldEqual, snerror.RuntimeMountErrorCode)
			} else {
				convey.So(err.Error(), convey.ShouldEqual, "failed to exec mount command")
			}
		})

		convey.Convey("test restore cpu limit error", func() {
			mockAddCustomContainerFinalizer(patch, nil)
			s.customContainerPreError = errors.New("failed to get RuntimeContainerID")
			mockExecuteMount(patch, nil)
			mockNeedCustomContainerPreLoad(patch, false)
			mockRestoreCPULimit(patch, errors.New("restore cpu limit error"))
			_, err := s.deployAndLoadCustomContainer(r)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "restore cpu limit error")
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestServer_uploadSnapshotToDS(t *testing.T) {
	var (
		patch              = gomonkey.NewPatches()
		snapshotDeployment = types.SnapshotDeploymentSpec{
			BucketID:  "test1",
			ObjectID:  "test.zip.123",
			BucketURL: "127.0.0.1:8088",
		}
	)

	convey.Convey("test uploadSnapshotToDS", t, func() {
		convey.Convey("test read body error", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/uploadSnapshotToDS", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			mockIOReadAll(patch, nil, errors.New("read resp body error"))
			s.uploadSnapshotToDS(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})

		convey.Convey("test json unmarshal error error", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/uploadSnapshotToDS", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			mockJsonUnmarshal(patch, errors.New("mock json unmarshal error"))
			s.uploadSnapshotToDS(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})

		convey.Convey("test upload snapshot error", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/uploadSnapshotToDS", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			mockJsonUnmarshal(patch, nil)
			s.deployer = &fakeDeployErr{}
			s.uploadSnapshotToDS(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})

		convey.Convey("test upload retry error", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/uploadSnapshotToDS", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			s.deployer = &fakeDeployErr{}
			patch.ApplyFunc(uploadSnapshotWithRetry, func(bucketID string, objectID string, traceID string) (bool, error) {
				return true, errors.New("upload retry error")
			})

			s.uploadSnapshotToDS(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})

		convey.Convey("test upload snapshot success", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/uploadSnapshotToDS", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			s.deployer = &fakeDeploy{}
			patch.ApplyFunc(uploadSnapshotWithRetry, func(bucketID string, objectID string, traceID string) (bool, error) {
				return false, nil
			})

			s.uploadSnapshotToDS(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestServer_deleteSnapshotData(t *testing.T) {
	var (
		patch              = gomonkey.NewPatches()
		snapshotDeployment = types.SnapshotDeploymentSpec{
			BucketID:  "test1",
			ObjectID:  "test.zip.123",
			BucketURL: "127.0.0.1:8088",
		}
	)

	convey.Convey("test deleteSnapshotData", t, func() {
		convey.Convey("test read body error", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/deleteSnapshotData", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			mockIOReadAll(patch, nil, errors.New("read resp body error"))
			s.deleteSnapshotData(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})

		convey.Convey("test json unmarshal error error", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/deleteSnapshotData", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			mockJsonUnmarshal(patch, errors.New("mock json unmarshal error"))
			s.deleteSnapshotData(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})

		convey.Convey("test delete snapshot error", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/deleteSnapshotData", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			mockJsonUnmarshal(patch, nil)
			s.deployer = &fakeDeployErr{}
			s.deleteSnapshotData(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		})

		convey.Convey("test delete snapshot success", func() {
			s := &Server{conf: &config.Configuration{}}
			bodyValue, _ := json.Marshal(snapshotDeployment)
			r := func() *http.Request {
				req := httptest.NewRequest("GET", "/deleteSnapshotData", bytes.NewReader(bodyValue))
				req.Header.Set(constants.HeaderTraceID, "test")
				return req
			}()
			w := httptest.NewRecorder()
			s.deployer = &fakeDeploy{}
			s.deleteSnapshotData(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func mockIOReadAll(patch *gomonkey.Patches, body []byte, err error) {
	patch.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
		return body, err
	})
}

func mockJsonUnmarshal(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
		return err
	})
}

func mockAddCustomContainerFinalizer(patch *gomonkey.Patches, err error) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&Server{}), "addCustomContainerFinalizer", func(_ *Server) error {
		return err
	})
}

func mockExecuteMount(patch *gomonkey.Patches, err error) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&Server{}), "executeMount", func(_ *Server,
		ctx context.Context) error {
		return err
	})
}

func mockNeedCustomContainerPreLoad(patch *gomonkey.Patches, res bool) {
	patch.ApplyFunc(utils.NeedCustomContainerPreLoad, func() bool {
		return res
	})
}

func mockRestoreCPULimit(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc(utils.RestoreCPULimit, func() error {
		return err
	})
}

func Test_uploadDumpFile(t *testing.T) {
	count := 0
	tests := []struct {
		name        string
		s           *Server
		patchesFunc mockUtils.PatchesFunc
		methodCount int
	}{
		{
			name: "test 1 customDeployer is nil",
			s:    &Server{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(waitForFileWriteToComplete, func(filePath string) error {
						count += 1
						return nil
					}),
				})
				return patches
			},
			methodCount: 0,
		},
		{
			name: "test 2 UploadDumpFile failed",
			s: &Server{customDeployer: &fakeDeployErr{}, tenantCommandExecuteLock: sync.RWMutex{},
				conf: &config.Configuration{CustomUploadConfig: types.CustomUploadConfig{CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{FilePath: "aaa"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(waitForFileWriteToComplete, func(filePath string) error {
						count += 1
						return nil
					}),
					ApplyFunc(os.Remove, func(name string) error {
						count += 1
						return nil
					}),
				})
				return patches
			},
			methodCount: 2,
		},
		{
			name: "test 3 remove dump file failed",
			s: &Server{customDeployer: &fakeDeploy{}, tenantCommandExecuteLock: sync.RWMutex{},
				conf: &config.Configuration{CustomUploadConfig: types.CustomUploadConfig{CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{FilePath: "aaa"}}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fakeDeploy).UploadDumpFile,
						func(_ *fakeDeploy, conf *config.Configuration, dumpFileDir string) error {
							count += 1
							return nil
						}),
					ApplyFunc(waitForFileWriteToComplete, func(filePath string) error {
						count += 1
						return nil
					}),
					ApplyFunc(os.Remove, func(name string) error {
						count += 1
						return errors.New("error")
					}),
				})
				return patches
			},
			methodCount: 3,
		},
		{
			name: "test 4 waitForFileWriteToComplete error",
			s: &Server{customDeployer: &fakeDeploy{},
				conf:                     &config.Configuration{CustomUploadConfig: types.CustomUploadConfig{CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{FilePath: "aaa"}}},
				tenantCommandExecuteLock: sync.RWMutex{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(waitForFileWriteToComplete, func(filePath string) error {
						count += 1
						return errors.New("test error")
					}),
					ApplyFunc((*fakeDeploy).UploadDumpFile,
						func(_ *fakeDeploy, conf *config.Configuration, dumpFileDir string) error {
							count += 1
							return nil
						}),
				})
				return patches
			},
			methodCount: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count = 0
			patches := tt.patchesFunc()
			tt.s.uploadDumpFile("/tmp/test.dump")
			patches.ResetAll()
			if tt.methodCount != count {
				t.Errorf("error, result: %d, expected: %d", tt.methodCount, count)
			}

		})
	}
}

func Test_saveCAEFuncProperty(t *testing.T) {
	tests := []struct {
		name        string
		s           *Server
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test1, json.Marshal error",
			s:    &Server{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v any) ([]byte, error) { return nil, errors.New("error") }),
				})
				return patches
			},
		},
		{
			name: "test2, json.Unmarshal error",
			s:    &Server{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v any) ([]byte, error) { return []byte{123, 123}, nil }),
					ApplyFunc(json.Unmarshal, func(data []byte, v any) error { return errors.New("error") }),
				})
				return patches
			},
		},
		{
			name: "test3, clear",
			s:    &Server{conf: &config.Configuration{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v any) ([]byte, error) { return []byte{123, 123}, nil }),
					ApplyFunc(json.Unmarshal, func(data []byte, v any) error { return nil }),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.s.saveCAEFuncProperty(map[string]string{"test": "test"})
			patches.ResetAll()
		})
	}

	test := []struct {
		name   string
		pre    string
		new    string
		result string
	}{
		{"test1", "", "", ""},
		{"test2", "", "testNew", "testNew"},
		{"test3", "testPre", "testNew", "testPre/testNew"},
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			res := buildFullPath(tt.pre, tt.new)
			if res != tt.result {
				t.Errorf("error, result: %s, exprct: %s", res, tt.result)
			}
		})
	}
}

func Test_caeDumpFileNotify(t *testing.T) {
	var count = 0
	tests := []struct {
		name         string
		s            *Server
		patchesFunc  mockUtils.PatchesFunc
		executeCount int
	}{
		{
			name: "test1, s.conf.IsCAECustomS3Config is false",
			s:    &Server{conf: &config.Configuration{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(signals.WaitForSignal, func() <-chan struct{} { return make(chan struct{}) }),
				})
				return patches
			},
			executeCount: 0,
		},
		{
			name: "test2, s.customDeployer is nil",
			s:    &Server{conf: &config.Configuration{IsCAECustomS3Config: true}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(signals.WaitForSignal, func() <-chan struct{} { return make(chan struct{}) }),
					ApplyFunc(commonutils.IsCAEFunc, func(businessType string) bool { return true }),
				})
				return patches
			},
			executeCount: 0,
		},
		{
			name: "test3, NewWatcher error",
			s:    &Server{conf: &config.Configuration{IsCAECustomS3Config: true}, customDeployer: &fakeDeploy{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(signals.WaitForSignal, func() <-chan struct{} { return make(chan struct{}) }),
					ApplyFunc(commonutils.IsCAEFunc, func(businessType string) bool { return true }),
					ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) { return nil, errors.New("error") }),
				})
				return patches
			},
			executeCount: 0,
		},
		{
			name: "test4, watcher.Add error",
			s:    &Server{conf: &config.Configuration{IsCAECustomS3Config: true}, customDeployer: &fakeDeploy{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(signals.WaitForSignal, func() <-chan struct{} { return make(chan struct{}) }),
					ApplyFunc(commonutils.IsCAEFunc, func(businessType string) bool { return true }),
					ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) { return &fsnotify.Watcher{}, nil }),
					ApplyFunc((*fsnotify.Watcher).Add, func(w *fsnotify.Watcher, name string) error { return errors.New("error") }),
					ApplyFunc((*fsnotify.Watcher).Close, func(watcher *fsnotify.Watcher) error { return nil }),
					ApplyFunc((*Server).uploadDumpFile, func(_ *Server, dumpFileDir string) {
						count += 1
					}),
				})
				return patches
			},
			executeCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			count = 0
			tt.s.dumpFileNotify()
			if count != tt.executeCount {
				t.Errorf("error, count: %d, executeCount: %d", count, tt.executeCount)
			}
			patches.ResetAll()
		})
	}
}

func Test_reportOOMEvent(t *testing.T) {
	tests := []struct {
		name        string
		fileName    string
		s           *Server
		patchesFunc mockUtils.PatchesFunc
		wantNil     bool
	}{
		{
			name:     "test1: invalid filename",
			fileName: "xxx.swp",
			s:        &Server{conf: &config.Configuration{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantNil: true,
		},
		{
			name:     "test2: invokeServer is nil",
			fileName: "xxx.hprof",
			s:        &Server{conf: &config.Configuration{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantNil: true,
		},
		{
			name:     "test3: failed to report oom event, bus is nil",
			fileName: "xxx.hprof",
			s: &Server{
				conf:         &config.Configuration{IsCAECustomS3Config: true},
				invokeServer: &invoke.Server{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantNil: true,
		},
		{
			name:     "test4: succeed to report oom event",
			fileName: "xxx.hprof",
			s: &Server{
				conf:         &config.Configuration{IsCAECustomS3Config: true},
				invokeServer: &invoke.Server{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*tcpbus.TCPBus).ReportErrMsgToBus, func(_ *tcpbus.TCPBus, ctx context.Context, URNWithVersion, errMsg, level string,
						properties map[string]string) (*commontypes.TCPResponse, error) {
						return &commontypes.TCPResponse{}, nil
					}),
				})
				return patches
			},
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := tt.s.reportOOMEvent(tt.fileName); (got != nil) == tt.wantNil {
				t.Errorf("reportOOMEvent() = %v is nil, want %v", got, tt.wantNil)
			}
			patches.ResetAll()
		})
	}
}

func Test_caeDumpFileNotify1(t *testing.T) {
	eventChan := make(chan fsnotify.Event, 1)
	errChan := make(chan error)
	stopChan := make(chan struct{})
	triggerChan := make(chan struct{})
	finishChan := make(chan struct{}, 1)
	watcher := &fsnotify.Watcher{Events: eventChan, Errors: errChan}
	var count = 0
	tests := []struct {
		name         string
		s            *Server
		patchesFunc  mockUtils.PatchesFunc
		executeCount int
	}{
		{
			name: "upload channel",
			s:    &Server{conf: &config.Configuration{IsCAECustomS3Config: true, CAECustomS3ConfigInfo: types.CAECustomS3ConfigInfo{}}, customDeployer: &fakeDeploy{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(signals.WaitForSignal, func() <-chan struct{} { return stopChan }),
					ApplyFunc(commonutils.IsCAEFunc, func(businessType string) bool { return true }),
					ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) { return watcher, nil }),
					ApplyFunc((*fsnotify.Watcher).Add, func(w *fsnotify.Watcher, name string) error {
						triggerChan <- struct{}{}
						return nil
					}),
					ApplyFunc((*fsnotify.Watcher).Close, func(watcher *fsnotify.Watcher) error {
						finishChan <- struct{}{}
						return nil
					}),
					ApplyFunc((*Server).uploadDumpFile, func(_ *Server, dumpFileDir string) {
						count += 1
					}),
				})
				return patches
			},
			executeCount: 1,
		},
	}
	for _, tt := range tests {
		count = 0
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			go tt.s.dumpFileNotify()
			<-triggerChan
			time.Sleep(1 * time.Second)
			eventChan <- fsnotify.Event{Op: fsnotify.Create, Name: "heapDump.hprof"}
			time.Sleep(1 * time.Millisecond)
			errChan <- errors.New("error")
			stopChan <- struct{}{}
			t.Logf("send chan finished")
			close(stopChan)
			<-finishChan
			patches.ResetAll()
			if count != tt.executeCount {
				t.Errorf("error, count: %d, executeCount: %d", count, tt.executeCount)
			}
		})
	}
}

func Test_prepareCaePoolPodUserMountFiles(t *testing.T) {
	convey.Convey("Test_prepareCaePoolPodUserMountFiles ok", t, func() {
		s3Deployer := &obs.S3Deployer{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(obs.NewDeployer, func(s3Config *types.S3Config) (*obs.S3Deployer, error) {
				return s3Deployer, nil
			}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("s3Deployer"), nil
			}),
			gomonkey.ApplyFunc(commonutils.WriteFileToPathWithMode, func(writePath string, buffer []byte, perm fs.FileMode) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(s3Deployer), "DownLoadFromObs",
				func(s *obs.S3Deployer, bucketID string, objectID string) ([]byte, error) {
					return []byte("aprilyolies"), nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fakeDeployObject := &fakeDeploy{}
		server := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					StreamResponse: true,
					Language:       "go",
					UsePoolPod:     true,
					FileMountConfig: types.FileMountConfig{
						S3Config: &types.S3Config{},
						MountFiles: []*types.MountFile{
							{"Appid", "BucketID", "ObjectID", "BucketURL", "FilePath"},
						},
						SecretFiles: []*types.SecretFileMount{
							{
								EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
								FileMountPath:        "/tmp/abc.txt",
							},
						},
					},
				},
			},
			deployer: fakeDeployObject,
		}
		err := server.prepareCaePoolPodUserMountFiles()
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test_prepareCaePoolPodUserMountFiles not UsePoolPod", t, func() {
		fakeDeployObject := &fakeDeploy{}
		server := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					StreamResponse: true,
					Language:       "go",
					FileMountConfig: types.FileMountConfig{
						S3Config: &types.S3Config{},
						MountFiles: []*types.MountFile{
							{"Appid", "BucketID", "ObjectID", "BucketURL", "FilePath"},
						},
						SecretFiles: []*types.SecretFileMount{
							{
								EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
								FileMountPath:        "/tmp/abc.txt",
							},
						},
					},
				},
			},
			deployer: fakeDeployObject,
		}
		err := server.prepareCaePoolPodUserMountFiles()
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test_prepareCaePoolPodUserMountFiles NewDeployer error", t, func() {
		s3Deployer := &obs.S3Deployer{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(obs.NewDeployer, func(s3Config *types.S3Config) (*obs.S3Deployer, error) {
				return s3Deployer, errors.New("NewDeployer error")
			}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("s3Deployer"), nil
			}),
			gomonkey.ApplyFunc(commonutils.WriteFileToPathWithMode, func(writePath string, buffer []byte, perm fs.FileMode) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(s3Deployer), "DownLoadFromObs",
				func(s *obs.S3Deployer, bucketID string, objectID string) ([]byte, error) {
					return []byte("aprilyolies"), nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fakeDeployObject := &fakeDeploy{}
		server := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					StreamResponse: true,
					Language:       "go",
					UsePoolPod:     true,
					FileMountConfig: types.FileMountConfig{
						S3Config: &types.S3Config{},
						MountFiles: []*types.MountFile{
							{"Appid", "BucketID", "ObjectID", "BucketURL", "FilePath"},
						},
						SecretFiles: []*types.SecretFileMount{
							{
								EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
								FileMountPath:        "/tmp/abc.txt",
							},
						},
					},
				},
			},
			deployer: fakeDeployObject,
		}
		err := server.prepareCaePoolPodUserMountFiles()
		convey.So(err.Error(), convey.ShouldEqual, "NewDeployer error")
	})
	convey.Convey("Test_prepareCaePoolPodUserMountFiles DownLoadFromObs error", t, func() {
		s3Deployer := &obs.S3Deployer{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(obs.NewDeployer, func(s3Config *types.S3Config) (*obs.S3Deployer, error) {
				return s3Deployer, nil
			}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("s3Deployer"), nil
			}),
			gomonkey.ApplyFunc(commonutils.WriteFileToPathWithMode, func(writePath string, buffer []byte, perm fs.FileMode) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(s3Deployer), "DownLoadFromObs",
				func(s *obs.S3Deployer, bucketID string, objectID string) ([]byte, error) {
					return []byte("aprilyolies"), errors.New("DownLoadFromObs error")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fakeDeployObject := &fakeDeploy{}
		server := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					StreamResponse: true,
					Language:       "go",
					UsePoolPod:     true,
					FileMountConfig: types.FileMountConfig{
						S3Config: &types.S3Config{},
						MountFiles: []*types.MountFile{
							{"Appid", "BucketID", "ObjectID", "BucketURL", "FilePath"},
						},
						SecretFiles: []*types.SecretFileMount{
							{
								EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
								FileMountPath:        "/tmp/abc.txt",
							},
						},
					},
				},
			},
			deployer: fakeDeployObject,
		}
		err := server.prepareCaePoolPodUserMountFiles()
		convey.So(strings.Contains(err.Error(), "failed to download file FilePath"), convey.ShouldBeTrue)
	})
	convey.Convey("Test_prepareCaePoolPodUserMountFiles WriteFileToPathWithMode error", t, func() {
		s3Deployer := &obs.S3Deployer{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(obs.NewDeployer, func(s3Config *types.S3Config) (*obs.S3Deployer, error) {
				return s3Deployer, nil
			}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("s3Deployer"), nil
			}),
			gomonkey.ApplyFunc(commonutils.WriteFileToPathWithMode, func(writePath string, buffer []byte, perm fs.FileMode) error {
				return errors.New("WriteFileToPathWithMode error")
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(s3Deployer), "DownLoadFromObs",
				func(s *obs.S3Deployer, bucketID string, objectID string) ([]byte, error) {
					return []byte("aprilyolies"), nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fakeDeployObject := &fakeDeploy{}
		server := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					StreamResponse: true,
					Language:       "go",
					UsePoolPod:     true,
					FileMountConfig: types.FileMountConfig{
						S3Config: &types.S3Config{},
						MountFiles: []*types.MountFile{
							{"Appid", "BucketID", "ObjectID", "BucketURL", "FilePath"},
						},
						SecretFiles: []*types.SecretFileMount{
							{
								EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
								FileMountPath:        "/tmp/abc.txt",
							},
						},
					},
				},
			},
			deployer: fakeDeployObject,
		}
		err := server.prepareCaePoolPodUserMountFiles()
		convey.So(strings.Contains(err.Error(), "WriteFileToPathWithMode error"), convey.ShouldBeTrue)
	})
	convey.Convey("Test_prepareCaePoolPodUserMountFiles Decrypt error", t, func() {
		s3Deployer := &obs.S3Deployer{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(obs.NewDeployer, func(s3Config *types.S3Config) (*obs.S3Deployer, error) {
				return s3Deployer, nil
			}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte("s3Deployer"), errors.New("Decrypt error")
			}),
			gomonkey.ApplyFunc(commonutils.WriteFileToPathWithMode, func(writePath string, buffer []byte, perm fs.FileMode) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(s3Deployer), "DownLoadFromObs",
				func(s *obs.S3Deployer, bucketID string, objectID string) ([]byte, error) {
					return []byte("aprilyolies"), nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fakeDeployObject := &fakeDeploy{}
		server := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					StreamResponse: true,
					Language:       "go",
					UsePoolPod:     true,
					FileMountConfig: types.FileMountConfig{
						S3Config: &types.S3Config{},
						SecretFiles: []*types.SecretFileMount{
							{
								EncryptedFileContent: "0000000100000001E76E7B9552B4B7DAA1AE6EE5ADA170CB681E8FFA23D459CD6B6EFC4376F617A4",
								FileMountPath:        "/tmp/abc.txt",
							},
						},
					},
				},
			},
			deployer: fakeDeployObject,
		}
		err := server.prepareCaePoolPodUserMountFiles()
		convey.So(strings.Contains(err.Error(), "Decrypt error"), convey.ShouldBeTrue)
	})
}

func Test_waitCodeDeployOver(t *testing.T) {
	Convey("Test_waitCodeDeployOver", t, func() {
		timer := time.NewTimer(time.Millisecond)
		defer timer.Stop()
		count := 0
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				count++
				return timer
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		waitCodeDeployOver(&config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				RunningMode: constants.RunningModeRestore,
				UsePoolPod:  true,
			},
		})
		convey.So(count, convey.ShouldEqual, 1)
	})
}

func Test_setSnapshotConfig(t *testing.T) {
	testCases := []struct {
		name        string
		rtCfg       *config.Configuration
		msg         *types.FunctionSpecRequest
		patchesFunc mockUtils.PatchesFunc
		want        string
	}{
		{
			name:  "test cae func runtime image tag is empty",
			rtCfg: &config.Configuration{},
			msg: &types.FunctionSpecRequest{
				Message: commontypes.FunctionMessage{
					Function:           commontypes.FunctionSpec{BusinessType: "CAE"},
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
			want: "",
		},
		{
			name:  "test fass func",
			rtCfg: &config.Configuration{},
			msg: &types.FunctionSpecRequest{
				Message: commontypes.FunctionMessage{
					Function: commontypes.FunctionSpec{BusinessType: ""},
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						ObjectID: "test.VERSION1.1001",
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "1.1001"
					}),
				})
				return patches
			},
			want: "test.VERSION1.1001",
		},
		{
			name:  "test cae func runtime image tag is different and size is also different",
			rtCfg: &config.Configuration{},
			msg: &types.FunctionSpecRequest{
				Message: commontypes.FunctionMessage{
					Function: commontypes.FunctionSpec{BusinessType: "CAE"},
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						ObjectID: "test.VERSION2.1001",
						SnapshotVersions: []commontypes.SnapshotVersion{
							{
								SnapshotCheckInfo: commontypes.SnapshotCheckInfo{JavaRuntimeSize: 20},
								SnapshotImageTag:  "2.1001",
								ObjectID:          "test.VERSION2.1001",
								SnapshotSize:      200,
							},
							{
								SnapshotCheckInfo: commontypes.SnapshotCheckInfo{JavaRuntimeSize: 20},
								SnapshotImageTag:  "1.1002",
								ObjectID:          "test.VERSION2.1002",
								SnapshotSize:      100,
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == constants.RuntimeManagerSize {
							return "10"
						}
						return "1.1001"
					}),
				})
				return patches
			},
			want: "",
		},
		{
			name:  "test cae func do checkpoint",
			rtCfg: &config.Configuration{},
			msg: &types.FunctionSpecRequest{
				Message: commontypes.FunctionMessage{
					Function: commontypes.FunctionSpec{BusinessType: "CAE"},
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						ObjectID: "test.VERSION1.1001",
						SnapshotVersions: []commontypes.SnapshotVersion{
							{
								ObjectID:     "",
								SnapshotSize: 0,
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == constants.RuntimeManagerSize {
							return "10"
						}
						return "1.1001"
					}),
				})
				return patches
			},
			want: "test.VERSION1.1001",
		},
		{
			name:  "test cae func runtime image tag match with tag",
			rtCfg: &config.Configuration{},
			msg: &types.FunctionSpecRequest{
				Message: commontypes.FunctionMessage{
					Function: commontypes.FunctionSpec{BusinessType: "CAE"},
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						ObjectID: "test.VERSION2.1001",
						SnapshotVersions: []commontypes.SnapshotVersion{
							{
								SnapshotCheckInfo: commontypes.SnapshotCheckInfo{JavaRuntimeSize: 20},
								SnapshotImageTag:  "2.1001",
								ObjectID:          "test.VERSION2.1001",
								SnapshotSize:      200,
							},
							{
								SnapshotCheckInfo: commontypes.SnapshotCheckInfo{JavaRuntimeSize: 10},
								SnapshotImageTag:  "1.1001",
								ObjectID:          "test.VERSION1.1001",
								SnapshotSize:      100,
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == constants.RuntimeManagerSize {
							return "10"
						}
						return "1.1001"
					}),
				})
				return patches
			},
			want: "test.VERSION1.1001",
		},
		{
			name:  "test cae func runtime snapshot match with size",
			rtCfg: &config.Configuration{},
			msg: &types.FunctionSpecRequest{
				Message: commontypes.FunctionMessage{
					Function: commontypes.FunctionSpec{BusinessType: "CAE"},
					SnapshotDeployment: commontypes.SnapshotDeploymentSpec{
						ObjectID: "test.VERSION2.1001",
						SnapshotVersions: []commontypes.SnapshotVersion{
							{
								SnapshotCheckInfo: commontypes.SnapshotCheckInfo{JavaRuntimeSize: 20},
								SnapshotImageTag:  "2.1001",
								ObjectID:          "test.VERSION2.1001",
								SnapshotSize:      100,
							},
							{
								ObjectID:          "test.VERSION1.1002",
								SnapshotCheckInfo: commontypes.SnapshotCheckInfo{JavaRuntimeSize: 10},
								SnapshotImageTag:  "1.1002",
								SnapshotSize:      200,
							},
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == constants.RuntimeManagerSize {
							return "10"
						}
						return "1.1001"
					}),
				})
				return patches
			},
			want: "test.VERSION1.1002",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			setSnapshotConfig(tt.rtCfg, tt.msg)
			assert.Equal(t, tt.want, tt.rtCfg.SnapshotConfig.SnapshotObjectID)
			patches.ResetAll()
		})
	}
}

func Test_setStorageConfig(t *testing.T) {
	convey.Convey("test setStorageConfig", t, func() {
		convey.Convey("case1: success, enableTenantAuth is true", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.EnableTenantAuth, func() bool {
					return true
				}),
			})
			defer patches.ResetAll()
			functionSpecRequest := types.FunctionSpecRequest{}
			body, _ := json.Marshal(functionSpecRequest)
			req, _ := http.NewRequest("POST", "http://example.com", bytes.NewBuffer(body))
			server := &Server{conf: &config.Configuration{}, invokeServer: &invoke.Server{}}
			server.conf.URN = "sn:cn:yrk:123456789:function:0@default@funcA:latest"
			err := server.setStorageConfig(req)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case2: success, enableTenantAuth is false", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.EnableTenantAuth, func() bool {
					return false
				}),
			})
			defer patches.ResetAll()
			functionSpecRequest := types.FunctionSpecRequest{}
			body, _ := json.Marshal(functionSpecRequest)
			req, _ := http.NewRequest("POST", "http://example.com", bytes.NewBuffer(body))
			server := &Server{conf: &config.Configuration{}, invokeServer: &invoke.Server{}}
			server.conf.URN = "sn:cn:yrk:123456789:function:0@default@funcA:latest"
			err := server.setStorageConfig(req)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case3: failed to parse urn", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.EnableTenantAuth, func() bool {
					return true
				}),
			})
			defer patches.ResetAll()
			functionSpecRequest := types.FunctionSpecRequest{}
			body, _ := json.Marshal(functionSpecRequest)
			req, _ := http.NewRequest("POST", "http://example.com", bytes.NewBuffer(body))
			server := &Server{conf: &config.Configuration{}, invokeServer: &invoke.Server{}}
			server.conf.URN = "xxx"
			err := server.setStorageConfig(req)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestHandleTenantCommands(t *testing.T) {
	var runtimeClientLock sync.Mutex
	t.Run("success", func(t *testing.T) {
		runtimeClientLock.Lock()
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return []byte(""), nil
			}),
			gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithTimeout, func(t *tcpmsg.SocketConn,
				msg *tcpmsg.Message, timeout time.Duration) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{Response: tcpmsg.RuntimeRSP{}}
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		s := &Server{conf: &config.Configuration{}}
		bodyValue, _ := json.Marshal([]byte("test"))
		request := func() *http.Request {
			req := httptest.NewRequest("POST", constants.ExecuteTenantCommands, bytes.NewReader(bodyValue))
			req.Header.Set(constants.HeaderTraceID, "test")
			return req
		}()
		response := httptest.NewRecorder()

		s.handleTenantCommands(response, request)

		assert.Equal(t, response.Code, http.StatusOK)
		runtimeClientLock.Unlock()
	})
	t.Run("RuntimeManagerClient is nil", func(t *testing.T) {
		runtimeClientLock.Lock()
		runtime.RuntimeManagerClient = nil

		s := &Server{conf: &config.Configuration{}}
		bodyValue, _ := json.Marshal([]byte("test"))
		request := func() *http.Request {
			req := httptest.NewRequest("POST", constants.ExecuteTenantCommands, bytes.NewReader(bodyValue))
			req.Header.Set(constants.HeaderTraceID, "test")
			return req
		}()
		response := httptest.NewRecorder()

		s.handleTenantCommands(response, request)

		assert.Equal(t, response.Code, http.StatusInternalServerError)
		runtimeClientLock.Unlock()
	})
	t.Run("read body error", func(t *testing.T) {
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return []byte(""), errors.New("test error")
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		s := &Server{conf: &config.Configuration{}}
		bodyValue, _ := json.Marshal([]byte("test"))
		request := func() *http.Request {
			req := httptest.NewRequest("POST", constants.ExecuteTenantCommands, bytes.NewReader(bodyValue))
			req.Header.Set(constants.HeaderTraceID, "test")
			return req
		}()
		response := httptest.NewRecorder()

		s.handleTenantCommands(response, request)

		assert.Equal(t, response.Code, http.StatusInternalServerError)
	})
	t.Run("resp error", func(t *testing.T) {
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return []byte(""), nil
			}),
			gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithTimeout, func(t *tcpmsg.SocketConn,
				msg *tcpmsg.Message, timeout time.Duration) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{Err: "test error"}
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		s := &Server{conf: &config.Configuration{}}
		bodyValue, _ := json.Marshal([]byte("test"))
		request := func() *http.Request {
			req := httptest.NewRequest("POST", constants.ExecuteTenantCommands, bytes.NewReader(bodyValue))
			req.Header.Set(constants.HeaderTraceID, "test")
			return req
		}()
		response := httptest.NewRecorder()

		s.handleTenantCommands(response, request)

		assert.Equal(t, response.Code, http.StatusInternalServerError)
	})
	t.Run("Unmarshal fail", func(t *testing.T) {
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return []byte(""), nil
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return errors.New("test error")
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		s := &Server{conf: &config.Configuration{}}
		bodyValue, _ := json.Marshal([]byte("test"))
		request := func() *http.Request {
			req := httptest.NewRequest("POST", constants.ExecuteTenantCommands, bytes.NewReader(bodyValue))
			req.Header.Set(constants.HeaderTraceID, "test")
			return req
		}()
		response := httptest.NewRecorder()

		s.handleTenantCommands(response, request)

		assert.Equal(t, response.Code, http.StatusInternalServerError)
	})
}

func Test_updateModifyTime(t *testing.T) {
	convey.Convey("test updateModifyTime", t, func() {
		isCalled := false
		stopCh := make(chan struct{})
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			ApplyFunc(signals.WaitForSignal, func() <-chan struct{} {
				return stopCh
			}),
			ApplyFunc(updateLogConfigFile, func(_ bool) {
				isCalled = true
			}),
		})
		defer patches.ResetAll()
		go updateModifyTime()
		time.Sleep(1 * time.Second)
		close(stopCh)
		time.Sleep(1 * time.Second)
		convey.So(isCalled, convey.ShouldBeFalse)
	})
}

func Test_checkNeedDeleteSnapshotFromHeader(t *testing.T) {
	convey.Convey("test checkNeedDeleteSnapshotFromHeader", t, func() {
		convey.Convey("test header['need_delete_snapshot'] is true", func() {
			s := &Server{conf: &config.Configuration{}}
			r := func() *http.Request {
				req := httptest.NewRequest("POST", "/load", nil)
				req.Header.Set(constants.NeedDeleteSnapshot, "true")
				return req
			}()
			flag := s.checkNeedDeleteSnapshotFromHeader(r)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test header['need_delete_snapshot'] is not true", func() {
			s := &Server{conf: &config.Configuration{}}
			r := func() *http.Request {
				req := httptest.NewRequest("POST", "/load", nil)
				req.Header.Set(constants.NeedDeleteSnapshot, "false")
				return req
			}()
			flag := s.checkNeedDeleteSnapshotFromHeader(r)
			convey.So(flag, convey.ShouldBeFalse)
		})

		convey.Convey("test no 'need_delete_snapshot' in header", func() {
			s := &Server{conf: &config.Configuration{}}
			r := func() *http.Request {
				req := httptest.NewRequest("POST", "/load", nil)
				return req
			}()
			flag := s.checkNeedDeleteSnapshotFromHeader(r)
			convey.So(flag, convey.ShouldBeFalse)
		})

	})
}
