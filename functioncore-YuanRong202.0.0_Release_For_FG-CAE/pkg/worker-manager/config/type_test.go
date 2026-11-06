package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	mockUtils "functioncore/test/utils"
)

func Test_addPoolContainerTags(t *testing.T) {
	type args struct {
		config *FaasConfig
	}

	tests := []struct {
		name        string
		args        args
		want        *FaasConfig
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1", args{&FaasConfig{}}, &FaasConfig{
			PoolMgrConfig: PoolMgrConfig{
				PoolContainerTags: PoolContainerImageTags{
					WorkerInitImageTag: "worker-init:0000",
					WorkerImageTag:     "worker:0000",
					RuntimeImageTag:    "runtime-manager:0000",
				},
			},
		}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					if key == "WORKER_INIT_TAG" {
						return "worker-init:0000"
					}
					if key == "WORKER_TAG" {
						return "worker:0000"
					}
					if key == "RUNTIME_MANAGER_TAG" {
						return "runtime-manager:0000"
					}
					v, _ := syscall.Getenv(key)
					return v
				}),
			})
			return patches
		}},
		{"case2 get env WORKER_INIT_TAG nil", args{&FaasConfig{}}, &FaasConfig{},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == "WORKER_INIT_TAG" {
							return ""
						}
						v, _ := syscall.Getenv(key)
						return v
					}),
				})
				return patches
			}},
		{"case3 get env WORKER_TAG nil", args{&FaasConfig{}}, &FaasConfig{
			PoolMgrConfig: PoolMgrConfig{
				PoolContainerTags: PoolContainerImageTags{
					WorkerInitImageTag: "worker-init:0000",
				},
			},
		},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == "WORKER_INIT_TAG" {
							return "worker-init:0000"
						}
						if key == "WORKER_TAG" {
							return ""
						}
						v, _ := syscall.Getenv(key)
						return v
					}),
				})
				return patches
			}},
		{"case4 get env RUNTIME_MANAGER_TAG nil", args{&FaasConfig{}}, &FaasConfig{
			PoolMgrConfig: PoolMgrConfig{
				PoolContainerTags: PoolContainerImageTags{
					WorkerInitImageTag: "worker-init:0000",
					WorkerImageTag:     "worker:0000",
				},
			},
		},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == "WORKER_INIT_TAG" {
							return "worker-init:0000"
						}
						if key == "WORKER_TAG" {
							return "worker:0000"
						}
						if key == "RUNTIME_MANAGER_TAG" {
							return ""
						}
						v, _ := syscall.Getenv(key)
						return v
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			err := addPoolContainerTags(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("addPoolContainerTags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.args.config, tt.want) {
				t.Errorf("addPoolContainerTags() got = %v, want %v", tt.args.config, tt.want)
			}
			patches.ResetAll()

		})
	}
}

func TestEnableHTTPS(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		want        bool
	}{
		{"case1 succeed to enable https", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(GetConfig, func() (*FaasConfig, error) {
					return &FaasConfig{
						HTTPSConfig: &tls.InternalHTTPSConfig{
							HTTPSEnable: true,
							TLSProtocol: "TLS",
							TLSCiphers:  "TLS",
						},
					}, nil
				}),
				gomonkey.ApplyFunc(tls.InitTLSConfig, func(tlsProtocols string, tlsCiphers string) error {
					return nil
				}),
			})
			return patches
		}, true},
		{"case2 get config error", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(GetConfig, func() (*FaasConfig, error) {
					return &FaasConfig{
						HTTPSConfig: &tls.InternalHTTPSConfig{
							HTTPSEnable: true,
							TLSProtocol: "TLS",
							TLSCiphers:  "TLS",
						},
					}, errors.New("get config error")
				}),
			})
			return patches
		}, false},
		{"case3 HTTPSConfig nil", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(GetConfig, func() (*FaasConfig, error) {
					return &FaasConfig{}, nil
				}),
			})
			return patches
		}, false},
		{"case4 init tls config error", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(GetConfig, func() (*FaasConfig, error) {
					return &FaasConfig{
						HTTPSConfig: &tls.InternalHTTPSConfig{
							HTTPSEnable: true,
							TLSProtocol: "TLS",
							TLSCiphers:  "TLS",
						},
					}, nil
				}),
				gomonkey.ApplyFunc(tls.InitTLSConfig, func(tlsProtocols string, tlsCiphers string) error {
					return errors.New("init tls config error")
				}),
			})
			return patches
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := EnableHTTPS(); got != tt.want {
				t.Errorf("EnableHTTPS() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestGetLocalAuth(t *testing.T) {
	tests := []struct {
		name string
		want *localauth.AuthConfig
	}{
		{"case1", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetLocalAuth(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetLocalAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetK8sCidr(t *testing.T) {
	tests := []struct {
		name string
		want *K8sCidr
	}{
		{"case1", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetK8sCidr(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetK8sCidr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetEnableRemoteWorkerFlag(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "config is nil",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetEnableRemoteWorkerFlag(); got != tt.want {
				t.Errorf("GetEnableRemoteWorkerFlag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetHotloadMonitorConf(t *testing.T) {
	tests := []struct {
		name    string
		want    string
		want2   redisclient.TimeoutConf
		wantErr bool
	}{
		{
			name:    "case01 config is nil",
			want:    "",
			want2:   redisclient.GetDefaultTimeoutConf(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, err := GetHotloadMonitorConf()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetHotloadMonitorConf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got1 != tt.want {
				t.Errorf("GetHotloadMonitorConf() got = %v, want %v", got1, tt.want)
			}
			if got2 != tt.want2 {
				t.Errorf("GetHotloadMonitorConf() got = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_GetConfig(t *testing.T) {
	patches := gomonkey.ApplyFunc(newConfig, func(key string) (*FaasConfig, error) {
		return &FaasConfig{
			LocalAuth: &localauth.AuthConfig{
				AKey:     "000",
				SKey:     "000",
				Duration: 5,
			},
			K8sCidr: K8sCidr{
				SvcCidr:  "000",
				PodCidr:  "000",
				HostCidr: "000",
			},
			PoolMgrConfig: PoolMgrConfig{
				PoolList: PoolList{
					HostAliases: []corev1.HostAlias{
						{IP: "127.0.0.1"},
					},
					PodGcTime: 5,
					Pool: []PoolConfig{
						{
							Name:          "a",
							RunTimeName:   "a",
							LimitCPU:      1,
							LimitMemory:   1000,
							RequestCPU:    1,
							RequestMemory: 1000,
							Annotations: map[string]string{
								"a": "a",
							},
						},
					},
				},
				VolumeConfig: VolumeConfig{
					LogMountPath:        "a",
					RuntimeLogMountPath: "a",
				},
			},
			RouterETCD:    &etcd3.ClientConfig{User: "root"},
			Etcd:          &etcd3.ClientConfig{User: "root"},
			CAERouterEtcd: &etcd3.ClientConfig{User: "root"},
		}, nil
	})
	defer patches.Reset()

	tests := []struct {
		name    string
		want    *FaasConfig
		wantErr bool
	}{
		{"case1", &FaasConfig{
			LocalAuth: &localauth.AuthConfig{
				AKey:     "000",
				SKey:     "000",
				Duration: 5,
			},
			K8sCidr: K8sCidr{
				SvcCidr:  "000",
				PodCidr:  "000",
				HostCidr: "000",
			},
			PoolMgrConfig: PoolMgrConfig{
				PoolList: PoolList{
					HostAliases: []corev1.HostAlias{
						{IP: "127.0.0.1"},
					},
					PodGcTime: 5,
					Pool: []PoolConfig{
						{
							Name:          "a",
							RunTimeName:   "a",
							LimitCPU:      1,
							LimitMemory:   1000,
							RequestCPU:    1,
							RequestMemory: 1000,
							Annotations: map[string]string{
								"a": "a",
							},
						},
					},
				},
				VolumeConfig: VolumeConfig{
					LogMountPath:        "a",
					RuntimeLogMountPath: "a",
				},
			},
			RouterETCD:    &etcd3.ClientConfig{User: "root"},
			Etcd:          &etcd3.ClientConfig{User: "root"},
			CAERouterEtcd: &etcd3.ClientConfig{User: "root"},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetConfig() got = %v, want %v", got, tt.want)
			}
			t.Log(got.String())
		})
	}
}

func Test_parseRadicalRedirectTimeout(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		want        time.Duration
	}{
		{"case1 succeed to get env RADICAL_REDIRECT_TIMEOUT",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return "2000"
						}),
				})
				return patches
			}, time.Duration(2000) * time.Millisecond},
		{"case2  env RADICAL_REDIRECT_TIMEOUT strToInt error",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return "test"
						}),
				})
				return patches
			}, defaultRadicalRedirectTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := parseRadicalRedirectTimeout(); got != tt.want {
				t.Errorf("parseRadicalRedirectTimeout() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_parseRadicalAutoscaler(t *testing.T) {
	tests := []struct {
		name        string
		want        bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 DISABLE_RADICAL_AUTOSCALER true", false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return "true"
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := parseRadicalAutoscaler(); got != tt.want {
				t.Errorf("parseRadicalAutoscaler() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestGetRadicalRedirectTimeout(t *testing.T) {
	tests := []struct {
		name string
		want time.Duration
	}{
		{"case get defaultRadicalRedirectTimeout", defaultRadicalRedirectTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetRadicalRedirectTimeout(); got != tt.want {
				t.Errorf("GetRadicalRedirectTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetElasticScalingConfig(t *testing.T) {
	tests := []struct {
		name        string
		want        *ElasticScalingConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to json Marshal", defaultElasticScalingConfig,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return "true"
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			configInstance = &FaasConfig{
				ElasticScalingConfig: defaultElasticScalingConfig,
			}
			if got := GetElasticScalingConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetElasticScalingConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestHotLoadPoolMgrProactiveConfig(t *testing.T) {
	type args struct {
		curConfig    *FaasConfig
		newMgrConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		want *FaasConfig
		args args
	}{
		{
			name: "case 1 skip to update pool manager proactive config because of nil of hot load config",
			want: &FaasConfig{
				PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
					UsedPodRecordResetAndUploadInterval: 10,
				},
			},
			args: args{
				newMgrConfig: nil,
				curConfig: &FaasConfig{
					PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
						UsedPodRecordResetAndUploadInterval: 10,
					},
				},
			},
		}, {
			name: "case 2 succeed to update pool manager proactive config",
			want: &FaasConfig{
				PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
					UsedPodRecordResetAndUploadInterval:  1,
					UsedPodRecordExpiration:              24,
					PoolPredictionResultDownloadInterval: 10,
					PoolPredictionResultLength:           30,
					ApplyPredictionResultToPoolInterval:  2,
					ScalingPlanTimeWindow:                5,
					ReactiveScalingUpResetInterval:       15,
					ScalingUpTriggerTimeThreshold:        5,
					CooldownTime:                         15,
					EnableFusionPolicy:                   0,
					UpperBoundFactor:                     1.5,
					LowerBoundFactor:                     0.2,
					MaxScalingStep:                       30,
					ScalingPlanFactor:                    1.35,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
						UsedPodRecordResetAndUploadInterval:  1,
						UsedPodRecordExpiration:              24,
						PoolPredictionResultDownloadInterval: 10,
						PoolPredictionResultLength:           30,
						ApplyPredictionResultToPoolInterval:  2,
						ScalingPlanTimeWindow:                5,
						ReactiveScalingUpResetInterval:       15,
						ScalingUpTriggerTimeThreshold:        5,
						CooldownTime:                         15,
						EnableFusionPolicy:                   0,
						UpperBoundFactor:                     1.5,
						LowerBoundFactor:                     0.2,
						MaxScalingStep:                       30,
						ScalingPlanFactor:                    1.35,
					},
				},
				curConfig: &FaasConfig{
					PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
						UsedPodRecordResetAndUploadInterval:  1,
						UsedPodRecordExpiration:              8 * 24,
						PoolPredictionResultDownloadInterval: 10,
						PoolPredictionResultLength:           30,
						ApplyPredictionResultToPoolInterval:  1,
						ScalingPlanTimeWindow:                5,
						ReactiveScalingUpResetInterval:       15,
						ScalingUpTriggerTimeThreshold:        3,
						CooldownTime:                         15,
						EnableFusionPolicy:                   1,
						UpperBoundFactor:                     1.2,
						LowerBoundFactor:                     0.3,
						MaxScalingStep:                       30,
						ScalingPlanFactor:                    1.35,
					},
				},
			},
		}, {
			name: "case 3 skip updating if invalid value",
			want: &FaasConfig{
				PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
					UsedPodRecordResetAndUploadInterval: 9,
					ScalingPlanTimeWindow:               6,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
						UsedPodRecordResetAndUploadInterval: 9,
						ScalingPlanTimeWindow:               -5,
					},
				},
				curConfig: &FaasConfig{
					PoolMgrProactiveConfig: &PoolManagerProactiveElasticConfig{
						UsedPodRecordResetAndUploadInterval: 10,
						ScalingPlanTimeWindow:               6,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadPoolMgrProactiveConfig(tt.args.curConfig, tt.args.newMgrConfig)
			if !reflect.DeepEqual(tt.args.curConfig, tt.want) {
				t.Errorf("GetPoolManagerProactiveElasticConfig() = %v, want %v", tt.args.curConfig, tt.want)
			}
		})
	}
}

func TestHotLoadProactiveEnableList(t *testing.T) {
	type args struct {
		curConfig    *FaasConfig
		newMgrConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		want *FaasConfig
		args args
	}{
		{
			name: "case 1 skip to update pool manager proactive config because of nil of hot load config",
			want: &FaasConfig{
				ProactiveEnableList: map[string]bool{
					"pool23": true,
				},
			},
			args: args{
				newMgrConfig: nil,
				curConfig: &FaasConfig{
					ProactiveEnableList: map[string]bool{
						"pool23": true,
					},
				},
			},
		}, {
			name: "case 2 succeed to update pool manager proactive config",
			want: &FaasConfig{
				ProactiveEnableList: map[string]bool{
					"pool23": true,
					"pool24": true,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					ProactiveEnableList: map[string]bool{
						"pool23": true,
						"pool24": true,
					},
				},
				curConfig: &FaasConfig{
					ProactiveEnableList: map[string]bool{
						"pool23": true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadProactiveEnableList(tt.args.curConfig, tt.args.newMgrConfig)
			if !reflect.DeepEqual(tt.args.curConfig, tt.want) {
				t.Errorf("GetPoolManagerProactiveElasticConfig() = %v, want %v", tt.args.curConfig, tt.want)
			}
		})
	}
}

func TestHotLoadBodySizeLimitWhitelist(t *testing.T) {
	type args struct {
		curConfig    *FaasConfig
		newMgrConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		want *FaasConfig
		args args
	}{
		{
			name: "case 1 succeed to add BodySizeLimitWhitelist config",
			want: &FaasConfig{
				BodySizeLimitWhitelist: map[string]int64{
					"tenant001": 20,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 20,
					},
				},
				curConfig: &FaasConfig{},
			},
		}, {
			name: "case 2 succeed to update BodySizeLimitWhitelist config",
			want: &FaasConfig{
				BodySizeLimitWhitelist: map[string]int64{
					"tenant001": 20,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 20,
					},
				},
				curConfig: &FaasConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 10,
					},
				},
			},
		}, {
			name: "case 3 succeed to update same BodySizeLimitWhitelist config",
			want: &FaasConfig{
				BodySizeLimitWhitelist: map[string]int64{
					"tenant001": 20,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 20,
					},
				},
				curConfig: &FaasConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 20,
					},
				},
			},
		}, {
			name: "case 4 skip invalid BodySizeLimitWhitelist config",
			want: &FaasConfig{
				BodySizeLimitWhitelist: map[string]int64{},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 200,
					},
				},
				curConfig: &FaasConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 20,
					},
				},
			},
		}, {
			name: "case 5 succeed to delete BodySizeLimitWhitelist config",
			want: &FaasConfig{
				BodySizeLimitWhitelist: map[string]int64{},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{},
				curConfig: &FaasConfig{
					BodySizeLimitWhitelist: map[string]int64{
						"tenant001": 20,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadBodySizeLimitWhitelist(tt.args.curConfig, tt.args.newMgrConfig)
			if !reflect.DeepEqual(tt.args.curConfig, tt.want) {
				t.Errorf("GetPoolManagerProactiveElasticConfig() = %v, want %v", tt.args.curConfig, tt.want)
			}
		})
	}
}

func TestHotLoadProactiveElasticScalingConfig(t *testing.T) {
	type args struct {
		curConfig    *FaasConfig
		newMgrConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		want *FaasConfig
		args args
	}{
		{
			name: "case 1 skip to update pool manager proactive config because of nil of hot load config",
			want: &FaasConfig{
				ProactiveElasticScalingConfig: &ElasticScalingConfig{
					UsedPodNumsExceedPercentage: 0.5,
				},
			},
			args: args{
				newMgrConfig: nil,
				curConfig: &FaasConfig{
					ProactiveElasticScalingConfig: &ElasticScalingConfig{
						UsedPodNumsExceedPercentage: 0.5,
					},
				},
			},
		}, {
			name: "case 2 succeed to update proactive scaling config",
			want: &FaasConfig{
				ProactiveElasticScalingConfig: &ElasticScalingConfig{
					UsedPodNumsExceedPercentage:      0.8,
					UsedPodNumsExceedFreePercentage:  0.8,
					UsedPodNumsExceedHeavyPercentage: 0.8,
					ScaleUpPodNumsPercentage:         0.3,
					ScaleUpPodNumsFreePercentage:     0.3,
					ScaleUpPodNumsHeavyPercentage:    0.3,
					ExtraScaleUpPodNumsPercentage:    0.2,
					EffectiveThresholdReplicas:       50,
					ThresholdResourceUsage:           0.8,
					MaxScaleUpPodNums:                500,
					UsedPodNumsBelowPercentage:       0.1,
					ScaleDownPodNumsPercentage:       0.125,
					ScaleDownInterval:                60,
					MaxTransitionScaleUpPercentage:   0.5,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					ProactiveElasticScalingConfig: &ElasticScalingConfig{
						UsedPodNumsExceedPercentage:      0.8,
						UsedPodNumsExceedFreePercentage:  0.8,
						UsedPodNumsExceedHeavyPercentage: 0.8,
						ScaleUpPodNumsPercentage:         0.3,
						ScaleUpPodNumsFreePercentage:     0.3,
						ScaleUpPodNumsHeavyPercentage:    0.3,
						ExtraScaleUpPodNumsPercentage:    0.2,
						EffectiveThresholdReplicas:       50,
						ThresholdResourceUsage:           0.8,
						MaxScaleUpPodNums:                500,
						UsedPodNumsBelowPercentage:       0.1,
						ScaleDownPodNumsPercentage:       0.125,
						ScaleDownInterval:                60,
						MaxTransitionScaleUpPercentage:   0.5,
					},
				},
				curConfig: &FaasConfig{
					ProactiveElasticScalingConfig: &ElasticScalingConfig{
						UsedPodNumsExceedPercentage:      0.9,
						UsedPodNumsExceedFreePercentage:  0.9,
						UsedPodNumsExceedHeavyPercentage: 0.9,
						ScaleUpPodNumsPercentage:         0.15,
						ScaleUpPodNumsFreePercentage:     0.15,
						ScaleUpPodNumsHeavyPercentage:    0.15,
						ExtraScaleUpPodNumsPercentage:    0.1,
						EffectiveThresholdReplicas:       0,
						ThresholdResourceUsage:           0.75,
						MaxScaleUpPodNums:                600,
						UsedPodNumsBelowPercentage:       0.1,
						ScaleDownPodNumsPercentage:       0.125,
						ScaleDownInterval:                60,
						MaxTransitionScaleUpPercentage:   0.5,
					},
				},
			},
		}, {
			name: "case 3 skip updating if invalid value",
			want: &FaasConfig{
				ProactiveElasticScalingConfig: &ElasticScalingConfig{
					UsedPodNumsExceedPercentage:     0.8,
					UsedPodNumsExceedFreePercentage: 0.9,
				},
			},
			args: args{
				newMgrConfig: &HotLoadConfig{
					ProactiveElasticScalingConfig: &ElasticScalingConfig{
						UsedPodNumsExceedPercentage:     0.8,
						UsedPodNumsExceedFreePercentage: -0.9,
					},
				},
				curConfig: &FaasConfig{
					ProactiveElasticScalingConfig: &ElasticScalingConfig{
						UsedPodNumsExceedPercentage:     0.9,
						UsedPodNumsExceedFreePercentage: 0.9,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadProactiveElasticScalingConfig(tt.args.curConfig, tt.args.newMgrConfig)
			if !reflect.DeepEqual(tt.args.curConfig, tt.want) {
				t.Errorf("GetPoolManagerProactiveElasticConfig() = %v, want %v", tt.args.curConfig, tt.want)
			}
		})
	}
}

func TestGetPoolManagerProactiveElasticConfig(t *testing.T) {
	tests := []struct {
		name        string
		want        *PoolManagerProactiveElasticConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 succeed to unmarshal env PoolManagerProactiveElasticConfig string",
			want: &PoolManagerProactiveElasticConfig{
				UsedPodRecordResetAndUploadInterval:  2,
				UsedPodRecordExpiration:              24,
				PoolPredictionResultDownloadInterval: 15,
				PoolPredictionResultLength:           30,
				ApplyPredictionResultToPoolInterval:  1,
				ScalingPlanTimeWindow:                5,
				ReactiveScalingUpResetInterval:       15,
				ScalingUpTriggerTimeThreshold:        3,
				CooldownTime:                         20,
				EnableFusionPolicy:                   1,
				UpperBoundFactor:                     1.2,
				LowerBoundFactor:                     0.3,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return `{"usedPodRecordResetAndUploadInterval":2,"usedPodRecordExpiration":24,` +
								`"poolPredictionResultDownloadInterval":15,"poolPredictionResultLength":30,` +
								`"applyPredictionResultToPoolInterval":1,"scalingPlanTimeWindow":5,` +
								`"reactiveScalingUpResetInterval":15,"enableFusionPolicy":1,` +
								`"LowerBoundFactor":0.3,"scalingUpTriggerTimeThreshold":3,` +
								`"cooldownTime":20,"upperBoundFactor":1.2}`
						},
					),
				})
				return patches
			},
		}, {
			name: "case 2 failed to unmarshal empty env PoolManagerProactiveElasticConfig string",
			want: defaultPoolManagerProactiveElasticConfig,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		}, {
			name: "case 3 failed to unmarshal env string because of unmatched type",
			want: defaultPoolManagerProactiveElasticConfig,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return `{"scalingUpTriggerTimeThreshold":3.6}`
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getPoolManagerProactiveElasticConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPoolManagerProactiveElasticConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestGetProactiveElasticScalingConfig(t *testing.T) {
	tests := []struct {
		name        string
		want        *ElasticScalingConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 succeed to unmarshal env string",
			want: &ElasticScalingConfig{
				UsedPodNumsExceedPercentage:      0.8,
				UsedPodNumsExceedFreePercentage:  0.8,
				UsedPodNumsExceedHeavyPercentage: 0.8,
				ScaleUpPodNumsPercentage:         0.15,
				ScaleUpPodNumsFreePercentage:     0.15,
				ScaleUpPodNumsHeavyPercentage:    0.15,
				ExtraScaleUpPodNumsPercentage:    0.1,
				EffectiveThresholdReplicas:       0,
				ThresholdResourceUsage:           0.75,
				MaxScaleUpPodNums:                600,
				// turn off scaling down in proactive mode
				UsedPodNumsBelowPercentage: 0.1,
				ScaleDownPodNumsPercentage: 0.125,
				ScaleDownInterval:          60,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return `{"usedPodNumsExceedPercentage":0.8,` +
								`"scaleUpPodNumsPercentage":0.15,"extraScaleUpPodNumsPercentage":0.1,` +
								`"usedPodNumsBelowPercentage":0.1,"scaleDownPodNumsPercentage":0.125,` +
								`"effectiveThresholdReplicas":0,"maxScaleUpPodNums":600,"scaleDownInterval":60,` +
								`"thresholdResourceUsage":0.75,"usedPodNumsExceedFreePercentage":0.8,` +
								`"usedPodNumsExceedHeavyPercentage":0.8,"scaleUpPodNumsFreePercentage":0.15,` +
								`"scaleUpPodNumsHeavyPercentage":0.15,"maxTransitionScaleUpPercentage":0}`
						}),
				})
				return patches
			},
		}, {
			name: "case 2 failed to unmarshal empty env string",
			want: defaultProactiveElasticScalingConfig,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		}, {
			name: "case 3 failed to unmarshal env string because of unmatched type",
			want: defaultProactiveElasticScalingConfig,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return `{"usedPodNumsExceedPercentage":"0.8"}`
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getProactiveElasticScalingConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetElasticScalingConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestGetProactiveModePoolList(t *testing.T) {
	tests := []struct {
		name        string
		want        map[string]bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 succeed to unmarshal proactive mode pool list env string",
			want: map[string]bool{
				"pool22-300-128-fusion": true,
				"pool23-400-256-fusion": false,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return "{\"pool22-300-128-fusion\":true,\"pool23-400-256-fusion\":false}"
						}),
				})
				return patches
			},
		}, {
			name: "case 2 failed to unmarshal empty env string",
			want: defaultProactiveModePoolList,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		}, {
			name: "case 3 failed to unmarshal env string because of unmatched type",
			want: defaultProactiveModePoolList,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return "{\"pool23-400-256-fusion\":\"false\"}"
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getProactiveModePoolList(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetElasticScalingConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestWatchConfig(t *testing.T) {
	type args struct {
		stopCh   <-chan struct{}
		callback ChangedCallback
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case_01 normal scenario",
			args: args{make(<-chan struct{}), func() {}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		}, {
			name: "case_02 abnormal scenario",
			args: args{make(<-chan struct{}), func() {}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(monitor.CreateFileWatcher,
						func(stopCh <-chan struct{}) (monitor.FileWatcher, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := WatchConfig(tt.args.stopCh, tt.args.callback); (err != nil) != tt.wantErr {
				t.Errorf("WatchConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_hotLoadPoolSize(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	poolSizeConfig := make(map[string]int32, 1)
	poolSizeConfig["pool22"] = 20
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case_01 normal scenario",
			args: args{config: &FaasConfig{PoolMgrConfig: PoolMgrConfig{PoolList: PoolList{Pool: []PoolConfig{
				{
					Name:          "pool22",
					RunTimeName:   "fusion",
					LimitCPU:      1,
					LimitMemory:   1000,
					RequestCPU:    1,
					RequestMemory: 1000,
					PoolSize:      15,
					Annotations: map[string]string{
						"a": "a",
					},
				},
			}}}},
				hotLoadConfig: &HotLoadConfig{
					PoolSizeConfig:       poolSizeConfig,
					ReservedAntiAffinity: "weak",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadPoolSize(tt.args.config, tt.args.hotLoadConfig)
		})
	}
}

func Test_hotLoadElasticScalingConfig(t *testing.T) {
	convey.Convey("Test_hotLoadElasticScalingConfig", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		oldCfg := &FaasConfig{ElasticScalingConfig: defaultElasticScalingConfig}
		hotLoadCfg := &HotLoadConfig{
			ElasticScalingConfig: &ElasticScalingConfig{
				UsedPodNumsExceedPercentage:      1,
				ScaleUpPodNumsPercentage:         1,
				ExtraScaleUpPodNumsPercentage:    1,
				UsedPodNumsBelowPercentage:       1,
				ScaleDownPodNumsPercentage:       1,
				EffectiveThresholdReplicas:       1,
				MaxScaleUpPodNums:                1,
				ScaleDownInterval:                1,
				ThresholdResourceUsage:           1,
				UsedPodNumsExceedFreePercentage:  1,
				UsedPodNumsExceedHeavyPercentage: 1,
				ScaleUpPodNumsFreePercentage:     1,
				ScaleUpPodNumsHeavyPercentage:    1,
				MaxTransitionScaleUpPercentage:   1,
			},
		}
		setHotLoadConfig(oldCfg, hotLoadCfg)
		convey.So(oldCfg.ElasticScalingConfig.UsedPodNumsExceedPercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.ScaleUpPodNumsPercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.ExtraScaleUpPodNumsPercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.UsedPodNumsBelowPercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.ScaleDownPodNumsPercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.EffectiveThresholdReplicas, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.MaxScaleUpPodNums, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.ThresholdResourceUsage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.UsedPodNumsExceedFreePercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.UsedPodNumsExceedHeavyPercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.ScaleUpPodNumsFreePercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.ScaleUpPodNumsHeavyPercentage, convey.ShouldEqual, 1)
		convey.So(oldCfg.ElasticScalingConfig.MaxTransitionScaleUpPercentage, convey.ShouldEqual, 1)
	})
}

func Test_hotLoadResourceLockLeaseConfig(t *testing.T) {
	convey.Convey("Test_hotLoadResourceLockLeaseConfig", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		oldCfg := &FaasConfig{ResourceLockLeaseConfig: &ResourceLockLeaseConfig{
			LeaseDuration: 10,
			RenewDeadline: 10,
			RetryPeriod:   10,
		}}
		hotLoadCfg := &HotLoadConfig{
			ResourceLockLeaseConfig: &ResourceLockLeaseConfig{
				LeaseDuration: 20,
				RenewDeadline: 20,
				RetryPeriod:   20,
			},
		}
		setHotLoadConfig(oldCfg, hotLoadCfg)
		convey.So(oldCfg.ResourceLockLeaseConfig.LeaseDuration, convey.ShouldEqual, 20)
		convey.So(oldCfg.ResourceLockLeaseConfig.RenewDeadline, convey.ShouldEqual, 20)
		convey.So(oldCfg.ResourceLockLeaseConfig.RetryPeriod, convey.ShouldEqual, 20)
	})
}

func Test_hotLoadConfig(t *testing.T) {
	type args struct {
		filename string
		opType   monitor.OpType
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case_01 failed to get new hotload config file",
			args: args{"/home/sn/config/worker-manager.conf", 1},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, fmt.Errorf("error")
						}),
				})
				return patches
			},
		}, {
			name: "case_02 new hotload config file param check error",
			args: args{"/home/sn/config/worker-manager.conf", 1},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, fmt.Errorf("error")
						}),
				})
				return patches
			},
		}, {
			name: "case_03 normal scenario",
			args: args{"/home/sn/config/worker-manager.conf", 1},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, nil
						}),
					gomonkey.ApplyFunc(setHotLoadConfig,
						func(config *FaasConfig, newHotLoadConfig *HotLoadConfig) {
						}),
				})
				return patches
			},
		},
	}
	configChangedCallbacks = append(configChangedCallbacks, func() {})
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			hotLoadConfig(tt.args.filename, tt.args.opType)
		})
		patches.ResetAll()
	}
}

func Test_hotLoadReservedAntiAffinity(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case_01 normal scenario",
			args: args{config: &FaasConfig{PoolMgrConfig: PoolMgrConfig{ReservedAntiAffinity: "weak"}},
				hotLoadConfig: &HotLoadConfig{
					PoolSizeConfig:       nil,
					ReservedAntiAffinity: "test",
				},
			},
		},
	}
	configInstance = &FaasConfig{PoolMgrConfig: PoolMgrConfig{ReservedAntiAffinity: "false"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if configInstance != nil {
				hotLoadReservedAntiAffinity(tt.args.config, tt.args.hotLoadConfig)
			}
		})
	}
	configInstance = nil
}

func Test_newConfig(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:    "case01 read hot load config failed",
			args:    args{p: "/home/sn/config/worker-manager.conf"},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, fmt.Errorf("error")
						}),
				})
				return patches
			},
		}, {
			name:    "case02 add pool tags failed",
			args:    args{p: "/home/sn/config/worker-manager.conf"},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, nil
						}),
					gomonkey.ApplyFunc(setHotLoadConfig,
						func(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
						}),
					gomonkey.ApplyFunc(addPoolContainerTags,
						func(config *FaasConfig) error {
							return fmt.Errorf("error")
						}),
				})
				return patches
			},
		}, {
			name:    "case03 normal scenario",
			args:    args{p: "/home/sn/config/worker-manager.conf"},
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, nil
						}),
					gomonkey.ApplyFunc(setHotLoadConfig,
						func(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
						}),
					gomonkey.ApplyFunc(addPoolContainerTags,
						func(config *FaasConfig) error {
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
			_, err := newConfig(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("newConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_readHotLoadConfig(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:    "case01 failed to open file",
			args:    args{"/home/sn/config/worker-manager.conf"},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, fmt.Errorf("open error")
						}),
				})
				return patches
			},
		}, {
			name:    "case02 failed to read file",
			args:    args{"/home/sn/config/worker-manager.conf"},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return nil, fmt.Errorf("read error")
						}),
				})
				return patches
			},
		}, {
			name:    "case03 failed to unmarshal",
			args:    args{"/home/sn/config/worker-manager.conf"},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return []byte(""), nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal failed")
						}),
				})
				return patches
			},
		}, {
			name:    "case04 normal scenario",
			args:    args{"/home/sn/config/worker-manager.conf"},
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return []byte(""), nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
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
			_, err := readHotLoadConfig(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("readHotLoadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_setHotLoadConfig(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	poolSizeConfig := make(map[string]int32, 1)
	poolSizeConfig["pool22"] = 20
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case01 normal scenario",
			args: args{
				config: &FaasConfig{PoolMgrConfig: PoolMgrConfig{PoolList: PoolList{Pool: []PoolConfig{
					{
						Name:          "pool22",
						RunTimeName:   "fusion",
						LimitCPU:      1,
						LimitMemory:   1000,
						RequestCPU:    1,
						RequestMemory: 1000,
						PoolSize:      15,
						Annotations: map[string]string{
							"a": "a",
						},
					},
				}},
					ReservedAntiAffinity: "weak"}},
				hotLoadConfig: &HotLoadConfig{
					PoolSizeConfig:       poolSizeConfig,
					ReservedAntiAffinity: "weak",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setHotLoadConfig(tt.args.config, tt.args.hotLoadConfig)
		})
	}
}

func Test_getEtcdConfig(t *testing.T) {
	config := "{\"user\":\"root\"}"
	tests := []struct {
		name        string
		want        *etcd3.ClientConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: &etcd3.ClientConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: &etcd3.ClientConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: &etcd3.ClientConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getEtcdConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getEtcdConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getMetaEtcdProxyConfig(t *testing.T) {
	config := "{\"user\":\"root\"}"
	tests := []struct {
		name        string
		want        *etcd3.ClientConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getMetaEtcdProxyConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getMetaEtcdProxyConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getCAEMetaEtcdProxyConfig(t *testing.T) {
	config := "{\"user\":\"root\"}"
	tests := []struct {
		name        string
		want        *etcd3.ClientConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getCAEMetaEtcdProxyConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCAEMetaEtcdProxyConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getRouterEtcdConfig(t *testing.T) {
	config := "{\"user\":\"root\"}"
	tests := []struct {
		name        string
		want        *etcd3.ClientConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: &etcd3.ClientConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: &etcd3.ClientConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: &etcd3.ClientConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getRouterEtcdConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRouterEtcdConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getEtcdClusterConfig(t *testing.T) {
	config := "{\"electionttl\":5}"
	tests := []struct {
		name        string
		want        EtcdClusterConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: EtcdClusterConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: EtcdClusterConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: EtcdClusterConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getEtcdClusterConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getEtcdClusterConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getK8sCidrConfig(t *testing.T) {
	config := "{\"podCidr\":\"192.168.0.1/24\"}"
	tests := []struct {
		name        string
		want        K8sCidr
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: K8sCidr{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: K8sCidr{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: K8sCidr{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getK8sCidrConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getK8sCidrConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getMonitoringRedisServerConfig(t *testing.T) {
	config := "{\"serverAddr\":\"192.168.2.10\"}"
	tests := []struct {
		name        string
		want        MonitoringRedisServerConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: MonitoringRedisServerConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: MonitoringRedisServerConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: MonitoringRedisServerConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getMonitoringRedisServerConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getMonitoringRedisServerConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getHTTPSConfig(t *testing.T) {
	config := "{\"tlsProtocol\":\"127.0.0.1\"}"
	tests := []struct {
		name        string
		want        *tls.InternalHTTPSConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: &tls.InternalHTTPSConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: &tls.InternalHTTPSConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: &tls.InternalHTTPSConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getHTTPSConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getHTTPSConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getLocalAuthConfig(t *testing.T) {
	config := "{\"aKey\":\"xxx\"}"
	tests := []struct {
		name        string
		want        *localauth.AuthConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: &localauth.AuthConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: &localauth.AuthConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: &localauth.AuthConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getLocalAuthConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getLocalAuthConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getPoolMgrConfig(t *testing.T) {
	config := "{\"reservedAntiAffinity\":\"weak\"}"
	tests := []struct {
		name        string
		want        PoolMgrConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: PoolMgrConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: PoolMgrConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: PoolMgrConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getPoolMgrConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getPoolMgrConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getAutoScalerConfig(t *testing.T) {
	config := "{\"reservedAntiAffinity\":\"weak\"}"
	tests := []struct {
		name        string
		want        []AutoScalerConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: make([]AutoScalerConfig, 0),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 env is nil",
			want: make([]AutoScalerConfig, 0),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getAutoScalerConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAutoScalerConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_hotLoadHeterogeneousSchedulingConfig(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case_01 normal scenario",
			args: args{
				config: &FaasConfig{HeterogeneousSchedulingConfig: HeterogeneousSchedulingConfig{
					Enable:   true,
					Strategy: "default",
				}},
				hotLoadConfig: &HotLoadConfig{HeterogeneousSchedulingConfig: HeterogeneousSchedulingConfig{
					Strategy: "force",
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadHeterogeneousSchedulingConfig(tt.args.config, tt.args.hotLoadConfig)
		})
	}
}

func Test_getHeterogeneousSchedulingConfig(t *testing.T) {
	config := "{\"strategy\":\"force\"}"
	tests := []struct {
		name        string
		want        HeterogeneousSchedulingConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case01 unmarshal error",
			want: HeterogeneousSchedulingConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
		}, {
			name: "case02 validate error",
			want: HeterogeneousSchedulingConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, fmt.Errorf("validate error")
					}),
				})
				return patches
			},
		}, {
			name: "case03 env is nil",
			want: HeterogeneousSchedulingConfig{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return ""
						}),
				})
				return patches
			},
		}, {
			name: "case04 normal scenario",
			want: HeterogeneousSchedulingConfig{
				Enable:   false,
				Strategy: "force",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return config
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getHeterogeneousSchedulingConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getHeterogeneousSchedulingConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestIsHeterogeneousScheduling(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "case_01 config is nil",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsHeterogeneousScheduling(); got != tt.want {
				t.Errorf("IsHeterogeneousScheduling() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsHeterogeneousSchedulingNotNil(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "case_01 config is not nil",
			want: true,
		},
	}
	for _, tt := range tests {
		configInstance = &FaasConfig{HeterogeneousSchedulingConfig: HeterogeneousSchedulingConfig{Enable: true}}
		t.Run(tt.name, func(t *testing.T) {
			if got := IsHeterogeneousScheduling(); got != tt.want {
				t.Errorf("IsHeterogeneousScheduling() = %v, want %v", got, tt.want)
			}
		})
		configInstance = nil
	}
}

func TestIsForceStrategy(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "case_01 config is nil",
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsForceStrategy(); got != tt.want {
				t.Errorf("IsForceStrategy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsForceStrategyNotNil(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "case_01 config is not nil",
			want: false,
		},
	}
	for _, tt := range tests {
		configInstance = &FaasConfig{HeterogeneousSchedulingConfig: HeterogeneousSchedulingConfig{Enable: true, Strategy: "profiler"}}
		t.Run(tt.name, func(t *testing.T) {
			if got := IsForceStrategy(); got != tt.want {
				t.Errorf("IsForceStrategy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hotLoadBinPackEnable(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"test binpackenablechanged",
			args{
				config: &FaasConfig{
					BinPackEnable: true,
				},
				hotLoadConfig: &HotLoadConfig{
					BinPackEnable: false,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadBinPackEnable(tt.args.config, tt.args.hotLoadConfig)
			if tt.args.config.BinPackEnable != tt.args.hotLoadConfig.BinPackEnable {
				t.Errorf("hotLoadBinPackEnable test error")
			}
		})
	}
}

func Test_hotLoadEnableMultiSnapshot(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test hotLoadEnableMultiSnapshot configmap load",
			args: args{
				config: &FaasConfig{
					PoolMgrConfig: PoolMgrConfig{
						MultiSnapshotConfig: MultiSnapshotConfig{
							Enable: false,
							SnapshotCheckInfo: types.SnapshotCheckInfo{
								JavaRuntimeSize:  11111,
								JavaRuntimeHash:  "11111hash",
								ApmJavaAgentSize: 22222,
								ImageOS:          "eulerosv2r9",
								RunTimeSize:      11111,
							},
						},
					},
				},
				hotLoadConfig: &HotLoadConfig{
					MultiSnapshotConfig: MultiSnapshotConfig{
						Enable: true,
						SnapshotCheckInfo: types.SnapshotCheckInfo{
							JavaRuntimeSize:  12345,
							JavaRuntimeHash:  "12345hash",
							ApmJavaAgentSize: 456789,
							ImageOS:          "eulerosv2r10",
							RunTimeSize:      12345,
						},
					},
				},
			},
		},
		{
			name: "test hotLoadEnableMultiSnapshot file load",
			args: args{
				config: &FaasConfig{
					PoolMgrConfig: PoolMgrConfig{
						MultiSnapshotConfig: MultiSnapshotConfig{
							Enable: false,
							SnapshotCheckInfo: types.SnapshotCheckInfo{
								JavaRuntimeSize:  11111,
								JavaRuntimeHash:  "11111hash",
								ApmJavaAgentSize: 22222,
								ImageOS:          "eulerosv2r9",
								RunTimeSize:      33333,
							},
						},
					},
				},
				hotLoadConfig: &HotLoadConfig{
					MultiSnapshotConfig: MultiSnapshotConfig{
						Enable: true,
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							var snapshot types.SnapshotCheckInfo
							snapshot = types.SnapshotCheckInfo{
								JavaRuntimeSize:  110110,
								ApmJavaAgentSize: 111111,
								JavaRuntimeHash:  "snapshotCheckInfoFile",
								ImageOS:          "eulerosv2r11",
								RunTimeSize:      110110,
							}
							if data, ok := v.(*types.SnapshotCheckInfo); ok {
								data.ImageOS = snapshot.ImageOS
								data.RunTimeSize = snapshot.RunTimeSize
								data.JavaRuntimeHash = snapshot.JavaRuntimeHash
								data.ApmJavaAgentSize = snapshot.ApmJavaAgentSize
								data.JavaRuntimeSize = snapshot.JavaRuntimeSize
							}
							return nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadFile,
						func(filename string) ([]byte, error) {
							return nil, nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// configmap > snapshotcheckinfo file
			multiSnapshotConfig := tt.args.hotLoadConfig.MultiSnapshotConfig
			if multiSnapshotConfig.SnapshotCheckInfo.JavaRuntimeSize == 0 {
				// use file info
				patches := tt.patchesFunc()
				hotLoadEnableMultiSnapshot(tt.args.config, tt.args.hotLoadConfig)
				config := tt.args.config.PoolMgrConfig.MultiSnapshotConfig
				if !config.Enable ||
					config.SnapshotCheckInfo.JavaRuntimeHash != "snapshotCheckInfoFile" ||
					config.SnapshotCheckInfo.ImageOS != "eulerosv2r11" ||
					config.SnapshotCheckInfo.JavaRuntimeSize != 110110 ||
					config.SnapshotCheckInfo.ApmJavaAgentSize != 111111 ||
					config.SnapshotCheckInfo.RunTimeSize != 110110 {
					t.Errorf("test hotLoadEnableMultiSnapshot file load error")
				}
				patches.ResetAll()
			} else {
				hotLoadEnableMultiSnapshot(tt.args.config, tt.args.hotLoadConfig)
				config := tt.args.config.PoolMgrConfig.MultiSnapshotConfig
				if !config.Enable ||
					config.SnapshotCheckInfo.JavaRuntimeHash != "12345hash" ||
					config.SnapshotCheckInfo.ImageOS != "eulerosv2r10" ||
					config.SnapshotCheckInfo.JavaRuntimeSize != 12345 ||
					config.SnapshotCheckInfo.ApmJavaAgentSize != 456789 ||
					config.SnapshotCheckInfo.RunTimeSize != 12345 {
					t.Errorf("test hotLoadEnableMultiSnapshot configmap load error")
				}
			}
		})
	}
}

func Test_hotLoadCheckpointPodConfig(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test hot load checkpoint pod config load",
			args: args{
				config: &FaasConfig{
					PoolMgrConfig: PoolMgrConfig{
						CheckpointPodConfig: CheckpointPodConfig{
							WaitReadyTimeout:     50,
							FailureRetryInterval: 50,
							FailureRetryTimes:    50,
							MaxConcurrency:       50,
							CRIUCheckpointOpts:   "50",
						},
					},
				},

				hotLoadConfig: &HotLoadConfig{
					CheckpointPodConfig: CheckpointPodConfig{
						WaitReadyTimeout:     100,
						FailureRetryInterval: 100,
						FailureRetryTimes:    100,
						MaxConcurrency:       100,
						CRIUCheckpointOpts:   "100",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadCheckpointPodConfig(tt.args.config, tt.args.hotLoadConfig)
			config := tt.args.config.PoolMgrConfig.CheckpointPodConfig
			if config.WaitReadyTimeout != 100 || config.FailureRetryInterval != 100 ||
				config.FailureRetryTimes != 100 || config.MaxConcurrency != 100 || config.CRIUCheckpointOpts != "100" {
				t.Errorf("test hotLoadCheckpointPodConfig, config-mapload error")
			}
		})
	}
}

func Test_hotLoadSnapshotTenanIds(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	var newArrString []string
	newArrString = append(newArrString, "111", "111", "111", "111", "123")
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test hotLoadSnapshotTenanIds configmap load",
			args: args{
				config: &FaasConfig{
					PoolMgrConfig: PoolMgrConfig{
						SnapshotTenantIDsConfig: SnapshotTenantIDsConfig{
							Enable: false,
						},
					},
				},

				hotLoadConfig: &HotLoadConfig{
					SnapshotTenantIDsConfig: SnapshotTenantIDsConfig{
						Enable:    true,
						TenantIDs: newArrString,
					},
				},
			},
		},
		{
			name: "test hotLoadSnapshotTenanIds load, config is duplicate",
			args: args{
				config: &FaasConfig{
					PoolMgrConfig: PoolMgrConfig{
						SnapshotTenantIDsConfig: SnapshotTenantIDsConfig{
							Enable: false,
						},
					},
				},

				hotLoadConfig: &HotLoadConfig{
					SnapshotTenantIDsConfig: SnapshotTenantIDsConfig{
						Enable:    true,
						TenantIDs: newArrString,
					},
				},
			},
		},
		{
			name: "test hotLoadSnapshotTenanIds load, tenantId is empty",
			args: args{
				config: &FaasConfig{
					PoolMgrConfig: PoolMgrConfig{
						SnapshotTenantIDsConfig: SnapshotTenantIDsConfig{
							Enable: false,
						},
					},
				},

				hotLoadConfig: &HotLoadConfig{
					SnapshotTenantIDsConfig: SnapshotTenantIDsConfig{
						Enable: true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// configmap > snapshotcheckinfo file
			hotLoadSnapshotTenanIds(tt.args.config, tt.args.hotLoadConfig)
			config := tt.args.config.PoolMgrConfig.SnapshotTenantIDsConfig
			if t.Name() != "Test_hotLoadSnapshotTenanIds/test_hotLoadSnapshotTenanIds_load,_tenantId_is_empty" {
				if !config.Enable || !strings.Contains(config.TenantIDs[0], "111") || !strings.Contains(config.TenantIDs[1], "123") || len(config.TenantIDs) != 2 {
					if len(config.TenantIDs) != 2 {
						t.Errorf("test hotLoadSnapshotTenanIds, tenantId is duplicate")
					} else {
						t.Errorf("test hotLoadSnapshotTenanIds, config-mapload error")
					}
				}
			} else {
				if !config.Enable || len(config.TenantIDs) != 0 {
					t.Errorf("test hotLoadSnapshotTenanIds, tenantId is empty")
				}
			}
		})
	}
}

func Test_hotLoadEtcdConfig(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	etcdConfig := &etcd3.ClientConfig{
		ServerList: []string{"100.100.100.100"},
		User:       "",
		Password:   "",
		CaFile:     "",
		CertFile:   "",
		KeyFile:    "",
		SslEnable:  false,
	}
	var a args
	oldConfig := &FaasConfig{
		Etcd: etcdConfig,
	}
	hotLoadConfig := &HotLoadConfig{
		Etcd: etcdConfig,
	}
	a.config = oldConfig
	a.hotLoadConfig = hotLoadConfig
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
		hotLoadEtcdConfig(a.config, a.hotLoadConfig)
	}
}

func Test_hotLoadWorkerMetricsTick(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	var a args
	oldConfig := &FaasConfig{}
	hotLoadConfig := &HotLoadConfig{}
	a.config = oldConfig
	a.hotLoadConfig = hotLoadConfig
	a.config.WorkerMetricsTick = 1
	a.hotLoadConfig.WorkerMetricsTick = 0
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
	hotLoadWorkerMetricsTick(a.config, a.hotLoadConfig)
}

func Test_hotLoadTaintsNodeCheckInterval(t *testing.T) {
	tests := []struct {
		name          string
		cfg           *FaasConfig
		hotLoadConfig *HotLoadConfig

		expected int64
	}{
		{
			name:          "default",
			cfg:           &FaasConfig{},
			hotLoadConfig: &HotLoadConfig{},

			expected: defaultTaintsNodeCheckInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadTaintsNodeCheckInterval(tt.cfg, tt.hotLoadConfig)
			assert.Equal(t, tt.expected, atomic.LoadInt64(&tt.cfg.TaintsNodeCheckInterval), tt.name)
		})
	}
}

func Test_hotLoadDisableEtcdRev(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	var a args
	oldConfig := &FaasConfig{}
	hotLoadConfig := &HotLoadConfig{}
	a.config = oldConfig
	a.hotLoadConfig = hotLoadConfig
	a.config.DisableEtcdRev = 1
	a.hotLoadConfig.DisableEtcdRev = 0
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
	hotLoadDisableEtcdRev(a.config, a.hotLoadConfig)
}

func Test_hotLoadDisableCircuitBreaker(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	var a args
	oldConfig := &FaasConfig{}
	hotLoadConfig := &HotLoadConfig{}
	a.config = oldConfig
	a.hotLoadConfig = hotLoadConfig
	a.config.PoolMgrConfig.DisableCircuitBreaker = 1
	a.hotLoadConfig.DisableCircuitBreaker = 0
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
	hotLoadDisableCircuitBreaker(a.config, a.hotLoadConfig)
}

func Test_hotLoadDisablePoolRollout(t *testing.T) {
	cfg := &FaasConfig{}

	tests := []struct {
		name   string
		config *HotLoadConfig

		expected uint32
	}{
		{
			name:     "update disabled",
			config:   &HotLoadConfig{DisablePoolRollout: 1},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadDisablePoolRollout(cfg, tt.config)
			assert.Equal(t, tt.expected, cfg.PoolMgrConfig.DisablePoolRollout, tt.name)
		})
	}
}

func Test_hotLoadDisablePriorityQueue(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	var a args
	oldConfig := &FaasConfig{}
	hotLoadConfig := &HotLoadConfig{}
	a.config = oldConfig
	a.hotLoadConfig = hotLoadConfig
	a.config.PoolMgrConfig.DisablePriorityQueue = 1
	a.hotLoadConfig.DisablePriorityQueue = 0
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
	hotLoadDisablePriorityQueue(a.config, a.hotLoadConfig)
}

func Test_hotLoadSchedulingEnable(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	var a args
	oldConfig := &FaasConfig{}
	hotLoadConfig := &HotLoadConfig{}
	a.config = oldConfig
	a.hotLoadConfig = hotLoadConfig
	a.config.SchedulingEnable = true
	a.hotLoadConfig.SchedulingEnable = false
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
	hotLoadSchedulingEnable(a.config, a.hotLoadConfig)
}

func Test_hotLoadScheduleWithDisk(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"case success",
			args{
				&FaasConfig{
					ScheduleWithDisk: true,
				},
				&HotLoadConfig{
					ScheduleWithDisk: false,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadScheduleWithDisk(tt.args.config, tt.args.hotLoadConfig)
			if tt.args.config.ScheduleWithDisk != tt.args.hotLoadConfig.ScheduleWithDisk {
				t.Errorf("failed to Test_hotLoadScheduleWithDisk")
			}
		})
	}
}

func Test_hotLoadRetryTimeWhenNoSpace(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"case success",
			args{
				&FaasConfig{
					RetryTimeWhenNoSpace: 0,
				},
				&HotLoadConfig{
					RetryTimeWhenNoSpace: 10,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadRetryTimeWhenNoSpace(tt.args.config, tt.args.hotLoadConfig)
			if tt.args.config.RetryTimeWhenNoSpace != tt.args.hotLoadConfig.RetryTimeWhenNoSpace {
				t.Errorf("failed to Test_hotLoadRetryTimeWhenNoSpace")
			}
		})
	}
}

func TestGetSchedulerWithDisk(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(GetConfig, func() (*FaasConfig, error) {
			return nil, errors.New("failed to get config")
		}),
	})
	GetSchedulerWithDisk()
	patches.ResetAll()

	patches = mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(GetConfig, func() (*FaasConfig, error) {
			return nil, nil
		}),
	})
	GetSchedulerWithDisk()
	patches.ResetAll()

	patches = mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(GetConfig, func() (*FaasConfig, error) {
			return &FaasConfig{}, nil
		}),
	})
	GetSchedulerWithDisk()
	patches.ResetAll()
}

func Test_getCAERouterEtcdConfig(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			return "AAAA"
		}),
	})
	getCAERouterEtcdConfig()
	patches.ResetAll()

	patches = mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			return "{}"
		}),
	})
	getCAERouterEtcdConfig()
	patches.ResetAll()
}

func Test_hotLoadImageAffinityCacheConfig(t *testing.T) {
	type args struct {
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
	}
	var a1 args
	a1.hotLoadConfig = &HotLoadConfig{}

	var a2 args
	a2.config = &FaasConfig{ImageAffinityCacheConfig: &ImageAffinityCacheConfig{
		DisableImageAffinityCache: 0,
		TenantWhiteList:           nil,
		CacheNodesNumber:          1,
		CacheNodesPercentage:      0.5,
	}}
	a2.hotLoadConfig = &HotLoadConfig{ImageAffinityCacheConfig: &ImageAffinityCacheConfig{
		DisableImageAffinityCache: 1,
		TenantWhiteList:           nil,
		CacheNodesNumber:          2,
		CacheNodesPercentage:      0.6,
	}}
	tests := []struct {
		name string
		args args
	}{
		{"case1", a1},
		{"case2", a2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadImageAffinityCacheConfig(tt.args.config, tt.args.hotLoadConfig)
		})
	}
}

func Test_getImageAffinityCacheConfig(t *testing.T) {
	expectedConfig := &ImageAffinityCacheConfig{
		DisableImageAffinityCache: 1,
		TenantWhiteList:           nil,
		CacheNodesNumber:          0,
		CacheNodesPercentage:      1,
	}
	assert.Equalf(t, expectedConfig, getImageAffinityCacheConfig(), "get empty ImageAffinityCacheConfig()")

	os.Setenv("IMAGE_AFFINITY_CACHE_CONFIG", "5")
	assert.Equalf(t, expectedConfig, getImageAffinityCacheConfig(), "get bad format ImageAffinityCacheConfig()")

	cacheConfig, _ := json.Marshal(expectedConfig)
	os.Setenv("IMAGE_AFFINITY_CACHE_CONFIG", string(cacheConfig))
	assert.Equalf(t, expectedConfig, getImageAffinityCacheConfig(), "get correct ImageAffinityCacheConfig()")
}

func Test_hotLoadSsdPoolLabels(t *testing.T) {
	tests := []struct {
		name          string
		config        *FaasConfig
		hotLoadConfig *HotLoadConfig
		patchesFunc   mockUtils.PatchesFunc
		want          map[string]string
	}{
		{
			name: "test old config json marshall failed",
			config: &FaasConfig{SSDPoolLabels: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			}},
			hotLoadConfig: &HotLoadConfig{SSDPoolLabels: map[string]string{
				"user2": "value2",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("json marshall failed")
					}),
				})
				return patches
			},
			want: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			},
		},
		{
			name: "test new config json unmarshall failed",
			config: &FaasConfig{SSDPoolLabels: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			}},
			hotLoadConfig: &HotLoadConfig{SSDPoolLabels: map[string]string{
				"user2": "value2",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFuncSeq(json.Marshal, []gomonkey.OutputCell{
						{Values: gomonkey.Params{nil, nil}},
						{Values: gomonkey.Params{nil, errors.New("json marshall failed")}},
					}),
				})
				return patches
			},
			want: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			},
		},
		{
			name: "test update ssd pool labels",
			config: &FaasConfig{SSDPoolLabels: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			}},
			hotLoadConfig: &HotLoadConfig{SSDPoolLabels: map[string]string{
				"user2":       "value2",
				"empty-value": "",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			want: map[string]string{
				"user2":       "value2",
				"empty-value": "",
			},
		},
		{
			name: "test ssd pool labels not changed",
			config: &FaasConfig{SSDPoolLabels: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			}},
			hotLoadConfig: &HotLoadConfig{SSDPoolLabels: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			want: map[string]string{
				"user1":      "value1",
				"test:test-": "value",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			hotLoadSsdPoolLabels(tt.config, tt.hotLoadConfig)
			assert.Equal(t, tt.want, tt.config.SSDPoolLabels)
			SSDPoolLabelsMap = sync.Map{}
			patches.ResetAll()
		})
	}
}

func TestHotLoadInstanceTimeoutExitWhiteList(t *testing.T) {
	tests := []struct {
		name              string
		config            *FaasConfig
		hotLoadConfig     *HotLoadConfig
		expectedWhiteList []string
	}{
		{
			name: "Changes to the whitelist content should update the configuration",
			config: &FaasConfig{
				InstanceTimeoutExitWhiteList: []string{"old1", "old2"},
			},
			hotLoadConfig: &HotLoadConfig{
				InstanceTimeoutExitWhiteList: []string{"new1", "new2"},
			},
			expectedWhiteList: []string{"new1", "new2"},
		},
		{
			name: "If the whitelist is not changed, the configuration should not be updated",
			config: &FaasConfig{
				InstanceTimeoutExitWhiteList: []string{"same1", "same2"},
			},
			hotLoadConfig: &HotLoadConfig{
				InstanceTimeoutExitWhiteList: []string{"same1", "same2"},
			},
			expectedWhiteList: []string{"same1", "same2"},
		},
		{
			name: "nil whitelist processing test",
			config: &FaasConfig{
				InstanceTimeoutExitWhiteList: nil,
			},
			hotLoadConfig: &HotLoadConfig{
				InstanceTimeoutExitWhiteList: []string{"new"},
			},
			expectedWhiteList: []string{"new"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalConfig := *tt.config
			testConfig := &originalConfig
			hotLoadInstanceTimeoutExitWhiteList(testConfig, tt.hotLoadConfig)
			if !reflect.DeepEqual(testConfig.InstanceTimeoutExitWhiteList, tt.expectedWhiteList) {
				t.Error("The whitelist should not be changed but was modified")

			}
		})
	}
}
