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

// Package utils gets function services from URNs
package utils

import (
	"errors"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/worker-manager/config"
	mockUtils "functioncore/test/utils"
)

func TestInitPoolResourceCache(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to GetStatePodActionPullUp",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						mockPool := config.PoolConfig{
							Name:                          "pool22-0",
							RunTimeName:                   "fusion",
							LimitCPU:                      300,
							LimitMemory:                   128,
							RequestCPU:                    100,
							RequestMemory:                 128,
							ActualCPU:                     0,
							EphemeralStorage:              14336,
							LivenessProbePeriodSeconds:    0,
							LivenessProbeFailureThreshold: 0,
							PoolSize:                      1,
							PoolScaleLimit:                0,
							CustomResources:               nil,
							Annotations:                   nil,
							Label:                         nil,
							NonIsolated:                   false,
						}
						mockPoolList := []config.PoolConfig{}
						mockPoolList = append(mockPoolList, mockPool)
						mockPoolConfig := config.PoolMgrConfig{
							PoolList: config.PoolList{
								Pool: mockPoolList,
							},
						}
						mockConfig := &config.FaasConfig{
							PoolMgrConfig: mockPoolConfig,
						}
						return mockConfig, nil
					}),
				})
				return patches
			}},
		{"case2 failed to get config",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("failed")
					}),
				})
				return patches
			}},
		{"case3 set actual cpu",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						mockPool := config.PoolConfig{
							Name:                          "pool23-0",
							RunTimeName:                   "fusion",
							LimitCPU:                      400,
							LimitMemory:                   256,
							RequestCPU:                    100,
							RequestMemory:                 256,
							ActualCPU:                     100,
							EphemeralStorage:              14336,
							LivenessProbePeriodSeconds:    0,
							LivenessProbeFailureThreshold: 0,
							PoolSize:                      1,
							PoolScaleLimit:                0,
							CustomResources:               nil,
							Annotations:                   nil,
							Label:                         nil,
							NonIsolated:                   false,
						}
						mockPoolList := []config.PoolConfig{}
						mockPoolList = append(mockPoolList, mockPool)
						mockPoolConfig := config.PoolMgrConfig{
							PoolList: config.PoolList{
								Pool: mockPoolList,
							},
						}
						mockConfig := &config.FaasConfig{
							PoolMgrConfig: mockPoolConfig,
						}
						return mockConfig, nil
					}),
				})
				return patches
			}},
		{"case4 Large-specification memory",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						mockPool := config.PoolConfig{
							Name:                          "pool30-0",
							RunTimeName:                   "fusion",
							LimitCPU:                      1800,
							LimitMemory:                   2048,
							RequestCPU:                    100,
							RequestMemory:                 2048,
							ActualCPU:                     100,
							EphemeralStorage:              14336,
							LivenessProbePeriodSeconds:    0,
							LivenessProbeFailureThreshold: 0,
							PoolSize:                      1,
							PoolScaleLimit:                0,
							CustomResources:               nil,
							Annotations:                   nil,
							Label:                         nil,
							NonIsolated:                   false,
						}
						mockPoolList := []config.PoolConfig{}
						mockPoolList = append(mockPoolList, mockPool)
						mockPoolConfig := config.PoolMgrConfig{
							PoolList: config.PoolList{
								Pool: mockPoolList,
							},
						}
						mockConfig := &config.FaasConfig{
							PoolMgrConfig: mockPoolConfig,
						}
						return mockConfig, nil
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			InitPoolResourceCache()
		})
	}
}

func TestGetPoolResource(t *testing.T) {
	var (
		requestCPU   int64
		requestMem   int64
		businessType string
		patch        = gomonkey.NewPatches()
	)

	convey.Convey("test get pool resource", t, func() {
		convey.Convey("test get fass pool failed", func() {
			_, err := GetPoolResource(requestCPU, requestMem, businessType)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "not found")
		})

		convey.Convey("test get cae pool failed", func() {
			mockGetCAEPoolResource(patch, PoolResource{}, errors.New("failed to get cae resource"))
			_, err := GetPoolResource(requestCPU, requestMem, "CAE")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to get cae resource")
		})

		convey.Convey("test get cae pool success", func() {
			mockGetCAEPoolResource(patch, PoolResource{}, nil)
			_, err := GetPoolResource(requestCPU, requestMem, "CAE")
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})

}

func TestGetCAEPoolResource(t *testing.T) {
	var (
		requestCPU int64
		requestMem int64
		patch      = gomonkey.NewPatches()
	)

	caeSpec := make(map[string]config.PoolSpec, 3)
	caeSpec["pool-300-128"] = config.PoolSpec{
		LimitCPU:         300,
		LimitMEM:         128,
		RequestCPU:       100,
		RequestMEM:       128,
		EphemeralStorage: 0,
	}

	caeSpec["pool-600-512"] = config.PoolSpec{
		LimitCPU:         600,
		LimitMEM:         512,
		RequestCPU:       200,
		RequestMEM:       256,
		EphemeralStorage: 0,
	}

	caeSpec["pool-1000-1024"] = config.PoolSpec{
		LimitCPU:         1000,
		LimitMEM:         1024,
		RequestCPU:       600,
		RequestMEM:       512,
		EphemeralStorage: 0,
	}

	mockConfig := &config.FaasConfig{
		CaeCustomizePoolSpec: caeSpec,
	}

	convey.Convey("test get cae pool", t, func() {
		convey.Convey("test get cae failed", func() {
			mockGetConfig(patch, &config.FaasConfig{}, nil)
			_, err := GetCAEPoolResource(requestCPU, requestMem)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test get 300-128 mem cae pool", func() {
			mockGetConfig(patch, mockConfig, nil)
			poolResource, err := GetCAEPoolResource(300, 128)
			convey.So(err, convey.ShouldBeNil)
			convey.So(poolResource.Worker.RequestCPU, convey.ShouldEqual, 50)
		})

		convey.Convey("test get 600-512 mem cae pool", func() {
			mockGetConfig(patch, mockConfig, nil)
			poolResource, err := GetCAEPoolResource(600, 512)
			convey.So(err, convey.ShouldBeNil)
			convey.So(poolResource.Worker.RequestCPU, convey.ShouldEqual, 100)
		})

		convey.Convey("test get 1000-1024 mem cae pool", func() {
			mockGetConfig(patch, mockConfig, nil)
			poolResource, err := GetCAEPoolResource(1000, 1024)
			convey.So(err, convey.ShouldBeNil)
			convey.So(poolResource.Worker.RequestCPU, convey.ShouldEqual, 100)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func mockGetConfig(patch *gomonkey.Patches, cfg *config.FaasConfig, err error) {
	patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
		return cfg, err
	})
}

func mockGetCAEPoolResource(patch *gomonkey.Patches, resource PoolResource, err error) {
	patch.ApplyFunc(GetCAEPoolResource, func(requestCPU, requestMem int64) (PoolResource, error) {
		return resource, err
	})
}

func TestGetCreateOptionResource(t *testing.T) {
	type args struct {
		cpu int64
		mem int64
	}
	poolResourceCache.Store(1, 1)
	poolResourceCache.Store("300", 1)
	poolResourceCache.Store("a-300", 1)
	poolResourceCache.Store("300-a", 1)
	poolResourceCache.Store("300-128", 1)
	poolResourceCache.Store("400-256", 1)
	poolResourceCache.Store("600-512", 1)
	poolResourceCache.Store("800-768", 1)
	var a1 args
	a1.mem = 256

	var a2 args
	a2.mem = 700

	var a3 args
	a3.mem = 4096

	var a4 args
	a4.mem = 0
	tests := []struct {
		name  string
		args  args
		want  int64
		want1 int64
	}{
		{"case1 get specific resource", a1, 400, 256},
		{"case2 get similar resource", a2, 800, 768},
		{"case3 get no resource", a3, 0, 0},
		{"case4 get 0 memory input", a4, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetCreateOptionResource(tt.args.cpu, tt.args.mem)
			assert.Equalf(t, tt.want, got, "GetCreateOptionResource(%v, %v)", tt.args.cpu, tt.args.mem)
			assert.Equalf(t, tt.want1, got1, "GetCreateOptionResource(%v, %v)", tt.args.cpu, tt.args.mem)
		})
	}
}

func TestGetCAERequestFactor(t *testing.T) {
	var (
		patch = gomonkey.NewPatches()
	)

	convey.Convey("test get cae request factor", t, func() {
		convey.Convey("test get cae with null env", func() {
			patch.ApplyFunc(os.Getenv, func() string {
				return ""
			})
			res := GetCAERequestFactor()
			convey.So(res, convey.ShouldEqual, caeRequestInitFactor)
		})

		convey.Convey("test get cae with env failed", func() {
			patch.ApplyFunc(os.Getenv, func() string {
				return "1a"
			})
			res := GetCAERequestFactor()
			convey.So(res, convey.ShouldEqual, caeRequestInitFactor)
		})

		convey.Convey("test get 600-512 mem cae pool", func() {
			patch.ApplyFunc(os.Getenv, func() string {
				return "0.4"
			})
			res := GetCAERequestFactor()
			convey.So(res, convey.ShouldEqual, caeRequestInitFactor)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}
