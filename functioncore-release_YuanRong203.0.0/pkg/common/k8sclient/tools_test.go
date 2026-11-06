/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
 */

package k8sclient

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
)

func TestGetKubeConfig(t *testing.T) {
	ctrl := ""
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(rest.InClusterConfig, func() (*rest.Config, error) {
			if ctrl == "returnError" {
				return nil, errors.New("some error")
			}
			return &rest.Config{}, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	ctrl = "returnError"
	cfg, err := GetKubeConfig()
	assert.NotNil(t, err)
	assert.Nil(t, cfg)
	ctrl = ""
	cfg, err = GetKubeConfig()
	assert.Nil(t, err)
	assert.NotNil(t, cfg)
}
