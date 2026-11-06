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

// Package k8sclient include some k8s client operation
package k8sclient

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// GetKubeConfig for create k8s client config
func GetKubeConfig() (*rest.Config, error) {
	kubeconfigEnv := os.Getenv("KUBECONFIG")
	var kubeconfigPath string
	if kubeconfigEnv == "" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		homeJudge(&home)
		kubeconfigPath = filepath.Join(home, ".kube", "config")
	}

	kubecfg, err := rest.InClusterConfig()
	if err != nil {
		fmt.Println("failed to init cluster config: ", err.Error())
		kubecfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, err
		}
	}
	return kubecfg, nil
}

// homeJudge judge home is nil or not
func homeJudge(home *string) {
	if *home == "" {
		for _, str := range []string{"HOME", "USERPROFILE"} {
			if *home = os.Getenv(str); *home != "" {
				break
			}
		}
	}
}
