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

// Package options define options of autoscaler
package options

// AutoScalerOptions defines AutoScalerOptions
type AutoScalerOptions struct {
	ListenAddress string
	ConfigFile    string `valid:",optional"`
	LogPath       string `valid:",optional"`
}

// NewAutoScalerOptions New API Server Options
func NewAutoScalerOptions() *AutoScalerOptions {
	s := AutoScalerOptions{
		ListenAddress: "",
		ConfigFile:    "/home/sn/config/autoscaler.conf",
		LogPath:       "/home/sn/log",
	}
	return &s
}
