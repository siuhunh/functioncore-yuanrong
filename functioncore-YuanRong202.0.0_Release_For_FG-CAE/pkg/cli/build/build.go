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

// Package build is build info of cli
package build

// Version is version of cli, it is dynamically set by the Makefile.
var Version = Dev

// CustomizeVer is build time of cli, it is dynamically set by the Makefile.
var CustomizeVer = ""

// Date is build time of cli, it is dynamically set by the Makefile.
var Date = "" // YYYY-MM-DD

const (
	// Dev develop version
	Dev = "DEV"
)
