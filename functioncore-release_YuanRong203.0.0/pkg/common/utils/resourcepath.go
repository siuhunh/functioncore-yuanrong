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

// Package utils for common functions
package utils

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// GetResourcePath Get Resource Path
func GetResourcePath() string {
	resourcePath := os.Getenv("ResourcePath")
	var err error
	if resourcePath == "" {
		resourcePath, err = filepath.Abs(filepath.Dir(os.Args[0]))
		// do not return this error
		if err != nil {
			fmt.Printf("invalid resourcepath")
		}
		resourcePath = strings.Replace(resourcePath, "\\", "/", -1)
		resourcePath = path.Join(path.Dir(resourcePath), "resource")
	} else {
		resourcePath, err = filepath.Abs(resourcePath)
		if err != nil {
			fmt.Printf("invalid resourcepath")
		}
		resourcePath = strings.Replace(resourcePath, "\\", "/", -1)
	}
	return resourcePath
}
