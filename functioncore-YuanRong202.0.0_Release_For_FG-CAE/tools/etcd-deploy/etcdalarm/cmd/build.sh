#!/bin/bash
# Copyright (c) 2022 Huawei Technologies Co., Ltd
#
# This software is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
# http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

set -e

# go module prepare
export GO111MODULE=on
export GOPROXY="https://go.cloudartifact.dgg.dragon.tools.huawei.com/artifactory/cbu-go-public/"
export NO_PROXY=${NO_PROXY},*.huawei.com
export GONOSUMDB=*

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
OUTPUT_DIR="${BASE_DIR}/output"
PROJECT_DIR="${BASE_DIR}"
MODULE_NAME="etcdAlarm"

#copy to output
[ -d "${OUTPUT_DIR}" ] && rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

echo "etcd_alarm module build"

FLAGS='-linkmode=external -extldflags "-fPIC -fstack-protector-strong -Wl,-z,now,-z,relro,-z,noexecstack,-s -Wall -Werror"'

#module build
cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"

CC='gcc -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2' go build -mod=mod -buildmode=pie -ldflags "${FLAGS}" -o \
${OUTPUT_DIR}/"${MODULE_NAME}" main.go
