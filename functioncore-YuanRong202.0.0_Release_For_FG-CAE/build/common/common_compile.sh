#!/bin/bash
# Copyright (c) 2021 Huawei Technologies Co., Ltd
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

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
. "${BASE_DIR}"/../build_function.sh

PROJECT_DIR="$1"
MODULE_NAME="$2"
OUTPUT_DIR="$3"
FLAGS='-linkmode=external -extldflags "-fPIC -fstack-protector-strong -Wl,-z,now,-z,relro,-z,noexecstack,-s -Wall -Werror"'

#module build
cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"
[ -d "build/_output" ] && rm -rf build/_output
mkdir -p build/_output/bin

CC='gcc -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2' go build -buildmode=pie -ldflags "${FLAGS}" -o \
build/_output/bin/"${MODULE_NAME}" ../../cmd/"${MODULE_NAME}"/main.go

#copy to output
[ -d "${OUTPUT_DIR}" ] && rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"/bin

[ -f "build/_output/bin/${MODULE_NAME}" ] && cp -ar build/_output/bin/"${MODULE_NAME}" "${OUTPUT_DIR}"/bin/
[ -f "../../cmd/${MODULE_NAME}/entrypoint" ] && cp -ar ../../cmd/${MODULE_NAME}/entrypoint "${OUTPUT_DIR}"/bin/
[ -f "../../cmd/${MODULE_NAME}/user_setup" ] && cp -ar ../../cmd/${MODULE_NAME}/user_setup "${OUTPUT_DIR}"/bin/
[ -f "../../cmd/${MODULE_NAME}/Dockerfile" ] && cp -ar ../../cmd/${MODULE_NAME}/Dockerfile* "${OUTPUT_DIR}"/
chmod -R 700 "${OUTPUT_DIR}"/bin

exit 0
