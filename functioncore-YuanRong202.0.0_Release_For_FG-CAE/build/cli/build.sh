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
PROJECT_DIR="${BASE_DIR}/../../pkg/cli"
OUTPUT_DIR="${BASE_DIR}/../../../output/tools/cli"
NEW_OUTPUT_DIR="${BASE_DIR}/../../../output/openYuanRong"
TARGET_OS=$1
CLI_NAME=${CLI_NAME:-"yrctl"}
PLATFORM_NAME=${PLATFORM_NAME:-"YuanRong"}

. "${BASE_DIR}"/../build_function.sh

BUILD_DATE=$(date -u "+%Y%m%d%H%M%S")
VERSION=$PROJECT_VERSION
if [ -z "$VERSION" ]; then
    VERSION="DEV"
fi
FLAG_VER="functioncore/pkg/cli/build.Version="
FLAG_DATE="functioncore/pkg/cli/build.Date=${BUILD_DATE}"
FLAG_CLI_NAME="functioncore/pkg/cli/constant.CliName=${CLI_NAME}"
FLAG_PLATFORM_NAME="functioncore/pkg/cli/constant.PlatformName=${PLATFORM_NAME}"
FLAG_EXT="-fPIC -fstack-protector-strong -Wl,-z,now,-z,relro,-z,noexecstack,-s -Wall -Werror"
FLAGS="-X '${FLAG_VER}${VERSION}' -X '${FLAG_DATE}' -X '${FLAG_CLI_NAME}' -X '${FLAG_PLATFORM_NAME}' -extldflags '${FLAG_EXT}'"
MODE='-buildmode=pie'

function build_linux() {
    if ! CC='gcc -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2' go build ${MODE} -ldflags "${FLAGS}" -o build/_output/bin/${CLI_NAME} ../../cmd/cli/main.go; then
        log_error "Failed to build ${PLATFORM_NAME} CLI!!!"
        return 1
    fi
    return 0
}

function build_windows() {
    if ! CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags "${FLAGS}" \
    -o build/_output/bin/"${CLI_NAME}".exe ../../cmd/cli/main.go; then
        log_error "Failed to build "${CLI_NAME}".exe!!!"
        return 1
    fi
    return 0
}

mkdir -p "${OUTPUT_DIR}"
cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"

# clean
rm -rf build/_output
rm -fr build/cli/usr/local/cli/bin/"${CLI_NAME}"
rm -fr build/cli/usr/local/cli/resource

cd "${PROJECT_DIR}"
mkdir -p build/_output/bin
mkdir -p build/_output/pkg
mkdir -p build/cli/usr/local/cli/bin
if [ "${TARGET_OS}" = "linux" ]; then
    build_linux
elif [ "${TARGET_OS}" = "windows" ]; then
    build_windows
else
    build_linux
    build_windows
fi
rm -fr build/cli/usr/local/cli/bin/"${CLI_NAME}"
rm -fr build/cli/usr/local/cli/resource
cp build/_output/bin/"${CLI_NAME}" build/cli/usr/local/cli/bin/
cp -a resource build/cli/usr/local/cli/
cp -r templates build/cli/usr/local/cli/resource/local-repo/
rm -f build/cli/usr/local/cli/resource/local-repo/.gitignore
tar -cvf build/_output/pkg/cli.tar -C ../../cmd/cli install.sh -C "${PROJECT_DIR}"/build/cli/usr/local cli

# cp to ${OUTPUT_DIR}
cp -ar build/_output/* "${OUTPUT_DIR}"/

mkdir -p "${NEW_OUTPUT_DIR}"/bin
cp -a build/_output/bin/"${CLI_NAME}" "${NEW_OUTPUT_DIR}"/bin/
cp -ar "${PROJECT_DIR}"/build/cli/usr/local/cli/resource/ "${NEW_OUTPUT_DIR}"/
exit 0
