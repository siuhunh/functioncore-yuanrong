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

RUNTIME_PATH="${BASE_DIR}/../.."
OUTPUT_DIR="${BASE_DIR}/../../output/functioncore"
local_os=$(head -1 /etc/os-release | tail -1 | awk -F "\"" '{print $2}')_$(uname -m)
pid_array=()

function pre_compile()
{
    #go get github.com/golang/mock@v1.4.4
    #cp -rf mock/mockgen ./

    # generate files from proto files by functiontask
    {
        cd "${PROJECT_DIR}"/build/functiontask
        if ! bash gen_pb.sh task; then
            log_error "Failed to generate task pb files!"
            exit 1
        fi
    } &
    pid_array+=($!)
    # generate files from proto files by worker
    {
        cd "${PROJECT_DIR}"/pkg/worker/script
        if ! bash pb_gen.sh; then
            log_error "Failed to generate worker pb files!"
            exit 1
        fi
    } &
    pid_array+=($!)
    # generate proto files for stream
    {
        cd "${PROJECT_DIR}"/build/common/
        if ! bash gen_pb.sh; then
            log_error "Failed to generate proto files for stream!"
            exit 1
        fi
    } &
    pid_array+=($!)
    for i in "${pid_array[@]}"
    do
        wait $i
        if [ $? -ne 0 ]; then
            exit 1
        fi
    done

    cd "${PROJECT_DIR}"
    go mod tidy
}

function runtime_compile()
{
    {
        if ! cd "${RUNTIME_PATH}/runtime-java/build" || ! bash build.sh; then
            log_error "Failed to build java-runtime"
            exit 1
        fi
    } &
    pid_array+=($!)
    {
        if ! cd "${RUNTIME_PATH}/runtime-nodejs/build" || ! bash build.sh -p "${PRODUCT}"; then
            log_error "Failed to build nodejs-runtime"
            exit 1
        fi
    } &
    pid_array+=($!)
    {
        if ! cd "${RUNTIME_PATH}/runtime-python/build" || ! bash build.sh -p "${PRODUCT}"; then
            log_error "Failed to build python3-runtime"
            exit 1
        fi
    } &
    pid_array+=($!)
    {
        if ! cd "${RUNTIME_PATH}/runtime-python2/build" || ! bash build.sh -p "${PRODUCT}"; then
            log_error "Failed to build python2-runtime"
            exit 1
        fi
    } &
    pid_array+=($!)
    {
        if ! cd "${RUNTIME_PATH}/runtime-cangjie/build" || ! bash build.sh; then
            log_error "Failed to build cangjie-runtime"
            exit 1
        fi
    } &
    pid_array+=($!)
    {
        if ! cd "${RUNTIME_PATH}/runtime-php/build" || ! bash build.sh; then
            log_error "Failed to build php-runtime"
            exit 1
        fi
    } &
    pid_array+=($!)
    {
        if [ "${local_os}"x != "EulerOS_aarch64"x ]; then
            {
                if ! cd "${RUNTIME_PATH}/runtime-csharp/CSharpRuntime/build" || ! bash build.sh; then
                    log_error "Failed to build csharp-runtime"
                    exit 1
                fi
            } &
        fi
    } &
    pid_array+=($!)
    {
        if ! cd "${RUNTIME_PATH}/runtime-go/build" || ! bash build.sh; then
            log_error "Failed to build go-runtime"
            exit 1
        fi
    } &
    pid_array+=($!)
    for i in "${pid_array[@]}"
    do
        wait $i
        if [ $? -ne 0 ]; then
            exit 1
        fi
    done
    if ! cd "${BASE_DIR}/runtimemanager" || ! bash build.sh -g "${PRODUCT}" -m; then
        log_error "Failed to build runtime-manager"
        exit 1
    fi
}

