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
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/"
PROJECT_DIR="${BASE_DIR}/../../pkg/runtime-manager"
local_os=$(head -1 /etc/os-release | tail -1 | awk -F "\"" '{print $2}')_$(uname -m)

# go module prepare
export GO111MODULE=on
export GOPROXY="https://cmc.centralrepo.rnd.huawei.com/artifactory/product_go/,https://cmc.centralrepo.rnd.huawei.com/artifactory/go-central-repo/"
export NO_PROXY=${NO_PROXY},*.huawei.com
export GONOSUMDB=*

. "${BASE_DIR}"/../build_function.sh

function runtime_manager_module_compile() {
    log_info "build module runtime-manager"
    mkdir -p "${OUTPUT_DIR}"/runtime-manager/bin
    mkdir -p "${OUTPUT_DIR}"/runtime-manager/script
    mkdir -p "${OUTPUT_DIR}"/runtime-manager/config

    # Build runtime-manager
    cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"
    if [ "${build_opentelekomcloud}" == "on" ]; then
        sed -i 's/com\.huawei\.services\.runtime\.rpc\.RuntimeServer/com.opentelekomcloud.services.runtime.rpc.RuntimeServer/g' ${PROJECT_DIR}/extends/runtimeargs/runtimeargs.go
    fi
    go mod tidy
    if ! CC='gcc -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2' make build-runtime-manager; then
        log_error "Failed to build runtime-manager!!!"
        return 1
    fi

    # copy to output
    cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"
    cp -ar ../../cmd/runtime-manager/entrypoint "${OUTPUT_DIR}"/runtime-manager/bin
    cp -ar build/_output/bin/runtime-manager "${OUTPUT_DIR}"/runtime-manager/bin/
    cp -ar ../../cmd/runtime-manager/script/* "${OUTPUT_DIR}"/runtime-manager/script/
    # copy runtime output to runtime-manager output
    cp -ar "${OUTPUT_DIR}"/nodejsruntime "${OUTPUT_DIR}"/runtime-manager/
    cp -ar "${OUTPUT_DIR}"/pythonruntime "${OUTPUT_DIR}"/runtime-manager/
    cp -ar "${OUTPUT_DIR}"/javaruntime "${OUTPUT_DIR}"/runtime-manager/
    cp -ar "${OUTPUT_DIR}"/cangjieruntime "${OUTPUT_DIR}"/runtime-manager/
    cp -ar "${OUTPUT_DIR}"/phpruntime "${OUTPUT_DIR}"/runtime-manager/
    cp -ar "${BASE_DIR}"/config/custom-runtime-config.yaml "${OUTPUT_DIR}"/runtime-manager/config/custom-runtime-config.yaml
    cp -ar "${BASE_DIR}"/config/runtime.json "${OUTPUT_DIR}"/runtime-manager/config/runtime.json

    if [ "${local_os}"x == "EulerOS_x86_64"x ]; then
        cp -ar "${OUTPUT_DIR}"/csharpruntime "${OUTPUT_DIR}"/runtime-manager/
        cp -ar ../../cmd/runtime-manager/Dockerfile "${OUTPUT_DIR}"/runtime-manager/
    elif [ "${local_os}"x == "EulerOS_aarch64"x ]; then
        cp -ar ../../cmd/runtime-manager/Dockerfile-arm "${OUTPUT_DIR}"/runtime-manager/
    fi

    # copy funcgraph_sdk to runtime-manager output
    [ -d "${OUTPUT_DIR}" ]

    return 0
}

while getopts 'g:mh' opt; do
    case "${opt}" in
    g)
        PRODUCT="${OPTARG}"
        ;;
    m)
        if ! runtime_manager_module_compile; then
            log_error "Failed to build runtime-manager"
            exit 1
        fi
        ;;
    h)
        usage
        exit 0
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0
