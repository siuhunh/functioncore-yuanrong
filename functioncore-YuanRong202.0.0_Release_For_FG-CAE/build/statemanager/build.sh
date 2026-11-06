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
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/state-manager"
PROJECT_DIR="${BASE_DIR}/../../pkg/state-manager"
CONFIG_DIR="${BASE_DIR}/../../config/state-manager"
# Executable file name
MODULE_NAME="state-manager"

. "${BASE_DIR}"/../build_function.sh

function generate_mock()
{
    if ! mockgen -destination "$2" -source "$1" -package mock; then
        log_error "Failed to generate mock file"
        return 1;
    fi

    return 0
}

function statemanager_module_compile() {
    log_info "state-manager module build"

    cd "${PROJECT_DIR}" || die "${PROJECT_DIR} not exist"
    [ -d mock ] && rm -rf mock
    mkdir -p mock
    cd mock
    #generate_mock "${PROJECT_DIR}"/scheduler/predicate/abstract.go predicate_mock.go
    #generate_mock "${PROJECT_DIR}"/scheduler/priority/abstract.go priority_mock.go

    cd "${BASE_DIR}" || die "${BASE_DIR} not exist"
    if ! sh "${BASE_DIR}"/../common/common_compile.sh "${PROJECT_DIR}" "${MODULE_NAME}" "${OUTPUT_DIR}" "${CONFIG_DIR}"; then
        log_error "Failed to build state-manager!!!"
        return 1
    fi

    cp -ar "${BASE_DIR}"/../../cmd/common/sudo_init.sh "${OUTPUT_DIR}"/bin/

    return 0
}

function bridgeplugin_compile() {
    PLUGIN_DIR="${PROJECT_DIR}/bridgemanager/bridgeplugin"
    cd "${PLUGIN_DIR}" || die "${PLUGIN_DIR} not exist"
    FLAGS='-linkmode=external -extldflags "-fPIC -fstack-protector-strong -Wl,-z,now,-z,relro,-z,noexecstack,-s -Wall -Werror"'
    CC='gcc -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2' go build -buildmode=plugin -ldflags "${FLAGS}" -o \
    "${BASE_DIR}"/../../../bridgeplugin.so "${PLUGIN_DIR}"/cmd/main.go
    [ ! -f "${BASE_DIR}"/../../../bridgeplugin.so ] && log_error "Failed to build bridgeplugin!!!" && return 1
    cp -ar "${BASE_DIR}/../../../bridgeplugin.so" "${OUTPUT_DIR}"
    return 0
}

while getopts 'm' opt; do
    case "${opt}" in
    m)
        statemanager_module_compile
        bridgeplugin_compile
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0