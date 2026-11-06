#!/bin/bash
# Copyright (c) 2024 Huawei Technologies Co., Ltd

# This software is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:

# http://license.coscl.org.cn/MulanPSL2

# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

set -e

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/web-terminal"
PROJECT_DIR="${BASE_DIR}/../../pkg/web-terminal"
# Executable file name
MODULE_NAME="web-terminal"

. "${BASE_DIR}"/../build_function.sh

function web_terminal_module_compile() {
    log_info "web-terminal module build"

    [ -d "${GOPATH}/src" ] && rm -rf "${GOPATH}"/src/*
    mkdir -p "${GOPATH}"/src/
    ln -s "${BASE_DIR}/../.." "${GOPATH}"/src/functioncore

    if ! sh "${BASE_DIR}"/../common/common_compile.sh "${PROJECT_DIR}" "${MODULE_NAME}" "${OUTPUT_DIR}"; then
        log_error "Failed to build web_terminal_module_compile!!!"
        return 1
    fi

    cp -ar "${BASE_DIR}"/../../cmd/common/sudo_init.sh "${OUTPUT_DIR}"/bin/

    return 0
}

while getopts 'm' opt; do
    case "${opt}" in
    m)
        web_terminal_module_compile
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0