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
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/functiontask"
PROJECT_DIR="${BASE_DIR}/../../pkg/functiontask/"
# Executable file name
MODULE_NAME="functiontask"

. "${BASE_DIR}"/../build_function.sh

function functiontask_module_compile() {
    log_info "functiontask module build"

    if ! sh "${BASE_DIR}"/gen_pb.sh task; then
        log_error "Failed to generate task pb files!"
        return 1
    fi

    if ! sh "${BASE_DIR}"/../common/common_compile.sh "${PROJECT_DIR}" "${MODULE_NAME}" "${OUTPUT_DIR}"; then
        log_error "Failed to build functiontask!!!"
        return 1
    fi

    cp -ar "${BASE_DIR}"/config/Dockerfile "${OUTPUT_DIR}"/
    cp -ar "${BASE_DIR}"/../../cmd/functiontask/sudo_init.sh "${OUTPUT_DIR}"/bin/

    return 0
}

while getopts 'm' opt; do
    case "${opt}" in
    m)
        functiontask_module_compile
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0
