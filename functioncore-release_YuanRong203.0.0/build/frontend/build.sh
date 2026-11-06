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
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/frontend-invoke"
PROJECT_DIR="${BASE_DIR}/../../pkg/frontend-invoke"
# Executable file name
MODULE_NAME="frontend"

. "${BASE_DIR}"/../build_function.sh
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${BASE_DIR}/../../../datasystem/output/sdk/go/lib
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH

function frontendinvoke_module_compile() {
    log_info "frontend-invoke module build"
    if ! sh "${PROJECT_DIR}"/../faas-scheduler/script/gen_pb.sh; then
        log_error "Failed to gen faas-scheduler pb!!"
        return 1
    fi

    if ! sh "${BASE_DIR}"/../common/common_compile.sh "${PROJECT_DIR}" "${MODULE_NAME}" "${OUTPUT_DIR}"; then
        log_error "Failed to build frontend-invoke!!!"
        return 1
    fi

    cp -ar "${BASE_DIR}"/../../cmd/common/sudo_init.sh "${OUTPUT_DIR}"/bin/

    mkdir -p "${OUTPUT_DIR}"/datasystem
    # copy datasystem go sdk so file lib dir to frontend
    cp -ar ${BASE_DIR}/../../../datasystem/output/sdk/go/lib "${OUTPUT_DIR}"/datasystem/lib
    cd "${OUTPUT_DIR}"/datasystem/lib
    rm -rf libcrypto.so libcrypto.so.1.1 libssl.so libssl.so.1.1
    cd -

    return 0
}

while getopts 'm' opt; do
    case "${opt}" in
    m)
        frontendinvoke_module_compile
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0