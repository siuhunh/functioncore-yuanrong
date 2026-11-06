#!/bin/bash
# Copyright (c) 2024 Huawei Technologies Co., Ltd
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
OUTPUT_DIR="${BASE_DIR}"/../../../output/functioncore/iam-server

. "${BASE_DIR}"/../build_function.sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${BASE_DIR}/../../../iam_server/lib
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
function iamserver_module_compile() {
    log_info "docker build module iamserver"
    get_base_image

    mkdir -p "${OUTPUT_DIR}"/bin
    cp -ar "${BASE_DIR}"/../../cmd/iam-server/Dockerfile "${OUTPUT_DIR}"/
    cp -ar "${BASE_DIR}"/../../cmd/iam-server/entrypoint "${OUTPUT_DIR}"/bin
    cp -ar "${BASE_DIR}"/../../cmd/iam-server/health-check "${OUTPUT_DIR}"/bin

    cp -ar ${BASE_DIR}/../../../iamserver/iam_server "${OUTPUT_DIR}"/bin
    cp -ar ${BASE_DIR}/../../../iamserver/lib "${OUTPUT_DIR}"/lib

    cd "${OUTPUT_DIR}" || die "${OUTPUT_DIR} not exist"
    if ! docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t iam-server:"${VERSION}" -f Dockerfile .; then
        log_error "Failed to docker build iam-server!!!"
        exit 1
    fi

    return 0
}

while getopts 'v:p:h' opt; do
    case "${opt}" in
    v)
        VERSION="${OPTARG}"
        iamserver_module_compile
        ;;
    p)
        exit 0
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