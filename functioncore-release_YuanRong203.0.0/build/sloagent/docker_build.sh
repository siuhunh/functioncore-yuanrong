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
OUTPUT_DIR="${BASE_DIR}"/../../../output/functioncore/slo-agent

. "${BASE_DIR}"/../build_function.sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${BASE_DIR}/../../../sloagent/lib
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
function sloagent_module_compile() {
    log_info "docker build module sloagent"
    get_base_image

    mkdir -p "${OUTPUT_DIR}"/bin
    cp -ar "${BASE_DIR}"/../../cmd/slo-agent/Dockerfile "${OUTPUT_DIR}"/
    cp -ar "${BASE_DIR}"/../../cmd/slo-agent/entrypoint "${OUTPUT_DIR}"/bin

    cp -ar ${BASE_DIR}/../../../sloagent/slo_agent "${OUTPUT_DIR}"/bin
    cp -ar ${BASE_DIR}/../../../sloagent/lib "${OUTPUT_DIR}"/lib

    cd "${OUTPUT_DIR}" || die "${OUTPUT_DIR} not exist"
    if ! docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t slo-agent:"${VERSION}" -f Dockerfile .; then
        log_error "Failed to docker build slo-agent!!!"
        exit 1
    fi

    return 0
}

while getopts 'v:p:h' opt; do
    case "${opt}" in
    v)
        VERSION="${OPTARG}"
        sloagent_module_compile
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