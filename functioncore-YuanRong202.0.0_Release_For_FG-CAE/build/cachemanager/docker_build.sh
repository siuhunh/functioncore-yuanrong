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
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/cache-manager"

. "${BASE_DIR}"/../build_function.sh

function cachemanager_module_compile() {
    log_info "docker build module cache-manager"
    get_base_image
    cd "${OUTPUT_DIR}" || die "${OUTPUT_DIR} not exist"
    if ! docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t cache-manager:"${VERSION}" -f Dockerfile .; then
        log_error "Failed to docker build cache-manager!!!"
        exit 1
    fi

    return 0
}

while getopts 'v:p:h' opt; do
    case "${opt}" in
    v)
        VERSION="${OPTARG}"
        cachemanager_module_compile
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
