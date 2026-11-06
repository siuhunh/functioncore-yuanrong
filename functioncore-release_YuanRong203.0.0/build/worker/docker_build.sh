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
BUILD_INIT=true
BUILD_WORKER=true
BUILD_CUSTOM=true
BUILD_CLI=true

. "${BASE_DIR}"/../build_function.sh

if [ "$1" != "-v" ]; then
    log_error "invalid command"
    usage
    exit 1
fi

if [ x"$2" = x ]; then
    log_error "invalid parameter"
    usage
    exit 1
fi

function copy_from_runtime() {
    return 0
}

get_base_image
VERSION="$2"
if [ "${BUILD_WORKER}" = true ]; then
    cd "${OUTPUT_DIR}"/worker || die "${OUTPUT_DIR}/worker not exist"
    if ! docker build -t worker:"${VERSION}" -f Dockerfile-worker .; then
        log_error "Failed to docker build worker!!!"
        exit 1
    fi
fi

if [ "${BUILD_INIT}" = true ]; then
    cd "${OUTPUT_DIR}"/workerinit || die "${OUTPUT_DIR}/workerinit not exist"
    if ! docker build -t worker-init:"${VERSION}" -f Dockerfile-init .; then
        log_error "Failed to docker build worker-init!!!"
        exit 1
    fi
fi

if [ "${BUILD_CUSTOM}" = true ]; then
    cd "${OUTPUT_DIR}"/workercustom || die "${OUTPUT_DIR}/workercustom not exist"
    if ! docker build -t worker-custom:"${VERSION}" -f Dockerfile-custom .; then
        log_error "Failed to docker build worker-custom!!!"
        exit 1
    fi
fi

if [ -d "${OUTPUT_DIR}/cppruntime" ] && [ "${BUILD_CLI}" = true ]; then
    [ ! -d "${OUTPUT_DIR}/cli/simulator/lib" ] && copy_from_runtime
    cd "${OUTPUT_DIR}"/cli/simulator || die "${OUTPUT_DIR}/cli/simulator not exist"
    if ! docker build -t cli:"${VERSION}" -f Dockerfile-cli .; then
        log_error "Failed to docker build cli!!!"
        exit 1
    fi
fi

exit 0
