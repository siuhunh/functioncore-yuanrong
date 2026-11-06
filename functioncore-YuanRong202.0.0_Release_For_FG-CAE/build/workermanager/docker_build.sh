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
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/worker-manager"

. "${BASE_DIR}"/../build_function.sh

get_base_image
redis_ssl_enable=false

function image_build ()
{
    local base_image_with_stunnel="${BASE_IMAGE}"
    cd "${OUTPUT_DIR}" || die "${OUTPUT_DIR} not exist"
    if [ "${redis_ssl_enable}" = "true" ]; then
        base_image_with_stunnel="${BASE_IMAGE}_stunnel"
        if ! docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t "${base_image_with_stunnel}" -f Dockerfile_stunnel .; then
            log_error "Failed to docker build base_image_with_stunnel!!!"
            exit 1
        fi
    fi
    if [ "${PRODUCT}" != "FunctionGraph" ];then
        if ! docker build --build-arg BASE_IMAGE="${base_image_with_stunnel}" -t worker-manager:"${VERSION}" -f Dockerfile .; then
            log_error "Failed to docker build worker-manager!!!"
            exit 1
        fi
    else
        cp -ar "${BASE_DIR}/../../../vpccontroller.so" "${BASE_DIR}/../../../app.conf" ./
        if ! docker build --build-arg BASE_IMAGE="${base_image_with_stunnel}" -t worker-manager:"${VERSION}" -f Dockerfile_vpc .; then
            log_error "Failed to docker build worker-manager!!!"
            exit 1
        fi
    fi
}

while getopts 'v:p:s[tunnel]h' opt; do
    case "${opt}" in
    v)
        VERSION="${OPTARG}"
        ;;
    p)
        PRODUCT="${OPTARG}"
        ;;
    s)
        redis_ssl_enable=true
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

image_build

exit 0
