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
local_os=$(head -1 /etc/os-release | tail -1 | awk -F "\"" '{print $2}')_$(uname -m)

. "${BASE_DIR}"/../build_function.sh

function image_build ()
{
    cd "${OUTPUT_DIR}"/runtime-manager || die "${OUTPUT_DIR}/runtime-manager not exist"
    if [ "${local_os}"x == "EulerOS_aarch64"x ]; then
        DOCKERFILE="Dockerfile-arm"
    else
        DOCKERFILE="Dockerfile"
    fi
    if ! docker build -t runtime-manager:"${VERSION}" --build-arg WORK_DIR=/opt/function -f "${DOCKERFILE}" .; then
        log_error "Failed to docker build runtime-manager!!!"
        exit 1
    fi
}

while getopts 'v:g:h' opt; do
    case "${opt}" in
    v)
        VERSION="${OPTARG}"
        ;;
    g)
        PRODUCT="${OPTARG}"
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
