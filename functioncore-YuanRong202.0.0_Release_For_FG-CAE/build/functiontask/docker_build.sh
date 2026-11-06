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

get_base_image
VERSION="$2"
cd "${OUTPUT_DIR}" || die "${OUTPUT_DIR} not exist"
if ! docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t functiontask:"${VERSION}" -f Dockerfile .; then
    log_error "Failed to docker build functiontask!!!"
    exit 1
fi

exit 0
