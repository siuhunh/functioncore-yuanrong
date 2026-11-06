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
PROJECT_DIR="${BASE_DIR}"/..
OUTPUT_DIR="${BASE_DIR}/../../output/functioncore"

# clean output dir
[ -d "${OUTPUT_DIR}" ] && rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

# check vpc dependancy version
if ! grep -E "^\s+golang.org/x/sys => golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c" \
    "${PROJECT_DIR}/go.mod"; then
    log_error "golang.org/x/sys version is not pinned"
    exit 1
fi

# remove hard coded cert file in net/http
cd "${GOROOT:?The GOROOT does not exist.}/src/net/http/internal"
rm -f testcert.go