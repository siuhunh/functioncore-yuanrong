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
OUTPUT_DIR="${BASE_DIR}/../../../output/functioncore/"

. "${BASE_DIR}"/../build_function.sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${BASE_DIR}/../../../sloagent/lib
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
function sloagent_module_compile() {
    log_info "sloagent module build"

    return 0
}

while getopts 'm' opt; do
    case "${opt}" in
    m)
        sloagent_module_compile
        ;;
    *)
        log_error "invalid command"
        usage
        exit 1
        ;;
    esac
done

exit 0