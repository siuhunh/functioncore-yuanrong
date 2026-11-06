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

# go module prepare
export GO111MODULE=on
export GOPROXY="https://cmc.centralrepo.rnd.huawei.com/artifactory/go-central-repo/,https://cmc.centralrepo.rnd.huawei.com/artifactory/product_go/"
export NO_PROXY=${NO_PROXY},*.huawei.com
export GONOSUMDB=*

. "${BASE_DIR}"/build_function.sh
. "${BASE_DIR}"/module_list.sh
. "${BASE_DIR}"/compile_functions.sh

# clean build history
bash "${BASE_DIR}"/clean.sh

function module_compile()
{
    if [ "$1" = "functioncore" ]; then
        get_module_list
        for list_num in ${!MODULE_LIST[*]}; do
            {
                if bash "${BASE_DIR}/${MODULE_LIST[${list_num}]}/build.sh" -m; then
                    log_info "${MODULE_LIST[${list_num}]} build successfully"
                else
                    log_error "Failed to build ${MODULE_LIST[${list_num}]}"
                    return 1
                fi
            } &
            wait
        done
    else
        module="$1"
        comp_list="${module//-/}"
        if ! bash "${BASE_DIR}/${comp_list}/build.sh" -m; then
            log_error "Failed to build ${comp_list}"
            return 1
        fi
    fi
    return 0
}

function all_compile() {
    if ! [ -d "${RUNTIME_PATH}" ]; then
        log_error "You must download the runtime paths first"
        usage
        exit 1
    fi
    runtime_compile

    cd "${PROJECT_DIR}"
    if ! module_compile "functioncore"; then
        log_error "Failed to build module"
        exit 1
    fi
}

while getopts 'gm:h' opt; do
    case "${opt}" in
    g)
        PRODUCT="FunctionGraph"
        ;;
    m)
        pre_compile
        if [ "${OPTARG//-/}" == "runtimemanager" ]; then
            runtime_compile
        elif [ "${OPTARG}" != "all" ]; then
            if ! module_compile "${OPTARG}"; then
                log_error "Failed to build module"
                exit 1
            fi
        else
            all_compile
        fi
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
