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
WORKFLOW_DIR="${BASE_DIR}"/../../workflow/build

. "${BASE_DIR}"/build_function.sh
. "${BASE_DIR}"/module_list.sh

function functioncore_build() {
    get_image_list
    for list_num in ${!IMAGES_LIST[*]}; do
        {
            if bash "${BASE_DIR}/${IMAGES_LIST[${list_num}]}/docker_build.sh" -v "${PROJECT_VERSION}" -p "${PRODUCT}"; then
            log_info "${IMAGES_LIST[${list_num}]} image build successfully"
            else
                log_error "Failed to build ${IMAGES_LIST[${list_num}]} image"
                exit 1
            fi
        } &
        wait
    done
}

function runtimemanager_build() {
    cd "${BASE_DIR}/runtimemanager" || die "${BASE_DIR}/runtimemanager not exist, check whether the runtime manager code has been downloaded."
    if ! bash docker_build.sh -v "${PROJECT_VERSION}" -g "${PRODUCT}"; then
        log_info "Failed to build runtime-Manager image"
        exit 1
    fi
}

function image_build() {
    if [ "${MODULE_NAME//-/}" = "runtimemanager" ]; then
        runtimemanager_build
    elif [ "${MODULE_NAME}" = "functioncore" ]; then
        functioncore_build
    elif [ "${MODULE_NAME}" = "all" ]; then
        functioncore_build &
        runtimemanager_build &
        wait
    else
        comp_list="${MODULE_NAME//-/}"
        if ! bash "${BASE_DIR}/${comp_list}/docker_build.sh" -v "${PROJECT_VERSION}" -p "${PRODUCT}"; then
            log_error "Failed to build ${comp_list} image"
            exit 1
        fi
    fi
}

while getopts 'gv:m:h' opt; do
    case "${opt}" in
    g)
        PRODUCT="FunctionGraph"
        ;;
    v)
        PROJECT_VERSION="${OPTARG}"
        ;;
    m)
        MODULE_NAME="${OPTARG}"
        log_info "MODULE_NAME=${MODULE_NAME}"
        log_info "PROJECT_VERSION=${PROJECT_VERSION:?The PROJECT_VERSION does not exist,expect a version,Example: bash docker_build.sh -v 123 -m functioncore}"
        bash "${BASE_DIR}"/load_baseimages.sh -p "${PRODUCT}"
        image_build
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