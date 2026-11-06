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
. "${BASE_DIR}"/build_function.sh

function load_baseimages() {
    log_info "Pull baseimages from docker-hub"
    declare -A image_list

    if [ "${PRODUCT}" = "FunctionGraph" ];then
        log_info "The current region is Green"
        docker_registry=registry-cbu.huawei.com/functiongraph
        image_list=([euleros]="v2r9" [worker-init]="1.0" [fusion]="1.0" [custom]="1.0")
    else
        log_info "The current region is Yellow"
        docker_registry=yuanrong.docker.tool.huawei.com/functiongraph_euleros_x86
        image_list=([euleros]="v2r9" [worker-init]="1.0" [fusion]="1.0" [custom]="1.0")
    fi

    for image_name in ${!image_list[*]}; do
        res=$(docker image ls "${image_name}":"${image_list["$image_name"]}" | grep -vE REPOSITORY | wc -l)
        if [ "${res}" -ne 0 ]; then
            log_info "image ${image_name} already exist in system, no need to pull."
        else
            docker pull "${docker_registry}"/"${image_name}":"${image_list["$image_name"]}"
            docker tag "${docker_registry}"/"${image_name}":"${image_list["$image_name"]}" "${image_name}":"${image_list["$image_name"]}"
            docker rmi "${docker_registry}"/"${image_name}":"${image_list["$image_name"]}"
            log_info "pull image ${image_name}."
        fi
    done
}

while getopts 'p:h' opt; do
    case "${opt}" in
    p)
        PRODUCT="${OPTARG}"
        load_baseimages
        ;;
    h)
        log_info "bash load_baseimages.sh -p "
        log_info "bash load_baseimages.sh -p FunctionGraph"
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