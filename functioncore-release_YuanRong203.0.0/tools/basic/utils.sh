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


function log_info() {
    echo -e "===== I$(date +'%m%d %H:%M:%S.%3N' -u)" "$@"
}

function log_debug() {
    echo -e "\033[34m""===== D$(date +'%m%d %H:%M:%S.%3N' -u)" "$@" "\033[0m"
}

function log_succ() {
    echo -e "\033[32m""===== S$(date +'%m%d %H:%M:%S.%3N' -u)" "$@" "\033[0m"
}

function log_error() {
    echo -e "\033[31m""===== E$(date +'%m%d %H:%M:%S.%3N' -u)" "$@" "\033[0m"
}

# please install libxml2 first
function init_config_var() {
    export CONFIG_FILE="$1"
    # shellcheck disable=SC2155
    export NIC_NAME="$(xmllint --xpath "string(//rancher/nic_name)" "${CONFIG_FILE}")"
    # shellcheck disable=SC2155
    export LOCAL_IP="$(xmllint --xpath "string(//rancher/local_ip)" "${CONFIG_FILE}")"
    # shellcheck disable=SC2155
    export RANCHER_HTTP_PORT="$(xmllint --xpath "string(//rancher/rancher_http)" "${CONFIG_FILE}")"
    # shellcheck disable=SC2155
    export RANCHER_HTTPS_PORT="$(xmllint --xpath "string(//rancher/rancher_https)" "${CONFIG_FILE}")"
    # shellcheck disable=SC2155
    export RANCHER_ADDR="${LOCAL_IP}:${RANCHER_HTTPS_PORT}"
    # shellcheck disable=SC2155
    export RANCHER_PASSWD="$(xmllint --xpath "string(//rancher/rancher_password)" "${CONFIG_FILE}")"
    # shellcheck disable=SC2155
    export NFS_SERVER_IP="$(xmllint --xpath "string(//rancher/nfs_serverip)" "${CONFIG_FILE}")"
    # shellcheck disable=SC2155
    export PROMETHEUS_IP="$(xmllint --xpath "string(//rancher/prometheus_ip)" "${CONFIG_FILE}")"
}

function get_pod_num() {
    # shellcheck disable=SC2005
    echo "$(kubectl get pod --all-namespaces | grep "$1" | grep Running | grep -cE "1/1|2/2")"
}

function is_docker_number() {
    local target=$1
    # shellcheck disable=SC2126
    [[ $(docker ps | grep -v STATUS | wc -l) -ge "${target}" ]] && return 0
    return 1
}

function wait_func_return_true(){
# usage: wait_func_return_true func time(s)
# user should define the target func first, which return 0 if condition satisfied, else 1.
    local func=$1
    local time=$2
    log_info "Waiting \"${func}\" return 0 for ${time}s......"

    ${func}  # func with parameter can be transmit like "is_docker_number 1"
    while [[ $? != 0 && ${time} != 0 ]] ;do
        sleep 1
        ((time-=1))
        ${func}
    done
    if [[ ${time} == 0 ]]; then
        log_error "Wait ${func} time out"
        return 1 # time out
    fi
    log_info "\"${func}\" return 0 when $(($2-time))s! "
    return 0
}
