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
readonly BASE_DIR=$(dirname "$(readlink -f "$0")")
readonly ARCH="$(echo "$(uname -m)")"
CONFIG_DIR="${BASE_DIR}/config"
CONFIG_FILE="${BASE_DIR}/../hpc_config.xml"
BIN_DIR="${BASE_DIR}/bin"
INSTALL_DIR="/opt"

[[ ! -f "${CONFIG_FILE}" ]] && echo "${CONFIG_FILE} is not exist" && exit 1
[[ ! -f "${BASE_DIR}/../utils.sh" ]] && echo "${BASE_DIR}/../utils.sh is not exist" && exit 1
. ${BASE_DIR}/../utils.sh

# help of deploy.sh
function usage()
{
    echo -e "Usage: bash deploy.sh [-o install/uninstall] [-m component_name/all] [-h help]"
    echo -e "Options:"
    echo -e "     -o install/uninstall, install component or uninstall component."
    echo -e "     -m component_name/all, such etcd, minio, redis"
    echo -e "     -h usage help"
    echo -e "      "
    echo -e "example:"
    echo -e "     bash deploy.sh -o install -m all"
    echo -e "     bash deploy.sh -o uninstall -m all"
    echo -e "     bash deploy.sh -o install -m etcd"
    echo -e "     bash deploy.sh -o uninstall -m etcd"
    echo -e "     bash deploy.sh -h"
}

function install_etcd()
{
    log_info "---- install etcd ----"
    local etcd_install_dir="${INSTALL_DIR}/basic/etcd"
    [ -d "${etcd_install_dir}" ] || mkdir -p "${etcd_install_dir}"
    rm -rf "${etcd_install_dir}"/*
    cp -rf "${BIN_DIR}"/etcd/* "${etcd_install_dir}"
    chmod 755 "${etcd_install_dir}"/etcd "${etcd_install_dir}"/etcdctl
    chmod 700 "${etcd_install_dir}"

    # load config
    local etcd_password="$(xmllint --xpath "string(//config/etcd_password)" "${CONFIG_FILE}")"
    [[ "${etcd_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} etcd_password first!" && exit 1
    local etcd_ip="$(xmllint --xpath "string(//config/etcd_ip)" "${CONFIG_FILE}")"
    [[ "${etcd_ip}X" == "IPX" ]] && log_error "please configure ${CONFIG_FILE} etcd_ip first!" && exit 1
    local etcd_port="$(xmllint --xpath "string(//config/etcd_port)" "${CONFIG_FILE}")"
    [[ "${etcd_port}X" == "etcd_portX" ]] && log_error "please configure ${CONFIG_FILE} etcd_port first!" && exit 1
    local etcd_peer_port="$(xmllint --xpath "string(//config/etcd_peer_port)" "${CONFIG_FILE}")"
    [[ "${etcd_peer_port}X" == "etcd_peer_portX" ]] && log_error "please configure ${CONFIG_FILE} etcd_peer_port first!" && exit 1
    local etcdctl_install_dir="$(xmllint --xpath "string(//config/etcdctl_install_dir)" "${CONFIG_FILE}")"
    [[ "${etcdctl_install_dir}X" == "etcdctl_install_dirX" ]] && etcdctl_install_dir=~/bin

    # start etcd
    "${etcd_install_dir}"/etcd \
        --name=etcd0 \
        --data-dir="${etcd_install_dir}" \
        --listen-client-urls="http://${etcd_ip}:${etcd_port}" \
        --advertise-client-urls="http://${etcd_ip}:${etcd_port}" \
        --listen-peer-urls="http://${etcd_ip}:${etcd_peer_port}" > "${etcd_install_dir}"/etcd-run.log 2>&1 &

    # add root user and open auth
    "${etcd_install_dir}"/etcdctl --endpoints="${etcd_ip}:${etcd_peer_port}" user add root:"${etcd_password}"
    "${etcd_install_dir}"/etcdctl --endpoints="${etcd_ip}:${etcd_peer_port}" auth enable
    if [ ! -d ${etcdctl_install_dir} ]; then
        mkdir -p ${etcdctl_install_dir}
    fi
    [ ! -L ${etcdctl_install_dir}/etcdctl ] && ln -s "${etcd_install_dir}"/etcdctl -n ${etcdctl_install_dir}/etcdctl
    log_info "etcdctl install ${etcdctl_install_dir}, please check the path has been added to your env PATH"
    log_info "---- install etcd success ----"
}

function uninstall_etcd()
{
    log_info "---- uninstall etcd ----"
    local etcd_install_dir="${INSTALL_DIR}/basic/etcd"
    local core_etcd_port="$(xmllint --xpath "string(//config/etcd_port)" "${CONFIG_FILE}")"
    local etcd_pid=`ps -ef | grep "${etcd_install_dir}/etcd" | grep "${etcd_port}" | grep -v grep | grep -v PPID | grep -v deploy.sh | awk '{ print $2 }'`
    local etcdctl_install_dir="$(xmllint --xpath "string(//config/etcdctl_install_dir)" "${CONFIG_FILE}")"
    [[ "${etcdctl_install_dir}X" == "etcdctl_install_dirX" ]] && etcdctl_install_dir=~/bin
    if ! kill -9 "${etcd_pid}"; then
        log_info "etcd is not running"
    fi
    [ -d "${etcd_install_dir}" ] && rm -rf "${etcd_install_dir}"
    [ -L "${etcdctl_install_dir}/etcdctl" ] && rm -rf ${etcdctl_install_dir}/etcdctl
    log_info "---- uninstall etcd success ----"
}

function install_redis()
{
    log_info "---- install redis ----"
    local redis_install_dir="${INSTALL_DIR}/basic/redis"
    [ -d "${redis_install_dir}" ] || mkdir -p "${redis_install_dir}"
    rm -rf "${redis_install_dir}"/*
    cp -rf "${BIN_DIR}"/redis/* "${redis_install_dir}"
    cp -rf "${CONFIG_DIR}"/redis/* "${redis_install_dir}"
    chmod 755 "${redis_install_dir}"/redis-server

    # load config
    local redis_password="$(xmllint --xpath "string(//config/redis_password)" "${CONFIG_FILE}")"
    [[ "${redis_password}X" == "passwordX" ]] && log_error "please configure ${CONFIG_FILE} redis_password first!" && exit 1
    local redis_ip="$(xmllint --xpath "string(//config/redis_ip)" "${CONFIG_FILE}")"
    [[ "${redis_ip}X" == "IPX" ]] && log_error "please configure ${CONFIG_FILE} redis_ip first!" && exit 1
    local redis_port="$(xmllint --xpath "string(//config/redis_port)" "${CONFIG_FILE}")"
    [[ "${redis_port}X" == "redis_portX" ]] && log_error "please configure ${CONFIG_FILE} redis_port first!" && exit 1
    sed -i "s/core_redis_ip/${redis_ip}/g" "${redis_install_dir}"/redis.conf
    sed -i "s/core_redis_port/${redis_port}/g" "${redis_install_dir}"/redis.conf
    sed -i "s/core_redis_password/${redis_password}/g" "${redis_install_dir}"/redis.conf

    # start redis
    "${redis_install_dir}"/redis-server "${redis_install_dir}"/redis.conf > "${redis_install_dir}"/redis-run.log 2>&1 &
    log_info "---- install redis success ----"
}

function uninstall_redis()
{
    log_info "---- uninstall redis ----"
    local redis_install_dir="${INSTALL_DIR}/basic/redis"
    local redis_port="$(xmllint --xpath "string(//config/redis_port)" "${CONFIG_FILE}")"
    local redis_pid=`ps -ef | grep "${redis_install_dir}"/redis-server | grep "${redis_port}" | grep -v grep | grep -v PPID | grep -v deploy.sh | awk '{ print $2 }'`
    if ! kill -9 "${redis_pid}"; then
        log_info "redis-server is not running"
    fi
    [ -d "${redis_install_dir}" ] && rm -rf "${redis_install_dir}"
    log_info "---- uninstall redis success ----"
}

function install_minio()
{
    log_info "---- install minio ----"
    local minio_install_dir="${INSTALL_DIR}/basic/minio"
    [ -d "${minio_install_dir}" ] || mkdir -p "${minio_install_dir}"
    rm -rf "${minio_install_dir}"/*
    cp -rf "${BIN_DIR}"/minio/* "${minio_install_dir}"
    chmod 755 "${minio_install_dir}"/minio

    # load config
    local minio_user="$(xmllint --xpath "string(//config/S3_AK)" "${CONFIG_FILE}")"
    [[ "${minio_user}X" == "accessKeyX" ]] && log_error "please configure ${CONFIG_FILE} S3_AK first!" && exit 1
    local minio_password="$(xmllint --xpath "string(//config/S3_SK)" "${CONFIG_FILE}")"
    [[ "${minio_password}X" == "secretKeyX" ]] && log_error "please configure ${CONFIG_FILE} S3_AK first!" && exit 1
    local minio_ip="$(xmllint --xpath "string(//config/minio_ip)" "${CONFIG_FILE}")"
    [[ "${minio_ip}X" == "IPX" ]] && log_error "please configure ${CONFIG_FILE} minio_ip first!" && exit 1
    local minio_port="$(xmllint --xpath "string(//config/minio_port)" "${CONFIG_FILE}")"
    [[ "${minio_port}X" == "minio_portX" ]] && log_error "please configure ${CONFIG_FILE} minio_port first!" && exit 1
    local console_port="$(xmllint --xpath "string(//config/minio_console_port)" "${CONFIG_FILE}")"
    [[ "${console_port}X" == "minio_console_portX" ]] && log_error "please configure ${CONFIG_FILE} minio_console_portX first!" && exit 1

    # start minio
    MINIO_ROOT_USER="${minio_user}" MINIO_ROOT_PASSWORD="${minio_password}" "${minio_install_dir}"/minio server \
        "${minio_install_dir}"/data --console-address "${minio_ip}:${console_port}" --address "${minio_ip}:${minio_port}" > "${minio_install_dir}"/minio-run.log 2>&1 &

    log_info "---- install minio success ----"
}

function uninstall_minio()
{
    log_info "---- uninstall minio ----"
    local minio_install_dir="${INSTALL_DIR}/basic/minio"
    local minio_port="$(xmllint --xpath "string(//config/minio_port)" "${CONFIG_FILE}")"
    local minio_pid=`ps -ef | grep "${minio_install_dir}/minio" | grep "${minio_port}" | grep -v grep | grep -v PPID | grep -v deploy.sh | awk '{ print $2 }'`
    if ! kill -9 "${minio_pid}"; then
        log_info "minio is not running"
    fi
    rm -rf "${minio_install_dir}"
    log_info "---- uninstall minio success ----"
}

# install module
function install_module()
{
    case "$1" in
    "etcd")
        install_etcd
        ;;
    "redis")
        install_redis
        ;;
    "minio")
        install_minio
        ;;
    "all")
        install_etcd
        install_redis
        install_minio
        ;;
    esac
}

# uninstall module
function uninstall_module()
{
    case "$1" in
    "etcd")
        uninstall_etcd
        ;;
    "redis")
        uninstall_redis
        ;;
    "minio")
        uninstall_minio
        ;;
    "all")
        uninstall_etcd
        uninstall_redis
        uninstall_minio
        ;;
    esac
}

function main()
{
    while getopts "o:m:h" opt; do
        case "$opt" in
        o)
            OPTION=$OPTARG
            ;;
        m)
            MODULE=$OPTARG
        ;;
        h)
            usage
            exit 0
        ;;
        *)
            log_error "Unknown parameter"
            echo -e ""
            usage
            exit 1
            ;;
        esac
    done

    [[ ! -f "${CONFIG_FILE}" ]] && log_error "${CONFIG_FILE} is not exist" && exit 1
    [[ ! -d "${CONFIG_DIR}" ]] && log_error "${CONFIG_DIR} is not exist" && exit 1
    [[ "${ARCH}X" != "aarch64X" && "${ARCH}X" != "x86_64X" ]] && log_error "Not supported ${ARCH}" exit 1
    INSTALL_DIR="$(xmllint --xpath "string(//config/deploy_path)" "${CONFIG_FILE}")"

    if [ "${OPTION}X" == "installX" ]; then
        install_module "${MODULE}"
    fi

    if [ "${OPTION}X" == "uninstallX" ]; then
        uninstall_module "${MODULE}"
    fi
}

main $@
