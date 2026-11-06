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
OPEN_SOURCE_DIR="${BASE_DIR}/../open_source"
CPU_NUM="$(grep -c 'processor' /proc/cpuinfo)"
# compile options
COMP_CFLAGS="-fPIC -fstack-protector-strong -Wl,-z,relro,-z,now,-z,noexecstack,-s -Wall -D_FORTIFY_SOURCE=2 -O2"
COMP_CXXFLAGS="-std=c++11 -fPIC -fstack-protector-strong -Wl,-z,relro,-z,now,-z,noexecstack,-s -Wall -D_FORTIFY_SOURCE=2 -O2"
# shellcheck disable=SC1090
. "${BASE_DIR}/build_function.sh"

# ----------------------------------------------------------------------
# funcname:     download_opensource_pkg.
# description:  download opensource package
# parameters:   pkg_name, pkg_version, save_path, pkg_list
# return value: NA
# ----------------------------------------------------------------------
function download_opensource_pkg() {
    local pkg_name="$1"
    local pkg_version="$2"
    local save_path="$3"
    local pkg_list="$4"
    [ -f "${save_path}" ] && return 0
    [[ -z "${pkg_list}" ]] && pkg_list="all"
    log_info "=== download opensource ${pkg_name}-${pkg_version} to ${save_path} ... ==="
    curl --noproxy huawei.com -o "${save_path}" -k "https://svr-centralrepo.rnd.huawei.com/cmc-oss/v1/opensource/download?softName=${pkg_name}&&softVersion=${pkg_version}&&fromPage=true&packages=${pkg_list}"
}

function build_cjson()
{
    [ -f "${OPEN_SOURCE_DIR}/json.hpp" ] && log_info "json is already installed to ${OPEN_SOURCE_DIR}" && return 0
    download_opensource_pkg 'JSON%20for%20Modern%20C%2B%2B' 3.7.3 "${OPEN_SOURCE_DIR}/json.hpp" json.hpp # JSON for Modern C++
    log_info "json is installed to ${OPEN_SOURCE_DIR}"
}
build_cjson

function build_glog()
{
    GLOG_OUTPUT_DIR="${OPEN_SOURCE_DIR}/glog-0.4.0/output"
    [ -f "${GLOG_OUTPUT_DIR}/lib/libglog.so" ] && log_info "glog is already installed to ${GLOG_OUTPUT_DIR}" && return 0
    download_opensource_pkg google/glog 0.4.0 "${OPEN_SOURCE_DIR}"/glog-0.4.0.tar.gz glog-0.4.0.tar.gz
    if ! cd "${OPEN_SOURCE_DIR}" || ! tar -xf "${OPEN_SOURCE_DIR}/glog-0.4.0.tar.gz"; then
        log_error "Failed to tar glog"
        return 1
    fi
    if ! cd "glog-0.4.0" || ! ./autogen.sh; then
        log_error "Failed to run autogen.sh in glog"
        return 1
    fi
    if ! ./configure --prefix="${GLOG_OUTPUT_DIR}" CFLAGS="${COMP_CFLAGS}" CXXFLAGS="${COMP_CXXFLAGS}"; then
        log_error "Failed to run configure in glog"
        return 1
    fi
    if ! make -j "${CPU_NUM}" || ! make install; then
        log_error "Failed to compile glog"
        return 1
    fi
    log_info "glog is installed to ${GLOG_OUTPUT_DIR}"
}
build_glog

function build_protobuf()
{
    PROTO_OUTPUT_DIR="${OPEN_SOURCE_DIR}/protobuf-3.13.0/output"
    [ -f "${PROTO_OUTPUT_DIR}/lib/libprotobuf.so" ] && log_info "protobuf is already installed to ${PROTO_OUTPUT_DIR}" && return 0
    download_opensource_pkg google/protobuf 3.13.0 "${OPEN_SOURCE_DIR}"/protobuf-3.13.0.zip protobuf-cpp-3.13.0.zip
    if ! cd "${OPEN_SOURCE_DIR}" || ! unzip -o -q "${OPEN_SOURCE_DIR}/protobuf-3.13.0.zip" -d "${BUILD_DIR}"; then
        log_error "Failed to tar protobuf"
        return 1
    fi
    if ! cd "protobuf-3.13.0" || ! ./autogen.sh; then
        log_error "Failed to run autogen.sh in protobuf"
        return 1
    fi
    if ! ./configure --prefix="${PROTO_OUTPUT_DIR}" CFLAGS="${COMP_CFLAGS}" CXXFLAGS="${COMP_CXXFLAGS}"; then
        log_error "Failed to run configure in protobuf"
        return 1
    fi
    sed -i '381c hardcode_libdir_flag_spec=""' libtool
    sed -i '382c hardcode_libdir_flag_spec=""' libtool
    sed -i '11737c hardcode_libdir_flag_spec=""' libtool
    sed -i '11743c hardcode_libdir_flag_spec=""' libtool
    sed -i '247c runpath_var=' libtool
    if ! make -j "${CPU_NUM}" || ! make install; then
        log_error "Failed to compile protobuf"
        return 1
    fi
    log_info "protobuf is installed to ${PROTO_OUTPUT_DIR}"
}
build_protobuf

function build_curl(){
    CURL_OUTPUT_DIR="${OPEN_SOURCE_DIR}/curl-7.71.1/output"
    [ -f "${CURL_OUTPUT_DIR}/lib/libcurl.so" ] && log_info "curl is already installed to ${CURL_OUTPUT_DIR}" && return 0
    download_opensource_pkg curl 7.71.1 "${OPEN_SOURCE_DIR}"/curl-7.71.1.tar.gz curl-7.71.1.tar.gz
    if ! cd "${OPEN_SOURCE_DIR}" || ! tar -xzf "${OPEN_SOURCE_DIR}"/curl-7.71.1.tar.gz; then
        log_error "Failed to tar curl"
        return 1
    fi
    cd curl-7.71.1
    ./configure --prefix="${CURL_OUTPUT_DIR}" CFLAGS="${COMP_CFLAGS}" CPPFLAGS="${COMP_CXXFLAGS}"
    if ! make -j "${CPU_NUM}" || ! make install; then
        log_error "Failed to compile curl"
        return 1
    fi
    log_info "curl is installed to ${CURL_OUTPUT_DIR}"
}
build_curl