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

# ----------------------------------------------------------------------
# funcname:     log_info.
# description:  Print build info log.
# parameters:   NA
# return value: NA
# ----------------------------------------------------------------------
log_info()
{
    echo "[BUILD_INFO][$(date +%b\ %d\ %H:%M:%S)]$*"
}

# ----------------------------------------------------------------------
# funcname:     log_warning.
# description:  Print build warning log.
# parameters:   NA
# return value: NA
# ----------------------------------------------------------------------
log_warning()
{
    echo "[BUILD_WARNING][$(date +%b\ %d\ %H:%M:%S)]$*"
}

# ----------------------------------------------------------------------
# funcname:     log_error.
# description:  Print build error log.
# parameters:   NA
# return value: NA
# ----------------------------------------------------------------------
log_error()
{
    echo "[BUILD_ERROR][$(date +%b\ %d\ %H:%M:%S)]$*"
}

# ----------------------------------------------------------------------
# funcname:     die.
# description:  Print build error log.
# parameters:   NA
# return value: NA
# ----------------------------------------------------------------------
die()
{
    log_error "$*"
    stty echo
    exit 1
}

# ----------------------------------------------------------------------
# funcname: usage
# description:  the build Instructions
# parameters:   void
# return value: void
# ----------------------------------------------------------------------
usage()
{
    echo -e "Usage: ./build.sh [-m subsystem_name] [-h help]"
    echo -e "Usage: ./docker_build.sh [-m subsystem_name -v version] [-h help]"
    echo -e "Options:"
    echo -e "     -m subsystem_name, such as nodemanager workermanager statemanager frontend worker"
    echo -e "                                functionstate functiontask"
    echo -e "     -m functioncore build functioncore containing all functioncore modules"
    echo -e "     -m all contains runtime and worker-flow in addition to functioncore"
    echo -e "Notes: If the parameter of -m is all, you must download the runtime project first,"
    echo -e "       and keep the the projects' path at the same level as the functioncore."
    echo -e "     -h usage help"
    echo -e "      "
    echo -e "Example:"
    echo -e "   sh  build.sh -m \"worker\""
    echo -e "   sh  build.sh -m \"functioncore\""
    echo -e "   sh  build.sh -m \"all\""
    echo -e "   sh  build.sh -g -m \"runtimemanager\""
    echo -e "   sh  docker_build.sh -v \"version\" -m \"worker\""
    echo -e "   sh  docker_build.sh -v \"version\" -m \"functioncore\""
    echo -e "   sh  docker_build.sh -v \"version\" -m \"all\""
    echo -e "   sh  docker_build.sh -g -v \"version\" -m \"runtimemanager\""
    echo -e ""
}

# ----------------------------------------------------------------------
# funcname:     get_base_image.
# description:  check the system
# parameters:   base images path
# return value: NA
# ----------------------------------------------------------------------
function get_base_image() {
    if grep -Eq "Ubuntu" /etc/*-release; then
        BASE_IMAGE="ubuntu:18.04"
    elif grep -Eq "EulerOS" /etc/*-release; then
        BASE_IMAGE="euleros:v2r9"
    fi
}
